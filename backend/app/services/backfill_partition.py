"""回填分区任务明细服务：按分区承载增量回填的任务明细、位点凭据与认领流转。

与 backfill_run（回填任务总单）的关系：一条回填总单按并行粒度「按分区」拆成
多条分区明细，每条明细独立维护自己的起止位点、实际已提交位点和认领 worker，
因此单个分区中断不会抹掉其他分区已经确认写入的凭据。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 只有处于这些状态的分区明细才能被 worker 认领
CLAIMABLE_STATUS = ("待认领", "已中断")


class BackfillPartitionService(BaseModuleService):
    MODULE = "backfill_partition"
    ENTITY = "回填分区明细"
    OPERATION_TERM = "回填"
    LIST_FIELDS = [
        "明细编号", "回填任务", "数据流", "分区", "起始位点", "截止位点",
        "已提交位点", "完成键区间",
    ]
    REQUIRED_FIELDS = ["明细编号", "回填任务", "数据流", "分区", "起始位点", "截止位点"]
    STATUS_ORDER = ["待认领", "回填中", "已中断", "已完成"]
    ACTION_RULES = {"认领明细": "回填中", "中断明细": "已中断", "确认完成": "已完成"}
    NEGATIVE_ACTIONS = ["中断明细"]

    # ---- 分区级认领 / 进度 / 中断：状态流转只允许出现在服务层 ----

    def claim_detail(self, detail_id: int, worker_id: str) -> tuple[dict[str, Any] | None, str]:
        """认领一条未完分区明细。

        - 待认领 / 已中断 的明细可被任意 worker 认领（中断后续跑靠新 worker 接手）；
        - 同一 worker 重复认领同一条明细按幂等成功处理；
        - 已被其他 worker 持有的回填中明细、已完成明细一律拒绝。
        """
        detail = store.find(self.MODULE, detail_id)
        if detail is None:
            return None, f"{self.ENTITY} {detail_id} 不存在或已归档"
        worker = str(worker_id or "").strip()
        if not worker:
            return None, "认领时必须提供 worker 标识"

        status = detail.get("status")
        if status == "已完成":
            return None, f"分区 {detail.get('分区')} 已完成，无需再次认领"
        if status == "回填中":
            owner = str(detail.get("认领worker", ""))
            if owner == worker:
                return detail, f"分区 {detail.get('分区')} 已由该 worker 持有，幂等返回"
            return None, f"分区 {detail.get('分区')} 正被 worker {owner} 处理，无法重复认领"
        if status not in CLAIMABLE_STATUS:
            return None, f"分区明细状态为「{status}」，不可认领"

        detail["status"] = "回填中"
        detail["认领worker"] = worker
        detail["pending"] = True
        detail["abnormal"] = False
        return detail, f"worker {worker} 已认领分区 {detail.get('分区')}"

    def report_progress(
        self,
        detail_id: int,
        *,
        committed_position: str | None = None,
        finished_key_range: str | None = None,
        finished: bool = False,
    ) -> tuple[dict[str, Any] | None, str]:
        """回写分区实际完成的键/行区间与已提交位点，并同步到检查点凭据。

        检查点上的「分区位点凭据」按分区独立写入，只更新当前分区，
        其他分区（包括位点为空的分区）的已有凭据不会被覆盖或清除。
        """
        detail = store.find(self.MODULE, detail_id)
        if detail is None:
            return None, f"{self.ENTITY} {detail_id} 不存在或已归档"
        if detail.get("status") == "已完成":
            return detail, f"分区 {detail.get('分区')} 已完成，忽略重复进度上报"

        if committed_position:
            detail["已提交位点"] = str(committed_position)
        if finished_key_range:
            detail["完成键区间"] = str(finished_key_range)

        if finished:
            detail["status"] = "已完成"
            detail["pending"] = False
            detail["abnormal"] = False
            # 完成时若调用方未再给位点，截止位点即最终确认位点
            if not detail.get("已提交位点"):
                detail["已提交位点"] = detail.get("截止位点", "")
        else:
            detail["status"] = "回填中"
            detail["pending"] = True

        self._sync_checkpoint_credential(detail)
        self._refresh_parent_progress(detail)
        return detail, f"分区 {detail.get('分区')} 进度已回写"

    def interrupt_detail(self, detail_id: int, reason: str = "") -> tuple[dict[str, Any] | None, str]:
        """模拟源端断连：保留已提交位点，仅把明细置为已中断，等待续跑认领。"""
        detail = store.find(self.MODULE, detail_id)
        if detail is None:
            return None, f"{self.ENTITY} {detail_id} 不存在或已归档"
        if detail.get("status") == "已完成":
            return None, f"分区 {detail.get('分区')} 已完成，不能标记中断"
        detail["status"] = "已中断"
        detail["abnormal"] = True
        detail["pending"] = True
        detail["中断原因"] = str(reason or "源端连接丢失")
        # 已提交位点与完成键区间原样保留，作为续跑起点
        self._sync_checkpoint_credential(detail)
        return detail, f"分区 {detail.get('分区')} 已中断，已提交位点 {detail.get('已提交位点')} 予以保留"

    def list_unfinished(self, *, batch_no: str | None = None) -> list[dict[str, Any]]:
        """列出批次内仍可续跑的分区明细（待认领 / 回填中 / 已中断）。"""
        rows = store.rows(self.MODULE)
        return [
            row for row in rows
            if row.get("批次号") == batch_no and row.get("status") != "已完成"
        ] if batch_no else [row for row in rows if row.get("status") != "已完成"]

    # ---- 内部同步 ----

    def _sync_checkpoint_credential(self, detail: dict[str, Any]) -> None:
        """把该分区的确认位点回写到检查点，按分区隔离、互不影响。"""
        cp_id = detail.get("检查点id")
        if cp_id is None:
            return
        checkpoint = store.find("checkpoint", int(cp_id))
        if checkpoint is None:
            return
        credentials = checkpoint.setdefault("分区位点凭据", {})
        # 只覆盖当前分区这一个键，绝不清理其他分区的凭据
        credentials[str(detail.get("分区"))] = {
            "已提交位点": detail.get("已提交位点", ""),
            "完成键区间": detail.get("完成键区间", ""),
            "状态": detail.get("status", ""),
            "认领worker": detail.get("认领worker", ""),
        }

    def _refresh_parent_progress(self, detail: dict[str, Any]) -> None:
        """按明细完成比例刷新回填总单的进度展示。"""
        batch_no = detail.get("批次号")
        if not batch_no:
            return
        siblings = [r for r in store.rows(self.MODULE) if r.get("批次号") == batch_no]
        if not siblings:
            return
        done = sum(1 for r in siblings if r.get("status") == "已完成")
        percent = round(done * 100 / len(siblings))
        for task in store.rows("backfill_run"):
            if task.get("批次号") == batch_no:
                task["当前进度"] = f"{percent}%"
                task["status"] = "已完成" if done == len(siblings) else "回填中"
                task["pending"] = done != len(siblings)
                break
