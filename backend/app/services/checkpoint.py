"""检查点服务：断点恢复时触发水位重置、整组分区回填派发与断点续跑。

整组派发（dispatch_partition_backfill）把同一批次里多个带起止位点的分区缺口
一次性调度成 backfill_partition 明细；批次号幂等，重复提交同一批不会产生第二份
有效任务。每个分区在检查点上独立保存「分区位点凭据」，单个分区的空位点或中断
不会丢掉其他分区已经确认写入的位置。
"""
from __future__ import annotations

import hashlib
from typing import Any

from app.services.base import BaseModuleService
from app.services.backfill_partition import CLAIMABLE_STATUS
from app.store import store


class CheckpointService(BaseModuleService):
    MODULE = "checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["检查点编号", "数据流", "保存位点", "保存时间", "恢复状态", "丢失行数", "回放代价", "恢复耗时"]
    REQUIRED_FIELDS = ["检查点编号", "数据流", "保存位点"]
    STATUS_ORDER = ["已保存", "恢复中", "已恢复", "恢复失败"]
    ACTION_RULES = {"触发恢复": "恢复中", "确认恢复": "已恢复", "标记失败": "恢复失败"}
    NEGATIVE_ACTIONS = []

    def trigger_watermark_reset_and_backfill(self, entry_id: int) -> dict[str, Any]:
        """断点恢复后触发水位重置和回填任务。

        数据流链路: checkpoint -> watermark_mgr + backfill_run
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None

        tag = str(entry.get("数据流", ""))
        results: dict[str, Any] = {"watermark": None, "backfill": None}

        if entry.get("status") == "已恢复":
            # 重置水位
            for wm in store.rows("watermark_mgr"):
                if str(wm.get("数据流", "")) == tag:
                    wm["当前位点"] = str(entry.get("保存位点", ""))
                    wm["status"] = "推进中"
                    wm["pending"] = True
                    wm["abnormal"] = False
                    results["watermark"] = wm
                    break

            # 如果有丢失行数，创建回填任务
            loss = int(entry.get("丢失行数", 0))
            if loss > 0:
                bf_rows = store.rows("backfill_run")
                new_bf_id = max((int(r.get("id", 0)) for r in bf_rows), default=0) + 1
                backfill = {
                    "id": new_bf_id,
                    "任务编号": f"BACK-{new_bf_id:04d}",
                    "目标表": tag,
                    "起始位点": str(entry.get("保存位点", "")),
                    "截止位点": "latest_binlog",
                    "回填模式": "批量并行回填",
                    "并行粒度": "按分区",
                    "当前进度": "0%",
                    "回填速率": "0 eps",
                    "status": "回填中",
                    "pending": True,
                    "abnormal": False,
                }
                bf_rows.append(backfill)
                results["backfill"] = backfill

        return results

    # ------------------------------------------------------------------
    # 整组派发：多分区缺口 -> 回填分区任务明细（幂等批次）
    # ------------------------------------------------------------------

    def dispatch_partition_backfill(
        self,
        checkpoint_id: int,
        gaps: list[dict[str, Any]],
        *,
        batch_no: str | None = None,
    ) -> dict[str, Any]:
        """把一批分区缺口批量调度为回填分区任务明细。

        gaps 中每项形如：
            {"分区": "p0", "起始位点": "...", "截止位点": "..."}
        同一 checkpoint + 同一批缺口签名对应固定批次号；重复提交时直接复用
        已派发的明细，不会新建第二份有效任务，也不会回退任何分区已提交位点。
        """
        checkpoint = store.find(self.MODULE, checkpoint_id)
        if checkpoint is None:
            return {"ok": False, "message": f"检查点 {checkpoint_id} 不存在或已归档"}

        normalized, invalid = self._normalize_gaps(gaps)
        if invalid:
            return {"ok": False, "message": f"缺口参数不合法：{'；'.join(invalid)}"}

        resolved_batch = batch_no.strip() if batch_no else self._derive_batch_no(checkpoint, normalized)
        partition_rows = store.rows("backfill_partition")
        existing = [row for row in partition_rows if row.get("批次号") == resolved_batch]

        # 幂等重发：同批次只返回已有明细，保留各分区实际已提交位点
        if existing:
            return {
                "ok": True,
                "idem": True,
                "message": f"批次 {resolved_batch} 已派发过，复用 {len(existing)} 条分区明细，未重复建单",
                "批次号": resolved_batch,
                "created": [],
                "details": existing,
            }

        parent_task = self._ensure_parent_backfill(checkpoint, normalized, resolved_batch)
        credentials = checkpoint.setdefault("分区位点凭据", {})

        created: list[dict[str, Any]] = []
        next_id = max((int(r.get("id", 0)) for r in partition_rows), default=0) + 1
        for index, gap in enumerate(normalized, start=1):
            partition = gap["分区"]
            # 续跑前已确认写入的位置直接作为该分区的起始凭据；空位点不影响其他分区
            prior = credentials.get(partition) or {}
            committed = str(prior.get("已提交位点", "") or "")
            finished_range = str(prior.get("完成键区间", "") or "")

            # 已有凭据覆盖到截止位点的分区视为已完成，不再派发有效任务
            already_done = bool(committed) and committed == gap["截止位点"]
            status = "已完成" if already_done else "待认领"

            detail = {
                "id": next_id,
                "明细编号": f"BFP-{resolved_batch}-{index:02d}",
                "批次号": resolved_batch,
                "回填任务": parent_task["任务编号"],
                "检查点id": checkpoint_id,
                "数据流": str(checkpoint.get("数据流", "")),
                "分区": partition,
                "起始位点": gap["起始位点"],
                "截止位点": gap["截止位点"],
                "已提交位点": committed,
                "完成键区间": finished_range,
                "认领worker": str(prior.get("认领worker", "") or ""),
                "中断原因": "",
                "status": status,
                "pending": not already_done,
                "abnormal": False,
            }
            partition_rows.append(detail)
            created.append(detail)
            next_id += 1

        return {
            "ok": True,
            "idem": False,
            "message": f"批次 {resolved_batch} 已受理 {len(normalized)} 个分区缺口，"
                       f"新建 {len(created)} 条回填分区明细",
            "批次号": resolved_batch,
            "created": created,
            "details": created,
            "backfill_run": parent_task,
        }

    def resume_batch(self, batch_no: str) -> dict[str, Any]:
        """中断后续跑：返回批次内仍可认领的分区，并保留各分区已提交位点。

        已完成的分区不再出现；可续跑分区带着各自的已提交位点返回，worker 据此
        从断点继续，而不是重放整段区间。
        """
        batch_no = str(batch_no or "").strip()
        if not batch_no:
            return {"ok": False, "message": "续跑必须提供批次号"}
        all_details = store.rows("backfill_partition")
        details = [row for row in all_details if row.get("批次号") == batch_no]
        if not details:
            return {"ok": False, "message": f"批次 {batch_no} 不存在或已归档"}

        unfinished = [row for row in details if row.get("status") != "已完成"]
        claimable = [row for row in unfinished if row.get("status") in CLAIMABLE_STATUS]
        finished = [row for row in details if row.get("status") == "已完成"]
        return {
            "ok": True,
            "message": f"批次 {batch_no} 可续跑分区 {len(claimable)} 个，"
                       f"已完成分区 {len(finished)} 个的确认数据直接沿用",
            "批次号": batch_no,
            "claimable": claimable,
            "finished": finished,
            "details": details,
        }

    # ---- 内部工具 ----

    @staticmethod
    def _normalize_gaps(gaps: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[str]]:
        normalized: list[dict[str, str]] = []
        invalid: list[str] = []
        seen: set[str] = set()
        for index, raw in enumerate(gaps or [], start=1):
            partition = str(raw.get("分区", "")).strip()
            start = str(raw.get("起始位点", "")).strip()
            end = str(raw.get("截止位点", "")).strip()
            if not partition or not start or not end:
                invalid.append(f"第{index}个缺口缺少分区/起止位点")
                continue
            if partition in seen:
                invalid.append(f"分区 {partition} 在同一批次重复出现")
                continue
            seen.add(partition)
            normalized.append({"分区": partition, "起始位点": start, "截止位点": end})
        if not normalized:
            invalid.append("没有可调度的有效分区缺口")
        return normalized, invalid

    @staticmethod
    def _derive_batch_no(checkpoint: dict[str, Any], gaps: list[dict[str, str]]) -> str:
        """检查点 + 有序分区缺口签名 -> 稳定批次号，保证重发幂等。"""
        signature = "|".join(
            f"{g['分区']}:{g['起始位点']}->{g['截止位点']}" for g in gaps
        )
        digest = hashlib.md5(  # noqa: S324 - 仅用于生成稳定批次号，非安全用途
            f"{checkpoint.get('检查点编号', checkpoint.get('id'))}|{signature}".encode("utf-8")
        ).hexdigest()[:10]
        return f"BF-BATCH-{digest}"

    def _ensure_parent_backfill(
        self,
        checkpoint: dict[str, Any],
        gaps: list[dict[str, str]],
        batch_no: str,
    ) -> dict[str, Any]:
        """为批次挂一条回填总单；同批次重复进入时复用，绝不产生第二份总单。"""
        bf_rows = store.rows("backfill_run")
        for task in bf_rows:
            if task.get("批次号") == batch_no:
                return task
        new_id = max((int(r.get("id", 0)) for r in bf_rows), default=0) + 1
        task = {
            "id": new_id,
            "任务编号": f"BACK-{new_id:04d}",
            "批次号": batch_no,
            "目标表": str(checkpoint.get("数据流", "")),
            "起始位点": gaps[0]["起始位点"],
            "截止位点": max(g["截止位点"] for g in gaps),
            "回填模式": "批量并行回填",
            "并行粒度": "按分区",
            "当前进度": "0%",
            "回填速率": "0 eps",
            "status": "回填中",
            "pending": True,
            "abnormal": False,
        }
        bf_rows.append(task)
        return task
