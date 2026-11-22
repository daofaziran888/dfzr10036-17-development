"""整组分区增量回填的断点恢复场景验证（不依赖 pytest，直接 python 运行）。

覆盖题目场景：
1. 一次受理两个独立分区的缺失区间，批量调度成回填分区任务明细；
2. 第一个分区完成写入、第二个分区写到一半源端断连；
3. 重发整批：批次幂等，不产生第二份有效任务，已提交位点不回退；
4. 中断续跑：各分区实际已提交位置保留，其他分区的空位点不会抹掉可用凭据；
5. 只有可认领的未完分区能被新 worker 认领，已确认分区的数据直接供下一次继续。
"""
from __future__ import annotations

import os
import sys

# 以隔离模式运行时也要能导入 backend/app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services import service_for  # noqa: E402
from app.store import store  # noqa: E402


PASS = 0
FAIL = 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {label}")
    else:
        FAIL += 1
        print(f"  ❌ {label} {detail}")


def main() -> int:
    cp_service = service_for("checkpoint")
    bp_service = service_for("backfill_partition")

    checkpoint = store.find("checkpoint", 1)
    gaps = [
        {"分区": "p0", "起始位点": "binlog.001234:891200", "截止位点": "binlog.001234:900000"},
        {"分区": "p1", "起始位点": "binlog.001234:900000", "截止位点": "binlog.001234:910000"},
    ]

    print("\n[1] 整组派发：两个独立分区的缺口一次受理")
    before_details = len(store.rows("backfill_partition"))
    before_runs = len(store.rows("backfill_run"))
    result = cp_service.dispatch_partition_backfill(1, gaps)
    check("派发受理成功", result["ok"] is True, result.get("message", ""))
    check("生成 2 条分区明细", len(result["created"]) == 2)
    check("同时挂 1 条回填总单", len(store.rows("backfill_run")) == before_runs + 1)
    check("明细初始为待认领", all(d["status"] == "待认领" for d in result["details"]))
    batch_no = result["批次号"]
    check("批次号稳定生成", batch_no.startswith("BF-BATCH-"), batch_no)

    print("\n[2] 确定性批次号：同缺口签名得到同一批次号")
    result_again_no = cp_service.dispatch_partition_backfill(
        1, gaps, batch_no=cp_service._derive_batch_no(checkpoint, cp_service._normalize_gaps(gaps)[0])
    )
    check("派生批次号与首次一致", result_again_no["批次号"] == batch_no)

    details = {d["分区"]: d for d in store.rows("backfill_partition") if d["批次号"] == batch_no}
    p0, p1 = details["p0"], details["p1"]

    print("\n[3] 分区 p0 完整写入并回写完成区间；p1 写到一半")
    entry, msg = bp_service.claim_detail(p0["id"], "worker-a")
    check("worker-a 认领 p0 成功", entry is not None, msg)
    bp_service.report_progress(
        p0["id"], committed_position="binlog.001234:900000",
        finished_key_range="order_id 200000~215300", finished=True,
    )
    entry, msg = bp_service.claim_detail(p1["id"], "worker-b")
    check("worker-b 认领 p1 成功", entry is not None, msg)
    bp_service.report_progress(
        p1["id"], committed_position="binlog.001234:904500",
        finished_key_range="order_id 215301~220880", finished=False,
    )

    print("\n[4] 源端断连：p1 中断，已提交位点必须原样保留")
    entry, msg = bp_service.interrupt_detail(p1["id"], reason="源端连接丢失")
    check("p1 置为已中断", entry is not None and entry["status"] == "已中断", msg)
    check("p1 已提交位点保留", entry["已提交位点"] == "binlog.001234:904500")
    check("p1 完成键区间保留", entry["完成键区间"] == "order_id 215301~220880")

    print("\n[5] 检查点分区凭据：按分区独立保存，互不覆盖")
    credentials = store.find("checkpoint", 1)["分区位点凭据"]
    check("p0 凭据已确认到截止位点",
          credentials["p0"]["已提交位点"] == "binlog.001234:900000"
          and credentials["p0"]["状态"] == "已完成")
    check("p1 凭据停在中断前位点",
          credentials["p1"]["已提交位点"] == "binlog.001234:904500"
          and credentials["p1"]["状态"] == "已中断")

    print("\n[6] 空位点隔离：加入从未写入的 p2，其空位点不能抹掉 p0/p1 的凭据")
    r2 = cp_service.dispatch_partition_backfill(1, [
        {"分区": "p2", "起始位点": "binlog.001234:910000", "截止位点": "binlog.001234:920000"},
    ], batch_no="BF-BATCH-EMPTY-P2")
    check("p2 批次受理成功", r2["ok"])
    p2 = r2["created"][0]
    bp_service.interrupt_detail(p2["id"], reason="源端不可用，尚无写入")
    credentials = store.find("checkpoint", 1)["分区位点凭据"]
    check("p2 空位点已登记", credentials["p2"]["已提交位点"] == "")
    check("p0 凭据未受空位点影响", credentials["p0"]["已提交位点"] == "binlog.001234:900000")
    check("p1 凭据未受空位点影响", credentials["p1"]["已提交位点"] == "binlog.001234:904500")

    print("\n[7] 重发整批：幂等复用，不产生第二份有效任务")
    detail_count = len(store.rows("backfill_partition"))
    run_count = len(store.rows("backfill_run"))
    resent = cp_service.dispatch_partition_backfill(1, gaps)
    check("重发返回幂等标记", resent["idem"] is True)
    check("重发未新建明细", len(store.rows("backfill_partition")) == detail_count)
    check("重发未新建回填总单", len(store.rows("backfill_run")) == run_count)
    check("重发未回退 p1 已提交位点",
          next(d for d in resent["details"] if d["分区"] == "p1")["已提交位点"]
          == "binlog.001234:904500")
    check("重发未回退 p0 完成状态",
          next(d for d in resent["details"] if d["分区"] == "p0")["status"] == "已完成")

    print("\n[8] 中断续跑：只有未完分区可被认领，已确认分区直接沿用")
    resume = cp_service.resume_batch(batch_no)
    check("续跑查询成功", resume["ok"])
    claimable_partitions = {d["分区"] for d in resume["claimable"]}
    finished_partitions = {d["分区"] for d in resume["finished"]}
    check("可续跑仅含 p1", claimable_partitions == {"p1"}, str(claimable_partitions))
    check("已完成 p0 直接沿用", finished_partitions == {"p0"})
    p1_live = next(d for d in resume["claimable"] if d["分区"] == "p1")
    check("续跑起点带中断前位点", p1_live["已提交位点"] == "binlog.001234:904500")

    entry, msg = bp_service.claim_detail(p1["id"], "worker-c")
    check("新 worker 可认领已中断的 p1", entry is not None and entry["status"] == "回填中", msg)
    entry, msg = bp_service.claim_detail(p0["id"], "worker-c")
    check("已完成的 p0 拒绝重复认领", entry is None)
    entry, msg = bp_service.claim_detail(p1["id"], "worker-d")
    check("p1 被 worker-c 持有时拒绝他人认领", entry is None)
    entry, msg = bp_service.claim_detail(p1["id"], "worker-c")
    check("同一 worker 重复认领幂等成功", entry is not None)

    print("\n[9] 续跑完成 p1，回写最终区间并核对检查点")
    bp_service.report_progress(
        p1["id"], committed_position="binlog.001234:910000",
        finished_key_range="order_id 215301~227640", finished=True,
    )
    p1_row = store.find("backfill_partition", p1["id"])
    check("p1 已完成", p1_row["status"] == "已完成")
    credentials = store.find("checkpoint", 1)["分区位点凭据"]
    check("检查点 p1 凭据推进到截止位点",
          credentials["p1"]["已提交位点"] == "binlog.001234:910000"
          and credentials["p1"]["状态"] == "已完成")
    check("检查点 p0 凭据仍完好", credentials["p0"]["状态"] == "已完成")
    parent = next(t for t in store.rows("backfill_run") if t.get("批次号") == batch_no)
    check("回填总单进度按分区刷新为 100%", parent["当前进度"] == "100%" and parent["status"] == "已完成")

    print("\n[10] 参数校验：非法缺口被拒绝")
    bad = cp_service.dispatch_partition_backfill(1, [
        {"分区": "px", "起始位点": "", "截止位点": "x:2"},
        {"分区": "px", "起始位点": "x:1", "截止位点": "x:2"},
    ])
    check("非法缺口拒绝受理", bad["ok"] is False)
    missing = cp_service.dispatch_partition_backfill(999, gaps)
    check("不存在的检查点拒绝受理", missing["ok"] is False)

    print(f"\n结果：通过 {PASS} 项，失败 {FAIL} 项")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
