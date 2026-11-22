"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import (
    ActionPayload,
    ActionResult,
    ClaimDetailPayload,
    DispatchBackfillPayload,
    EntryPayload,
    InterruptDetailPayload,
    PageResult,
    ReportProgressPayload,
)
from app.services import service_for
from app.services.backfill_partition import BackfillPartitionService
from app.services.checkpoint import CheckpointService

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)


# ---- 断点恢复：整组分区增量回填派发与续跑 ----

@router.post("/checkpoint/{checkpoint_id}/backfill-batches")
def dispatch_partition_backfill(checkpoint_id: int, payload: DispatchBackfillPayload) -> dict[str, Any]:
    """把多条带起止位点的分区缺口批量调度成回填分区任务明细（批次号幂等）。"""
    service = service_for("checkpoint")
    if not isinstance(service, CheckpointService):
        raise HTTPException(status_code=404, detail="断点恢复模块不可用")
    gaps = [gap.model_dump() for gap in payload.gaps]
    return service.dispatch_partition_backfill(checkpoint_id, gaps, batch_no=payload.batch_no)


@router.get("/checkpoint/backfill-batches/{batch_no}")
def resume_backfill_batch(batch_no: str) -> dict[str, Any]:
    """中断后续跑：返回批次内可认领的未完分区，已确认分区的数据直接沿用。"""
    service = service_for("checkpoint")
    if not isinstance(service, CheckpointService):
        raise HTTPException(status_code=404, detail="断点恢复模块不可用")
    return service.resume_batch(batch_no)


@router.post("/backfill_partition/{detail_id}/claim")
def claim_backfill_partition(detail_id: int, payload: ClaimDetailPayload) -> ActionResult:
    """认领未完分区明细；已中断的分区可被新 worker 接手续跑。"""
    service = service_for("backfill_partition")
    if not isinstance(service, BackfillPartitionService):
        raise HTTPException(status_code=404, detail="回填分区明细模块不可用")
    entry, message = service.claim_detail(detail_id, payload.worker_id)
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.post("/backfill_partition/{detail_id}/progress")
def report_backfill_partition_progress(
    detail_id: int, payload: ReportProgressPayload
) -> ActionResult:
    """把实际完成的键/行区间与已提交位点回写给断点恢复核对。"""
    service = service_for("backfill_partition")
    if not isinstance(service, BackfillPartitionService):
        raise HTTPException(status_code=404, detail="回填分区明细模块不可用")
    entry, message = service.report_progress(
        detail_id,
        committed_position=payload.committed_position,
        finished_key_range=payload.finished_key_range,
        finished=payload.finished,
    )
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.post("/backfill_partition/{detail_id}/interrupt")
def interrupt_backfill_partition(detail_id: int, payload: InterruptDetailPayload) -> ActionResult:
    """模拟源端断连：保留已提交位点，仅把明细置为已中断。"""
    service = service_for("backfill_partition")
    if not isinstance(service, BackfillPartitionService):
        raise HTTPException(status_code=404, detail="回填分区明细模块不可用")
    entry, message = service.interrupt_detail(detail_id, payload.reason or "")
    return ActionResult(ok=entry is not None, message=message, entry=entry)


# ---- 跨模块编排流端点 ----

@router.post("/flows/cdc-to-lake")
def trigger_flow_cdc_to_lake(cdc_entry_id: int) -> dict[str, Any]:
    """主线链路: CDC源端 -> Binlog解析 -> 事件转换 -> 维表关联 -> 水位 -> 落盘 -> 湖仓提交 -> 小文件合并"""
    from app.services.orchestrator import flow_cdc_to_lake
    return flow_cdc_to_lake(cdc_entry_id)


@router.post("/flows/lag-recovery")
def trigger_flow_lag_recovery(cdc_entry_id: int = 1) -> dict[str, Any]:
    """主线链路: 乱序处理 -> 延迟监控 -> 断点恢复 -> 回填"""
    from app.services.orchestrator import flow_lag_recovery
    return flow_lag_recovery(cdc_entry_id)


@router.post("/flows/ddl-propagation")
def trigger_flow_ddl_propagation(ddl_entry_id: int) -> dict[str, Any]:
    """主线链路: DDL适配 -> Binlog调整 -> Schema演进"""
    from app.services.orchestrator import flow_ddl_propagation
    return flow_ddl_propagation(ddl_entry_id)


@router.post("/flows/commit-failure-recovery")
def trigger_flow_commit_failure_recovery(commit_entry_id: int) -> dict[str, Any]:
    """主线链路: Iceberg提交冲突 -> 数据比对 -> 告警 -> 回填修复"""
    from app.services.orchestrator import flow_commit_failure_recovery
    return flow_commit_failure_recovery(commit_entry_id)


@router.post("/flows/cache-invalidation")
def trigger_flow_cache_invalidation(dim_entry_id: int) -> dict[str, Any]:
    """主线链路: 维表缓存失效 -> 告警 -> 延迟指标"""
    from app.services.orchestrator import flow_cache_invalidation
    return flow_cache_invalidation(dim_entry_id)


@router.post("/flows/out-of-order-to-checkpoint")
def trigger_flow_out_of_order_to_checkpoint(ooo_entry_id: int) -> dict[str, Any]:
    """主线链路: 乱序超窗口 -> checkpoint保底 + 告警"""
    from app.services.orchestrator import flow_out_of_order_to_checkpoint
    return flow_out_of_order_to_checkpoint(ooo_entry_id)


@router.post("/flows/rate-propagation")
def trigger_flow_rate_propagation() -> dict[str, Any]:
    """主线链路: 解析速率下降 -> 事件过载 -> 延迟上升"""
    from app.services.orchestrator import flow_rate_propagation
    return flow_rate_propagation()


@router.post("/flows/sink-block-to-compaction")
def trigger_flow_sink_block_to_compaction(sink_entry_id: int) -> dict[str, Any]:
    """主线链路: 落盘阻塞 -> 小文件合并清理"""
    from app.services.orchestrator import flow_sink_block_to_compaction
    return flow_sink_block_to_compaction(sink_entry_id)


@router.get("/flows/list")
def list_flows() -> dict[str, Any]:
    """列出所有可编排流链路"""
    from app.services.orchestrator import FLOWS
    return {"flows": list(FLOWS.keys()), "count": len(FLOWS)}