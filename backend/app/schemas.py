"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class PartitionGap(BaseModel):
    """一条带起止位点的分区缺口。"""
    分区: str
    起始位点: str
    截止位点: str


class DispatchBackfillPayload(BaseModel):
    """整组派发：一次受理多个不同分区的缺失区间。"""
    gaps: list[PartitionGap]
    batch_no: str | None = None


class ClaimDetailPayload(BaseModel):
    worker_id: str


class ReportProgressPayload(BaseModel):
    """回填明细把实际完成的键/行区间与确认位点回写给断点恢复。"""
    committed_position: str | None = None
    finished_key_range: str | None = None
    finished: bool = False


class InterruptDetailPayload(BaseModel):
    reason: str | None = None
