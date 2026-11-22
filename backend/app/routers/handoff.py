"""解析映射改版交接端点：转换批次交付、两侧接纳回执、提交位点核定。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services.handoff import SIDES, handoff_service

router = APIRouter(prefix="/api/handoff", tags=["映射交接"])

ACTIONS = {
    "arrive_new_batch": handoff_service.arrive_new_batch,
    "pause_old_ack": handoff_service.pause_old_dedup_ack,
    "resend_both": handoff_service.resend_both,
    "receive_old_ack": handoff_service.receive_old_ack,
    "recover": handoff_service.recover,
    "interrupt": handoff_service.interrupt,
    "commit": handoff_service.commit,
    "run_scenario": handoff_service.run_scenario,
    "reset": handoff_service.reset,
}


@router.get("/state")
def get_state() -> dict[str, Any]:
    """完整交接账本：两侧接纳区间、排队、重送次数和提交位点。"""
    return handoff_service.snapshot()


@router.get("/workbench")
def get_workbench() -> dict[str, Any]:
    """工作台所需的三块视图：关联订单、去重缺口、解析可提交位点。"""
    state = handoff_service.snapshot()
    return {
        "workbench": state["workbench"],
        "committed_position": state["committed_position"],
        "committable_position": state["committable_position"],
        "backlog_segments": state["backlog_segments"],
        "consistency": state["consistency"],
    }


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    return {"batches": handoff_service.snapshot()["batches"]}


@router.get("/versions")
def list_versions() -> dict[str, Any]:
    state = handoff_service.snapshot()
    return {
        "versions": state["versions"],
        "mapping_ranges": state["mapping_ranges"],
        "committed_position": state["committed_position"],
        "committable_position": state["committable_position"],
    }


@router.get("/acceptances")
def list_acceptances(
    side: str | None = Query(default=None),
    version: str | None = Query(default=None),
    position: int | None = Query(default=None),
) -> dict[str, Any]:
    """按侧、映射版本、位点核对实际接纳记录，每条都带批次与回执号。"""
    if side and side not in SIDES:
        raise HTTPException(status_code=400, detail=f"未知交接侧：{side}")
    rows = handoff_service.snapshot()["acceptance_receipts"]
    if side:
        rows = [row for row in rows if row["side"] == side]
    if version:
        rows = [row for row in rows if row["version"] == version]
    if position is not None:
        rows = [row for row in rows if row["position"] == position]
    return {"items": rows, "total": len(rows)}


@router.get("/queues")
def list_queues(side: str | None = Query(default=None)) -> dict[str, Any]:
    """排队记录：旧批去重回执未到期间新段的排队情况仍可查询。"""
    rows = handoff_service.snapshot()["queues"]
    if side:
        rows = [row for row in rows if row["side"] == side]
    return {"items": rows, "total": len(rows)}


@router.post("/actions/{action}")
def run_handoff_action(action: str) -> dict[str, Any]:
    handler = ACTIONS.get(action)
    if handler is None:
        raise HTTPException(status_code=404, detail=f"未知交接动作：{action}")
    return handler()


@router.get("/consistency")
def check_consistency() -> dict[str, Any]:
    """解析侧核对记录一致性：两侧接纳区间、批次版本归属与订单键顺序。"""
    from app.services.binlog_parse import BinlogParseService
    from app.services.dedup_stream import DedupStreamService
    from app.services.dim_join import DimJoinService

    state = handoff_service.snapshot()
    return {
        "consistency": state["consistency"],
        "parse": BinlogParseService().resolve_committable_position(),
        "dim_join": DimJoinService().acceptance_ledger(),
        "dedup_stream": DedupStreamService().acceptance_ledger(),
    }
