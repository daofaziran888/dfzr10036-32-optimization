"""Binlog解析服务：启动解析后会生产事件并推动下游event_trans转换管道。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.handoff import handoff_service
from app.store import store


class BinlogParseService(BaseModuleService):
    MODULE = "binlog_parse"
    ENTITY = "解析规则"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["规则编号", "源端", "日志格式", "事件过滤", "字段映射", "DDL处理", "大事务拆分", "解析速率"]
    REQUIRED_FIELDS = ["规则编号", "源端", "日志格式"]
    STATUS_ORDER = ["草稿", "解析中", "速率下降", "解析中断"]
    ACTION_RULES = {"启动解析": "解析中", "调整速率": "速率下降", "标记中断": "解析中断"}
    NEGATIVE_ACTIONS = []

    def trigger_downstream_event_trans(self, entry_id: int) -> dict[str, Any] | None:
        """binlog解析变为「解析中」后，级联创建 event_trans 转换管道。

        数据流链路: binlog_parse -> event_trans
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "解析中":
            return None

        trans_rows = store.rows("event_trans")
        existing = [r for r in trans_rows if str(r.get("源端", "")) == str(entry.get("源端", ""))]
        if existing:
            existing[0]["status"] = "转换中"
            existing[0]["pending"] = True
            existing[0]["abnormal"] = False
            existing[0]["最近吞吐"] = "12.5k eps"
            return existing[0]

        new_id = max((int(r.get("id", 0)) for r in trans_rows), default=0) + 1
        downstream = {
            "id": new_id,
            "管道编号": f"EVEN-{new_id:04d}",
            "源端": str(entry.get("源端", "")),
            "事件类型": str(entry.get("事件过滤", "INSERT,UPDATE,DELETE")),
            "转换函数": "flatten_nested_json, cast_timestamp, normalize_pk",
            "输出格式": "Avro",
            "失败策略": "写入死信队列",
            "转换顺序": "先扁平后类型转换",
            "最近吞吐": "12.5k eps",
            "status": "转换中",
            "pending": True,
            "abnormal": False,
        }
        trans_rows.append(downstream)
        return downstream

    def propagate_rate_adjustment(self) -> dict[str, Any]:
        """解析速率下降时，将上游速率事件传播到下游。

        数据流链路: binlog_parse(速率下降) -> event_trans(过载) -> lag_monitor(延迟上升)
        """
        rows = store.rows(self.MODULE)
        slow_rows = [r for r in rows if r.get("status") == "速率下降"]
        result = {"affected_downstream": 0}

        for slow in slow_rows:
            tag = str(slow.get("源端", ""))
            # 传播到事件转换
            for tr in store.rows("event_trans"):
                if str(tr.get("源端", "")) == tag and tr.get("status") == "转换中":
                    tr["status"] = "转换过载"
                    tr["pending"] = True
                    tr["abnormal"] = True
                    result["affected_downstream"] += 1
            # 传播到延迟监控
            for lm in store.rows("lag_monitor"):
                if str(lm.get("数据流", "")) == tag and lm.get("status") == "正常":
                    lm["status"] = "延迟偏高"
                    lm["pending"] = True
                    lm["abnormal"] = True
                    result["affected_downstream"] += 1

        return result

    def resolve_committable_position(self) -> dict[str, Any]:
        """依据两侧接纳回执核定解析可提交位点：只取从 101 起连续确认的前缀。"""
        state = handoff_service.snapshot()
        return {
            "committed_position": state["committed_position"],
            "committable_position": state["committable_position"],
            "final_position": state["final_position"],
            "committed_offset": state["stream"]["committed_offset"],
            "dim_join_accepted": state["sides"]["dim_join"]["accepted_ranges"],
            "dedup_accepted": state["sides"]["dedup_stream"]["accepted_ranges"],
            "dedup_missing": state["sides"]["dedup_stream"]["missing_batches"],
            "consistency": state["consistency"],
            "backlog_segments": state["backlog_segments"],
        }
