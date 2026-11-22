"""事件转换服务：转换完成后触发维表关联和Dedup去重。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.handoff import handoff_service
from app.store import store


class EventTransService(BaseModuleService):
    MODULE = "event_trans"
    ENTITY = "转换管道"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["管道编号", "源端", "事件类型", "转换函数", "输出格式", "失败策略", "转换顺序", "最近吞吐"]
    REQUIRED_FIELDS = ["管道编号", "源端", "事件类型"]
    STATUS_ORDER = ["草稿", "转换中", "转换过载", "已停止"]
    ACTION_RULES = {"启动转换": "转换中", "扩容处理": "转换过载", "停止管道": "已停止"}
    NEGATIVE_ACTIONS = []

    def trigger_dim_join_and_dedup(self, entry_id: int) -> dict[str, Any]:
        """事件转换管道启动后，级联创建维表关联和去重窗口。

        数据流链路: event_trans -> dim_join + dedup_stream
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "转换中":
            return None

        tag = str(entry.get("源端", ""))
        results: dict[str, Any] = {"dim_join": None, "dedup": None}

        # 创建/恢复 dim_join 维表关联
        dim_rows = store.rows("dim_join")
        has_dim = [r for r in dim_rows if str(r.get("事实表", "")) == tag]
        if has_dim:
            has_dim[0]["status"] = "关联中"
            has_dim[0]["pending"] = True
            has_dim[0]["abnormal"] = False
            has_dim[0]["命中率"] = "87.3%"
            results["dim_join"] = has_dim[0]
        else:
            new_dim_id = max((int(r.get("id", 0)) for r in dim_rows), default=0) + 1
            dim_config = {
                "id": new_dim_id,
                "配置编号": f"DIM_-{new_dim_id:04d}",
                "事实表": tag,
                "维度表": "dim_customer_profile",
                "关联键": "customer_id",
                "关联类型": "LEFT JOIN",
                "缺失策略": "填充默认值",
                "缓存TTL": "600s",
                "命中率": "87.3%",
                "status": "关联中",
                "pending": True,
                "abnormal": False,
            }
            dim_rows.append(dim_config)
            results["dim_join"] = dim_config

        # 创建/恢复 dedup_stream 去重窗口
        dedup_rows = store.rows("dedup_stream")
        has_dedup = [r for r in dedup_rows if str(r.get("数据流", "")) == tag]
        if has_dedup:
            has_dedup[0]["status"] = "去重中"
            has_dedup[0]["pending"] = True
            has_dedup[0]["abnormal"] = False
            results["dedup"] = has_dedup[0]
        else:
            new_dedup_id = max((int(r.get("id", 0)) for r in dedup_rows), default=0) + 1
            dedup_config = {
                "id": new_dedup_id,
                "窗口编号": f"DEDU-{new_dedup_id:04d}",
                "数据流": tag,
                "去重键": "event_id+timestamp",
                "窗口宽度": "10min",
                "去重算法": "BloomFilter",
                "误杀率": "0.01%",
                "内存占用": "128MB",
                "最近命中": "1420次/h",
                "status": "去重中",
                "pending": True,
                "abnormal": False,
            }
            dedup_rows.append(dedup_config)
            results["dedup"] = dedup_config

        return results

    def deliver_batches_to_sides(self, action: str = "resend_both") -> dict[str, Any]:
        """按转换批次交付数据，把各版事件同时送入维表关联和去重窗口。

        数据流链路: event_trans(转换批次) -> dim_join + dedup_stream
        每个批次带明确的映射版本和位点区间；重叠位点沿用旧批归属，只交付不重复接纳。
        """
        result = {
            "arrive_new_batch": handoff_service.arrive_new_batch,
            "resend_both": handoff_service.resend_both,
        }.get(action, handoff_service.resend_both)()
        state = result["state"]
        return {
            "ok": result["ok"],
            "message": result["message"],
            "deliveries": state["deliveries"],
            "batches": state["batches"],
            "dim_join_acceptance": state["sides"]["dim_join"]["accepted_ranges"],
            "dedup_acceptance": state["sides"]["dedup_stream"]["accepted_ranges"],
            "dedup_queue": state["sides"]["dedup_stream"]["queued_ranges"],
        }
