"""解析映射改版交接服务。

一次映射改版会同时存在两个版本的事件批次：旧批已经进入维表关联，新批在切换后
到达，并且会重送旧批末尾的一段事件。这个服务把"转换批次交付数据"作为唯一交接
凭据，分别记录维表关联、去重窗口实际接纳了哪些事件，再按连续前缀核定日志解析
可以提交的位点。

关键约束：
* 重叠事件沿用旧批的批次归属和映射版本，每侧每条只接纳一次；
* 新批独有事件使用新映射，按订单键保持事件顺序；
* 每条接纳记录都能核回批次、版本、实际位点和回执号；
* 提交位点只能取从 101 开始、两侧都按所属版本确认过的连续前缀。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable

SIDES = ("dim_join", "dedup_stream")
SIDE_LABELS = {"dim_join": "维表关联", "dedup_stream": "去重窗口"}

V1 = "map-v1"
V2 = "map-v2"
VERSION_LABELS = {V1: "旧映射", V2: "新映射"}

OLD_BATCH = "BATCH-V1-101-120"
NEW_BATCH = "BATCH-V2-111-130"

STREAM_START = 101
STREAM_END = 130
COMMITTED_START = 100
FINAL_POSITION = 130

# 订单键分组：111-120 与 121-130 分属两组互不重叠的订单键。
ORDER_GROUPS = (
    ("ORD-A", 101, 105),
    ("ORD-B", 106, 110),
    ("ORD-C", 111, 115),
    ("ORD-D", 116, 120),
    ("ORD-E", 121, 125),
    ("ORD-F", 126, 130),
)


def _order_key(position: int) -> str:
    for key, start, end in ORDER_GROUPS:
        if start <= position <= end:
            return key
    return f"ORD-{position}"


def _order_seq(position: int) -> int:
    for _key, start, _end in ORDER_GROUPS:
        if start <= position <= _end:
            return position - start + 1
    return 1


def _attribution(position: int) -> tuple[str, str]:
    """事件位点 -> (批次, 映射版本)。

    101-120 属于旧批；新批虽然带着 111-130 到达，但 111-120 是旧批末尾的
    重送，沿用旧批归属，只有 121-130 使用新映射。
    """
    if 101 <= position <= 120:
        return OLD_BATCH, V1
    if 121 <= position <= 130:
        return NEW_BATCH, V2
    return "", ""


def _ranges(positions: Iterable[int]) -> list[list[int]]:
    ordered = sorted(set(int(pos) for pos in positions))
    if not ordered:
        return []
    result: list[list[int]] = []
    start = previous = ordered[0]
    for position in ordered[1:]:
        if position == previous + 1:
            previous = position
            continue
        result.append([start, previous])
        start = previous = position
    result.append([start, previous])
    return result


def _positions_in(ranges: Iterable[list[int]]) -> list[int]:
    positions: list[int] = []
    for start, end in ranges:
        positions.extend(range(int(start), int(end) + 1))
    return positions


class MappingHandoffService:
    """内存版映射交接账本；所有推进都通过动作完成，便于回放和核对。"""

    def __init__(self) -> None:
        self.reset()

    # ------------------------------------------------------------------ 基础
    def reset(self) -> None:
        self._tick = 0
        self._receipt_seq = 0
        self._delivery_seq = 0
        self.events: list[dict[str, Any]] = []
        self.acceptances: list[dict[str, Any]] = []
        self.duplicates: list[dict[str, Any]] = []
        self.queues: list[dict[str, Any]] = []
        self.receipts: list[dict[str, Any]] = []
        self.recoveries: list[dict[str, Any]] = []
        self.deliveries: list[dict[str, Any]] = []
        self._lost_queues: list[dict[str, Any]] = []

        self.committed_position = COMMITTED_START
        self.resend_count = 0
        self.resend_by_version: dict[str, int] = {V1: 0, V2: 0}
        self.old_dedup_ack_paused = False
        self.old_ack_received = False
        self.new_batch_arrived = False
        self.new_batch_arrived_at: str | None = None
        self.interrupted = False

        self.batches: list[dict[str, Any]] = [
            {
                "batch_id": OLD_BATCH,
                "label": "旧批",
                "version": V1,
                "version_label": VERSION_LABELS[V1],
                "arrived_positions": [101, 120],
                "assigned_positions": [101, 120],
                "carried_over_positions": [],
                "delivery_attempts": 1,
                "resend_count": 0,
                "status": "已交付",
            }
        ]

        self.pending_receipts: list[dict[str, Any]] = [
            {
                "receipt_id": "PENDING-DEDUP-OLD-001",
                "side": "dedup_stream",
                "batch_id": OLD_BATCH,
                "version": V1,
                "positions": [101, 120],
                "state": "待回执",
                "opened_at": self._now(),
            }
        ]

        # 初始状态：旧映射 101-120 已送维表关联并接纳；去重尚未回执。
        delivery = self._new_delivery(OLD_BATCH, V1, "dim_join", list(range(101, 121)), 1)
        for position in range(101, 121):
            self._accept("dim_join", position, OLD_BATCH, V1, delivery["delivery_id"], "初始交付")

        self._log("初始化", "订单流已提交到 100；旧映射 101-120 已由维表关联接纳，去重待回执")

    def _now(self) -> str:
        self._tick += 1
        moment = datetime(2026, 11, 22, 10, 0, 0) + timedelta(seconds=self._tick * 15)
        return moment.strftime("%Y-%m-%d %H:%M:%S")

    def _log(self, action: str, detail: str) -> dict[str, Any]:
        event = {"seq": len(self.events) + 1, "action": action, "detail": detail, "at": self._now()}
        self.events.append(event)
        return event

    def _new_delivery(self, batch_id: str, version: str, side: str, positions: list[int], attempt: int) -> dict[str, Any]:
        self._delivery_seq += 1
        delivery = {
            "delivery_id": f"DEL-{self._delivery_seq:04d}",
            "batch_id": batch_id,
            "version": version,
            "side": side,
            "positions": [min(positions), max(positions)] if positions else [],
            "position_list": list(positions),
            "attempt": attempt,
            "delivered_at": self._now(),
        }
        self.deliveries.append(delivery)
        return delivery

    def _find_batch(self, batch_id: str) -> dict[str, Any] | None:
        return next((batch for batch in self.batches if batch["batch_id"] == batch_id), None)

    def _accepted(self, side: str, position: int) -> dict[str, Any] | None:
        return next(
            (row for row in self.acceptances if row["side"] == side and row["position"] == position),
            None,
        )

    def _queued(self, side: str, position: int) -> dict[str, Any] | None:
        return next(
            (row for row in self.queues if row["side"] == side and row["position"] == position),
            None,
        )

    def _receipt_id(self, side: str, position: int) -> str:
        self._receipt_seq += 1
        tag = "DIM" if side == "dim_join" else "DED"
        return f"RCPT-{tag}-{self._receipt_seq:04d}-{position}"

    def _accept(self, side: str, position: int, batch_id: str, version: str, delivery_id: str, reason: str) -> dict[str, Any] | None:
        """接纳一条事件；已经接纳过的事件不会被接纳第二次。"""
        if position < STREAM_START or position > STREAM_END:
            return None
        existing = self._accepted(side, position)
        if existing is not None:
            return existing
        # 接纳前清掉同侧同位的排队记录
        self.queues = [
            row for row in self.queues
            if not (row["side"] == side and row["position"] == position)
        ]
        record = {
            "receipt_id": self._receipt_id(side, position),
            "side": side,
            "side_label": SIDE_LABELS[side],
            "position": position,
            "actual_position": f"binlog.001234:{position}",
            "order_key": _order_key(position),
            "order_seq": _order_seq(position),
            "batch_id": batch_id,
            "version": version,
            "version_label": VERSION_LABELS.get(version, version),
            "delivery_id": delivery_id,
            "state": "已接纳",
            "reason": reason,
            "accepted_at": self._now(),
        }
        self.acceptances.append(record)
        # 账本按「侧 -> 位点」排序保存，保证同一订单键的接纳顺序始终递增。
        side_rank = {side: index for index, side in enumerate(SIDES)}
        self.acceptances.sort(key=lambda row: (side_rank.get(row["side"], 9), row["position"]))
        return record

    def _enqueue(self, side: str, position: int, batch_id: str, version: str, delivery_id: str, reason: str) -> dict[str, Any] | None:
        if self._accepted(side, position) is not None:
            return None
        existing = self._queued(side, position)
        if existing is not None:
            return existing
        record = {
            "queue_id": f"QUE-{side}-{position}",
            "side": side,
            "side_label": SIDE_LABELS[side],
            "position": position,
            "actual_position": f"binlog.001234:{position}",
            "order_key": _order_key(position),
            "order_seq": _order_seq(position),
            "batch_id": batch_id,
            "version": version,
            "delivery_id": delivery_id,
            "state": "已排队",
            "reason": reason,
            "queued_at": self._now(),
        }
        self.queues.append(record)
        return record

    def _record_duplicate(self, side: str, position: int, batch_id: str, version: str, delivery_id: str, reason: str) -> None:
        token = (side, position, delivery_id)
        if any((row["side"], row["position"], row["delivery_id"]) == token for row in self.duplicates):
            return
        self.duplicates.append({
            "duplicate_id": f"DUP-{len(self.duplicates) + 1:04d}",
            "side": side,
            "position": position,
            "batch_id": batch_id,
            "version": version,
            "delivery_id": delivery_id,
            "reason": reason,
            "at": self._now(),
        })

    # ------------------------------------------------------------------ 动作
    def arrive_new_batch(self) -> dict[str, Any]:
        """切换映射后新批到达：111-130，其中 111-120 是旧批末尾的重送。"""
        if self.new_batch_arrived:
            return {"ok": True, "message": "新批已经到达，无需重复登记", "state": self.snapshot()}

        self.new_batch_arrived = True
        self.batches.append({
            "batch_id": NEW_BATCH,
            "label": "新批",
            "version": V2,
            "version_label": VERSION_LABELS[V2],
            "arrived_positions": [111, 130],
            "assigned_positions": [121, 130],
            "carried_over_positions": [111, 120],
            "overlap_batch_id": OLD_BATCH,
            "overlap_version": V1,
            "delivery_attempts": 0,
            "resend_count": 0,
            "status": "已到达待交付",
        })
        self._log(
            "新批到达",
            "新批携带 111-130 到达；111-120 沿用旧批归属与旧映射，121-130 使用新映射且订单键独立",
        )
        return {"ok": True, "message": "新批 111-130 已登记：重叠 111-120 归旧批，新映射只处理 121-130", "state": self.snapshot()}

    def pause_old_dedup_ack(self) -> dict[str, Any]:
        self.old_dedup_ack_paused = True
        for receipt in self.pending_receipts:
            if receipt["batch_id"] == OLD_BATCH and receipt["side"] == "dedup_stream":
                receipt["state"] = "已暂停"
        self._log("暂停旧批去重回执", "旧批 101-120 的去重回执暂停，新批两侧保持可用")
        return {"ok": True, "message": "旧批去重回执已暂停", "state": self.snapshot()}

    def resend_both(self) -> dict[str, Any]:
        """同时重送旧映射 101-120 与新映射 121-130。"""
        if not self.new_batch_arrived:
            self.arrive_new_batch()
        if not self.old_dedup_ack_paused:
            self.pause_old_dedup_ack()

        for version, batch_id, positions in (
            (V1, OLD_BATCH, list(range(101, 121))),
            (V2, NEW_BATCH, list(range(121, 131))),
        ):
            self.resend_count += 1
            self.resend_by_version[version] += 1
            batch = self._find_batch(batch_id)
            if batch is not None:
                batch["resend_count"] = int(batch.get("resend_count", 0)) + 1
                batch["delivery_attempts"] = int(batch.get("delivery_attempts", 0)) + 1
                batch["status"] = "已重送"

            for side in SIDES:
                attempt = int(batch.get("delivery_attempts", 1)) if batch else 2
                delivery = self._new_delivery(batch_id, version, side, positions, attempt)
                for position in positions:
                    if self._accepted(side, position) is not None:
                        reason = "新批重叠重送" if 111 <= position <= 120 else "整批重送"
                        self._record_duplicate(side, position, batch_id, version, delivery["delivery_id"], reason)
                        continue
                    if side == "dedup_stream" and version == V1 and not self.old_ack_received:
                        self._enqueue(
                            side, position, batch_id, version, delivery["delivery_id"],
                            "旧批去重回执未到，先排队",
                        )
                    else:
                        self._accept(
                            side, position, batch_id, version, delivery["delivery_id"],
                            "重送后接纳",
                        )

        self._log(
            "重送两版",
            "旧映射 101-120、新映射 121-130 同时重送；维表关联对重复事件只保留一次，去重新段先接纳、旧段排队",
        )
        self._sync_commit()
        return {"ok": True, "message": "两版数据已重送：新批两侧可用，旧批去重仍在排队", "state": self.snapshot()}

    def receive_old_ack(self) -> dict[str, Any]:
        """收到旧批补交回执：去重接纳排队中的 101-120，缺口补齐。"""
        if self.old_ack_received:
            return {"ok": True, "message": "旧批回执已经收到，无需重复处理", "state": self.snapshot()}

        self.old_ack_received = True
        self.old_dedup_ack_paused = False
        batch = self._find_batch(OLD_BATCH)
        if batch is not None:
            batch["status"] = "回执已到"

        for receipt in self.pending_receipts:
            if receipt["batch_id"] == OLD_BATCH and receipt["side"] == "dedup_stream":
                receipt["state"] = "已回执"
                receipt["closed_at"] = self._now()

        queued = sorted(
            (
                row for row in self.queues
                if row["side"] == "dedup_stream" and row["version"] == V1
            ),
            key=lambda row: row["position"],
        )
        for row in queued:
            self._accept(
                "dedup_stream", row["position"], OLD_BATCH, V1,
                row.get("delivery_id", ""), "旧批补交回执到达后接纳",
            )

        self.receipts.append({
            "receipt_id": "ACK-DEDUP-OLD-101-120",
            "side": "dedup_stream",
            "batch_id": OLD_BATCH,
            "version": V1,
            "positions": [101, 120],
            "accepted_count": len(queued),
            "received_at": self._now(),
        })
        self._log(
            "收到旧批回执",
            f"去重窗口补齐 101-120 共 {len(queued)} 条，解析提交位点推进到 {self._committable()}",
        )
        self._sync_commit()
        return {"ok": True, "message": "旧批补交回执已入账，两侧 101-130 对齐", "state": self.snapshot()}

    def recover(self) -> dict[str, Any]:
        """中断恢复：只依据两侧真实接纳记录和排队记录补齐缺口。"""
        before = self._gaps()
        known_end = STREAM_END if self.new_batch_arrived else 120
        filled: list[dict[str, Any]] = []
        # 中断前已经存在的排队记录是真实凭据，恢复时先回填，而不是重新造一条。
        restored = 0
        for row in self._lost_queues:
            batch_id, version = _attribution(row["position"])
            if self._accepted(row["side"], row["position"]) is not None:
                continue
            if self._queued(row["side"], row["position"]) is not None:
                continue
            self.queues.append(dict(row))
            restored += 1
        self._lost_queues = []
        for position in range(STREAM_START, known_end + 1):
            batch_id, version = _attribution(position)
            for side in SIDES:
                if self._accepted(side, position) is not None:
                    continue
                if side == "dedup_stream" and version == V1 and not self.old_ack_received:
                    if self._queued(side, position) is None:
                        delivery = self._new_delivery(batch_id, version, side, [position], 2)
                        self._enqueue(
                            side, position, batch_id, version, delivery["delivery_id"],
                            "恢复时按真实接纳记录重排缺口",
                        )
                        filled.append({"side": side, "position": position, "batch_id": batch_id, "version": version, "state": "已排队"})
                    continue
                delivery = self._new_delivery(batch_id, version, side, [position], 2)
                self._accept(side, position, batch_id, version, delivery["delivery_id"], "中断恢复补齐")
                filled.append({"side": side, "position": position, "batch_id": batch_id, "version": version, "state": "已接纳"})

        self.interrupted = False
        after = self._gaps()
        report = {
            "before": before,
            "after": after,
            "filled": filled,
            "restored_queues": restored,
            "acceptances": len(self.acceptances),
            "queues": len(self.queues),
        }
        self.recoveries.append(report)
        self._log(
            "中断恢复",
            f"按两侧真实接纳记录核对 {STREAM_START}-{known_end}，补齐 {len(filled)} 条记录",
        )
        self._sync_commit()
        return {"ok": True, "message": f"恢复完成，补齐 {len(filled)} 条接纳/排队记录", "report": report, "state": self.snapshot()}

    def interrupt(self) -> dict[str, Any]:
        """模拟进程中断：派生队列从视图消失，但已落账的排队凭据仍在账本里。"""
        self._lost_queues = [dict(row) for row in self.queues]
        self.queues = []
        self.interrupted = True
        self._log("模拟中断", "派生队列丢失，两侧已接纳记录保留，等待恢复")
        return {"ok": True, "message": "已模拟中断，可执行恢复从真实接纳记录补齐", "state": self.snapshot()}

    def commit(self) -> dict[str, Any]:
        position = self._committable()
        advanced = position > self.committed_position
        self.committed_position = position
        self._log("核定提交位点", f"解析可提交位点 {position}")
        return {
            "ok": True,
            "message": f"解析提交位点推进到 {position}" if advanced else f"提交位点保持 {position}，连续前缀尚未对齐",
            "state": self.snapshot(),
        }

    def run_scenario(self) -> dict[str, Any]:
        """一次跑完题述场景，便于演示和端到端核对。"""
        self.arrive_new_batch()
        self.pause_old_dedup_ack()
        self.resend_both()
        before_ack = self.snapshot()
        self.receive_old_ack()
        return {
            "ok": True,
            "message": "场景已走完：旧批回执到达后提交位点推进到 130",
            "before_old_ack": before_ack,
            "state": self.snapshot(),
        }

    # ------------------------------------------------------------------ 派生
    def _sync_commit(self) -> None:
        self.committed_position = max(self.committed_position, self._committable())

    def _committable(self) -> int:
        position = COMMITTED_START
        for candidate in range(STREAM_START, STREAM_END + 1):
            if candidate != position + 1:
                break
            batch_id, version = _attribution(candidate)
            if not self._side_confirms("dim_join", candidate, batch_id, version):
                break
            if not self._side_confirms("dedup_stream", candidate, batch_id, version):
                break
            position = candidate
        return position

    def _side_confirms(self, side: str, position: int, batch_id: str, version: str) -> bool:
        record = self._accepted(side, position)
        if record is None:
            return False
        return record["batch_id"] == batch_id and record["version"] == version

    def _accepted_positions(self, side: str) -> list[int]:
        return sorted(row["position"] for row in self.acceptances if row["side"] == side)

    def _side_ranges(self, side: str, version: str | None = None) -> list[list[int]]:
        positions = [
            row["position"] for row in self.acceptances
            if row["side"] == side and (version is None or row["version"] == version)
        ]
        return _ranges(positions)

    def _queued_ranges(self, side: str) -> list[list[int]]:
        return _ranges(row["position"] for row in self.queues if row["side"] == side)

    def _missing_batches(self, side: str) -> list[dict[str, Any]]:
        missing: list[dict[str, Any]] = []
        for batch in self.batches:
            positions = _positions_in([batch["assigned_positions"]])
            if not positions:
                continue
            accepted = [pos for pos in positions if self._accepted(side, pos) is not None]
            if len(accepted) == len(positions):
                continue
            queued = [pos for pos in positions if self._queued(side, pos) is not None]
            missing.append({
                "batch_id": batch["batch_id"],
                "version": batch["version"],
                "version_label": batch.get("version_label", batch["version"]),
                "missing_positions": _ranges(
                    pos for pos in positions if self._accepted(side, pos) is None
                ),
                "queued_positions": _ranges(queued),
                "state": "回执未到" if batch["batch_id"] == OLD_BATCH and side == "dedup_stream" else "待交付",
            })
        return missing

    def _gaps(self) -> list[dict[str, Any]]:
        gaps: list[dict[str, Any]] = []
        for side in SIDES:
            accepted = set(self._accepted_positions(side))
            missing = [pos for pos in range(STREAM_START, STREAM_END + 1) if pos not in accepted]
            if missing:
                gaps.append({"side": side, "side_label": SIDE_LABELS[side], "positions": _ranges(missing)})
        return gaps

    def _order_states(self, side: str) -> list[dict[str, Any]]:
        records = [row for row in self.acceptances if row["side"] == side]
        by_key: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            by_key.setdefault(record["order_key"], []).append(record)
        result: list[dict[str, Any]] = []
        for key in sorted(by_key):
            rows = sorted(by_key[key], key=lambda row: row["position"])
            positions = [row["position"] for row in rows]
            result.append({
                "order_key": key,
                "positions": _ranges(positions),
                "last_position": positions[-1],
                "accepted_count": len(positions),
                "order_ok": positions == sorted(positions),
            })
        return result

    def _order_violations(self) -> list[dict[str, Any]]:
        violations: list[dict[str, Any]] = []
        for side in SIDES:
            records = [row for row in self.acceptances if row["side"] == side]
            by_key: dict[str, list[dict[str, Any]]] = {}
            for record in records:
                by_key.setdefault(record["order_key"], []).append(record)
            for key, rows in by_key.items():
                ordered = sorted(rows, key=lambda row: row["accepted_at"])
                positions = [row["position"] for row in ordered]
                if positions != sorted(positions):
                    violations.append({
                        "side": side,
                        "order_key": key,
                        "accepted_sequence": positions,
                    })
        return violations

    def _consistency(self) -> dict[str, Any]:
        issues: list[dict[str, Any]] = []
        checked = 0
        known_end = STREAM_END if self.new_batch_arrived else 120
        for position in range(STREAM_START, known_end + 1):
            batch_id, version = _attribution(position)
            checked += 1
            for side in SIDES:
                record = self._accepted(side, position)
                if record is None:
                    continue
                if record["batch_id"] != batch_id or record["version"] != version:
                    issues.append({
                        "type": "批次版本不一致",
                        "position": position,
                        "side": side,
                        "expected": {"batch_id": batch_id, "version": version},
                        "actual": {"batch_id": record["batch_id"], "version": record["version"]},
                    })
        for violation in self._order_violations():
            issues.append({"type": "订单键顺序异常", **violation})
        duplicate_keys = [
            (side, position) for side in SIDES for position in range(STREAM_START, STREAM_END + 1)
            if sum(1 for row in self.acceptances if row["side"] == side and row["position"] == position) > 1
        ]
        for side, position in duplicate_keys:
            issues.append({"type": "重复接纳", "side": side, "position": position})
        return {
            "consistent": not issues,
            "checked_positions": checked,
            "issues": issues,
            "duplicate_acceptances": len(duplicate_keys),
        }

    def _version_summary(self, version: str) -> dict[str, Any]:
        by_side = {side: self._side_ranges(side, version) for side in SIDES}
        confirmed_positions = [
            position for position in range(STREAM_START, STREAM_END + 1)
            if _attribution(position)[1] == version
            and self._accepted("dim_join", position) is not None
            and self._accepted("dedup_stream", position) is not None
        ]
        total_positions = sum(1 for position in range(STREAM_START, STREAM_END + 1) if _attribution(position)[1] == version)
        return {
            "version": version,
            "name": VERSION_LABELS[version],
            "batch_id": OLD_BATCH if version == V1 else NEW_BATCH,
            "by_side": by_side,
            "confirmed": _ranges(confirmed_positions),
            "confirmed_count": len(confirmed_positions),
            "total_positions": total_positions,
            "resend_count": self.resend_by_version.get(version, 0),
        }

    def snapshot(self) -> dict[str, Any]:
        dim_accepted = self._accepted_positions("dim_join")
        dedup_accepted = self._accepted_positions("dedup_stream")
        committable = self._committable()

        workbench = {
            "dim_processing_orders": self._order_states("dim_join"),
            "dedup_missing_batches": self._missing_batches("dedup_stream"),
            "dedup_pending_queues": [
                {
                    "batch_id": row["batch_id"],
                    "version": row["version"],
                    "position": row["position"],
                    "order_key": row["order_key"],
                    "state": row["state"],
                    "reason": row["reason"],
                }
                for row in sorted(self.queues, key=lambda row: row["position"])
            ],
            "parse_committable_position": committable,
            "parse_committed_position": self.committed_position,
            "backlog_segments": [
                {
                    "side": "dedup_stream",
                    "side_label": SIDE_LABELS["dedup_stream"],
                    "batch_id": OLD_BATCH,
                    "version": V1,
                    "positions": _ranges(row["position"] for row in self.queues),
                    "reason": "旧批去重回执未到",
                }
            ] if any(row["side"] == "dedup_stream" for row in self.queues) else [],
        }

        return {
            "stream": {
                "positions": f"{STREAM_START}-{STREAM_END}",
                "committed_start": COMMITTED_START,
                "committed_position": self.committed_position,
                "committable_position": committable,
                "final_position": FINAL_POSITION,
                "committed_offset": f"binlog.001234:{self.committed_position}",
            },
            "committed_position": self.committed_position,
            "committable_position": committable,
            "final_position": FINAL_POSITION,
            "versions": [self._version_summary(V1), self._version_summary(V2)],
            "mapping_ranges": [
                {
                    "version": version,
                    "name": VERSION_LABELS[version],
                    "batch_id": OLD_BATCH if version == V1 else NEW_BATCH,
                    "dim_join": self._side_ranges("dim_join", version),
                    "dedup_stream": self._side_ranges("dedup_stream", version),
                    "confirmed": self._version_summary(version)["confirmed"],
                }
                for version in (V1, V2)
            ],
            "batches": self.batches,
            "sides": {
                side: {
                    "side": side,
                    "label": SIDE_LABELS[side],
                    "accepted_positions": dim_accepted if side == "dim_join" else dedup_accepted,
                    "accepted_ranges": self._side_ranges(side),
                    "accepted_count": len(dim_accepted if side == "dim_join" else dedup_accepted),
                    "queued_ranges": self._queued_ranges(side),
                    "queued_count": sum(1 for row in self.queues if row["side"] == side),
                    "pending_batches": [
                        {
                            "batch_id": receipt["batch_id"],
                            "version": receipt["version"],
                            "positions": receipt["positions"],
                            "state": receipt["state"],
                        }
                        for receipt in self.pending_receipts
                        if receipt["side"] == side and receipt["state"] != "已回执"
                    ],
                    "last_accepted_position": (
                        dim_accepted[-1] if side == "dim_join" and dim_accepted else
                        dedup_accepted[-1] if side == "dedup_stream" and dedup_accepted else None
                    ),
                    "missing_batches": self._missing_batches(side),
                    "order_states": self._order_states(side),
                    "acceptance_ranges_by_version": {
                        version: self._side_ranges(side, version) for version in (V1, V2)
                    },
                }
                for side in SIDES
            },
            "workbench": workbench,
            "backlog_segments": workbench["backlog_segments"],
            "resend": {
                "total": self.resend_count,
                "by_version": dict(self.resend_by_version),
                "by_batch": [
                    {
                        "batch_id": batch["batch_id"],
                        "version": batch["version"],
                        "resend_count": int(batch.get("resend_count", 0)),
                        "delivery_attempts": int(batch.get("delivery_attempts", 0)),
                    }
                    for batch in self.batches
                ],
                "duplicate_count": len(self.duplicates),
                "overlap_duplicate_count": sum(
                    1 for row in self.duplicates if 111 <= row["position"] <= 120
                ),
            },
            "duplicates": self.duplicates[-60:],
            "queues": sorted(self.queues, key=lambda row: (row["side"], row["position"])),
            "receipts": self.acceptances[-80:],
            "acceptance_receipts": self.acceptances,
            "deliveries": self.deliveries,
            "delivery_receipts": self.receipts,
            "pending_receipts": self.pending_receipts,
            "consistency": self._consistency(),
            "recoveries": self.recoveries,
            "scenario": {
                "old_dedup_ack_paused": self.old_dedup_ack_paused,
                "old_ack_received": self.old_ack_received,
                "new_batch_arrived": self.new_batch_arrived,
                "interrupted": self.interrupted,
            },
            "events": self.events,
        }


handoff_service = MappingHandoffService()
