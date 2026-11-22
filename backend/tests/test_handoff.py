"""映射交接账本的回归测试：python3 -m unittest discover -s tests"""
from __future__ import annotations

import unittest

from app.services.handoff import MappingHandoffService


def receipts(state: dict, side: str, position: int) -> list[dict]:
    return [
        row for row in state["acceptance_receipts"]
        if row["side"] == side and row["position"] == position
    ]


class MappingHandoffTest(unittest.TestCase):
    def setUp(self) -> None:
        self.service = MappingHandoffService()

    def test_initial_position_is_100(self) -> None:
        state = self.service.snapshot()
        self.assertEqual(state["committed_position"], 100)
        self.assertEqual(state["committable_position"], 100)
        self.assertEqual(state["sides"]["dim_join"]["accepted_ranges"], [[101, 120]])
        self.assertEqual(state["sides"]["dedup_stream"]["accepted_ranges"], [])

    def test_overlap_keeps_old_batch_and_both_sides_get_new_segment(self) -> None:
        self.service.arrive_new_batch()
        self.service.pause_old_dedup_ack()
        state = self.service.resend_both()["state"]

        for position in range(111, 121):
            dim = receipts(state, "dim_join", position)
            self.assertEqual(len(dim), 1)
            self.assertEqual(dim[0]["batch_id"], "BATCH-V1-101-120")
            self.assertEqual(dim[0]["version"], "map-v1")
            # 旧批去重回执未到，重叠部分不得在去重侧接纳
            self.assertEqual(receipts(state, "dedup_stream", position), [])

        for position in range(121, 131):
            for side in ("dim_join", "dedup_stream"):
                rows = receipts(state, side, position)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["version"], "map-v2")

    def test_commit_waits_for_both_sides_until_old_ack(self) -> None:
        self.service.arrive_new_batch()
        self.service.pause_old_dedup_ack()
        state = self.service.resend_both()["state"]
        self.assertEqual(state["committable_position"], 100)
        self.assertEqual(state["sides"]["dedup_stream"]["queued_ranges"], [[101, 120]])

        workbench = state["workbench"]
        self.assertEqual(workbench["parse_committable_position"], 100)
        self.assertEqual(workbench["backlog_segments"][0]["positions"], [[101, 120]])
        self.assertEqual(workbench["dim_processing_orders"][-1]["last_position"], 130)
        self.assertEqual(workbench["dedup_missing_batches"][0]["queued_positions"], [[101, 120]])

        final = self.service.receive_old_ack()["state"]
        self.assertEqual(final["committable_position"], 130)
        self.assertEqual(final["committed_position"], 130)
        self.assertTrue(final["consistency"]["consistent"])
        self.assertEqual(final["sides"]["dedup_stream"]["accepted_ranges"], [[101, 130]])

    def test_each_side_accepts_each_position_once(self) -> None:
        self.service.arrive_new_batch()
        self.service.pause_old_dedup_ack()
        self.service.resend_both()
        state = self.service.receive_old_ack()["state"]

        for side in ("dim_join", "dedup_stream"):
            positions = state["sides"][side]["accepted_positions"]
            self.assertEqual(positions, list(range(101, 131)))
        self.assertEqual(state["consistency"]["duplicate_acceptances"], 0)
        self.assertEqual(state["resend"]["overlap_duplicate_count"], 10)
        self.assertEqual(state["resend"]["total"], 2)
        self.assertEqual(
            state["versions"][0]["by_side"],
            {"dim_join": [[101, 120]], "dedup_stream": [[101, 120]]},
        )
        self.assertEqual(
            state["versions"][1]["by_side"],
            {"dim_join": [[121, 130]], "dedup_stream": [[121, 130]]},
        )

    def test_recovery_uses_recorded_queues(self) -> None:
        self.service.arrive_new_batch()
        self.service.pause_old_dedup_ack()
        self.service.resend_both()
        self.service.interrupt()
        report = self.service.recover()["report"]
        self.assertEqual(report["restored_queues"], 20)
        self.assertEqual(report["filled"], [])
        self.assertEqual(self.service.committed_position, 100)

        state = self.service.receive_old_ack()["state"]
        self.assertEqual(state["committed_position"], 130)
        self.assertTrue(state["consistency"]["consistent"])

    def test_recovery_rebuilds_missing_new_segment(self) -> None:
        self.service.arrive_new_batch()
        self.service.queues = []
        report = self.service.recover()["report"]
        self.assertTrue(any(item["state"] == "已接纳" for item in report["filled"]))
        state = self.service.snapshot()
        self.assertEqual(state["sides"]["dedup_stream"]["queued_ranges"], [[101, 120]])
        self.assertEqual(state["committable_position"], 100)


if __name__ == "__main__":
    unittest.main()
