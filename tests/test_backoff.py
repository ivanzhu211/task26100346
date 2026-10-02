"""验收二：一直锁冲突的步骤恰好提交 4 次，
相邻提交间隔依次不少于 5 秒、15 秒、45 秒。"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rpa_engine import (
    Engine, MockSAP, SAPConfig, Workflow, Step, PostingType,
)


class BackoffTest(unittest.TestCase):
    def test_lock_conflict_escalating_backoff(self):
        d = tempfile.mkdtemp()
        sap = MockSAP(os.path.join(d, "sap.db"), SAPConfig(lock_conflict_prob=1.0))
        eng = Engine(os.path.join(d, "engine.db"), sap)
        eid = eng.submit_execution(Workflow(steps=(
            Step(PostingType.CREATE_ORDER, quantity=1),
        ),))
        eng.run_forever()

        self.assertEqual(eng.execution_status(eid), "failed")
        attempts = eng.store.conn.execute(
            "SELECT started_at, result FROM attempts WHERE execution_id = ?"
            " ORDER BY seq", (eid,),
        ).fetchall()
        self.assertEqual(len(attempts), 4)  # 1 次首次 + 3 次重试
        for row in attempts:
            self.assertEqual(row["result"], "lock_conflict")
        starts = [row["started_at"] for row in attempts]
        expected = (5.0, 15.0, 45.0)
        for i, minimum in enumerate(expected):
            gap = starts[i + 1] - starts[i]
            self.assertGreaterEqual(gap, minimum,
                                    f"第 {i+1} 次间隔 {gap:.2f}s < {minimum}s")
        # SAP 里一张凭证都没有
        self.assertIsNone(sap.get_by_reference(f"E{eid}S0"))
        eng.close()
        sap.close()


if __name__ == "__main__":
    unittest.main()
