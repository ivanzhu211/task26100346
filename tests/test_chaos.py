"""验收一：高故障率 + 反复杀进程重启下的恰好一次语义。

调高超时、更新中止、锁冲突概率，拉长生效延迟，跑 200 个
“建单、收货、开票”执行，期间 SIGKILL 引擎进程再重启至少 30 次。
结束后逐条核对 SAP 数据。
"""
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rpa_engine import (
    Engine, EngineConfig, MockSAP, SAPConfig, Workflow, Step, PostingType,
)

N_EXECUTIONS = 200
MIN_KILLS = 30

SAP_CONFIG = {
    "lock_conflict_prob": 0.2,
    "timeout_prob": 0.2,
    "timeout_accepted_prob": 0.5,
    "abort_prob": 0.2,
    "min_effect_delay": 0.5,
    "max_effect_delay": 3.0,
    "effect_deadline": 4.0,
}
ENGINE_CONFIG = {
    "backoffs": [0.2, 0.4, 0.8],
    "effect_deadline": 4.0,
    "poll_interval": 0.2,
}


class ChaosTest(unittest.TestCase):
    def test_kill_and_resume(self):
        d = tempfile.mkdtemp()
        engine_db = os.path.join(d, "engine.db")
        sap_db = os.path.join(d, "sap.db")

        # 登记 200 个“建单、收货、开票”执行
        sap = MockSAP(sap_db, SAPConfig(**SAP_CONFIG), seed=1)
        eng = Engine(engine_db, sap, EngineConfig(
            backoffs=tuple(ENGINE_CONFIG["backoffs"]),
            effect_deadline=ENGINE_CONFIG["effect_deadline"],
            poll_interval=ENGINE_CONFIG["poll_interval"],
        ))
        for _ in range(N_EXECUTIONS):
            eng.submit_execution(Workflow(steps=(
                Step(PostingType.CREATE_ORDER, quantity=10),
                Step(PostingType.GOODS_RECEIPT, quantity=10, order_step=0),
                Step(PostingType.INVOICE, quantity=10, receipt_step=1),
            )))
        eng.close()
        sap.close()

        def all_finished():
            conn = sqlite3.connect(engine_db)
            n = conn.execute(
                "SELECT COUNT(*) FROM executions WHERE status = 'running'"
            ).fetchone()[0]
            conn.close()
            return n == 0

        # 阶段一：反复启动-杀掉引擎进程，至少 MIN_KILLS 次
        kills = 0
        start = time.time()
        rng = __import__("random").Random(7)

        def spawn():
            nonlocal kills
            proc = subprocess.Popen([
                sys.executable, "-m", "rpa_engine.runner",
                "--engine-db", engine_db, "--sap-db", sap_db,
                "--sap-config", json.dumps(SAP_CONFIG),
                "--engine-config", json.dumps(ENGINE_CONFIG),
                "--seed", str(kills + 2),
            ], cwd=os.path.join(os.path.dirname(__file__), ".."))
            return proc

        while kills < MIN_KILLS and not all_finished():
            proc = spawn()
            time.sleep(rng.uniform(0.5, 1.2))
            if proc.poll() is None:
                proc.send_signal(signal.SIGKILL)
                kills += 1
            proc.wait()
            if kills % 20 == 0:
                remaining = sqlite3.connect(engine_db).execute(
                    "SELECT COUNT(*) FROM executions WHERE status='running'"
                ).fetchone()[0]
                print(f"kills={kills} remaining={remaining} "
                      f"elapsed={time.time()-start:.0f}s", flush=True)
        self.assertGreaterEqual(kills, MIN_KILLS, f"只杀了 {kills} 次")

        # 阶段二：不再杀进程，重启直到全部完结
        while not all_finished():
            proc = spawn()
            proc.wait(timeout=300)
            self.assertLess(time.time() - start, 900, "整体超时")

        self._verify(engine_db, sap_db)

    def _verify(self, engine_db, sap_db):
        eng_conn = sqlite3.connect(engine_db)
        eng_conn.row_factory = sqlite3.Row
        sap_conn = sqlite3.connect(sap_db)
        sap_conn.row_factory = sqlite3.Row
        sap_conn.execute(
            "UPDATE documents SET status = CASE will_abort WHEN 1 THEN 'aborted'"
            " ELSE 'effective' END WHERE status = 'pending' AND effective_at <= ?",
            (time.time(),))

        executions = eng_conn.execute("SELECT * FROM executions").fetchall()
        self.assertEqual(len(executions), N_EXECUTIONS)
        n_completed = 0
        for execution in executions:
            # 1) 每个执行都处于完成或失败状态
            self.assertIn(execution["status"], ("completed", "failed"))
            if execution["status"] == "completed":
                n_completed += 1
            steps = eng_conn.execute(
                "SELECT * FROM steps WHERE execution_id = ? ORDER BY step_idx",
                (execution["id"],)).fetchall()
            doc_nos = {}
            for step in steps:
                ref = step["reference"]
                effective = sap_conn.execute(
                    "SELECT * FROM documents WHERE reference = ?"
                    " AND status = 'effective'", (ref,)).fetchall()
                if step["status"] == "done":
                    # 2) 成功步骤在 SAP 里恰好一张生效凭证，且凭证号一致
                    self.assertEqual(len(effective), 1, f"ref={ref}")
                    self.assertEqual(str(effective[0]["doc_no"]), step["doc_no"])
                    doc_nos[step["step_idx"]] = effective[0]["doc_no"]
                else:
                    # 2) 未成功的步骤一张生效凭证都没有
                    self.assertEqual(len(effective), 0, f"ref={ref}")
            # 3) 发票的收货、收货的订单都属于同一个执行
            if 1 in doc_nos:
                gr = sap_conn.execute(
                    "SELECT * FROM documents WHERE doc_no = ?",
                    (doc_nos[1],)).fetchone()
                self.assertIn(0, doc_nos)
                self.assertEqual(gr["order_no"], doc_nos[0])
            if 2 in doc_nos:
                inv = sap_conn.execute(
                    "SELECT * FROM documents WHERE doc_no = ?",
                    (doc_nos[2],)).fetchone()
                self.assertIn(1, doc_nos)
                self.assertEqual(inv["receipt_no"], doc_nos[1])

        # 4) SAP 收到的每一次提交都能在尝试记录里找到
        refs = {r["reference"] for r in eng_conn.execute(
            "SELECT reference FROM steps")}
        for doc in sap_conn.execute("SELECT * FROM documents").fetchall():
            self.assertIn(doc["reference"], refs)
        for row in eng_conn.execute(
                "SELECT reference, COUNT(*) c FROM steps GROUP BY reference"):
            sap_docs = sap_conn.execute(
                "SELECT COUNT(*) FROM documents WHERE reference = ?",
                (row["reference"],)).fetchone()[0]
            attempts = eng_conn.execute(
                "SELECT COUNT(*) FROM attempts a JOIN steps s"
                " ON s.execution_id = a.execution_id AND s.step_idx = a.step_idx"
                " WHERE s.reference = ?", (row["reference"],)).fetchone()[0]
            # 每次被 SAP 受理的提交都对应一次尝试记录
            self.assertLessEqual(sap_docs, attempts)
        # 没有悬空的尝试记录
        pending = eng_conn.execute(
            "SELECT COUNT(*) FROM attempts WHERE result = 'pending'").fetchone()[0]
        self.assertEqual(pending, 0)
        print(f"\nchaos ok: {n_completed}/{N_EXECUTIONS} completed")
        eng_conn.close()
        sap_conn.close()


if __name__ == "__main__":
    unittest.main()
