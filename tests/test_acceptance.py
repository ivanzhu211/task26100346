"""验收一：高故障率 + 拉长生效延迟下，200个执行经受30+次杀进程重启后，
仍满足"成功步骤恰好一张生效凭证、失败步骤一张都没有、引用链同属一个执行、
SAP每次提交都有平台尝试记录"。
"""
import os
import re
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXECUTIONS = 200
MIN_KILLS = 30
OVERALL_TIMEOUT = 900  # 秒


def connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


class TestAcceptance(unittest.TestCase):
    def test_survives_repeated_kills(self):
        workdir = tempfile.mkdtemp()
        sap_db = os.path.join(workdir, "sap.db")
        engine_db = os.path.join(workdir, "engine.db")
        cmd = [
            sys.executable, "-m", "tests.acceptance_runner",
            "--sap-db", sap_db, "--engine-db", engine_db,
            "--executions", str(EXECUTIONS),
        ]
        env = dict(os.environ, PYTHONPATH=REPO_ROOT)

        kills = 0
        deadline = time.time() + OVERALL_TIMEOUT
        while True:
            self.assertLess(time.time(), deadline, "整体超时，执行未全部到达终态")
            proc = subprocess.Popen(
                cmd, cwd=REPO_ROOT, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            if kills < MIN_KILLS:
                # 运行一小段时间后 SIGKILL，模拟引擎随时被杀
                run_for = 1.0 + (kills % 5) * 0.4
                wait_until = time.time() + run_for
                while time.time() < wait_until and proc.poll() is None:
                    time.sleep(0.1)
                if proc.poll() is None:
                    proc.send_signal(signal.SIGKILL)
                    kills += 1
                out, _ = proc.communicate(timeout=60)
            else:
                # 杀够次数后不再杀，让引擎跑到全部终态
                try:
                    out, _ = proc.communicate(timeout=max(deadline - time.time(), 1))
                except subprocess.TimeoutExpired:
                    proc.send_signal(signal.SIGKILL)
                    proc.communicate()
                    self.fail("整体超时，执行未全部到达终态")
            if "ALL_TERMINAL" in (out or ""):
                break

        self.assertGreaterEqual(kills, MIN_KILLS)
        print(f"\n共杀进程 {kills} 次")
        # 等SAP里所有pending凭证落定（生效窗口8秒 + 余量）
        time.sleep(10)
        self._verify(sap_db, engine_db)

    @staticmethod
    def _all_terminal(engine_db):
        if not os.path.exists(engine_db):
            return False
        try:
            conn = connect(engine_db)
            row = conn.execute(
                "SELECT COUNT(*) c, SUM(status = 'running') r FROM executions"
            ).fetchone()
            conn.close()
            return row["c"] == EXECUTIONS and (row["r"] or 0) == 0
        except sqlite3.Error:
            return False

    def _verify(self, sap_db, engine_db):
        eng = connect(engine_db)
        sap = connect(sap_db)

        executions = eng.execute("SELECT * FROM executions").fetchall()
        self.assertEqual(len(executions), EXECUTIONS)
        statuses = {e["status"] for e in executions}
        self.assertTrue(statuses <= {"completed", "failed"})

        steps = eng.execute("SELECT * FROM steps").fetchall()
        attempts = eng.execute("SELECT * FROM attempts").fetchall()
        docs = sap.execute("SELECT * FROM documents").fetchall()

        # SAP里不应残留pending凭证（受理后120秒窗口内必须落定）
        pending = [d for d in docs if d["status"] == "pending"]
        self.assertEqual(pending, [])

        effective_by_ref = {}
        for d in docs:
            if d["status"] == "effective":
                effective_by_ref.setdefault(d["reference"], []).append(d)

        step_by_key = {(s["execution_id"], s["step_idx"]): s for s in steps}
        doc_by_no = {d["doc_no"]: d for d in docs}

        # 1) 每个成功步骤恰好一张生效凭证；失败/未执行步骤一张都没有
        for s in steps:
            ref = s["reference"]
            effective = effective_by_ref.get(ref, [])
            if s["status"] == "done":
                self.assertEqual(
                    len(effective), 1,
                    f"步骤 {ref} 完成但生效凭证数为 {len(effective)}",
                )
                self.assertEqual(effective[0]["doc_no"], s["doc_no"])
            else:
                self.assertEqual(
                    len(effective), 0,
                    f"步骤 {ref} 未成功但存在生效凭证",
                )

        # 2) 完成的执行三步全done；失败的执行其失败步骤之后没有过账
        for e in executions:
            esteps = sorted(
                (s for s in steps if s["execution_id"] == e["id"]),
                key=lambda s: s["step_idx"],
            )
            if e["status"] == "completed":
                self.assertTrue(all(s["status"] == "done" for s in esteps))
            else:
                first_not_done = next(
                    (s["step_idx"] for s in esteps if s["status"] != "done"), None
                )
                self.assertIsNotNone(first_not_done)
                for s in esteps:
                    if s["step_idx"] >= first_not_done:
                        self.assertEqual(
                            len(effective_by_ref.get(s["reference"], [])), 0
                        )

        # 3) 引用链同属一个执行：收货引用的订单、发票引用的收货
        for s in steps:
            if s["status"] != "done":
                continue
            doc = doc_by_no[s["doc_no"]]
            eid = s["execution_id"]
            if doc["doc_type"] == "GR":
                order_step = step_by_key[(eid, 0)]
                self.assertEqual(doc["order_no"], order_step["doc_no"])
                self.assertEqual(order_step["status"], "done")
            elif doc["doc_type"] == "INV":
                receipt_step = step_by_key[(eid, 1)]
                self.assertEqual(doc["receipt_no"], receipt_step["doc_no"])
                self.assertEqual(receipt_step["status"], "done")

        # 4) SAP收到的每次提交都能在平台尝试记录里找到
        attempt_keys = {(a["execution_id"], a["step_idx"]) for a in attempts}
        for d in docs:
            m = re.fullmatch(r"E(\d+)S(\d+)", d["reference"])
            self.assertIsNotNone(m, f"SAP凭证 {d['doc_no']} 参考字段异常")
            key = (int(m.group(1)), int(m.group(2)))
            self.assertIn(key, attempt_keys, f"SAP凭证 {d['doc_no']} 无对应尝试记录")

        # 5) 尝试记录完整：提交内容、结果、耗时齐全
        for a in attempts:
            self.assertIsNotNone(a["posting"])
            self.assertIsNotNone(a["result"])
            if a["result"] != "interrupted":
                self.assertIsNotNone(a["duration"])

        eng.close()
        sap.close()
        completed = sum(1 for e in executions if e["status"] == "completed")
        print(f"完成 {completed}/{EXECUTIONS}，SAP凭证 {len(docs)} 张，"
              f"尝试 {len(attempts)} 次")


if __name__ == "__main__":
    unittest.main()
