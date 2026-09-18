"""HTTP 端到端测试：含双进程共享同一份流水的并发演练与恢复演练。"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
import urllib.error
import warnings
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer

warnings.simplefilter("ignore", ResourceWarning)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def http(method, url, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def wait_healthy(url, tries=50):
    for _ in range(tries):
        try:
            status, _ = http("GET", url)
            if status == 200:
                return
        except Exception:  # noqa: BLE001
            time.sleep(0.1)
    raise RuntimeError(f"服务未就绪: {url}")


class HttpApiTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.ledger_path = os.path.join(self.tmpdir, "ledger.jsonl")
        os.environ["LEDGER_PATH"] = self.ledger_path
        os.environ["PORT"] = "0"
        # 重新导入以读取环境变量
        import importlib
        import service.main as main
        importlib.reload(main)
        self.main = main
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), main.Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.port}"
        wait_healthy(f"{self.base}/health")

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def cmd(self, **kwargs):
        return http("POST", f"{self.base}/events", kwargs)

    def setup_invoice(self, iid="INV-1"):
        events = [
            {"event_id": f"reg-{iid}", "type": "INVOICE_REGISTERED",
             "actor": "u", "role": "ops", "invoice_id": iid,
             "currency": "CNY", "amount": "100000"},
            {"event_id": f"ship-{iid}", "type": "SHIPMENT_RECORDED",
             "actor": "u", "role": "ops", "invoice_id": iid},
            {"event_id": f"rcpt-{iid}", "type": "CARRIER_RECEIPT_RECORDED",
             "actor": "u", "role": "ops", "invoice_id": iid,
             "late": True,
             "impact": "补单后可融资余额恢复为 80000.00 CNY"},
            {"event_id": f"asn-{iid}", "type": "ASSIGNMENT_NOTIFIED",
             "actor": "u", "role": "ops", "invoice_id": iid,
             "assignee": "FACTOR-A"},
        ]
        for e in events:
            status, body = http("POST", f"{self.base}/events", e)
            self.assertEqual(status, 200, body)

    def test_health_and_full_drill(self):
        self.setup_invoice()
        status, snap = http("GET", f"{self.base}/invoices/INV-1")
        self.assertEqual(status, 200)
        self.assertEqual(snap["available"], "80000.00")
        self.assertTrue(snap["annotations"][0]["late"])

        # 超额放款被拒，HTTP 422 + 依据
        status, body = self.cmd(event_id="adv-big",
                                type="FINANCING_APPROVED", actor="fm",
                                role="finance_manager", invoice_id="INV-1",
                                amount="80001")
        self.assertEqual(status, 422)
        self.assertEqual(body["code"], "OVER_LIMIT")
        self.assertTrue(any("融资上限" in b for b in body["basis"]))

        # 重复质押：同票再通知给银行
        status, body = self.cmd(event_id="asn-bank",
                                type="ASSIGNMENT_NOTIFIED", actor="u",
                                role="ops", invoice_id="INV-1",
                                assignee="BANK-B")
        self.assertEqual(status, 422)
        self.assertEqual(body["code"], "DUPLICATE_PLEDGE")

        # 正常放款 + 幂等重放
        status, first = self.cmd(event_id="adv-1",
                                 type="FINANCING_APPROVED", actor="fm",
                                 role="finance_manager", invoice_id="INV-1",
                                 amount="80000")
        self.assertEqual(status, 200)
        status, again = self.cmd(event_id="adv-1",
                                 type="FINANCING_APPROVED", actor="fm",
                                 role="finance_manager", invoice_id="INV-1",
                                 amount="80000")
        self.assertEqual(status, 200)
        self.assertTrue(again["duplicate"])
        self.assertEqual(first["seq"], again["seq"])

    def test_batch_backfill_and_rebuild(self):
        self.setup_invoice("INV-2")
        batch = [
            {"event_id": "b-1", "type": "FINANCING_APPROVED", "actor": "fm",
             "role": "finance_manager", "invoice_id": "INV-2",
             "amount": "40000"},
            {"event_id": "b-2", "type": "REPAYMENT_RECORDED", "actor": "u",
             "role": "ops", "invoice_id": "INV-2", "amount": "10000"},
        ]
        status, body = http("POST", f"{self.base}/events/batch", batch)
        self.assertEqual(status, 200)
        # 断网后重复补传整批：全部去重
        status, body = http("POST", f"{self.base}/events/batch", batch)
        self.assertTrue(all(r["duplicate"] for r in body["results"]))

        status, snap = http("GET", f"{self.base}/invoices/INV-2")
        self.assertEqual(snap["outstanding"], "30000.00")

        # 服务恢复：完整重算
        status, info = http("POST", f"{self.base}/admin/rebuild", {})
        self.assertEqual(status, 200)
        self.assertGreaterEqual(info["last_seq"], 6)
        status, verify = http("GET", f"{self.base}/admin/verify")
        self.assertEqual(verify["ok"], True)

    def test_concurrent_http_claims_single_winner(self):
        self.setup_invoice("INV-3")

        def claim(i):
            return self.cmd(event_id=f"adv-c{i}",
                            type="FINANCING_APPROVED", actor="fm",
                            role="finance_manager", invoice_id="INV-3",
                            amount="80000")

        with ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(claim, range(10)))
        winners = [r for s, r in results if s == 200]
        losers = [r for s, r in results if s == 422]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 9)
        self.assertTrue(all(r["code"] == "OVER_LIMIT" for r in losers))

    def test_freeze_permissions_over_http(self):
        self.setup_invoice("INV-4")
        status, body = self.cmd(event_id="fz", type="FREEZE_APPLIED",
                                actor="fm", role="finance_manager",
                                invoice_id="INV-4", reason="争议",
                                confirmed=True)
        self.assertEqual(status, 403)
        self.assertEqual(body["code"], "FORBIDDEN")
        status, body = self.cmd(event_id="fz2", type="FREEZE_APPLIED",
                                actor="rm", role="risk_manager",
                                invoice_id="INV-4", reason="争议")
        self.assertEqual(status, 422)
        self.assertEqual(body["code"], "CONFIRMATION_REQUIRED")


def start_server_process(path):
    """以 PORT=0 启动子进程，解析其 stdout 上的 LISTENING 握手端口。"""
    env = dict(os.environ, LEDGER_PATH=path, PORT="0", PYTHONPATH=ROOT)
    proc = subprocess.Popen(
        [sys.executable, "-u", "-m", "service.main"],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    port = None
    for _ in range(100):
        line = proc.stdout.readline().decode().strip()
        if line.startswith("LISTENING "):
            port = int(line.split()[1])
            break
        if not line:
            time.sleep(0.05)
    if port is None:
        proc.kill()
        err = proc.stderr.read().decode()
        raise RuntimeError(f"子进程未报告监听端口: {err}")
    return proc, port


class CrossProcessConcurrencyTest(unittest.TestCase):
    """两个服务进程挂同一份流水文件：fcntl 锁 + 追平重放演练。"""

    def test_two_processes_one_winner(self):
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, "ledger.jsonl")
        p1, port_a = start_server_process(path)
        p2, port_b = start_server_process(path)
        self.addCleanup(self._kill, p1)
        self.addCleanup(self._kill, p2)
        base_a = f"http://127.0.0.1:{port_a}"
        base_b = f"http://127.0.0.1:{port_b}"
        wait_healthy(f"{base_a}/health")
        wait_healthy(f"{base_b}/health")

        # 通过 A 建票并齐套
        for e in [
            {"event_id": "reg", "type": "INVOICE_REGISTERED", "actor": "u",
             "role": "ops", "invoice_id": "INV-P", "currency": "CNY",
             "amount": "100000"},
            {"event_id": "ship", "type": "SHIPMENT_RECORDED", "actor": "u",
             "role": "ops", "invoice_id": "INV-P"},
            {"event_id": "rcpt", "type": "CARRIER_RECEIPT_RECORDED",
             "actor": "u", "role": "ops", "invoice_id": "INV-P"},
            {"event_id": "asn", "type": "ASSIGNMENT_NOTIFIED", "actor": "u",
             "role": "ops", "invoice_id": "INV-P", "assignee": "FACTOR-A"},
        ]:
            status, body = http("POST", f"{base_a}/events", e)
            self.assertEqual(status, 200, body)

        # B 进程先追平 A 写入的流水
        status, info = http("POST", f"{base_b}/admin/rebuild", {})
        self.assertEqual(status, 200, info)

        def claim(port_tag):
            port, tag = port_tag
            return http("POST", f"http://127.0.0.1:{port}/events",
                        {"event_id": f"adv-{tag}",
                         "type": "FINANCING_APPROVED", "actor": "fm",
                         "role": "finance_manager", "invoice_id": "INV-P",
                         "amount": "80000"})

        targets = [(port_a, f"a{i}") for i in range(4)] + \
                  [(port_b, f"b{i}") for i in range(4)]
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(claim, targets))
        winners = [body for status, body in results if status == 200]
        losers = [body for status, body in results if status == 422]
        self.assertEqual(len(winners), 1, results)
        self.assertTrue(all(b["code"] == "OVER_LIMIT" for b in losers), results)

        # 两进程各自重放后视图一致
        status, snap_a = http("GET", f"{base_a}/invoices/INV-P")
        http("POST", f"{base_b}/admin/rebuild", {})
        status, snap_b = http("GET", f"{base_b}/invoices/INV-P")
        self.assertEqual(snap_a["outstanding"], "80000.00")
        self.assertEqual(snap_b["outstanding"], "80000.00")
        status, verify = http("GET", f"{base_a}/admin/verify")
        self.assertEqual(verify["ok"], True)

    @staticmethod
    def _kill(proc):
        if proc is None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    unittest.main()
