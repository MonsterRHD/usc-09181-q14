"""贸易发票融资台账 HTTP 入口（标准库，零第三方依赖）。

路由
----
GET  /health                  健康检查
POST /events                  提交一条业务指令（发票/装运/受让/回款/退货/冻结…）
POST /events/batch            断网补传：一批指令逐条幂等受理
GET  /invoices                台账全量快照
GET  /invoices/{id}           单票台账（含可融资余额依据 basis）
POST /admin/rebuild           服务恢复：从原始流水完整重算
GET  /admin/verify            校验哈希链（原始凭证是否被改动）

数据文件由环境变量 LEDGER_PATH 指定，默认 data/ledger.jsonl。
"""
from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .service import CommandError, LedgerService
from .store import CorruptLogError, WalStore

_LEDGER_PATH = os.getenv("LEDGER_PATH", "data/ledger.jsonl")
_service: LedgerService | None = None
_service_lock = threading.Lock()


def get_service() -> LedgerService:
    global _service
    if _service is None:
        with _service_lock:
            if _service is None:
                _service = LedgerService(WalStore(_LEDGER_PATH))
    return _service


class Handler(BaseHTTPRequestHandler):
    server_version = "InvoiceLedger/1.0"

    def do_GET(self):
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            if path == "/health":
                self._write(200, {"status": "ok"})
            elif path == "/invoices":
                self._write(200, {"invoices": get_service().all_invoices()})
            elif path.startswith("/invoices/"):
                invoice_id = path[len("/invoices/"):]
                self._write(200, get_service().invoice(invoice_id))
            elif path == "/admin/verify":
                self._write(200, get_service().verify_chain())
            else:
                self._write(404, {"code": "NOT_FOUND", "message": path})
        except CommandError as exc:
            self._write(exc.http_status, exc.to_dict())
        except CorruptLogError as exc:
            self._write(500, {"code": "CORRUPT_LOG", "message": str(exc)})
        except Exception as exc:  # noqa: BLE001 - 边界统一兜底
            self._write(500, {"code": "INTERNAL", "message": str(exc)})

    def do_POST(self):
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            body = self._read_json()
            svc = get_service()
            if path == "/events":
                result = svc.submit(body)
                status = 200 if result.get("accepted") else result.get(
                    "http_status", 422)
                self._write(status, result)
            elif path == "/events/batch":
                if not isinstance(body, list):
                    raise CommandError("BAD_REQUEST", "批量接口要求 JSON 数组",
                                       http_status=400)
                self._write(200, svc.submit_batch(body))
            elif path == "/admin/rebuild":
                self._write(200, svc.rebuild())
            else:
                self._write(404, {"code": "NOT_FOUND", "message": path})
        except CommandError as exc:
            self._write(exc.http_status, exc.to_dict())
        except CorruptLogError as exc:
            self._write(500, {"code": "CORRUPT_LOG", "message": str(exc)})
        except json.JSONDecodeError:
            self._write(400, {"code": "BAD_JSON", "message": "请求体不是合法 JSON"})
        except Exception as exc:  # noqa: BLE001
            self._write(500, {"code": "INTERNAL", "message": str(exc)})

    # ---- 工具 -----------------------------------------------------------
    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            raise CommandError("BAD_REQUEST", "缺少请求体", http_status=400)
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _write(self, status: int, payload) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):  # 静默默认访问日志
        pass


def run():  # 供 `service` 控制台脚本与 python -m service.main 使用
    port = int(os.getenv("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    actual_port = server.server_address[1]
    print(f"LISTENING {actual_port}", flush=True)
    print(f"贸易发票融资台账已启动: 0.0.0.0:{actual_port}, 流水文件={_LEDGER_PATH}",
          flush=True)
    server.serve_forever()


if __name__ == "__main__":
    run()
