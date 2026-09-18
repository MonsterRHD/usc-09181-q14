"""贸易发票融资台账 HTTP 服务。

端点
----
GET  /health                  健康检查
POST /events                  提交命令（发票/装运/承运人回执/受让通知/拆分/
                              放款/回款/退货/冻结/解冻/汇率）。请求体即命令，
                              角色取 X-Role 头（或 body.role）。
GET  /invoices/<ref>          台账视图：可融资余额、已放款、追索、冻结（实时重算）
GET  /invoices/<ref>/journal  该发票的不可变流水
GET  /journal                 全部流水

持久化：LEDGER_PATH 环境变量指定 JSONL（默认 data/ledger.jsonl）。
服务重启即重放流水恢复全部状态——恢复运行后补传同一 event_id 仍幂等。
"""
from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

from .ledger import Ledger
from .models import Command, LedgerError

LEDGER_PATH = os.getenv("LEDGER_PATH", "data/ledger.jsonl")

_REJECT_STATUS = {
    "FORBIDDEN": 403,
    "INVOICE_NOT_FOUND": 404,
    "BAD_REQUEST": 400,
    "INVOICE_EXISTS": 409,
    "NOTICE_DUPLICATE": 409,
    "FX_RATE_MISSING": 400,
    "UNKNOWN_COMMAND": 400,
    "LIMIT_EXCEEDED": 409,
    "DUPLICATE_ASSIGNMENT": 409,
    "INVOICE_FROZEN": 409,
    "NOTICE_MISSING": 409,
    "SPLIT_EXCEEDED": 409,
    "REPAYMENT_EXCEEDED": 409,
    "ALREADY_FROZEN": 409,
    "NOT_FROZEN": 409,
}


class Handler(BaseHTTPRequestHandler):
    ledger: Ledger  # 由 run() 注入到类属性

    # ------------------------------------------------------------ 工具
    def _send(self, status: int, obj: dict | list) -> None:
        body = json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            raise LedgerError("BAD_REQUEST", "缺少请求体")
        try:
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise LedgerError("BAD_REQUEST", f"请求体不是合法 JSON: {exc}")
        if not isinstance(body, dict):
            raise LedgerError("BAD_REQUEST", "请求体必须是 JSON 对象")
        return body

    # ------------------------------------------------------------ 路由
    def do_GET(self) -> None:
        path = self.path.rstrip("/")
        if path == "/health":
            self._send(200, {"status": "ok"})
            return
        if path == "/journal":
            self._send(200, self.ledger.journal())
            return
        if path.startswith("/invoices/"):
            rest = unquote(path[len("/invoices/"):])
            if rest.endswith("/journal"):
                ref = rest[: -len("/journal")]
                self._send(200, self.ledger.journal(ref))
                return
            try:
                self._send(200, self.ledger.statement(rest))
            except LedgerError as err:
                self._send(_REJECT_STATUS.get(err.code, 400), err.to_dict())
            return
        self._send(404, {"code": "NOT_FOUND", "reason": self.path})

    def do_POST(self) -> None:
        if self.path.rstrip("/") != "/events":
            self._send(404, {"code": "NOT_FOUND", "reason": self.path})
            return
        try:
            body = self._read_body()
            event_id = body.get("event_id")
            if not event_id:
                raise LedgerError("BAD_REQUEST", "缺少 event_id（唯一业务标识）")
            payload = body.get("payload")
            if not isinstance(payload, dict) or "type" not in payload:
                raise LedgerError("BAD_REQUEST", "payload 必须是含 type 的对象")
            role = self.headers.get("X-Role") or body.get("role")
            if not role:
                raise LedgerError("FORBIDDEN", "缺少角色（X-Role 头或 body.role）")
            cmd = Command(
                event_id=str(event_id), role=role, payload=payload,
                occurred_at=body.get("occurred_at"),
                idempotency_key=body.get("idempotency_key"),
            )
            replay = self.ledger.store.lookup(str(event_id), body.get("idempotency_key")) is not None
            evt = self.ledger.handle(cmd)
            resp = {"receipt": evt.to_dict(), "idempotent_replay": replay}
            status = 200 if evt.accepted else _REJECT_STATUS.get(evt.reject["code"], 400)
            self._send(status, resp)
        except LedgerError as err:
            self._send(_REJECT_STATUS.get(err.code, 400), err.to_dict())

    def log_message(self, *_args) -> None:  # 静默默认访问日志
        pass


def run() -> None:
    from .store import EventStore

    store = EventStore(LEDGER_PATH)
    Handler.ledger = Ledger(store)
    port = int(os.getenv("PORT", "8000"))
    print(f"ledger listening on 0.0.0.0:{port} store={LEDGER_PATH} events={len(store.all_events())}")
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    run()
