"""应用服务：受理指令 -> 校验当前台账 -> 追加不可变事件。

并发安全：指令的“校验 + 追加”在 WAL 跨进程文件锁内完成，执行前先追平
其他进程/断网补传写入的事件，因此并发提交同票融资时只有一个能成功。
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from decimal import Decimal

from .domain import (
    LedgerProjection, Rejection, money, rate, REQUIRED_DOCS, ZERO,
)
from .store import WalStore, CorruptLogError

ROLE_RISK = "risk_manager"          # 有权冻结/解除冻结
ROLE_FINANCE = "finance_manager"    # 有权放款
ROLE_OPS = "ops"                     # 凭证登记

# 各指令允许的角色
PERMISSIONS = {
    "INVOICE_REGISTERED": {ROLE_OPS, ROLE_FINANCE},
    "SHIPMENT_RECORDED": {ROLE_OPS},
    "CARRIER_RECEIPT_RECORDED": {ROLE_OPS},
    "ASSIGNMENT_NOTIFIED": {ROLE_OPS, ROLE_FINANCE},
    "FX_RATE_REGISTERED": {"treasury", ROLE_FINANCE},
    "INVOICE_SPLIT": {ROLE_FINANCE},
    "FINANCING_APPROVED": {ROLE_FINANCE},
    "REPAYMENT_RECORDED": {ROLE_OPS, ROLE_FINANCE},
    "RETURN_RECORDED": {ROLE_OPS, ROLE_FINANCE},
    "FREEZE_APPLIED": {ROLE_RISK},
    "FREEZE_RELEASED": {ROLE_RISK},
}


class CommandError(Exception):
    def __init__(self, code: str, message: str, basis=None, http_status: int = 422):
        super().__init__(message)
        self.code = code
        self.message = message
        self.basis = basis or []
        self.http_status = http_status

    def to_dict(self):
        return {"code": self.code, "message": self.message, "basis": self.basis}


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class LedgerService:
    def __init__(self, store: WalStore):
        self.store = store
        self.projection = LedgerProjection()
        # flock 只在“打开文件描述”之间互斥，同一进程的线程共享该描述，
        # 因此“校验+追加”还需要进程内可重入锁串行化。
        self._gate = threading.RLock()
        self._records: dict[int, dict] = {}
        self._rebuild_locked()

    # ---- 重放与恢复 -----------------------------------------------------
    def _rebuild_locked(self):
        proj = LedgerProjection()
        records: dict[int, dict] = {}
        with self.store._file_lock():
            for rec in self.store._read_all_unlocked(verify=True):
                proj.fold_event(rec)
                records[rec["seq"]] = rec
        self.projection = proj
        self._records = records

    def rebuild(self) -> dict:
        """服务恢复运行后从原始流水完整重算，并校验哈希链。"""
        with self._gate:
            self._rebuild_locked()
        return {"rebuilt": True, "last_seq": self.projection.last_seq,
                "invoices": len(self.projection.invoices)}

    def verify_chain(self) -> dict:
        with self._gate, self.store._file_lock():
            count = len(self.store._read_all_unlocked(verify=True))
        return {"ok": True, "records": count, "last_seq": self.projection.last_seq}

    def _catch_up_locked(self):
        """追平其他进程在本实例快照之后追加的事件。"""
        for rec in self.store._read_all_unlocked(verify=False):
            if rec["seq"] > self.projection.last_seq:
                self.projection.fold_event(rec)
                self._records[rec["seq"]] = rec

    # ---- 指令入口 -------------------------------------------------------
    def submit(self, command: dict) -> dict:
        self._check_command_shape(command)
        event_id = command["event_id"]
        with self._gate, self.store._file_lock():
            self._catch_up_locked()

            # 幂等：同一业务标识此前已处理过，直接回放原结果，绝不重复入账
            prior_seq = self.projection.event_id_to_seq.get(event_id)
            if prior_seq is not None:
                return self._replay_result(prior_seq)

            try:
                event = self._validate(command)
            except CommandError as exc:
                stored = self.store._append_unlocked({
                    "event_id": event_id,
                    "type": "REJECTED",
                    "actor": command.get("actor"),
                    "role": command.get("role"),
                    "recorded_at": utcnow(),
                    "payload": {
                        "command_type": command["type"],
                        "invoice_id": command.get("invoice_id"),
                        "reason_code": exc.code,
                        "http_status": exc.http_status,
                        "message": exc.message,
                        "basis": exc.basis,
                    },
                })
                self._records[stored["seq"]] = stored
                self.projection.fold_event(stored)
                return {"accepted": False, "duplicate": False, "code": exc.code,
                        "message": exc.message, "basis": exc.basis,
                        "http_status": exc.http_status,
                        "seq": stored["seq"], "hash": stored["hash"]}

            stored = self.store._append_unlocked(event)
            self._records[stored["seq"]] = stored
            self.projection.fold_event(stored)
            return {"accepted": True, "duplicate": False,
                    "seq": stored["seq"], "hash": stored["hash"],
                    "type": stored["type"]}

    def submit_batch(self, commands: list[dict]) -> dict:
        """断网补传：逐条幂等受理，整体保持同一批输入可安全重放。"""
        results = [self.submit(cmd) for cmd in commands]
        return {"count": len(results), "results": results}

    # ---- 各指令校验 -----------------------------------------------------
    def _validate(self, cmd: dict) -> dict:
        etype = cmd["type"]
        allowed = PERMISSIONS.get(etype)
        if allowed is None:
            raise CommandError("UNKNOWN_COMMAND", f"未知指令类型: {etype}")
        if cmd.get("role") not in allowed:
            raise CommandError(
                "FORBIDDEN",
                f"角色 {cmd.get('role')} 无权执行 {etype}，需要 {sorted(allowed)}",
                http_status=403)

        payload = {k: v for k, v in cmd.items()
                   if k not in ("event_id", "type", "actor", "role", "recorded_at")}
        p = self.projection
        iid = cmd.get("invoice_id")

        if etype == "FX_RATE_REGISTERED":
            self._require(payload, "currency", "rate_to_cny", "as_of")
            r = rate(payload["rate_to_cny"])
            if r <= 0:
                raise CommandError("INVALID_AMOUNT", "汇率必须为正数")
            return self._event(cmd, etype, {
                "currency": payload["currency"], "rate_to_cny": str(r),
                "as_of": payload["as_of"]})

        inv = p.invoices.get(iid) if iid else None
        if etype != "INVOICE_REGISTERED" and inv is None:
            raise CommandError("INVOICE_NOT_FOUND",
                               f"发票 {iid} 尚未登记，无法应用 {etype}")

        if etype == "INVOICE_REGISTERED":
            self._require(cmd, "invoice_id", "currency", "amount")
            if iid in p.invoices:
                raise CommandError(
                    "DUPLICATE_INVOICE", f"发票 {iid} 已登记，禁止重复建账",
                    p.invoices[iid].basis())
            amt = money(cmd["amount"])
            if amt <= 0:
                raise CommandError("INVALID_AMOUNT", "发票金额必须为正数")
            ar = rate(cmd.get("advance_rate", "0.8"))
            if not ZERO < ar <= 1:
                raise CommandError("INVALID_RATE", "融资比例须在 (0,1] 区间")
            return self._event(cmd, etype, {
                "invoice_id": iid, "currency": cmd["currency"],
                "amount": str(amt), "advance_rate": str(ar)})

        if etype == "INVOICE_SPLIT":
            return self._validate_split(cmd, inv)

        if etype in ("SHIPMENT_RECORDED", "CARRIER_RECEIPT_RECORDED"):
            doc = "shipment" if etype == "SHIPMENT_RECORDED" else "carrier_receipt"
            payload_out = {"invoice_id": iid, "doc_type": doc}
            for key in ("recorded_at", "expected_by", "late", "note", "impact"):
                if key in cmd:
                    payload_out[key] = cmd[key]
            if cmd.get("late") and "recorded_at" not in payload_out:
                payload_out["recorded_at"] = utcnow()
            if doc in inv.docs and not cmd.get("late"):
                # 同一张凭证重复提交：属于重复通知，拒绝并留痕
                raise CommandError(
                    "DUPLICATE_DOC", f"{doc} 凭证此前已提交（seq={inv.last_seq}）",
                    inv.basis())
            return self._event(cmd, etype, payload_out)

        if etype == "ASSIGNMENT_NOTIFIED":
            self._require(cmd, "assignee")
            if inv.assignee and inv.assignee != cmd["assignee"]:
                raise CommandError(
                    "DUPLICATE_PLEDGE",
                    f"发票 {iid} 已受让给 {inv.assignee}"
                    f"（受让通知流水 seq={inv.assignment_seq}），"
                    f"不得再向 {cmd['assignee']} 重复质押/转让",
                    inv.basis() + [
                        f"重复质押依据：首次受让方={inv.assignee}，"
                        f"受让通知 seq={inv.assignment_seq}",
                        f"最近台账流水 seq={inv.last_seq}"])
            if inv.assignee == cmd["assignee"]:
                raise CommandError(
                    "DUPLICATE_NOTICE",
                    f"受让方 {inv.assignee} 的通知已存在，重复通知不再重复入账",
                    inv.basis())
            return self._event(cmd, etype,
                               {"invoice_id": iid, "assignee": cmd["assignee"]})

        if etype == "FINANCING_APPROVED":
            return self._validate_financing(cmd, inv)

        if etype == "REPAYMENT_RECORDED":
            return self._validate_repayment(cmd, inv)

        if etype == "RETURN_RECORDED":
            return self._validate_return(cmd, inv)

        if etype in ("FREEZE_APPLIED", "FREEZE_RELEASED"):
            return self._validate_freeze(cmd, inv, etype)

        raise CommandError("UNKNOWN_COMMAND", f"未实现的指令: {etype}")

    def _validate_financing(self, cmd, inv):
        self._require(cmd, "amount")
        amt = money(cmd["amount"])
        if amt <= 0:
            raise CommandError("INVALID_AMOUNT", "放款金额必须为正数")
        if inv.child_ids:
            raise CommandError("INVOICE_SPLIT",
                               f"母票 {inv.invoice_id} 已拆分，不得再放款", inv.basis())
        if inv.frozen:
            raise CommandError("INVOICE_FROZEN",
                               f"发票 {inv.invoice_id} 处于冻结状态：{inv.freeze_reason}",
                               inv.basis())
        missing = [d for d in REQUIRED_DOCS if d not in inv.docs]
        if missing:
            raise CommandError("DOCS_INCOMPLETE",
                               f"单据未齐套，缺失：{missing}", inv.basis())
        if inv.advances > 0 and inv.outstanding <= 0:
            raise CommandError(
                "INVOICE_SETTLED",
                f"发票 {inv.invoice_id} 已随买方回款结清，不得就同一应收重复放款",
                inv.basis())
        if amt > inv.available:
            raise CommandError(
                "OVER_LIMIT",
                f"放款 {amt} {inv.currency} 超过可融资余额 {inv.available}，"
                "系统拒绝超额放款",
                inv.basis() + [
                    f"本次申请={amt}",
                    f"拒绝依据：在贷 {inv.outstanding} + 本次 {amt} = "
                    f"{inv.outstanding + amt} > 融资上限 {inv.finance_limit}",
                    f"最近台账流水 seq={inv.last_seq}"])
        event_payload = {"invoice_id": inv.invoice_id, "amount": str(amt),
                         "outstanding_after": str(inv.outstanding + amt)}
        if inv.currency != "CNY":
            event_payload["fx_rate_to_cny"] = str(
                self._require_fx(inv).rate_to_cny)
            event_payload["amount_cny"] = str(
                self.projection.to_cny(amt, inv.currency, utcnow()))
        return self._event(cmd, "FINANCING_APPROVED", event_payload)

    def _validate_repayment(self, cmd, inv):
        self._require(cmd, "amount")
        amt = money(cmd["amount"])
        if amt <= 0:
            raise CommandError("INVALID_AMOUNT", "回款金额必须为正数")
        if inv.outstanding <= 0:
            raise CommandError("NO_OUTSTANDING",
                               f"发票 {inv.invoice_id} 当前无在贷余额，回款无对应放款",
                               inv.basis())
        applied = min(amt, inv.outstanding)
        payload = {"invoice_id": inv.invoice_id, "received_amount": str(amt),
                   "applied_principal": str(applied),
                   "outstanding_after": str(inv.outstanding - applied)}
        if amt > inv.outstanding:
            payload["remark"] = "回款超出在贷本金部分未入本金账"
        if inv.currency != "CNY":
            fx = self._require_fx(inv)
            payload["fx_rate_to_cny"] = str(fx.rate_to_cny)
            payload["applied_principal_cny"] = str(
                self.projection.to_cny(applied, inv.currency, utcnow()))
        return self._event(cmd, "REPAYMENT_RECORDED", payload)

    def _validate_return(self, cmd, inv):
        self._require(cmd, "amount")
        amt = money(cmd["amount"])
        if amt <= 0:
            raise CommandError("INVALID_AMOUNT", "退货金额必须为正数")
        if amt > inv.net_receivable:
            raise CommandError(
                "RETURN_EXCEEDS_RECEIVABLE",
                f"退货 {amt} 超过应收净额 {inv.net_receivable}", inv.basis())
        new_limit = (max(ZERO, inv.net_receivable - amt) * inv.advance_rate
                     ).quantize(Decimal("0.01"))
        payload = {"invoice_id": inv.invoice_id, "amount": str(amt),
                   "finance_limit_after": str(new_limit),
                   "recourse_due_after": str(max(ZERO, inv.outstanding - new_limit))}
        return self._event(cmd, "RETURN_RECORDED", payload)

    def _validate_freeze(self, cmd, inv, etype):
        if not cmd.get("confirmed", False):
            raise CommandError(
                "CONFIRMATION_REQUIRED",
                f"{etype} 必须由有权限角色显式确认（confirmed=true）", inv.basis())
        if etype == "FREEZE_APPLIED":
            if inv.frozen:
                raise CommandError("ALREADY_FROZEN",
                                   f"发票 {inv.invoice_id} 已处于冻结状态", inv.basis())
            self._require(cmd, "reason")
            return self._event(cmd, etype,
                               {"invoice_id": inv.invoice_id, "reason": cmd["reason"]})
        if not inv.frozen:
            raise CommandError("NOT_FROZEN",
                               f"发票 {inv.invoice_id} 未冻结，无需解除", inv.basis())
        return self._event(cmd, etype, {"invoice_id": inv.invoice_id})

    def _validate_split(self, cmd, inv):
        children = cmd.get("children")
        if not children or not isinstance(children, list) or len(children) < 2:
            raise CommandError("INVALID_SPLIT", "拆分至少需要两个子票")
        total = ZERO
        norm = []
        for c in children:
            cid = c.get("invoice_id")
            amt = money(c.get("amount"))
            if not cid or amt <= 0:
                raise CommandError("INVALID_SPLIT", "子票标识与正数金额均必填")
            if cid in self.projection.invoices:
                raise CommandError("DUPLICATE_INVOICE",
                                   f"子票 {cid} 已存在", inv.basis())
            total += amt
            norm.append({"invoice_id": cid, "amount": str(amt)})
        if total != inv.net_receivable:
            raise CommandError(
                "SPLIT_AMOUNT_MISMATCH",
                f"子票金额合计 {total} 不等于母票应收净额 {inv.net_receivable}",
                inv.basis())
        if inv.outstanding != 0:
            raise CommandError(
                "SPLIT_WITH_OUTSTANDING",
                f"母票尚有在贷 {inv.outstanding}，须结清后再拆分", inv.basis())
        return self._event(cmd, "INVOICE_SPLIT",
                           {"invoice_id": inv.invoice_id, "children": norm})

    # ---- 辅助 -----------------------------------------------------------
    def _require_fx(self, inv):
        fx = self.projection.fx_at(inv.currency, utcnow())
        if fx is None:
            raise CommandError(
                "FX_RATE_MISSING",
                f"缺少 {inv.currency} -> CNY 的已登记汇率，币种换算无法入账",
                inv.basis())
        return fx

    def _replay_result(self, prior_seq: int) -> dict:
        prior = self._records.get(prior_seq)
        if prior is None:
            raise CommandError("INTERNAL", "幂等回放找不到原始记录", http_status=500)
        if prior["type"] == "REJECTED":
            pl = prior["payload"]
            return {"accepted": False, "duplicate": True,
                    "code": pl["reason_code"], "message": pl["message"],
                    "basis": pl["basis"],
                    "http_status": pl.get("http_status", 422),
                    "seq": prior["seq"], "hash": prior["hash"]}
        return {"accepted": True, "duplicate": True,
                "seq": prior["seq"], "hash": prior["hash"], "type": prior["type"]}

    @staticmethod
    def _event(cmd, etype, payload):
        return {
            "event_id": cmd["event_id"],
            "type": etype,
            "actor": cmd.get("actor"),
            "role": cmd.get("role"),
            "recorded_at": cmd.get("recorded_at") or utcnow(),
            "payload": payload,
        }

    @staticmethod
    def _check_command_shape(command):
        if not isinstance(command, dict):
            raise CommandError("BAD_REQUEST", "指令必须是 JSON 对象", http_status=400)
        if not command.get("event_id"):
            raise CommandError("EVENT_ID_REQUIRED",
                               "每条指令必须带唯一业务标识 event_id", http_status=400)
        if not command.get("type"):
            raise CommandError("TYPE_REQUIRED", "指令缺少 type", http_status=400)

    @staticmethod
    def _require(obj, *keys):
        for k in keys:
            if obj.get(k) in (None, ""):
                raise CommandError("MISSING_FIELD", f"缺少必填字段: {k}")

    # ---- 查询 -----------------------------------------------------------
    def invoice(self, invoice_id: str) -> dict:
        if invoice_id not in self.projection.invoices:
            raise CommandError("INVOICE_NOT_FOUND", f"发票 {invoice_id} 不存在",
                               http_status=404)
        return self.projection.snapshot(invoice_id)

    def all_invoices(self) -> list[dict]:
        return [self.projection.snapshot(i) for i in sorted(self.projection.invoices)]
