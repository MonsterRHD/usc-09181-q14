"""贸易发票融资台账核心。

设计原则
========
1. 事件溯源：余额 = 对只增事件流的纯函数折叠（``fold``）。任何时刻删掉派生
   状态、从头重放流水，结果必须一致——这就是"可重算"。
2. 凭证不可变：迟到的承运人回执、退货、部分回款都只能 *追加* 新事件；原始
   凭证摘要没有任何修改入口。
3. 幂等：``event_id``（及 idempotency_key）全局唯一；重复提交原样返回首次
   回执，被拒绝的命令同样留痕并可重放。
4. 串行写入：所有命令在同一把锁内"折叠→校验→入账"，并发同票融资不可能
   双花可用余额。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from .models import Command, D, Event, LedgerError, Role
from .store import EventStore

CENT = Decimal("0.01")
FX_REF = "*"  # 汇率事件的全局发票引用

_DOC_ROLES = {Role.OPERATOR.value, Role.FINANCE_MANAGER.value}


def _money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _day(ts: str) -> str:
    return ts[:10]


# ===========================================================================
# 派生状态
# ===========================================================================

@dataclass
class InvoiceState:
    ref: str
    root: str
    parent: str | None = None
    exists: bool = False
    seller: str = ""
    debtor: str = ""
    currency: str = ""
    face: Decimal = Decimal(0)              # 票面（发票币种）
    qty: Decimal = Decimal(0)
    advance_ratio: Decimal = Decimal("0.8")
    provisional_ratio: Decimal = Decimal("0.7")
    split_out: Decimal = Decimal(0)         # 已拆分出去的比例
    confirmed_qty: Decimal | None = None    # 承运人确认数量；None=回执未到
    receipts: list[dict[str, Any]] = field(default_factory=list)
    assignee: str | None = None             # 当前受让人（保理商/银行）
    notice_id: str | None = None
    notice_event_id: str | None = None
    notice_inherited: bool = False
    dup_party: str | None = None            # 重复质押的第二受让人
    returns: Decimal = Decimal(0)           # 累计退货（发票币种）
    cashback_claims: Decimal = Decimal(0)   # 先回款后退货形成的对出口商追索
    loans: list[dict[str, Any]] = field(default_factory=list)
    repayments: list[dict[str, Any]] = field(default_factory=list)
    manual_frozen: bool = False
    freeze_reason: str | None = None
    frozen_event: str | None = None

    # -- 规模 -------------------------------------------------------------
    @property
    def base_face(self) -> Decimal:
        """拆出子票后，本实体剩余的票面基数。"""
        return self.face * (Decimal(1) - self.split_out)

    @property
    def retained_qty(self) -> Decimal:
        """拆出子票后，本实体剩余的数量基数。"""
        return self.qty * (Decimal(1) - self.split_out)

    @property
    def confirmed_fraction(self) -> Decimal:
        if self.confirmed_qty is None:
            return Decimal(1)  # 回执到达前按全额 provisional 比例融资
        base = self.retained_qty
        if base <= 0:
            return Decimal(1)
        return min(Decimal(1), self.confirmed_qty / base)

    @property
    def ratio(self) -> Decimal:
        return self.advance_ratio if self.confirmed_qty is not None else self.provisional_ratio

    @property
    def eligible_face(self) -> Decimal:
        """凭承运人确认数量与票面取小，再扣除退货。"""
        return max(Decimal(0), self.base_face * self.confirmed_fraction - self.returns)

    @property
    def limit(self) -> Decimal:
        """可融资额度（发票币种）。"""
        return _money(self.eligible_face * self.ratio)

    @property
    def loaned(self) -> Decimal:
        return _money(sum((D(x["converted"]) for x in self.loans), Decimal(0)))

    @property
    def repaid(self) -> Decimal:
        return _money(sum((D(x["converted"]) for x in self.repayments), Decimal(0)))

    @property
    def outstanding(self) -> Decimal:
        """未偿放款：放款 − 回款（不低于 0）。"""
        return _money(max(Decimal(0), self.loaned - self.repaid))

    @property
    def available(self) -> Decimal:
        return max(Decimal(0), self.limit - self.outstanding)

    @property
    def excess(self) -> Decimal:
        """放款超额 = 未偿 − 当前限额；>0 即对保理头寸触发追索。"""
        return max(Decimal(0), self.outstanding - self.limit)

    @property
    def recourse_total(self) -> Decimal:
        """追索总额：未偿超额 + 已回款后退货应向出口商追回的现金。"""
        return self.excess + self.cashback_claims

    @property
    def system_frozen(self) -> bool:
        return self.recourse_total > 0 or self.dup_party is not None

    @property
    def frozen(self) -> bool:
        return self.manual_frozen or self.system_frozen

    def limit_basis(self) -> dict[str, Any]:
        """放款/拒绝时给出的额度构成依据。"""
        return {
            "face": str(self.face),
            "currency": self.currency,
            "split_out_ratio": str(self.split_out),
            "base_face_after_split": str(_money(self.base_face)),
            "retained_qty": str(self.retained_qty),
            "carrier_confirmed_qty": None if self.confirmed_qty is None else str(self.confirmed_qty),
            "receipt_status": "confirmed" if self.confirmed_qty is not None else "pending_provisional",
            "confirmed_fraction": str(self.confirmed_fraction),
            "applied_ratio": str(self.ratio),
            "returns": str(self.returns),
            "eligible_face": str(_money(self.eligible_face)),
            "finance_limit": str(self.limit),
            "loaned": str(self.loaned),
            "repaid": str(self.repaid),
            "outstanding": str(self.outstanding),
            "available": str(self.available),
            "excess": str(self.excess),
            "cashback_claims": str(self.cashback_claims),
            "recourse_total": str(self.recourse_total),
            "frozen": self.frozen,
        }


@dataclass
class LedgerState:
    invoices: dict[str, InvoiceState] = field(default_factory=dict)
    fx: dict[str, list[dict[str, Any]]] = field(default_factory=dict)  # ccy -> [{effective, rate, event_id}]

    def get(self, ref: str) -> InvoiceState:
        return self.invoices.setdefault(ref, InvoiceState(ref=ref, root=ref))

    def family(self, st: InvoiceState) -> list[InvoiceState]:
        return [s for s in self.invoices.values() if s.root == st.root]

    def convert(self, amount: Decimal, ccy: str, target: str, on_date: str) -> tuple[Decimal, Decimal]:
        """返回 (换算后金额, 所用汇率)。按发生日取当时生效的不可变汇率。"""
        if ccy == target:
            return _money(amount), Decimal(1)
        series = self.fx.get(ccy)
        if not series:
            raise LedgerError("FX_RATE_MISSING", f"币种 {ccy} 尚未登记汇率，无法换算为 {target}")
        effective = [r for r in series if r["effective"] <= on_date]
        rate = (effective or series)[-1]["rate"]
        return _money(amount * rate), D(rate)


# ===========================================================================
# 纯折叠：事件流 -> 状态
# ===========================================================================

def fold(events: list[Event]) -> LedgerState:
    state = LedgerState()
    for e in sorted(events, key=lambda x: x.seq):
        _apply(state, e)
    return state


def _apply(state: LedgerState, e: Event) -> None:
    t, d, accepted = e.command_type, e.data, e.accepted

    if t == "FX_RATE" and accepted:
        state.fx.setdefault(d["currency"], []).append(
            {"effective": d["effective_date"], "rate": D(d["rate"]), "event_id": e.event_id})
        state.fx[d["currency"]].sort(key=lambda r: r["effective"])
        return

    if t == "INVOICE" and accepted:
        st = state.get(e.invoice_ref)
        st.exists = True
        st.seller = d["seller"]
        st.debtor = d["debtor"]
        st.currency = d["currency"]
        st.face = D(d["face"])
        st.qty = D(d.get("qty", 0))
        st.advance_ratio = D(d.get("advance_ratio", "0.8"))
        st.provisional_ratio = D(d.get("provisional_ratio", "0.7"))
        return

    if t == "SPLIT" and accepted:
        parent = state.get(e.invoice_ref)
        ratio = D(d["ratio"])
        sb_before = parent.split_out
        parent.split_out += ratio
        child = state.get(d["child_ref"])
        child.exists = True
        child.root = parent.root
        child.parent = parent.ref
        child.seller, child.debtor, child.currency = parent.seller, parent.debtor, parent.currency
        child.face = _money(parent.face * ratio)
        child.qty = parent.qty * ratio
        child.advance_ratio, child.provisional_ratio = parent.advance_ratio, parent.provisional_ratio
        # 已确认数量按拆分份额摊到母票（留存部分）与子票
        if parent.confirmed_qty is not None and (1 - sb_before) > 0:
            share = parent.confirmed_qty * ratio / (1 - sb_before)
            parent.confirmed_qty -= share
            child.confirmed_qty = share
        if parent.assignee:  # 受让覆盖拆分后的各部分
            child.assignee, child.notice_id, child.notice_inherited = parent.assignee, parent.notice_id, True
            child.notice_event_id = parent.notice_event_id
        child.dup_party = parent.dup_party  # 重复质押冻结随拆分延续
        return

    if t == "ASSIGNMENT_NOTICE":
        st = state.get(e.invoice_ref)
        if accepted:
            # 受让覆盖同一发票族（母票及各拆分子票）
            for s in state.family(st):
                s.assignee, s.notice_id, s.notice_inherited = d["assignee"], d["notice_id"], s.ref != st.ref
                s.notice_event_id = e.event_id
                s.dup_party = None
        elif e.reject and e.reject["code"] == "DUPLICATE_ASSIGNMENT":
            # 重复质押：整族（母票/子票）冻结；第二受让人记录在拒绝依据中
            dup_party = e.reject["basis"]["rejected_assignee"]
            for s in state.family(st):
                s.dup_party = dup_party
        return

    if t == "CARRIER_RECEIPT" and accepted:
        st = state.get(e.invoice_ref)
        confirmed = D(d["confirmed_qty"])
        family = state.family(st)
        total_qty = sum((s.retained_qty for s in family), Decimal(0))
        if total_qty > 0 and len(family) > 1:
            # 回执针对整批货物：按各实体剩余数量份额摊到母票/各子票
            for s in family:
                s.confirmed_qty = s.retained_qty * confirmed / total_qty
        else:
            st.confirmed_qty = confirmed
        st.receipts.append(d)
        return

    if t == "LOAN" and accepted:
        state.get(e.invoice_ref).loans.append(
            {"event_id": e.event_id, "amount": d["amount"], "currency": d["currency"],
             "converted": d["converted"], "rate": d["rate"], "ts": e.occurred_at})
        return

    if t == "REPAYMENT" and accepted:
        state.get(e.invoice_ref).repayments.append(
            {"event_id": e.event_id, "amount": d["amount"], "currency": d["currency"],
             "converted": d["converted"], "rate": d["rate"], "ts": e.occurred_at})
        return

    if t == "RETURN" and accepted:
        st = state.get(e.invoice_ref)
        st.returns += D(d["converted"])
        st.cashback_claims += D(d["cashback_due_from_seller"])
        return

    if t in ("FREEZE", "UNFREEZE") and accepted:
        st = state.get(e.invoice_ref)
        st.manual_frozen = (t == "FREEZE")
        st.freeze_reason = d.get("reason") if t == "FREEZE" else None
        st.frozen_event = e.event_id
        return


# ===========================================================================
# 台账：命令处理
# ===========================================================================

COMMAND_TYPES = {
    "FX_RATE", "INVOICE", "SHIPMENT", "CARRIER_RECEIPT", "ASSIGNMENT_NOTICE",
    "SPLIT", "LOAN", "REPAYMENT", "RETURN", "FREEZE", "UNFREEZE",
}


class Ledger:
    def __init__(self, store: EventStore | None = None):
        self.store = store or EventStore()
        # 命令执行与存储共用一把锁：折叠→校验→入账 原子完成
        self._lock = self.store.lock

    # -- 公共 ---------------------------------------------------------------
    def handle(self, cmd: Command) -> Event:
        with self._lock:
            existing = self.store.lookup(cmd.event_id, cmd.idempotency_key)
            if existing is not None:
                return existing  # 断网补传 / 恢复后重放：原样返回，幂等
            kind = cmd.payload.get("type")
            if kind not in COMMAND_TYPES:
                raise LedgerError("UNKNOWN_COMMAND", f"未知命令类型: {kind!r}")
            ref = FX_REF if kind == "FX_RATE" else str(cmd.payload.get("invoice_ref", ""))
            state = fold(self.store.all_events())
            ts = _now()
            occurred = cmd.occurred_at or ts
            try:
                data = self._dispatch(state, cmd, occurred)
                reject = None
            except LedgerError as err:
                data, reject = {}, err.to_dict()
            evt = Event(
                event_id=cmd.event_id, command_type=kind, invoice_ref=ref, seq=0,
                ts=ts, occurred_at=occurred, role=cmd.role, accepted=reject is None,
                data=data, reject=reject, supersedes=cmd.payload.get("supersedes"),
                idempotency_key=cmd.idempotency_key,
            )
            return self.store.append(evt)

    # -- 分派 ---------------------------------------------------------------
    def _dispatch(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        kind = cmd.payload["type"]
        p = cmd.payload
        handler = {
            "FX_RATE": self._fx_rate,
            "INVOICE": self._invoice,
            "SHIPMENT": self._shipment,
            "CARRIER_RECEIPT": self._carrier,
            "ASSIGNMENT_NOTICE": self._notice,
            "SPLIT": self._split,
            "LOAN": self._loan,
            "REPAYMENT": self._repayment,
            "RETURN": self._return,
            "FREEZE": self._freeze,
            "UNFREEZE": self._unfreeze,
        }[kind]
        return handler(state, cmd, occurred)

    # -- 工具 ---------------------------------------------------------------
    @staticmethod
    def _require_role(cmd: Command, allowed: set[str]) -> None:
        if cmd.role not in allowed:
            raise LedgerError("FORBIDDEN", f"角色 {cmd.role} 无权执行该操作，需要 {'/'.join(sorted(allowed))}")

    @staticmethod
    def _invoice_of(state: LedgerState, ref: str) -> InvoiceState:
        st = state.invoices.get(ref)
        if not st or not st.exists:
            raise LedgerError("INVOICE_NOT_FOUND", f"发票 {ref} 尚未登记")
        return st

    # -- 各命令 -------------------------------------------------------------
    def _fx_rate(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, _DOC_ROLES)
        d = cmd.payload
        for k in ("currency", "rate", "effective_date"):
            if k not in d:
                raise LedgerError("BAD_REQUEST", f"缺少字段 {k}")
        rate = D(d["rate"])
        if rate <= 0:
            raise LedgerError("BAD_REQUEST", "汇率必须为正数")
        return {"currency": d["currency"], "rate": str(rate), "effective_date": d["effective_date"]}

    def _invoice(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, _DOC_ROLES)
        d = cmd.payload
        ref = d["invoice_ref"]
        existing = state.invoices.get(ref)
        if existing and existing.exists:
            raise LedgerError("INVOICE_EXISTS", f"发票 {ref} 已存在，原始凭证不可重复登记/修改")
        face = D(d["face"])
        if face <= 0:
            raise LedgerError("BAD_REQUEST", "票面金额必须为正数")
        return {
            "invoice_ref": ref, "seller": d["seller"], "debtor": d["debtor"],
            "currency": d["currency"], "face": str(face), "qty": str(D(d.get("qty", 0))),
            "advance_ratio": str(D(d.get("advance_ratio", "0.8"))),
            "provisional_ratio": str(D(d.get("provisional_ratio", "0.7"))),
            # 原始凭证摘要原样留痕，一经入账不可修改
            "summary": d.get("summary", ""),
        }

    def _shipment(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, _DOC_ROLES)
        d = cmd.payload
        st = self._invoice_of(state, d["invoice_ref"])
        # 装运凭证本身不改变可融资余额；承运人回执到达前按 provisional 比例。
        return {"invoice_ref": st.ref, "bl_no": d["bl_no"], "shipped_at": d.get("shipped_at", occurred[:10]),
                "limit_impact": "none_pending_carrier_receipt", "finance_limit": str(st.limit)}

    def _carrier(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, _DOC_ROLES)
        d = cmd.payload
        st = self._invoice_of(state, d["invoice_ref"])
        confirmed_qty = D(d["confirmed_qty"])
        prior_financing = any(x.loans or x.repayments or x.returns for x in state.family(st))
        is_amendment = any(x.confirmed_qty is not None for x in state.family(st))
        late = bool(d.get("late")) or prior_financing or is_amendment
        # 与 fold 一致：回执按整批确认数量摊到母票/各子票（同一确认比例）
        family = state.family(st)
        total_qty = sum((s.retained_qty for s in family), Decimal(0))
        fraction = min(Decimal(1), confirmed_qty / total_qty) if total_qty > 0 else Decimal(1)
        projections: list[tuple[InvoiceState, Decimal]] = []
        for s in family:
            new_limit = _money(max(Decimal(0), s.base_face * fraction - s.returns) * s.advance_ratio)
            projections.append((s, new_limit))
        limit_before = _money(sum((s.limit for s in family), Decimal(0)))
        limit_after = _money(sum((lim for _, lim in projections), Decimal(0)))
        outstanding = _money(sum((s.outstanding for s in family), Decimal(0)))
        excess_before = max(Decimal(0), outstanding - limit_before)
        excess_after = max(Decimal(0), outstanding - limit_after)
        target_limit_after = next(lim for s, lim in projections if s.ref == st.ref)
        return {
            "carrier_ref": d["carrier_ref"], "confirmed_qty": str(confirmed_qty),
            "invoice_qty": str(total_qty),
            "short_qty": str(max(Decimal(0), total_qty - confirmed_qty)),
            "received_at": occurred, "late": late,
            "late_reason": ("prior_financing_or_amendment" if late else None),
            "supersedes_provisional": not is_amendment,
            "impact_scope": "family" if len(family) > 1 else "invoice",
            "impact_on_current_balance": {
                "limit_before": str(limit_before),
                "limit_after": str(limit_after),
                "delta": str(limit_after - limit_before),
                "target_invoice_limit_after": str(target_limit_after),
                "outstanding": str(outstanding),
                "excess_before": str(excess_before),
                "excess_after": str(excess_after),
                "recourse_triggered": excess_after > excess_before,
                "available_after": str(max(Decimal(0), limit_after - outstanding)),
            },
        }

    def _notice(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, _DOC_ROLES)
        d = cmd.payload
        st = self._invoice_of(state, d["invoice_ref"])
        assignee = d["assignee"]
        # 重复质押：同一发票（含拆分后的母/子票）受让给不同受让人
        current = st.assignee or next((s.assignee for s in state.family(st) if s.assignee), None)
        prior_notice = st.notice_event_id or next(
            (s.notice_event_id for s in state.family(st) if s.notice_event_id), None)
        if current and assignee != current:
            basis = {
                "invoice_ref": st.ref, "root_ref": st.root,
                "first_assignee": current, "first_notice_event_id": prior_notice,
                "rejected_assignee": assignee, "notice_id": d["notice_id"],
                "action": "family_frozen_for_duplicate_assignment",
            }
            raise LedgerError("DUPLICATE_ASSIGNMENT",
                              f"发票 {st.ref} 已受让给 {current}，再次受让给 {assignee} 构成重复质押，已冻结整族发票",
                              basis)
        if st.assignee == assignee and st.notice_id == d["notice_id"]:
            raise LedgerError("NOTICE_DUPLICATE", "同一受让通知重复提交")
        return {"assignee": assignee, "notice_id": d["notice_id"], "notice_event_id": None}

    def _split(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, _DOC_ROLES)
        d = cmd.payload
        st = self._invoice_of(state, d["invoice_ref"])
        ratio = D(d["ratio"])
        if not (Decimal(0) < ratio <= Decimal(1)):
            raise LedgerError("BAD_REQUEST", "拆分比例必须在 (0,1] 之间")
        if st.split_out + ratio > Decimal(1):
            raise LedgerError("SPLIT_EXCEEDED", "累计拆分比例超过 1",
                              {"split_out": str(st.split_out), "requested": str(ratio)})
        child = state.invoices.get(d["child_ref"])
        if child and child.exists:
            raise LedgerError("INVOICE_EXISTS", f"子票 {d['child_ref']} 已存在")
        return {"child_ref": d["child_ref"], "ratio": str(ratio),
                "child_face": str(_money(st.face * ratio)),
                "notice_inherited": bool(st.assignee)}

    def _guard_loan_ready(self, st: InvoiceState) -> None:
        if st.frozen:
            why = "manual_freeze" if st.manual_frozen else "duplicate_assignment" if st.dup_party else "excess_recourse"
            raise LedgerError("INVOICE_FROZEN", f"发票 {st.ref} 处于冻结状态（{why}），禁止放款",
                              st.limit_basis())
        if not st.assignee:
            raise LedgerError("NOTICE_MISSING", f"发票 {st.ref} 缺少受让通知，保理商/银行未取得受让地位")

    def _loan(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, {Role.FINANCE_MANAGER.value})
        d = cmd.payload
        st = self._invoice_of(state, d["invoice_ref"])
        self._guard_loan_ready(st)
        amount = D(d["amount"])
        if amount <= 0:
            raise LedgerError("BAD_REQUEST", "放款金额必须为正数")
        ccy = d.get("currency", st.currency)
        converted, rate = state.convert(amount, ccy, st.currency, _day(occurred))
        basis = st.limit_basis()
        basis.update({"request_amount": str(amount), "request_currency": ccy,
                      "converted_request": str(converted), "fx_rate": str(rate)})
        if converted > st.available:
            raise LedgerError("LIMIT_EXCEEDED",
                              f"放款 {converted} {st.currency} 超过可用余额 {st.available}，拒绝超额放款",
                              basis)
        return {"amount": str(amount), "currency": ccy, "converted": str(converted),
                "rate": str(rate), "finance_limit": str(st.limit),
                "outstanding_after": str(_money(st.outstanding + converted)),
                "available_after": str(st.available - converted)}

    def _repayment(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, _DOC_ROLES)
        d = cmd.payload
        st = self._invoice_of(state, d["invoice_ref"])
        amount = D(d["amount"])
        if amount <= 0:
            raise LedgerError("BAD_REQUEST", "回款金额必须为正数")
        ccy = d.get("currency", st.currency)
        converted, rate = state.convert(amount, ccy, st.currency, _day(occurred))
        if converted > st.outstanding:
            raise LedgerError("REPAYMENT_EXCEEDED",
                              f"回款 {converted} 超过未偿余额 {st.outstanding}",
                              {"outstanding": str(st.outstanding), "converted_request": str(converted)})
        out_after = _money(st.outstanding - converted)
        return {"amount": str(amount), "currency": ccy, "converted": str(converted), "rate": str(rate),
                "outstanding_after": str(out_after),
                "excess_after": str(max(Decimal(0), out_after - st.limit))}

    def _return(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, _DOC_ROLES)
        d = cmd.payload
        st = self._invoice_of(state, d["invoice_ref"])
        amount = D(d["amount"])
        if amount <= 0:
            raise LedgerError("BAD_REQUEST", "退货金额必须为正数")
        ccy = d.get("currency", st.currency)
        converted, rate = state.convert(amount, ccy, st.currency, _day(occurred))
        new_returns = st.returns + converted
        new_limit = _money(max(Decimal(0), st.base_face * st.confirmed_fraction - new_returns) * st.ratio)
        new_excess = max(Decimal(0), st.outstanding - new_limit)
        # 先回款后退货：退货对应货款已由买方打回、且放款已结清的部分，
        # 形成对出口商的现金追索；累计封顶于已回款总额。
        cashback_total = min(st.repaid, new_returns)
        cashback_delta = cashback_total - st.cashback_claims
        recourse_total = new_excess + cashback_total
        return {"amount": str(amount), "currency": ccy, "converted": str(converted), "rate": str(rate),
                "returns_after": str(new_returns), "limit_before": str(st.limit),
                "limit_after": str(new_limit), "outstanding": str(st.outstanding),
                "excess_after": str(new_excess),
                "recourse_triggered": recourse_total > (st.excess + st.cashback_claims),
                "repayments_received": str(st.repaid),
                "cashback_due_from_seller": str(_money(cashback_delta)),
                "cashback_claims_total": str(_money(cashback_total)),
                "recourse_total": str(_money(recourse_total)),
                "freeze_effect": "system_frozen" if recourse_total > 0 else "none"}

    def _freeze(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, {Role.RISK_OFFICER.value})  # 仅风控有权
        d = cmd.payload
        st = self._invoice_of(state, d["invoice_ref"])
        if st.manual_frozen:
            raise LedgerError("ALREADY_FROZEN", "发票已处于人工冻结状态")
        return {"reason": d.get("reason", "manual")}

    def _unfreeze(self, state: LedgerState, cmd: Command, occurred: str) -> dict[str, Any]:
        self._require_role(cmd, {Role.RISK_OFFICER.value})
        d = cmd.payload
        st = self._invoice_of(state, d["invoice_ref"])
        if not st.frozen:
            raise LedgerError("NOT_FROZEN", "发票当前无冻结可解除")
        # 风控确认解除人工冻结；系统风险（超额/重复质押）未消除时仍保持冻结
        effective_frozen = st.system_frozen
        reason = "duplicate_assignment" if effective_frozen and st.dup_party else (
            "excess_recourse" if effective_frozen else None)
        return {"reason": d.get("reason", "reviewed"), "manual_freeze_cleared": st.manual_frozen,
                "effective_frozen": effective_frozen,
                "effective_frozen_reason": reason}

    # -- 查询（全部由流水实时重算）------------------------------------------
    def statement(self, ref: str) -> dict[str, Any]:
        with self._lock:
            events = self.store.all_events()
        state = fold(events)
        if ref not in state.invoices or not state.invoices[ref].exists:
            raise LedgerError("INVOICE_NOT_FOUND", f"发票 {ref} 尚未登记")
        st = state.invoices[ref]
        return {"invoice_ref": ref, "root_ref": st.root, "seller": st.seller, "debtor": st.debtor,
                "assignee": st.assignee, "currency": st.currency, **st.limit_basis(),
                "manual_frozen": st.manual_frozen, "system_frozen": st.system_frozen,
                "repayments_total": str(st.repaid),
                "recourse": {"active": st.system_frozen,
                             "loan_excess": str(st.excess),
                             "cashback_from_seller": str(st.cashback_claims),
                             "total": str(st.recourse_total),
                             "dup_party": st.dup_party}}

    def journal(self, ref: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            evts = self.store.all_events()
        if ref:
            evts = [e for e in evts if e.invoice_ref == ref]
        return [e.to_dict() for e in evts]
