"""贸易发票融资台账领域模型：事件折叠（fold）出的可重算台账。

设计要点
========
* 台账状态 *只能* 由事件流折叠得到：replay(events) 是唯一的状态来源，
  断网补传、进程重启后重放同一批事件必然得到同一结果（幂等可重算）。
* 所有金额在发票币种内用 Decimal 计算；人民币台账金额按事件发生时
  已登记的最新汇率换算，汇率快照写进事件，重放时不再依赖“当前汇率”。
* 可融资余额的依据（单据齐套、冻结、退货、在贷余额、融资比例）全部
  显式体现在投影里，超额放款被拒绝时可逐条引用。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum

TWO = Decimal("0.01")
SIX = Decimal("0.000001")
ZERO = Decimal("0.00")
ONE = Decimal("1")


def money(value) -> Decimal:
    """业务金额：两位小数，负数在具体规则里另行禁止。"""
    return Decimal(str(value)).quantize(TWO, rounding=ROUND_HALF_UP)


def rate(value) -> Decimal:
    return Decimal(str(value)).quantize(SIX, rounding=ROUND_HALF_UP)


class InvoiceStatus(str, Enum):
    ACTIVE = "ACTIVE"                    # 正常可跟进
    SPLIT = "SPLIT"                      # 已拆分，余额归子票
    FROZEN = "FROZEN"                    # 冻结中，禁止放款
    PARTIAL_RETURN = "PARTIAL_RETURN"    # 部分退货
    RETURNED = "RETURNED"                # 全额退货
    RECOURSE = "RECOURSE"                # 触发追索
    SETTLED = "SETTLED"                  # 货款两清


# 融资放款前必须齐套的单据
REQUIRED_DOCS = ("invoice", "shipment", "carrier_receipt", "assignment")


@dataclass
class InvoiceState:
    invoice_id: str
    currency: str = ""
    amount: Decimal = ZERO               # 发票金额（原币）
    advance_rate: Decimal = Decimal("0.8")
    returns: Decimal = ZERO              # 累计退货金额
    advances: Decimal = ZERO             # 累计放款（原币）
    repayments: Decimal = ZERO           # 累计回款（原币本金）
    docs: set = field(default_factory=set)
    assignee: str | None = None          # 受让方（保理商/银行）
    assignment_seq: int | None = None    # 受让通知所在流水
    parent_id: str | None = None
    child_ids: list = field(default_factory=list)
    frozen: bool = False
    freeze_reason: str | None = None
    created_seq: int | None = None
    last_seq: int | None = None
    # 迟到凭证的余额影响标注：{doc: {"late": bool, "note": ..., "impact": ...}}
    annotations: list = field(default_factory=list)

    # ---- 派生值（全部可由流水重算）------------------------------------
    @property
    def net_receivable(self) -> Decimal:
        return self.amount - self.returns

    @property
    def finance_limit(self) -> Decimal:
        """可融资上限 = 应收净额 × 融资比例。"""
        net = max(ZERO, self.net_receivable)
        return (net * self.advance_rate).quantize(TWO, rounding=ROUND_HALF_UP)

    @property
    def outstanding(self) -> Decimal:
        """在贷（尚未收回的本金）。"""
        return self.advances - self.repayments

    @property
    def recourse_due(self) -> Decimal:
        """退货等原因导致在贷超过融资上限的部分，即追索敞口。"""
        return max(ZERO, self.outstanding - self.finance_limit)

    @property
    def docs_complete(self) -> bool:
        return all(d in self.docs for d in REQUIRED_DOCS)

    @property
    def is_settled(self) -> bool:
        """已发生过放款且本金已随回款结清：同一应收不得重复融资。"""
        return self.advances > 0 and self.outstanding <= 0

    @property
    def available(self) -> Decimal:
        """当前可融资余额（原币）。冻结/单据不齐/已拆分/已结清时为 0。"""
        if (self.frozen or self.child_ids or not self.docs_complete
                or self.is_settled):
            return ZERO
        return max(ZERO, self.finance_limit - self.outstanding)

    @property
    def status(self) -> InvoiceStatus:
        if self.child_ids:
            return InvoiceStatus.SPLIT
        if self.recourse_due > 0:
            return InvoiceStatus.RECOURSE
        if self.frozen:
            return InvoiceStatus.FROZEN
        if self.net_receivable <= 0 and self.amount > 0:
            return InvoiceStatus.RETURNED
        if self.returns > 0:
            return InvoiceStatus.PARTIAL_RETURN
        if self.advances > 0 and self.outstanding <= 0:
            return InvoiceStatus.SETTLED
        return InvoiceStatus.ACTIVE

    def basis(self) -> list[str]:
        """可融资余额的计算依据，供拒绝放款时引用。"""
        lines = [
            f"发票金额={self.amount} {self.currency}",
            f"累计退货={self.returns}，应收净额={self.net_receivable}",
            f"融资比例={self.advance_rate}，融资上限={self.finance_limit}",
            f"累计放款={self.advances}，累计回款={self.repayments}，"
            f"在贷余额={self.outstanding}",
        ]
        missing = [d for d in REQUIRED_DOCS if d not in self.docs]
        lines.append(f"单据情况：已齐套={self.docs_complete}"
                     + (f"，缺失={missing}" if missing else ""))
        if self.assignee:
            lines.append(f"受让方={self.assignee}")
        if self.frozen:
            lines.append(f"冻结中（原因：{self.freeze_reason}），禁止放款")
        if self.child_ids:
            lines.append(f"已拆分为子票={self.child_ids}，母票不得再融资")
        blocked = (self.frozen or bool(self.child_ids) or bool(missing)
                   or self.is_settled)
        avail = ZERO if blocked else max(ZERO, self.finance_limit - self.outstanding)
        if self.is_settled:
            lines.append("发票已随回款结清，同一应收不得重复融资")
        lines.append(f"可融资余额={avail}")
        return lines


class Rejection(Exception):
    """业务规则拒绝；code 为机器可读原因，basis 为人工可读依据。"""

    def __init__(self, code: str, message: str, basis: list[str] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.basis = basis or []


@dataclass
class FxRate:
    currency: str
    rate_to_cny: Decimal       # 1 单位外币兑人民币
    as_of: str                 # ISO 时间戳
    seq: int


class LedgerProjection:
    """事件流的折叠结果。fold_event 是唯一的状态变更入口。"""

    def __init__(self):
        self.invoices: dict[str, InvoiceState] = {}
        self.fx: dict[str, FxRate] = {}          # 外币 -> 截至当前最新汇率
        self.processed_event_ids: set[str] = set()
        self.event_id_to_seq: dict[str, int] = {}
        self.last_seq = 0

    # ---- 汇率 -----------------------------------------------------------
    def fx_at(self, currency: str, as_of: str | None = None) -> FxRate | None:
        fx = self.fx.get(currency)
        if fx is None:
            return None
        if as_of is not None and fx.as_of > as_of:
            return None
        return fx

    def to_cny(self, amount: Decimal, currency: str, as_of: str) -> Decimal:
        if currency == "CNY":
            return amount
        fx = self.fx_at(currency, as_of)
        if fx is None:
            raise Rejection("FX_RATE_MISSING",
                            f"缺少 {currency} 在 {as_of} 之前登记的汇率，无法换算")
        return (amount * fx.rate_to_cny).quantize(TWO, rounding=ROUND_HALF_UP)

    # ---- 折叠 -----------------------------------------------------------
    def fold_event(self, env: dict):
        """把一条信封事件折进投影。拒绝类事件只登记幂等键，不改业务余额。"""
        etype = env["type"]
        seq = env["seq"]
        self.last_seq = max(self.last_seq, seq)
        eid = env.get("event_id")
        if eid:
            self.processed_event_ids.add(eid)
            self.event_id_to_seq.setdefault(eid, seq)
        p = env.get("payload", {})

        if etype == "FX_RATE_REGISTERED":
            cur = p["currency"]
            fx = FxRate(cur, rate(p["rate_to_cny"]), p["as_of"], seq)
            old = self.fx.get(cur)
            if old is None or fx.as_of >= old.as_of:
                self.fx[cur] = fx
            return
        if etype == "REJECTED":
            return

        if etype == "INVOICE_REGISTERED":
            iid = p["invoice_id"]
            if iid in self.invoices:
                raise Rejection("DUPLICATE_INVOICE", f"发票 {iid} 已登记")
            inv = InvoiceState(
                invoice_id=iid,
                currency=p["currency"],
                amount=money(p["amount"]),
                advance_rate=rate(p.get("advance_rate", "0.8")),
                docs={"invoice"},
                created_seq=seq,
                last_seq=seq,
            )
            self.invoices[iid] = inv
            return

        inv = self._invoice_for(p.get("invoice_id"), required=True)
        inv.last_seq = seq

        if etype == "INVOICE_SPLIT":
            self._fold_split(inv, p, seq)
        elif etype == "SHIPMENT_RECORDED":
            inv.docs.add("shipment")
            self._annotate_if_late(inv, p, seq)
        elif etype == "CARRIER_RECEIPT_RECORDED":
            inv.docs.add("carrier_receipt")
            self._annotate_if_late(inv, p, seq)
        elif etype == "ASSIGNMENT_NOTIFIED":
            inv.docs.add("assignment")
            inv.assignee = p["assignee"]
            inv.assignment_seq = seq
        elif etype == "FINANCING_APPROVED":
            inv.advances += money(p["amount"])
        elif etype == "REPAYMENT_RECORDED":
            inv.repayments += money(p["applied_principal"])
        elif etype == "RETURN_RECORDED":
            inv.returns += money(p["amount"])
        elif etype == "FREEZE_APPLIED":
            inv.frozen = True
            inv.freeze_reason = p.get("reason")
        elif etype == "FREEZE_RELEASED":
            inv.frozen = False
            inv.freeze_reason = None
        else:
            raise Rejection("UNKNOWN_EVENT", f"未知事件类型: {etype}")

    def _annotate_if_late(self, inv: InvoiceState, p: dict, seq: int):
        """迟到的承运人回执/装运凭证要标注其对当前余额的影响。"""
        if p.get("late"):
            inv.annotations.append({
                "seq": seq,
                "doc": p.get("doc_type", "carrier_receipt"),
                "late": True,
                "received_at": p.get("recorded_at"),
                "expected_by": p.get("expected_by"),
                "note": p.get("note", "凭证迟于融资/放款时点到达"),
                "impact": p.get(
                    "impact",
                    f"单据补全后可融资余额更新为 {inv.available} {inv.currency}"),
            })

    def _fold_split(self, parent: InvoiceState, p: dict, seq: int):
        total = ZERO
        for child in p["children"]:
            cid = child["invoice_id"]
            amt = money(child["amount"])
            total += amt
            child_state = InvoiceState(
                invoice_id=cid,
                currency=parent.currency,
                amount=amt,
                advance_rate=parent.advance_rate,
                docs=set(parent.docs),
                assignee=parent.assignee,
                assignment_seq=parent.assignment_seq,
                parent_id=parent.invoice_id,
                created_seq=seq,
                last_seq=seq,
            )
            self.invoices[cid] = child_state
            parent.child_ids.append(cid)
        if total != parent.amount - parent.returns:
            raise Rejection(
                "SPLIT_AMOUNT_MISMATCH",
                f"拆分金额合计 {total} 不等于母票应收净额 {parent.amount - parent.returns}")

    def _invoice_for(self, invoice_id, required):
        if not invoice_id:
            raise Rejection("INVOICE_ID_REQUIRED", "事件缺少 invoice_id")
        inv = self.invoices.get(invoice_id)
        if inv is None and required:
            raise Rejection("INVOICE_NOT_FOUND",
                            f"发票 {invoice_id} 尚未登记，无法应用事件")
        return inv

    # ---- 快照 -----------------------------------------------------------
    def snapshot(self, invoice_id: str, as_of: str | None = None) -> dict:
        inv = self._invoice_for(invoice_id, required=True)
        cny_rate = None
        fx = self.fx_at(inv.currency, as_of)
        if fx is not None:
            cny_rate = str(fx.rate_to_cny)
        return {
            "invoice_id": inv.invoice_id,
            "status": inv.status.value,
            "currency": inv.currency,
            "amount": str(inv.amount),
            "returns": str(inv.returns),
            "net_receivable": str(inv.net_receivable),
            "advance_rate": str(inv.advance_rate),
            "finance_limit": str(inv.finance_limit),
            "advances": str(inv.advances),
            "repayments": str(inv.repayments),
            "outstanding": str(inv.outstanding),
            "available": str(inv.available),
            "recourse_due": str(inv.recourse_due),
            "docs": sorted(inv.docs),
            "docs_complete": inv.docs_complete,
            "assignee": inv.assignee,
            "frozen": inv.frozen,
            "freeze_reason": inv.freeze_reason,
            "parent_id": inv.parent_id,
            "child_ids": inv.child_ids,
            "annotations": list(inv.annotations),
            "fx_rate_to_cny": cny_rate,
            "available_cny": str(self._cny(inv.available, inv.currency, as_of)),
            "outstanding_cny": str(self._cny(inv.outstanding, inv.currency, as_of)),
            "last_seq": inv.last_seq,
            "basis": inv.basis(),
        }

    def _cny(self, amount, currency, as_of):
        try:
            return self.to_cny(amount, currency, as_of or "9999-12-31T23:59:59Z")
        except Rejection:
            return None
