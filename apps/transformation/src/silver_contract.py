"""Pure normalization for the silver-flat-v1 contract."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from decimal import (
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    DecimalException,
    DivisionByZero,
    InvalidOperation,
    Overflow,
)
from typing import Any

CONTRACT_VERSION = "silver-flat-v1"
#: Issue recorded when an amount carried more than 18 significant fractional digits and was
#: quantized to the DECIMAL(38,18) column. Informational: the persisted value is resolved
#: (the dropped digits are below the 1e-18 quantum), so it does not make a row 'unresolved'.
AMOUNT_ROUNDED = "amount_rounded"
INFORMATIONAL_ISSUE_CODES = frozenset({AMOUNT_ROUNDED})
_AMOUNT_QUANTUM = Decimal(1).scaleb(-18)
#: Explicit arithmetic context for every amount computation: wide enough that a product of
#: two DECIMAL(38,18)-sized inputs is exact (the default context would silently round to 28
#: significant digits), HALF_EVEN, and trapping instead of silently producing NaN/Infinity.
_AMOUNT_CONTEXT = Context(
    prec=100,
    rounding=ROUND_HALF_EVEN,
    Emin=-999_999,
    Emax=999_999,
    traps=[InvalidOperation, DivisionByZero, Overflow],
)
ACTIONS = {
    "swap": "swap",
    "addliquidity": "add_liquidity",
    "increaseliquidity": "increase_liquidity",
    "decreaseliquidity": "decrease_liquidity",
    "claim": "claim",
}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class IdentityError(ValueError):
    """Raised when source fields cannot provide a semantic event identity."""


def _first(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row[name] is not None and str(row[name]).strip() != "":
            return row[name]
    return None


def _text(value: Any) -> str | None:
    return str(value).strip() if value is not None and str(value).strip() else None


def _ingested_at(value: Any) -> datetime | None:
    try:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo is not None else None
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            return (
                parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None
            )
    except (OverflowError, ValueError):
        return None
    return None


def _action(value: Any) -> str:
    if value is None:
        return "unknown"
    normalized = re.sub(r"[^a-z0-9]", "", str(value).strip().lower())
    return ACTIONS.get(normalized, "unknown")


def _decimal(value: Any) -> Decimal:
    if value is None or str(value).strip() == "":
        raise ValueError("missing")
    try:
        result = Decimal(str(value).strip())
    except (DecimalException, ValueError):
        raise ValueError("invalid") from None
    if not result.is_finite():
        raise ValueError("invalid")
    return result


def _decimal_text(value: Any) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    number = _decimal(value)
    parts = number.as_tuple()
    digits = list(parts.digits)
    exponent = parts.exponent
    if not isinstance(exponent, int):
        raise ValueError("invalid")
    if number == 0:
        return "0e0"
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1
    sign = "-" if parts.sign else ""
    return f"{sign}{''.join(map(str, digits))}e{exponent}"


def _timestamp(row: dict[str, Any], dataset: str) -> datetime | None:
    raw = _first(row, "timestamp", "tx_time", "txTime")
    if raw is None:
        return None
    try:
        number = _decimal(raw)
        if number < 0:
            return None
        adjusted = number.adjusted() if number != 0 else 0
        max_adjusted = 11 if dataset == "A" else 14
        min_adjusted = -6 if dataset == "A" else -3
        if adjusted > max_adjusted or (number != 0 and adjusted < min_adjusted):
            return None
        multiplier = 1_000_000 if dataset == "A" else 1_000
        numerator, denominator = number.as_integer_ratio()
        numerator *= multiplier
        if numerator % denominator:
            return None
        return EPOCH + timedelta(microseconds=numerator // denominator)
    except (ValueError, OverflowError, DecimalException):
        return None


def _identity_parts(row: dict[str, Any], dataset: str) -> list[Any]:
    if dataset not in {"A", "B"}:
        raise IdentityError("unsupported dataset")
    tx_hash = _first(row, "tx_hash", "hash")
    if dataset == "B":
        if tx_hash is None:
            raise IdentityError("missing B transaction hash")
        return [CONTRACT_VERSION, "B", str(tx_hash).strip()]
    if tx_hash is not None and str(tx_hash).strip().lower() != "hash":
        return [CONTRACT_VERSION, "A", str(tx_hash).strip()]

    event_time = _timestamp(row, "A")
    pool_id = _first(row, "pool_id")
    raw_action = _first(row, "action", "action_type")
    if event_time is None or pool_id is None or raw_action is None:
        raise IdentityError("incomplete A legacy composite key")
    identity_action = str(raw_action).strip().lower()
    parts: list[Any] = [
        CONTRACT_VERSION,
        "A-composite",
        str(pool_id),
        event_time.isoformat(timespec="microseconds"),
        _first(row, "from", "from_principal", "from_principal_id"),
        _first(row, "to", "to_principal", "to_principal_id"),
        identity_action,
    ]
    quantities: list[str] = []
    for slot in (0, 1):
        amount = _first(row, f"amount_token{slot}", f"token{slot}_amount")
        if amount is None:
            amount = _first(row, f"token{slot}_change", f"token{slot}_change_amount")
        try:
            normalized = _decimal_text(amount) if amount is not None else None
            if normalized is None:
                raise IdentityError("incomplete A composite quantity")
            quantities.append(normalized)
        except ValueError as exc:
            raise IdentityError("invalid A composite quantity") from exc
    if any(value is None for value in quantities):
        raise IdentityError("incomplete A legacy composite quantities")
    parts.extend(quantities)
    try:
        parts.append(_decimal_text(_first(row, "amount_usd")))
    except ValueError as exc:
        raise IdentityError("invalid A composite amount_usd") from exc
    return parts


def silver_identity(row: dict[str, Any], dataset: str) -> str:
    """Return namespaced SHA-256 event ID, independent of occurrence lineage and role mapping."""
    parts = _identity_parts(row, dataset.upper())
    encoded = json.dumps(parts, ensure_ascii=False, separators=(",", ":"), default=str)
    return f"{CONTRACT_VERSION}:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"


def _bounded_amount(
    value: Any, issues: list[dict[str, str]] | None = None, field: str | None = None
) -> Decimal:
    """Nonnegative DECIMAL(38,18) magnitude: at most 20 integral digits, scale <= 18.

    A value with more than 18 significant fractional digits (the canister feed serialises
    float64, so small magnitudes print e.g. 19-23 fractional digits) is QUANTIZED to 18
    places with ``ROUND_HALF_EVEN`` under the explicit ``_AMOUNT_CONTEXT`` (never the ambient
    thread context) and an ``amount_rounded`` issue is appended to ``issues``; the dropped
    digits are below the 1e-18 quantum of the column. Integral overflow is still rejected.
    """
    number = _decimal(value)
    if number < 0:
        raise ValueError("negative")
    if number == 0:
        return Decimal(0)
    parts = number.as_tuple()
    digits = list(parts.digits)
    exponent = parts.exponent
    if not isinstance(exponent, int):
        raise ValueError("out_of_range")
    if number.adjusted() >= 20:
        raise ValueError("out_of_range")
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1
    if max(0, -exponent) > 18:
        number = number.quantize(_AMOUNT_QUANTUM, context=_AMOUNT_CONTEXT)
        if issues is not None:
            issues.append({"code": AMOUNT_ROUNDED, "field": field or "amount"})
        # rounding can carry into a 21st integral digit
        if number != 0 and number.adjusted() >= 20:
            raise ValueError("out_of_range")
    return number


def _raw_b_slot(
    row: dict[str, Any], slot: int
) -> tuple[Decimal | None, Decimal | None, Decimal | None, list[dict[str, str]]]:
    issues: list[dict[str, str]] = []
    raw_in = _first(row, f"token{slot}_amount_in", f"token{slot}AmountIn")
    raw_out = _first(row, f"token{slot}_amount_out", f"token{slot}AmountOut")
    values: list[Decimal | None] = []
    for raw in (raw_in, raw_out):
        try:
            values.append(_bounded_amount(raw, issues, f"token{slot}_amount"))
        except ValueError as exc:
            code = (
                "missing_quantity"
                if str(exc) == "missing"
                else ("invalid_decimal" if str(exc) == "invalid" else "invalid_amount")
            )
            issues.append({"code": code, "field": f"token{slot}_amount"})
            values.append(None)
    amount_in, amount_out = values
    if (
        amount_in is not None
        and amount_out is not None
        and amount_in > 0
        and amount_out > 0
    ):
        issues.append({"code": "mixed_in_out", "field": f"token{slot}_amount"})
        return amount_in, amount_out, None, issues
    if amount_in is None or amount_out is None:
        magnitude = None
    else:
        magnitude = amount_in if amount_in > 0 else amount_out
    return amount_in, amount_out, magnitude, issues


def _raw_slot(
    row: dict[str, Any], dataset: str, slot: int
) -> tuple[Decimal | None, list[dict[str, str]]]:
    if dataset == "B":
        _, _, magnitude, issues = _raw_b_slot(row, slot)
        return magnitude, issues
    issues: list[dict[str, str]] = []
    amount = _first(row, f"amount_token{slot}", f"amountToken{slot}")
    change = _first(
        row,
        f"token{slot}_change",
        f"token{slot}ChangeAmount",
        f"token{slot}_change_amount",
    )
    try:
        left = _bounded_amount(amount, issues, f"token{slot}_amount")
        right = _bounded_amount(change, issues, f"token{slot}_amount")
    except ValueError as exc:
        code = (
            "missing_a_quantity"
            if str(exc) == "missing"
            else ("invalid_decimal" if str(exc) == "invalid" else "invalid_amount")
        )
        issues.append({"code": code, "field": f"token{slot}_amount"})
        return None, issues
    if left != right:
        issues.append(
            {"code": "amount_change_conflict", "field": f"token{slot}_amount"}
        )
        return None, issues
    return left, issues


def _amount_usd(
    row: dict[str, Any], dataset: str, token0_amount: Decimal | None
) -> tuple[Decimal | None, list[dict[str, str]]]:
    """Derive the persisted USD magnitude of the event.

    Dataset A carries it directly as a raw ``amount_usd`` column. Dataset B has no such
    column: prefer its raw ``token0_tx_value``/``token1_tx_value`` (whichever is present),
    else fall back to ``token0_price * token0_amount``. Always a nonnegative magnitude,
    never signed — missing/unavailable input is left ``None`` without an issue (legitimate
    for non-swap actions and gaps in either feed); a *present but unparsable* input is an
    issue so it is visible in ``quality_status`` rather than silently dropped.
    """
    issues: list[dict[str, str]] = []
    source = (
        _first(row, "amount_usd")
        if dataset == "A"
        else _first(row, "token0_tx_value", "token1_tx_value")
    )
    if source is None and dataset == "B":
        price = _first(row, "token0_price")
        if price is not None and token0_amount is not None:
            try:
                # Exact product under the explicit context (never the ambient 28-digit one);
                # _bounded_amount then quantizes it to 18 places HALF_EVEN.
                magnitude = _AMOUNT_CONTEXT.multiply(
                    token0_amount.copy_abs(), _decimal(price)
                )
            except (ValueError, DecimalException):
                issues.append({"code": "invalid_decimal", "field": "amount_usd"})
                return None, issues
            try:
                return _bounded_amount(magnitude, issues, "amount_usd"), issues
            except ValueError:
                issues.append({"code": "invalid_amount", "field": "amount_usd"})
                return None, issues
        return None, issues
    if source is None:
        return None, issues
    try:
        # copy_abs() is exact; abs() would round to the ambient context precision.
        magnitude = _decimal(source).copy_abs()
    except (ValueError, DecimalException):
        issues.append({"code": "invalid_decimal", "field": "amount_usd"})
        return None, issues
    try:
        return _bounded_amount(magnitude, issues, "amount_usd"), issues
    except ValueError:
        issues.append({"code": "invalid_amount", "field": "amount_usd"})
        return None, issues


def normalize_silver(
    raw_row: dict[str, Any], dataset: str, lineage: dict[str, Any]
) -> tuple[dict[str, Any] | None, list[dict[str, str]]]:
    """Map one A/B source observation to the flat fact; preserve uncertainty as issues/NULLs."""
    issues: list[dict[str, str]] = []
    dataset = dataset.upper()
    try:
        event_id = silver_identity(raw_row, dataset)
    except IdentityError as exc:
        return None, [{"code": "identity_unresolved", "detail": str(exc)}]
    ingested_at = (
        _ingested_at(lineage.get("ingested_at")) if isinstance(lineage, dict) else None
    )
    if (
        not isinstance(lineage, dict)
        or not lineage.get("lineage_ref")
        or ingested_at is None
    ):
        return None, [{"code": "lineage_unresolved", "field": "lineage_ref"}]

    action_raw = _first(raw_row, "action", "action_type", "actionType")
    action = _action(action_raw)
    if action == "unknown":
        issues.append({"code": "unknown_action", "field": "action"})
    event_timestamp = _timestamp(raw_row, dataset)
    if event_timestamp is None:
        issues.append({"code": "invalid_timestamp", "field": "event_timestamp"})

    pool_id = _first(raw_row, "pool_id", "poolId")
    if pool_id is None:
        issues.append({"code": "missing_pool_id", "field": "pool_id"})
    token_ids = [
        _text(_first(raw_row, "token0_id", "token0_ledger_id", "token0LedgerId")),
        _text(_first(raw_row, "token1_id", "token1_ledger_id", "token1LedgerId")),
    ]
    for slot, token_id in enumerate(token_ids):
        if token_id is None:
            issues.append(
                {"code": "missing_ledger_id", "field": f"token{slot}_ledger_id"}
            )

    amounts: list[Decimal | None] = [None, None]
    if action != "unknown":
        if dataset == "B":
            slots = []
            for slot in (0, 1):
                amount_in, amount_out, magnitude, slot_issues = _raw_b_slot(
                    raw_row, slot
                )
                slots.append((amount_in, amount_out))
                amounts[slot] = magnitude
                issues.extend(slot_issues)
            if action == "swap":
                positive_inputs = [
                    i
                    for i, pair in enumerate(slots)
                    if pair[0] is not None and pair[0] > 0
                ]
                positive_outputs = [
                    i
                    for i, pair in enumerate(slots)
                    if pair[1] is not None and pair[1] > 0
                ]
                shape_ok = all(value is not None for pair in slots for value in pair)
                shape_ok = (
                    shape_ok
                    and len(positive_inputs) == 1
                    and len(positive_outputs) == 1
                )
                if shape_ok:
                    shape_ok = positive_inputs[0] != positive_outputs[0]
                shape_ok = (
                    shape_ok
                    and token_ids[0] is not None
                    and token_ids[1] is not None
                    and token_ids[0] != token_ids[1]
                )
                if not shape_ok:
                    issues.append(
                        {"code": "invalid_swap_shape", "field": "token_amounts"}
                    )
                    amounts = [None, None]
            elif action in {"add_liquidity", "increase_liquidity"}:
                for i, pair in enumerate(slots):
                    amount_out = pair[1]
                    if amount_out is not None and amount_out != 0:
                        issues.append(
                            {"code": "action_shape", "field": f"token{i}_amount_out"}
                        )
                        amounts[i] = None
            elif action in {"decrease_liquidity", "claim"}:
                for i, pair in enumerate(slots):
                    amount_in = pair[0]
                    if amount_in is not None and amount_in != 0:
                        issues.append(
                            {"code": "action_shape", "field": f"token{i}_amount_in"}
                        )
                        amounts[i] = None
        else:
            for slot in (0, 1):
                amounts[slot], slot_issues = _raw_slot(raw_row, dataset, slot)
                issues.extend(slot_issues)
        if dataset == "A" and action == "swap":
            if (
                any(value is None or value <= 0 for value in amounts)
                or not all(token_ids)
                or token_ids[0] == token_ids[1]
            ):
                issues.append({"code": "invalid_swap_shape", "field": "token_amounts"})
                amounts = [None, None]

    amount_usd, usd_issues = _amount_usd(raw_row, dataset, amounts[0])
    issues.extend(usd_issues)

    principals = [
        _first(
            raw_row, "from", "from_principal", "from_principal_id", "fromPrincipalId"
        ),
        _first(raw_row, "to", "to_principal", "to_principal_id", "toPrincipalId"),
    ]
    record = {
        "event_id": event_id,
        "event_timestamp": event_timestamp,
        "action": action,
        "pool_id": _text(pool_id),
        "token0_ledger_id": token_ids[0],
        "token1_ledger_id": token_ids[1],
        "token0_amount": amounts[0],
        "token1_amount": amounts[1],
        "amount_usd": amount_usd,
        "from_principal": _text(principals[0]),
        "to_principal": _text(principals[1]),
        "contract_version": CONTRACT_VERSION,
        "lineage_ref": str(lineage["lineage_ref"]),
        # Informational issues (amount_rounded) leave the row resolved.
        "quality_status": (
            "unresolved"
            if any(issue["code"] not in INFORMATIONAL_ISSUE_CODES for issue in issues)
            else "ok"
        ),
        "ingested_at": ingested_at,
    }
    return record, issues
