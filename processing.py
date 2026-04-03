"""
Merge mismatch rows with matching lanes and rate-card / accessorial cost metadata.

Reads partly_df JSON outputs: mismatch_processed, Matched_Shipments_with,
Filtered_Rate_Card_with_Conditions_<RA>, accessorial_costs_<RA>.
Writes merged JSON and Excel.
"""
from __future__ import annotations

import glob
import json
import math
import os
import re
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_DOWN, Decimal
from typing import Any, Optional

import pandas as pd

from rate_card_input import (
    extract_ra_id_from_carrier_agreement,
    filtered_rate_card_json_path_for_ra_id,
    sanitize_filtered_rate_card_json_object,
)

from matching import (
    VALIDITY_DATE_COLUMNS,
    _get_lane_value_columns,
    _lane_valid_for_shipment_date,
    _parse_date_for_validity,
    compare_shipment_to_lane,
)

try:
    from mismatch_report import DEFAULT_MISMATCH_PROCESSED_JSON
except ImportError:
    DEFAULT_MISMATCH_PROCESSED_JSON = "mismatch_processed.json"

DEFAULT_MATCHED_JSON = "Matched_Shipments_with.json"
DEFAULT_OUTPUT_JSON = "mismatch_enriched.json"
DEFAULT_OUTPUT_XLSX = "mismatch_enriched.xlsx"

# Appended column names (after all mismatch fields)
COL_AGREEMENT_RA = "Agreement RA"
COL_BEST_LANES = "best_lane(s)"
COL_APPLIES_IF = "Applies_if"
COL_RATE_BY = "Rate_by"
COL_ROUNDING_RULE = "Rounding_rule"
COL_RATE_COST = "Rate cost"
COL_RATE_COST_CALCULATED = "Rate_cost_calculated"
COL_RATE_COST_COMMENT = "Rate_cost_comment"
COL_RATE_COST_FILE = "Rate_cost_file"
COL_CARRIER_RATE_FILE = "Carrier_rate_file"
COL_POSSIBLE_RATE_CARD_VALUE_USED = "Possible rate card value used"
COL_POSSIBLE_CARRIER_EXCHANGE_RATE = "Possible carrier exchange rate"
COL_POSSIBLE_CARRIER_USED_UNITS = "Possible_Carrier_used_Units"
COL_POSSIBLE_CARRIER_USED_UNITS_COMMENT = "Possible_Carrier_used_Units_comment"
COL_ANOTHER_RATE_CARD_CARRIER_USED_CRF = "Another_rate_card_Carrier_used(Carrier_rate_file)"
COL_ANOTHER_RATE_CARD_CARRIER_USED_INV = "Another_rate_card_Carrier_used(Inv cost)"
COL_BEST_MATCH_ANOTHER_RATE_CARD = "Best match from another rate card"
COL_ANOTHER_RC_LANE_VS_SHIPMENT = "Another rate card lane vs shipment"

MSG_NO_LANE_SAME_COST = "No lane with the same cost found"

# Mismatch export column (see mismatch_report.TRAILING_COST_COLUMNS)
COL_PRECALC_INV_CURR = "Pre-calc. cost (in inv curr)"
# Two spaces before '(' — matches mismatch export column name
COL_INV_STMT_INV_CURR = "Invoice statement cost  (in inv curr)"
COL_EXCHANGE_RATE = "Exchange rate"

# Fuel Surcharge: ``Rate_cost_file`` / ``Carrier_rate_file`` = 100 × fuel / base (same ETOF),
# where base ``Cost type`` matches accessorial ``Applies over cost`` (e.g. Transport cost).
FUEL_SURCHARGE_COST_TYPE_CANON = "fuel surcharge"

CARRIER_AGREEMENT_KEYS = ("Carrier agreement #", "Carrier Agreement #")


def _norm(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def _norm_etof(v: Any) -> str:
    return _norm(v).upper()


def _carrier_agreement_value(row: dict[str, Any]) -> Optional[str]:
    for k in CARRIER_AGREEMENT_KEYS:
        if k in row and row[k] is not None:
            s = _norm(row[k])
            if s:
                return row[k]
    for k, v in row.items():
        if isinstance(k, str) and "carrier" in k.lower() and "agreement" in k.lower():
            if v is not None and _norm(v):
                return v
    return None


def _merge_multi(values: list[Optional[str]], sep: str = "\n---\n") -> str:
    cleaned = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            cleaned.append(s)
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    return sep.join(cleaned)


def _load_json(path: str) -> Any:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _cache_filtered_rate_card(path: str, cache: dict[str, Any]) -> None:
    """Load ``Filtered_Rate_Card_with_Conditions_*.json`` with cost-definition sanitization."""
    if path in cache or not os.path.isfile(path):
        return
    data = _load_json(path)
    if isinstance(data, dict):
        data = sanitize_filtered_rate_card_json_object(data)
    cache[path] = data


def _accessorial_json_path(partly_df: str, ra_id: str) -> str:
    return os.path.join(partly_df, f"accessorial_costs_{ra_id}.json")


def _cost_type_matches_row_to_card(row_cost_type: str, card_cost_type: Any) -> bool:
    """
    True if mismatch ``Cost type`` matches a rate-card definition or lane ``Cost Type`` string.
    Exact match, or card name starts with row name followed by space or ``(`` (e.g. row
    ``Transport cost`` matches ``Transport cost (PTP cost per CBM. Min 1 CBM*)``).
    Row ``Transport cost`` also matches grouped titles ``Grouped cost: Transport cost (…)``.
    """
    r = _norm(row_cost_type)
    d = _norm(card_cost_type)
    if not r or not d:
        return False
    if d == r:
        return True
    if r.lower() == "transport cost":
        dl = d.lower()
        if "grouped cost:" in dl and "transport cost" in dl:
            return True
    dl = d.lower()
    if "grouped cost:" in dl:
        parts = re.split(r"(?i)grouped\s*cost\s*:", d, maxsplit=1)
        if len(parts) > 1:
            tail = _norm(parts[1]).lower()
            rl = r.lower()
            if rl in tail or tail.startswith(rl) or rl.startswith(tail):
                return True
    if len(d) > len(r) and d.startswith(r):
        return d[len(r) : len(r) + 1] in (" ", "(")
    return False


def _cost_defs_from_filtered(
    filtered: dict[str, Any],
    cost_type: str,
    row: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    """
    cost_definitions rows for this shipment cost type. Prefer exact ``Cost_type`` match;
    else prefix match (short row label vs long card label). When ``row`` is set, keep only
    definitions whose ``Applies_if`` passes :func:`applies_if_allows`.
    """
    defs = filtered.get("cost_definitions") or []
    ct = _norm(cost_type)
    exact = [d for d in defs if _norm(d.get("Cost_type")) == ct]
    candidates = exact if exact else [d for d in defs if _cost_type_matches_row_to_card(ct, d.get("Cost_type"))]
    if row is None:
        return candidates
    allowed = [d for d in candidates if applies_if_allows(d.get("Applies_if") or "", row)]
    allowed = [
        d
        for d in allowed
        if _applies_if_validity_period_contains_shipment(d.get("Applies_if") or "", row)
    ]
    if row is not None and _norm(cost_type).lower() == "transport cost":
        grouped = [
            d
            for d in allowed
            if d.get("grouped_cost")
            and "grouped cost:" in _norm(d.get("Cost_type")).lower()
            and "transport cost" in _norm(d.get("Cost_type")).lower()
        ]
        if grouped:
            allowed = [d for d in grouped if _grouped_transport_def_valid_for_ship_date(d, row)]
    return allowed


def _row_ship_date_as_date(row: dict[str, Any]) -> Any:
    """Shipment date as ``datetime.date`` (from ``SHIP_DATE``), or None."""
    s = _normalize_ship_date_for_matching(row)
    if not s:
        return None
    return _parse_date_for_validity(s)


def _applies_if_validity_period_contains_shipment(
    applies_if: str, row: dict[str, Any]
) -> bool:
    """
    False when ``Applies_if`` embeds a ``Validity period: to …`` / ``from …`` line that
    excludes :func:`_row_ship_date_as_date` (e.g. two Consolidated rows with different windows).
    """
    ship = _row_ship_date_as_date(row)
    if ship is None:
        return True
    text = str(applies_if or "").replace("\r\n", "\n")
    if not text.strip():
        return True
    m_to = re.search(
        r"(?i)validity\s+period\s*:\s*to\s*(\d{1,2}\.\d{1,2}\.\d{4})",
        text,
    )
    if m_to:
        end = _parse_date_for_validity(m_to.group(1))
        if end is not None and ship > end:
            return False
    m_from = re.search(
        r"(?i)validity\s+period\s*:\s*from\s*(\d{1,2}\.\d{1,2}\.\d{4})",
        text,
    )
    if m_from:
        start = _parse_date_for_validity(m_from.group(1))
        if start is not None and ship < start:
            return False
    return True


def _quantity_container_size_digits_from_rate_by(rate_by: str) -> str:
    """e.g. ``Quantity/Container 40`` → ``40``."""
    rb = _normalize_rate_by_string(rate_by)
    m = re.search(r"(?i)quantity[/\s]*container\s+(\d+)", rb)
    return m.group(1) if m else ""


def _measurement_lists_quantity_container_size(row: dict[str, Any], size_digits: str) -> bool:
    if not size_digits:
        return False
    for seg in _measurement_segments_normalized(row):
        s = seg.lower().replace(" ", "")
        if "container" not in s:
            continue
        if re.search(rf"container/.*?{re.escape(size_digits)}", s):
            return True
    return False


def _def_quantity_container_matches_measurement(d: dict[str, Any], row: dict[str, Any]) -> bool:
    """True if this definition is not Quantity/Container, or MEASUREMENT lists that container size."""
    rb = _normalize_rate_by_string(d.get("Rate_by") or "")
    if not _rate_by_is_quantity_container(rb):
        return True
    sz = _quantity_container_size_digits_from_rate_by(rb)
    if not sz:
        return True
    return _measurement_lists_quantity_container_size(row, sz)


def _pick_lane_cost_definition_and_rows(
    defs_for_type: list[dict[str, Any]],
    costs: list[dict[str, Any]],
    row: dict[str, Any],
) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
    """
    Choose one ``cost_definitions`` row and matching ``Costs`` lines (exact ``Cost Type``,
    index-aligned when several defs share one long name e.g. FCL 20 vs 40).
    """
    if not defs_for_type:
        return None, costs
    defs_work = [d for d in defs_for_type if isinstance(d, dict)]
    if not defs_work:
        return None, costs

    quantity_defs = [d for d in defs_work if _rate_by_is_quantity_container(_normalize_rate_by_string(d.get("Rate_by") or ""))]
    if len(quantity_defs) >= 1 and len(quantity_defs) == len(defs_work):
        matched = [d for d in defs_work if _def_quantity_container_matches_measurement(d, row)]
        if not matched:
            return None, costs
        d0 = matched[0]
    else:
        d0 = defs_work[0]

    ct_full = _norm(d0.get("Cost_type"))
    narrow = [c for c in costs if _norm(c.get("Cost Type")) == ct_full]
    if not narrow:
        return d0, costs

    siblings = [d for d in defs_work if _norm(d.get("Cost_type")) == ct_full]
    if len(siblings) > 1 and len(narrow) == len(siblings):
        try:
            idx = siblings.index(d0)
        except ValueError:
            idx = 0
        if 0 <= idx < len(narrow):
            return d0, [narrow[idx]]
    return d0, narrow


def rate_card_quantity_container_missing_comment(
    defs_for_type: list[dict[str, Any]],
    row: dict[str, Any],
    rate_by_merged: str,
) -> str:
    """
    When ``Rate_by`` is Quantity/Container but no definition's container size appears in
    ``MEASUREMENT`` (e.g. only ``Cost/FCL`` without ``Container/20``).
    """
    rb_m = _normalize_rate_by_string(rate_by_merged).lower()
    if "quantity" not in rb_m or "container" not in rb_m:
        return ""
    labels: list[str] = []
    for d in defs_for_type:
        if not isinstance(d, dict):
            continue
        r = _normalize_rate_by_string(d.get("Rate_by") or "")
        if not _rate_by_is_quantity_container(r):
            continue
        sz = _quantity_container_size_digits_from_rate_by(r)
        if sz:
            labels.append(f"Container {sz}")
    if not labels:
        return ""
    if any(_def_quantity_container_matches_measurement(d, row) for d in defs_for_type if isinstance(d, dict)):
        return ""
    seen: list[str] = []
    for lab in labels:
        if lab not in seen:
            seen.append(lab)
    return f"Not provided {' / '.join(seen)} in MEASUREMENT (required for Quantity/Container rate)."


def _grouped_transport_def_valid_for_ship_date(
    definition: dict[str, Any], row: dict[str, Any]
) -> bool:
    """``grouped_cost_details`` validity window contains :func:`_row_ship_date_as_date`."""
    det = definition.get("grouped_cost_details") or {}
    vf = det.get("validity_from")
    vt = det.get("validity_to")
    ship = _row_ship_date_as_date(row)
    if ship is None:
        return True
    df = _parse_date_for_validity(str(vf)) if vf else None
    dt = _parse_date_for_validity(str(vt)) if vt else None
    if df is not None and ship < df:
        return False
    if dt is not None and ship > dt:
        return False
    return True


def _norm_measurement_token(s: str) -> str:
    return s.strip().replace("\\/", "/").replace(" ", "")


def _units_for_container_rate_by(row: dict[str, Any], container_rate_by: str) -> Optional[float]:
    """
    ``Rate_by`` like ``Container/20CZ``: find the same token in ``MEASUREMENT`` segments
    and return the paired ``UNITS_MEASUREMENT`` value.
    """
    if not container_rate_by or not str(container_rate_by).strip():
        return None
    target = _norm_measurement_token(str(container_rate_by)).lower()
    meas = row.get("MEASUREMENT") or row.get("Measurement")
    units = row.get("UNITS_MEASUREMENT") or row.get("Units_measurement")
    if meas is None or units is None:
        return None
    if isinstance(meas, float) and pd.isna(meas):
        return None
    if isinstance(units, float) and pd.isna(units):
        return None
    mparts = [p.strip() for p in str(meas).split(";") if p.strip()]
    uparts = [p.strip() for p in str(units).split(";") if p.strip()]
    if not mparts or len(mparts) != len(uparts):
        return None
    for mseg, useg in zip(mparts, uparts):
        if _norm_measurement_token(mseg).lower() == target:
            try:
                return float(useg)
            except (TypeError, ValueError):
                return None
    return None


def _container_rate_by_size_label_for_comment(container_rate_by: str) -> str:
    """``Container/20CN`` → ``Container 20`` for Rate_cost_comment text; else the full token."""
    s = _norm(container_rate_by)
    m = re.match(r"(?i)^container/(\d{2})", s)
    if m:
        return f"Container {m.group(1)}"
    return s


def _cost_line_valid_for_ship_date(cost_line: dict[str, Any], ship: Any) -> bool:
    """Lane ``Costs`` row: ``Validity from`` / ``Validity to`` (DD.MM.YYYY) vs shipment date."""
    if ship is None:
        return True
    vf = cost_line.get("Validity from") or cost_line.get("Validity From")
    vt = cost_line.get("Validity to") or cost_line.get("Validity To")
    if not vf and not vt:
        return True
    df = _parse_date_for_validity(str(vf)) if vf else None
    dt = _parse_date_for_validity(str(vt)) if vt else None
    if df is None and dt is None:
        return True
    if df is not None and ship < df:
        return False
    if dt is not None and ship > dt:
        return False
    return True


def _lane_cost_for_grouped_sub(
    lane: dict[str, Any],
    sub_cost_name: str,
    grouped_under: str,
    ship: Any,
) -> Optional[dict[str, Any]]:
    """Pick the lane ``Costs`` line for this sub-cost, grouped block, and shipment date."""
    gu_want = _norm(grouped_under)
    sn_want = _norm(sub_cost_name)
    for c in lane.get("Costs") or []:
        if _norm(c.get("Cost Type")) != sn_want:
            continue
        gu = _norm(c.get("Grouped under") or c.get("Grouped Under") or "")
        if gu and gu != gu_want:
            continue
        if not _cost_line_valid_for_ship_date(c, ship):
            continue
        return c
    return None


def _cost_blocks_from_accessorial(
    accessorial: list[dict[str, Any]], cost_type: str
) -> list[dict[str, Any]]:
    ct = _norm(cost_type)
    return [b for b in accessorial if _norm(b.get("Cost type")) == ct]


def _is_fuel_surcharge_cost_type(cost_type: Optional[str]) -> bool:
    return _norm(cost_type or "").lower() == FUEL_SURCHARGE_COST_TYPE_CANON


def _accessorial_applies_over_cost(blocks: list[dict[str, Any]]) -> str:
    """From accessorial JSON (e.g. % over Transport): base cost name for the same ETOF."""
    for b in blocks:
        v = b.get("Applies over cost")
        if v is not None and _norm(str(v)):
            return _norm(str(v))
    return ""


def _build_etof_cost_type_row_index(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Map (ETOF, Cost type) → row (last wins if duplicates)."""
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        etof = _norm_etof(r.get("ETOF_NUMBER") or r.get("ETOF"))
        ct = _norm(r.get("Cost type"))
        if etof and ct:
            out[(etof, ct)] = r
    return out


def _row_for_etof_and_cost_type(
    index: dict[tuple[str, str], dict[str, Any]],
    etof: str,
    cost_type: str,
) -> Optional[dict[str, Any]]:
    """Same-ETOF row for ``Cost type``; exact match first, then case-insensitive."""
    if not etof or not cost_type:
        return None
    ct = _norm(cost_type)
    hit = index.get((etof, ct))
    if hit is not None:
        return hit
    low = ct.lower()
    for (e, c), row in index.items():
        if e == etof and c.lower() == low:
            return row
    return None


def compute_fuel_surcharge_rate_cost_file(
    fuel_row: dict[str, Any],
    base_cost_row: dict[str, Any],
) -> Optional[float]:
    """
    ``100 * Pre-calc fuel / Pre-calc base`` (same ETOF), then ÷ Exchange rate like other file rates.
    """
    pc_f = _precalc_cost_inv_curr(fuel_row)
    pc_b = _precalc_cost_inv_curr(base_cost_row)
    if pc_f is None or pc_b is None:
        return None
    if abs(float(pc_b)) < 1e-12:
        return None
    val = 100.0 * float(pc_f) / float(pc_b)
    return _divide_by_exchange_rate(val, fuel_row)


def compute_fuel_surcharge_carrier_rate_file(
    fuel_row: dict[str, Any],
    base_cost_row: dict[str, Any],
) -> Optional[float]:
    """``100 * Invoice fuel / Invoice base`` (same ETOF), then ÷ Exchange rate."""
    inv_f = _invoice_statement_cost_inv_curr(fuel_row)
    inv_b = _invoice_statement_cost_inv_curr(base_cost_row)
    if inv_f is None or inv_b is None:
        return None
    if abs(float(inv_b)) < 1e-12:
        return None
    val = 100.0 * float(inv_f) / float(inv_b)
    return _divide_by_exchange_rate(val, fuel_row)


def _fields_from_filtered_definitions(matches: list[dict[str, Any]]) -> tuple[str, str, str]:
    if not matches:
        return "", "", ""
    applies = _merge_multi([m.get("Applies_if") for m in matches])
    rates = _merge_multi([m.get("Rate_by") for m in matches])
    rounds = _merge_multi([m.get("Rounding_rule") for m in matches])
    return applies, rates, rounds


def _normalize_rate_by_string(rate_by: str) -> str:
    """Strip Excel/XML artifacts (e.g. ``_x000D_`` carriage-return placeholders) from Rate by."""
    if not rate_by:
        return ""
    s = str(rate_by).replace("_x000D_", "").replace("\r", "").replace("\n", " ")
    return " ".join(s.split()).strip()


def _fields_from_accessorial_blocks(blocks: list[dict[str, Any]]) -> tuple[str, str, str]:
    """Accessorial uses 'Rate by', tier 'Applies if'; rounding rarely present."""
    if not blocks:
        return "", "", ""
    rate_bys: list[str] = []
    applies: list[str] = []
    rounds: list[str] = []
    for b in blocks:
        rb = b.get("Rate by")
        if rb is not None and _norm(rb):
            rate_bys.append(_normalize_rate_by_string(str(rb).strip()))
        for t in b.get("Tiers") or []:
            a = t.get("Applies if") or t.get("Applies_if")
            if a is not None and _norm(a):
                applies.append(str(a).strip())
        r = b.get("Rounding_rule") or b.get("Rounding rule")
        if r is not None and _norm(r):
            rounds.append(str(r).strip())
    return (
        _merge_multi(applies),
        _merge_multi(rate_bys),
        _merge_multi(rounds),
    )


def _shipment_service(row: dict[str, Any]) -> str:
    return _norm(row.get("SERVICE") or row.get("SERVICE_ETOF") or row.get("SERVICE_ISD"))


def _chargeable_weight(row: dict[str, Any]) -> Optional[float]:
    w = row.get("CHARGEABLE WEIGHT")
    if w is None or (isinstance(w, float) and pd.isna(w)):
        return None
    try:
        return float(w)
    except (TypeError, ValueError):
        return None


def _weight_etof(row: dict[str, Any]) -> Optional[float]:
    """``WEIGHT_ETOF`` when ``Rate_by`` is ``Weight/kg`` (see :func:`_rate_by_is_weight_kg_etof`)."""
    w = row.get("WEIGHT_ETOF")
    if w is None or (isinstance(w, float) and pd.isna(w)):
        return None
    try:
        return float(w)
    except (TypeError, ValueError):
        return None


def _cbm_value(row: dict[str, Any]) -> Optional[float]:
    w = row.get("CBM") or row.get("cbm")
    if w is None or (isinstance(w, float) and pd.isna(w)):
        return None
    try:
        return float(w)
    except (TypeError, ValueError):
        return None


def apply_rounding_for_cbm(cbm: float, rounding_rule: str) -> float:
    """
    Typical: ``Upper to 1 (range <=1); No rounding (range >1)`` — volumes above 1 CBM unchanged;
    at or below 1 CBM, enforce minimum 1 CBM when the rule mentions upper/range <=1.
    """
    rr = (rounding_rule or "").lower()
    if not rr.strip():
        return cbm
    if cbm > 1.0 + 1e-9:
        return cbm
    if "upper" in rr and "<=1" in rr.replace(" ", ""):
        return float(max(1.0, cbm))
    return cbm


def _rate_by_is_volume_cbm(rate_by: str) -> bool:
    rb = _normalize_rate_by_string(rate_by).lower()
    return "volume" in rb and "cbm" in rb


def _rate_by_is_cost_measurement_token(rate_by: str) -> bool:
    """``Cost/CBS``, ``Cost/CON``, …: unit count comes from the matching MEASUREMENT segment."""
    rb = _normalize_rate_by_string(rate_by).lower()
    return rb.startswith("cost/")


def _units_for_measurement_token_rate_by(
    row: dict[str, Any], rate_by: str
) -> Optional[float]:
    """UNITS_MEASUREMENT segment aligned with ``Rate_by`` (same pairing as ``Container/…``)."""
    return _units_for_container_rate_by(row, rate_by)


def _measurement_segments_normalized(row: dict[str, Any]) -> list[str]:
    """Semicolon-separated MEASUREMENT tokens (normalized slashes)."""
    m = row.get("MEASUREMENT") or row.get("Measurement") or ""
    if m is None or (isinstance(m, float) and pd.isna(m)):
        return []
    parts = [p.strip().replace("\\/", "/") for p in str(m).split(";") if p.strip()]
    return parts


def quantity_container_units_from_row(row: dict[str, Any]) -> Optional[float]:
    """
    For ``Rate by`` Quantity/Container: sum UNITS_MEASUREMENT for each MEASUREMENT segment
    whose label contains ``Container`` (case-insensitive). Pairs MEASUREMENT and UNITS_MEASUREMENT by position.
    """
    meas = row.get("MEASUREMENT") or row.get("Measurement")
    units = row.get("UNITS_MEASUREMENT") or row.get("Units_measurement")
    if meas is None or units is None:
        return None
    if isinstance(meas, float) and pd.isna(meas):
        return None
    if isinstance(units, float) and pd.isna(units):
        return None
    mparts = [p.strip().replace("\\/", "/") for p in str(meas).split(";") if p.strip()]
    uparts = [p.strip() for p in str(units).split(";") if p.strip()]
    if not mparts or len(mparts) != len(uparts):
        return None
    total = 0.0
    for mseg, useg in zip(mparts, uparts):
        if "container" not in mseg.lower():
            continue
        try:
            total += float(useg)
        except (TypeError, ValueError):
            return None
    return total


def _rate_by_is_quantity_container(rate_by: str) -> bool:
    rb = _normalize_rate_by_string(rate_by).lower()
    return "quantity" in rb and "container" in rb


def _equipment_type_raw(row: dict[str, Any]) -> Optional[str]:
    """Accessorial ``Equipment Type`` maps to shipment ``CONT_LOAD`` (equipment / load type)."""
    for k in ("CONT_LOAD", "Cont_load", "Equipment type", "Equipment Type"):
        v = row.get(k)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def _equipment_type_display_for_comment(row: dict[str, Any]) -> str:
    """Human-readable equipment for Rate_cost_comment (e.g. ``FCL - 20CZ``)."""
    raw = _equipment_type_raw(row)
    if raw is None:
        return "(not provided)"
    s = raw.replace("\\/", "/")
    s = re.sub(r"/+", " - ", s)
    return s.strip()


def accessorial_tier_applies(row: dict[str, Any], applies_if_tier: str) -> bool:
    """
    Evaluate accessorial tier ``Applies if`` (AND of parts): e.g.
    ``Cost/LCK is available and Service does not equal to 'SPECIAL'``,
    ``Equipment Type contains 'BCL'`` (``Equipment Type`` = shipment ``CONT_LOAD``).
    Empty → True.
    """
    if not applies_if_tier or not str(applies_if_tier).strip():
        return True
    cl = str(applies_if_tier).strip()
    cl = re.sub(r"^\d+\.\s*", "", cl)
    # Split on " and " preserving Service / Cost phrases
    parts = re.split(r"\s+and\s+", cl, flags=re.IGNORECASE)
    service = _shipment_service(row)
    segs = _measurement_segments_normalized(row)
    segs_l = [s.lower() for s in segs]

    for part in parts:
        p = part.strip()
        if not p:
            continue
        low = p.lower()
        # Equipment Type contains 'X' (CONT_LOAD)
        m_et = re.search(
            r"Equipment\s+Type\s+contains\s+['\"]([^'\"]+)['\"]",
            p,
            re.I,
        )
        if m_et:
            needle = m_et.group(1).strip()
            raw = _equipment_type_raw(row)
            if raw is None or needle.lower() not in raw.lower():
                return False
            continue
        # Cost/XXX is available
        m_cost = re.search(r"(Cost/[^\s]+)\s+is\s+available", p, re.I)
        if m_cost:
            code = m_cost.group(1).replace("\\/", "/").strip()
            if code.lower() not in segs_l and code not in segs:
                return False
            continue
        # Service does not equal to 'X'
        m_ne = re.search(
            r"Service\s+does\s+not\s+equal\s+to\s+['\"]([^'\"]+)['\"]",
            p,
            re.I,
        )
        if m_ne:
            forbidden = m_ne.group(1).strip()
            if service == forbidden:
                return False
            continue
        if "service" in low and "does not equal" in low:
            m_ne2 = re.search(r"['\"]([^'\"]+)['\"]\s*$", p)
            if m_ne2 and service == m_ne2.group(1).strip():
                return False
            continue
        # Service equals 'X'
        if re.search(r"Service\s+equals", p, re.I):
            codes = re.findall(r"'([^']+)'", p)
            if codes and (not service or service not in codes):
                return False
            continue
    return True


def accessorial_tier_applies_excluding_equipment(
    row: dict[str, Any], applies_if_tier: str
) -> bool:
    """
    Same as :func:`accessorial_tier_applies` but ignores ``Equipment Type contains`` subclauses
    (treats them as satisfied). Used to decide if the only blocking reason is equipment type.
    """
    if not applies_if_tier or not str(applies_if_tier).strip():
        return True
    cl = str(applies_if_tier).strip()
    cl = re.sub(r"^\d+\.\s*", "", cl)
    parts = re.split(r"\s+and\s+", cl, flags=re.IGNORECASE)
    service = _shipment_service(row)
    segs = _measurement_segments_normalized(row)
    segs_l = [s.lower() for s in segs]

    for part in parts:
        p = part.strip()
        if not p:
            continue
        low = p.lower()
        if re.search(r"Equipment\s+Type\s+contains", p, re.I):
            continue
        # Cost/XXX is available
        m_cost = re.search(r"(Cost/[^\s]+)\s+is\s+available", p, re.I)
        if m_cost:
            code = m_cost.group(1).replace("\\/", "/").strip()
            if code.lower() not in segs_l and code not in segs:
                return False
            continue
        # Service does not equal to 'X'
        m_ne = re.search(
            r"Service\s+does\s+not\s+equal\s+to\s+['\"]([^'\"]+)['\"]",
            p,
            re.I,
        )
        if m_ne:
            forbidden = m_ne.group(1).strip()
            if service == forbidden:
                return False
            continue
        if "service" in low and "does not equal" in low:
            m_ne2 = re.search(r"['\"]([^'\"]+)['\"]\s*$", p)
            if m_ne2 and service == m_ne2.group(1).strip():
                return False
            continue
        # Service equals 'X'
        if re.search(r"Service\s+equals", p, re.I):
            codes = re.findall(r"'([^']+)'", p)
            if codes and (not service or service not in codes):
                return False
            continue
    return True


def _equipment_type_subclause_fails(row: dict[str, Any], applies_if_tier: str) -> bool:
    """True if ``applies_if_tier`` includes an ``Equipment Type contains`` clause that fails on ``CONT_LOAD``."""
    if not applies_if_tier or not str(applies_if_tier).strip():
        return False
    cl = re.sub(r"^\d+\.\s*", "", str(applies_if_tier).strip())
    parts = re.split(r"\s+and\s+", cl, flags=re.IGNORECASE)
    for part in parts:
        m_et = re.search(
            r"Equipment\s+Type\s+contains\s+['\"]([^'\"]+)['\"]",
            part.strip(),
            re.I,
        )
        if not m_et:
            continue
        needle = m_et.group(1).strip()
        raw = _equipment_type_raw(row)
        if raw is None or needle.lower() not in raw.lower():
            return True
    return False


def accessorial_equipment_type_not_met_comment(
    blocks: list[dict[str, Any]],
    cost_type: str,
    lane_nums: list[str],
    row: dict[str, Any],
) -> str:
    """
    When a tier is blocked solely because ``Equipment Type contains …`` does not match ``CONT_LOAD``.
    """
    ct = _norm(cost_type)
    seen: set[str] = set()
    for ln in lane_nums:
        lane_key = str(ln).strip() if ln is not None else ""
        if not lane_key or lane_key in seen:
            continue
        seen.add(lane_key)
        for b in blocks:
            if _norm(b.get("Cost type")) != ct:
                continue
            for t in b.get("Tiers") or []:
                if str(t.get("Lane #", "")).strip() != lane_key:
                    continue
                applies = t.get("Applies if") or t.get("Applies_if") or ""
                if not re.search(r"Equipment\s+Type\s+contains", applies, re.I):
                    continue
                if not _equipment_type_subclause_fails(row, applies):
                    continue
                if not accessorial_tier_applies_excluding_equipment(row, applies):
                    continue
                disp = _equipment_type_display_for_comment(row)
                return (
                    f"Could not apply cost, applies if {applies.strip()}, "
                    f"while provided Equipment type {disp}"
                )
    return ""


def accessorial_measurement_missing_comment(
    blocks: list[dict[str, Any]],
    cost_type: str,
    lane_num: str,
    row: dict[str, Any],
) -> str:
    """
    If a tier requires ``Cost/… is available`` but the code is not in MEASUREMENT, return
    the standard comment. Checks tiers for ``lane_num`` first, then any tier in the block
    (accessorial often uses Lane # 1 while ``best_lane(s)`` is another lane).
    """
    ct = _norm(cost_type)

    def _scan_tiers(tiers: list[dict[str, Any]], lane_filter: Optional[str]) -> str:
        for t in tiers:
            if lane_filter is not None and str(t.get("Lane #", "")).strip() != lane_filter:
                continue
            applies = t.get("Applies if") or t.get("Applies_if") or ""
            reqs = re.findall(r"(Cost/[^\s]+)\s+is\s+available", applies, re.I)
            if not reqs:
                continue
            segs = _measurement_segments_normalized(row)
            segs_l = [s.lower() for s in segs]
            for code in reqs:
                c = code.replace("\\/", "/").strip()
                if c.lower() not in segs_l and c not in segs:
                    return "Measurement is not present for the calculation."
        return ""

    for b in blocks:
        if _norm(b.get("Cost type")) != ct:
            continue
        tiers = b.get("Tiers") or []
        msg = _scan_tiers(tiers, lane_num)
        if msg:
            return msg
        msg = _scan_tiers(tiers, None)
        if msg:
            return msg
    return ""


def _precalc_cost_inv_curr(row: dict[str, Any]) -> Optional[float]:
    v = row.get(COL_PRECALC_INV_CURR)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _invoice_statement_cost_inv_curr(row: dict[str, Any]) -> Optional[float]:
    v = row.get(COL_INV_STMT_INV_CURR)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def ceil_two_decimals_up(x: float) -> float:
    """
    If the value is already exact at 2 decimal places, return it unchanged.

    Otherwise round **up** to 2 decimal places (e.g. 37.231 → 37.24).

    Uses ``Decimal`` so binary-float noise (e.g. on 74.43) does not change amounts that
    are already valid 2dp money values.
    """
    try:
        d = Decimal(str(float(x)))
    except (ArithmeticError, ValueError, TypeError):
        return float(x)
    q = Decimal("0.01")
    low = d.quantize(q, rounding=ROUND_DOWN)
    high = d.quantize(q, rounding=ROUND_CEILING)
    if low == high:
        return float(low)
    return float(high)


def _tiers_matching_target_price(
    amount: Optional[float],
    filtered: Optional[dict[str, Any]],
    cost_type: Optional[str],
) -> list[tuple[str, str]]:
    """
    (Lane #, Weight Bracket) pairs whose tier ``Price`` equals ``amount`` at 2 decimal places.
    Order follows the rate card; pairs are deduplicated.
    """
    if filtered is None or cost_type is None or amount is None:
        return []
    try:
        target = round(float(amount) + 1e-8, 2)
    except (TypeError, ValueError):
        return []
    ct = _norm(str(cost_type))
    if not ct:
        return []
    out: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for lane in filtered.get("rate_card_data") or []:
        lane_num = str(lane.get("Lane #", "")).strip()
        for c in lane.get("Costs") or []:
            if not _cost_type_matches_row_to_card(ct, c.get("Cost Type")):
                continue
            p = c.get("Price")
            if p is None:
                continue
            try:
                pf = float(p)
            except (TypeError, ValueError):
                continue
            if round(pf, 2) != target:
                continue
            wb = str(c.get("Weight Bracket", "")).strip()
            key = (lane_num, wb)
            if key in seen:
                continue
            seen.add(key)
            out.append(key)
    return out


def _match_cost_tiers_body_for_target_price(
    amount: Optional[float],
    filtered: Optional[dict[str, Any]],
    cost_type: Optional[str],
) -> str:
    """
    Compare ``amount`` to tier ``Price`` at **2 decimal places** (half-up style via ``round``).
    We do **not** use :func:`ceil_two_decimals_up` here: division-derived floats (e.g.
    invoice ÷ weight) are often slightly above the mathematical value in binary, and ceil
    would become 3.43 while the rate card shows 3.42.
    Returns ``"; ".join(matches)``, ``MSG_NO_LANE_SAME_COST``, or ``""`` if inputs unusable.
    """
    if filtered is None or cost_type is None:
        return ""
    if amount is None:
        return ""
    pairs = _tiers_matching_target_price(amount, filtered, cost_type)
    if not pairs:
        return MSG_NO_LANE_SAME_COST
    return "; ".join(f"Lane #: {ln}, Weight Bracket: {wb}" for ln, wb in pairs)


def primary_rate_card_alternate_lane_tier_strings(
    amount: Optional[float],
    filtered: Optional[dict[str, Any]],
    cost_type: Optional[str],
    best_lane_str: str,
    agreement_ra: Optional[str],
) -> str:
    """
    Same agreement RA: lanes whose tier price matches ``amount`` but whose lane id differs
    from the first ``best_lane(s)`` id. Used when there is no secondary rate card file but
    another lane (e.g. different Equipment Type) carries the same tier price.
    """
    if not isinstance(filtered, dict):
        return ""
    best_ln = _first_lane_number(best_lane_str) or ""
    pairs = _tiers_matching_target_price(amount, filtered, cost_type)
    alt = [(ln, wb) for ln, wb in pairs if ln and ln != best_ln]
    if not alt:
        return ""
    body = "; ".join(f"Lane #: {ln}, Weight Bracket: {wb}" for ln, wb in alt)
    return _prefix_rate_agreement_id(body, agreement_ra)


def price_matched_alternate_lanes_vs_shipment_note(
    row: dict[str, Any],
    filtered: dict[str, Any],
    agreement_ra: Optional[str],
    amount: Optional[float],
    cost_type: Optional[str],
    best_lane_str: str,
    etof_mappings: dict[str, str],
) -> str:
    """
    For lanes on the **primary** RA that match ``amount`` but are not the chosen best lane,
    run ``compare_shipment_to_lane`` so Equipment Type vs ``CONT_LOAD`` (via ``etof_mappings``)
    and other differences appear under ``Another rate card lane vs shipment``.
    """
    if not etof_mappings:
        return ""
    best_ln = _first_lane_number(best_lane_str) or ""
    pairs = _tiers_matching_target_price(amount, filtered, cost_type)
    alt_lanes: list[str] = []
    seen: set[str] = set()
    for ln, _wb in pairs:
        k = str(ln).strip()
        if not k or k == best_ln or k in seen:
            continue
        seen.add(k)
        alt_lanes.append(k)
    if not alt_lanes:
        return ""
    rate_card_data = filtered.get("rate_card_data") or []
    conditions_list = filtered.get("conditions") or []
    business_rules_list = filtered.get("business_rules") or []
    ship_view = shipment_view_for_rate_card_compare(row, etof_mappings)
    ship_date_str = _normalize_ship_date_for_matching(row)
    lines: list[str] = []
    for lane_num in alt_lanes:
        lane = _find_lane(rate_card_data, str(lane_num).strip())
        if not lane:
            continue
        value_columns = _lane_scalar_value_columns(lane)
        _dc, diffs = compare_shipment_to_lane(
            ship_view, lane, conditions_list, business_rules_list, value_columns
        )
        msgs = _format_lane_vs_shipment_messages(lane, ship_view, diffs)
        if ship_date_str and not _lane_valid_for_shipment_date(lane, ship_date_str):
            msgs.append("shipment date is outside Valid from–Valid to")
        if not msgs:
            continue
        et_msgs = [m for m in msgs if m.startswith("Equipment Type:")]
        rest = [m for m in msgs if not m.startswith("Equipment Type:")]
        if et_msgs:
            inner = et_msgs[0].replace("Equipment Type:", "", 1).strip()
            core = (
                f"Cost is provided for Equipment Type: {inner} (lane {lane_num})"
            )
        else:
            core = f"Cost is provided for same-tier lane {lane_num}: " + "; ".join(
                msgs
            )
        if rest:
            core += "; " + "; ".join(rest)
        lines.append(core)
    return "\n".join(lines)


def _prefix_rate_agreement_id(body: str, rate_agreement_id: Optional[str]) -> str:
    ra = (rate_agreement_id or "").strip()
    if not ra:
        return body
    return f"[{ra}] {body}"


def _money_amount_display_2dp(x: float) -> str:
    """Stable 2dp display for labels (avoids binary float tails like 3.4200000000000004)."""
    r = round(float(x), 2)
    if abs(r - int(r)) < 1e-9:
        return str(int(round(r)))
    s = f"{r:.2f}"
    if s.endswith(".00"):
        return s[:-3]
    return s.rstrip("0").rstrip(".") if "." in s else s


def format_possible_rate_card_value_used(
    carrier_rate_file: Optional[float],
    filtered: Optional[dict[str, Any]],
    cost_type: Optional[str],
    rate_agreement_id: Optional[str] = None,
    invoice_statement_inv_curr: Optional[float] = None,
) -> str:
    """
    Find rate-card ``Price`` tiers matching this cost type for:

    - **Carrier_rate_file** (invoice per unit after weight/volume rules), and
    - **Invoice statement cost (in inv curr)** when provided (total invoice — often no tier
      match for per-kg cards, but the column still reports the lookup).

    Each lookup uses 2 decimal money comparison. If ``rate_agreement_id`` is set, prefix once
    with ``[RA########]``.
    """
    segments: list[str] = []
    if carrier_rate_file is not None:
        m = _match_cost_tiers_body_for_target_price(carrier_rate_file, filtered, cost_type)
        lab = _money_amount_display_2dp(float(carrier_rate_file))
        segments.append(f"Carrier rate file ({lab}): {m}")
    if invoice_statement_inv_curr is not None:
        m = _match_cost_tiers_body_for_target_price(
            invoice_statement_inv_curr, filtered, cost_type
        )
        lab = _money_amount_display_2dp(float(invoice_statement_inv_curr))
        segments.append(f"Invoice amount ({lab}): {m}")
    if not segments:
        return ""
    body = " | ".join(segments)
    return _prefix_rate_agreement_id(body, rate_agreement_id)


def _filtered_rate_card_ra_ids_in_partly_df(partly_df: str) -> list[str]:
    """RA ids from ``Filtered_Rate_Card_with_Conditions_RA*.json`` in ``partly_df``."""
    pattern = os.path.join(partly_df, "Filtered_Rate_Card_with_Conditions_RA*.json")
    ids: list[str] = []
    for path in glob.glob(pattern):
        base = os.path.basename(path)
        m = re.search(r"Filtered_Rate_Card_with_Conditions_(RA\d+)\.json$", base, re.I)
        if m:
            ids.append(m.group(1).upper())
    return sorted(set(ids))


def _secondary_ra_id_for_row(agreement_ra: Optional[str], partly_df: str) -> Optional[str]:
    """
    When more than one filtered rate card JSON exists, return one other RA (alphabetically
    first among those not equal to this row's Agreement RA).
    """
    all_ra = _filtered_rate_card_ra_ids_in_partly_df(partly_df)
    if len(all_ra) <= 1 or not agreement_ra:
        return None
    cur = str(agreement_ra).strip().upper()
    others = [r for r in all_ra if r != cur]
    if not others:
        return None
    return others[0]


def another_rate_card_lane_match_for_amount(
    amount: Optional[float],
    agreement_ra: Optional[str],
    partly_df: str,
    cost_type: Optional[str],
    rate_card_cache: dict[str, Any],
) -> str:
    """
    Match ``amount`` to secondary ``Filtered_Rate_Card_with_Conditions_<RA>.json`` (same
    tier logic as Possible rate card value used). Empty when only one RA file or no secondary.
    """
    sec_ra = _secondary_ra_id_for_row(agreement_ra, partly_df)
    if not sec_ra:
        return ""
    path = filtered_rate_card_json_path_for_ra_id(sec_ra, partly_df)
    _cache_filtered_rate_card(path, rate_card_cache)
    filtered2 = rate_card_cache.get(path)
    if not isinstance(filtered2, dict):
        return ""
    body = _match_cost_tiers_body_for_target_price(amount, filtered2, cost_type)
    if body == "":
        return ""
    return _prefix_rate_agreement_id(body, sec_ra)


def _divide_by_exchange_rate(numerator: Optional[float], row: dict[str, Any]) -> Optional[float]:
    """Rate_cost_file / Carrier_rate_file: divide result by ``Exchange rate`` (1 if missing)."""
    if numerator is None:
        return None
    v = row.get(COL_EXCHANGE_RATE)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return float(numerator) / 1.0
    try:
        er = float(v)
    except (TypeError, ValueError):
        return float(numerator) / 1.0
    if abs(er) < 1e-12:
        return None
    return float(numerator) / er


def _json_numeric_output(v: Optional[float]) -> Any:
    if v is None:
        return None
    if abs(v - round(v)) < 1e-9:
        return int(round(v))
    return v


def _json_money_two_dp(v: Optional[float]) -> Any:
    """JSON output for currency-like floats (drops binary noise e.g. 3.4200000000000004 → 3.42)."""
    if v is None:
        return None
    r = round(float(v), 2)
    if abs(r - int(r)) < 1e-9:
        return int(round(r))
    return r


def _rate_by_is_weight_chargeable(rate_by: str) -> bool:
    """True for ``Weight/chargeable kg``-style lines; uses :func:`_chargeable_weight`."""
    rb = _normalize_rate_by_string(rate_by).lower()
    return "weight" in rb and "chargeable" in rb


def _rate_by_is_weight_kg_etof(rate_by: str) -> bool:
    """
    True for ``Weight/kg`` (rate by weight in kg without “chargeable” in the card text).
    Uses :func:`_weight_etof` (``WEIGHT_ETOF``).
    """
    rb = _normalize_rate_by_string(rate_by).lower()
    if "chargeable" in rb:
        return False
    return bool(re.search(r"weight\s*/\s*kg", rb))


def _rate_by_uses_weight_tier_column(rate_by: str) -> bool:
    """Either chargeable-weight or Weight/kg pricing (bracket / per-kg logic)."""
    rb = _normalize_rate_by_string(rate_by)
    return _rate_by_is_weight_kg_etof(rb) or _rate_by_is_weight_chargeable(rb)


def _weight_for_weight_rate_by(rate_by: str, row: dict[str, Any]) -> Optional[float]:
    """Resolve kg used for weight-based ``Rate_by`` (chargeable vs ETOF)."""
    rb = _normalize_rate_by_string(rate_by)
    if _rate_by_is_weight_kg_etof(rb):
        return _weight_etof(row)
    if _rate_by_is_weight_chargeable(rb):
        return _chargeable_weight(row)
    return None


def _rate_by_is_per_shipment(rate_by: str) -> bool:
    rb = _normalize_rate_by_string(rate_by).lower().strip()
    return "per shipment" in rb or rb == "per shipment"


def _measurement_norm(measurement: Optional[str]) -> str:
    return (measurement or "").strip().lower()


def _measurement_is_flat(measurement: Optional[str]) -> bool:
    return _measurement_norm(measurement) == "flat"


def _measurement_is_p_unit(measurement: Optional[str]) -> bool:
    m = _measurement_norm(measurement)
    return m == "p/unit" or "p/unit" in m


def compute_rate_cost_calculated(
    rate_cost: Optional[float],
    rate_by: str,
    row: dict[str, Any],
    cost_tier_measurement: Optional[str],
    rounding_rule: str = "",
) -> Optional[float]:
    """
    per shipment: Rate cost × 1.
    Weight/chargeable kg: Flat → ×1; p/unit → Rate cost × CHARGEABLE WEIGHT.
    Weight/kg: same tier rules but × WEIGHT_ETOF.
    Quantity/Container: p/unit → Rate cost × sum of units for MEASUREMENT segments containing ``Container``.
    Volume/cbm: p/unit → Rate cost × CBM (after ``Rounding_rule`` for CBM when present).
    Cost/CBS, Cost/CON, …: p/unit → Rate cost × UNITS_MEASUREMENT segment for that ``Cost/…`` token.
    """
    if rate_cost is None:
        return None
    rc = float(rate_cost)
    rb = _normalize_rate_by_string(rate_by)
    if _rate_by_is_per_shipment(rb):
        return rc * 1.0
    if _rate_by_uses_weight_tier_column(rb):
        if _measurement_is_flat(cost_tier_measurement):
            return rc * 1.0
        if _measurement_is_p_unit(cost_tier_measurement):
            w = _weight_for_weight_rate_by(rb, row)
            if w is None:
                return None
            return rc * w
        return None
    if _rate_by_is_quantity_container(rb):
        if _measurement_is_p_unit(cost_tier_measurement):
            q = quantity_container_units_from_row(row)
            if q is None:
                return None
            return rc * q
        return None
    if _rate_by_is_volume_cbm(rb):
        if _measurement_is_p_unit(cost_tier_measurement):
            v = _cbm_value(row)
            if v is None:
                return None
            v_eff = apply_rounding_for_cbm(v, rounding_rule)
            return rc * v_eff
        return None
    if _rate_by_is_cost_measurement_token(rb):
        if _measurement_is_p_unit(cost_tier_measurement):
            u = _units_for_measurement_token_rate_by(row, rb)
            if u is None:
                return None
            return rc * float(u)
        return None
    return None


def _format_number_for_rate_comment(n: float) -> str:
    """Readable number in Rate_cost_comment (avoid float noise)."""
    if abs(n - round(n)) < 1e-9:
        return str(int(round(n)))
    s = f"{n:.4f}".rstrip("0").rstrip(".")
    return s if s else "0"


def format_rate_cost_comment(
    rate_cost: Optional[float],
    rate_by: str,
    row: dict[str, Any],
    cost_tier_measurement: Optional[str],
    cost_tier: Optional[dict[str, Any]],
    rcc: Optional[float],
    rounding_rule: str = "",
) -> str:
    """
    Short English sentence explaining how Rate_cost_calculated was derived from the rate card
    (Rate_by, Measurement, bracket, Price, chargeable weight or WEIGHT_ETOF when relevant).
    """
    if rate_cost is None or rcc is None:
        return ""
    rc = float(rate_cost)
    price_s = _format_number_for_rate_comment(rc)
    result_s = _format_number_for_rate_comment(float(rcc))
    bracket_s = ""
    if cost_tier:
        bracket_s = _norm(cost_tier.get("Weight Bracket"))
    bracket_part = f", bracket {bracket_s}" if bracket_s else ""

    rb = _normalize_rate_by_string(rate_by)
    if _rate_by_is_per_shipment(rb):
        return (
            f"Calculated according to the rate card. Per shipment: {price_s} = {result_s}."
        )

    if _rate_by_uses_weight_tier_column(rb):
        w = _weight_for_weight_rate_by(rb, row)
        w_s = _format_number_for_rate_comment(w) if w is not None else ""
        w_label = "WEIGHT_ETOF" if _rate_by_is_weight_kg_etof(rb) else "chargeable weight"
        if _measurement_is_flat(cost_tier_measurement):
            if w is None:
                return (
                    f"Calculated according to the rate card. Flat rate {price_s}{bracket_part} = {result_s}."
                )
            return (
                f"Calculated according to the rate card. {w_s} ({w_label}) → "
                f"{price_s} (Flat{bracket_part}) = {result_s}."
            )
        if _measurement_is_p_unit(cost_tier_measurement):
            if w is None:
                return ""
            return (
                f"Calculated according to the rate card. {w_s} ({w_label}) × "
                f"{price_s} (p/unit{bracket_part}) = {result_s}."
            )
        return ""

    if _rate_by_is_quantity_container(rb) and _measurement_is_p_unit(cost_tier_measurement):
        q = quantity_container_units_from_row(row)
        if q is None:
            return ""
        q_s = _format_number_for_rate_comment(q)
        return (
            f"Calculated according to the rate card. {q_s} (Quantity/Container) × "
            f"{price_s} (p/unit, rate) = {result_s}."
        )

    if _rate_by_is_volume_cbm(rb) and _measurement_is_p_unit(cost_tier_measurement):
        v = _cbm_value(row)
        if v is None:
            return ""
        v_eff = apply_rounding_for_cbm(v, rounding_rule)
        v_s = _format_number_for_rate_comment(v_eff)
        return (
            f"Calculated according to the rate card. {v_s} (Volume/cbm) × "
            f"{price_s} (p/unit, rate) = {result_s}."
        )

    if _rate_by_is_cost_measurement_token(rb) and _measurement_is_p_unit(cost_tier_measurement):
        u = _units_for_measurement_token_rate_by(row, rb)
        if u is None:
            return ""
        u_s = _format_number_for_rate_comment(float(u))
        tok = _normalize_rate_by_string(rate_by).strip() or rb
        return (
            f"Calculated according to the rate card. {u_s} ({tok}) × "
            f"{price_s} (p/unit, rate) = {result_s}."
        )

    return ""


def compute_rate_cost_file(
    rate_by: str,
    row: dict[str, Any],
    cost_tier_measurement: Optional[str],
    rounding_rule: str = "",
) -> Optional[float]:
    """
    per shipment: Pre-calc ÷ 1.
    Weight/chargeable kg: Flat → ÷1; p/unit → Pre-calc ÷ CHARGEABLE WEIGHT.
    Weight/kg: p/unit → Pre-calc ÷ WEIGHT_ETOF.
    Quantity/Container p/unit: Pre-calc ÷ sum of container units (paired with MEASUREMENT).
    Volume/cbm p/unit: Pre-calc ÷ CBM (after CBM rounding rule).
    Then ÷ Exchange rate (1 if missing).
    """
    pc = _precalc_cost_inv_curr(row)
    if pc is None:
        return None
    base: Optional[float] = None
    rb = _normalize_rate_by_string(rate_by)
    if _rate_by_is_per_shipment(rb):
        base = pc / 1.0
    elif _rate_by_uses_weight_tier_column(rb):
        if _measurement_is_flat(cost_tier_measurement):
            base = pc / 1.0
        elif _measurement_is_p_unit(cost_tier_measurement):
            w = _weight_for_weight_rate_by(rb, row)
            if w is None or abs(w) < 1e-12:
                return None
            base = pc / w
    elif _rate_by_is_quantity_container(rb) and _measurement_is_p_unit(cost_tier_measurement):
        q = quantity_container_units_from_row(row)
        if q is None or abs(q) < 1e-12:
            return None
        base = pc / q
    elif _rate_by_is_volume_cbm(rb) and _measurement_is_p_unit(cost_tier_measurement):
        v = _cbm_value(row)
        if v is None or abs(v) < 1e-12:
            return None
        v_eff = apply_rounding_for_cbm(v, rounding_rule)
        if abs(v_eff) < 1e-12:
            return None
        base = pc / v_eff
    elif _rate_by_is_cost_measurement_token(rb) and _measurement_is_p_unit(cost_tier_measurement):
        u = _units_for_measurement_token_rate_by(row, rb)
        if u is None or abs(u) < 1e-12:
            return None
        base = pc / float(u)
    if base is None:
        return None
    return _divide_by_exchange_rate(base, row)


def compute_carrier_rate_file(
    rate_by: str,
    row: dict[str, Any],
    cost_tier_measurement: Optional[str],
    rounding_rule: str = "",
) -> Optional[float]:
    """
    Same rules as Rate_cost_file, but uses Invoice statement cost (in inv curr) instead of Pre-calc.
    Then ÷ Exchange rate (1 if missing).
    """
    inv = _invoice_statement_cost_inv_curr(row)
    if inv is None:
        return None
    base: Optional[float] = None
    rb = _normalize_rate_by_string(rate_by)
    if _rate_by_is_per_shipment(rb):
        base = inv / 1.0
    elif _rate_by_uses_weight_tier_column(rb):
        if _measurement_is_flat(cost_tier_measurement):
            base = inv / 1.0
        elif _measurement_is_p_unit(cost_tier_measurement):
            w = _weight_for_weight_rate_by(rb, row)
            if w is None or abs(w) < 1e-12:
                return None
            base = inv / w
    elif _rate_by_is_quantity_container(rb) and _measurement_is_p_unit(cost_tier_measurement):
        q = quantity_container_units_from_row(row)
        if q is None or abs(q) < 1e-12:
            return None
        base = inv / q
    elif _rate_by_is_volume_cbm(rb) and _measurement_is_p_unit(cost_tier_measurement):
        v = _cbm_value(row)
        if v is None or abs(v) < 1e-12:
            return None
        v_eff = apply_rounding_for_cbm(v, rounding_rule)
        if abs(v_eff) < 1e-12:
            return None
        base = inv / v_eff
    elif _rate_by_is_cost_measurement_token(rb) and _measurement_is_p_unit(cost_tier_measurement):
        u = _units_for_measurement_token_rate_by(row, rb)
        if u is None or abs(u) < 1e-12:
            return None
        base = inv / float(u)
    if base is None:
        return None
    return _divide_by_exchange_rate(base, row)


def compute_possible_carrier_exchange_rate(
    rate_by: str,
    row: dict[str, Any],
    rate_cost: Optional[float],
) -> Optional[float]:
    """
    When Rate_by is weight/chargeable kg or Weight/kg:
    Invoice statement cost (in inv curr) / Rate cost / (CHARGEABLE WEIGHT or WEIGHT_ETOF).
    """
    if not _rate_by_uses_weight_tier_column(rate_by):
        return None
    if rate_cost is None:
        return None
    try:
        rc = float(rate_cost)
    except (TypeError, ValueError):
        return None
    if abs(rc) < 1e-12:
        return None
    inv = _invoice_statement_cost_inv_curr(row)
    if inv is None:
        return None
    w = _weight_for_weight_rate_by(rate_by, row)
    if w is None or abs(w) < 1e-12:
        return None
    return float(inv) / rc / float(w)


def compute_possible_carrier_used_units(
    rate_by: str,
    row: dict[str, Any],
    rate_cost: Optional[float],
) -> Optional[float]:
    """
    When Rate_by is not per shipment: Invoice statement cost (in inv curr) / Rate cost / Exchange rate.
    Example: 60 / 20 / 1 = 3.
    """
    if _rate_by_is_per_shipment(rate_by):
        return None
    if rate_cost is None:
        return None
    try:
        rc = float(rate_cost)
    except (TypeError, ValueError):
        return None
    if abs(rc) < 1e-12:
        return None
    inv = _invoice_statement_cost_inv_curr(row)
    if inv is None:
        return None
    v = row.get(COL_EXCHANGE_RATE)
    er = 1.0
    if v is not None and not (isinstance(v, float) and pd.isna(v)):
        try:
            er = float(v)
        except (TypeError, ValueError):
            er = 1.0
    if abs(er) < 1e-12:
        return None
    return float(inv) / rc / er


def format_possible_carrier_used_units_comment(units: Optional[float]) -> str:
    """E.g. Cost is provided per 3 units."""
    if units is None:
        return ""
    u_s = _format_number_for_rate_comment(float(units))
    return f"Cost is provided per {u_s} units."


def _applies_if_clause_looks_structured(clause: str) -> bool:
    """
    True only for clauses :func:`_applies_clause_ok` can evaluate (Service / weight).
    Prose such as ``invoiced by Carrier`` (no Service rule) is not structured — we must not
    treat it as "failed", or definitions like ``Transport cost (Total per Kg rate)`` disappear
    from :func:`_cost_defs_from_filtered` and lane pricing stays empty.
    """
    low = clause.lower()
    if "service equals" in low:
        return True
    if "service" in low and "weight" in low and " and " in low:
        return True
    return False


def applies_if_allows(applies_if: str, row: dict[str, Any]) -> bool:
    """
    True if shipment satisfies at least one ----or---- clause in Applies_if (Service / weight rules).
    Empty Applies_if → True.
    Non-structured text (billing notes, etc.) → True (cannot be evaluated against shipment).
    """
    if not applies_if or not str(applies_if).strip():
        return True
    service = _shipment_service(row)
    w = _chargeable_weight(row)

    parts = re.split(r"\n----or----\n", applies_if, flags=re.IGNORECASE)
    saw_structured = False
    for raw in parts:
        clause = raw.strip()
        if not clause:
            continue
        clause = re.sub(r"^\d+\.\s*", "", clause)
        if not _applies_if_clause_looks_structured(clause):
            continue
        saw_structured = True
        if _applies_clause_ok(clause, service, w):
            return True
    if not saw_structured:
        return True
    return False


def _applies_clause_ok(clause: str, service: str, w: Optional[float]) -> bool:
    cl = clause
    low = cl.lower()
    # AND: Service equals '…' and Weight/chargeable kg …
    if " and " in low and "weight" in low and "service equals" in low:
        sm = re.search(
            r"Service equals\s+(.+?)\s+and\s+Weight",
            cl,
            re.I | re.DOTALL,
        )
        if sm:
            codes = re.findall(r"'([^']+)'", sm.group(1))
            if not (service and codes and service in codes):
                return False
            wm = re.search(
                r"less\s*than\s*or\s*equal\s*to\s*['\"]?(\d+(?:\.\d+)?)['\"]?",
                cl,
                re.I,
            )
            if wm and w is not None:
                return w <= float(wm.group(1)) + 1e-9
            return False
    if re.search(r"Service equals", cl, re.I):
        rest_m = re.search(r"Service equals\s+(.+)", cl, re.I | re.DOTALL)
        if rest_m:
            body = rest_m.group(1)
            if " and " in body.lower() and "weight" in body.lower():
                body = body.split(" and ")[0]
            codes = re.findall(r"'([^']+)'", body)
            if not codes:
                return False
            return bool(service) and service in codes
    return False


def apply_rounding_for_rate(weight: float, rounding_rule: str) -> float:
    """
    Typical text: "No rounding (range <=25); Upper to 1 (range >25)".
    weight ≤ 25 → unchanged; > 25 → ceil to whole number (upper to 1 kg).
    """
    rr = (rounding_rule or "").lower()
    if not rr.strip():
        return weight
    if weight <= 25.0 + 1e-9:
        return weight
    if "upper" in rr and ">25" in rr.replace(" ", ""):
        return float(math.ceil(weight))
    if "upper to 1" in rr or "upper to" in rr:
        return float(math.ceil(weight))
    return weight


def bracket_matches(weight: float, bracket: str) -> bool:
    b = (bracket or "").strip()
    if not b:
        return False
    if b.upper().startswith("<="):
        try:
            cap = float(b[2:].strip())
        except ValueError:
            return False
        return weight <= cap + 1e-9
    if b.startswith(">") and not b.startswith(">="):
        try:
            lo = float(b[1:].strip())
        except ValueError:
            return False
        return weight > lo + 1e-9
    if b.upper().startswith(">="):
        try:
            lo = float(b[2:].strip())
        except ValueError:
            return False
        return weight >= lo - 1e-9
    return False


def parse_another_rate_card_lane_brackets(value: str) -> list[tuple[str, str]]:
    """Parse ``[RA…] Lane #: n, Weight Bracket: …`` segments into (lane, bracket) pairs."""
    if not value or not str(value).strip():
        return []
    s = str(value).strip()
    s = re.sub(r"^\[[^\]]+\]\s*", "", s)
    if not s or s == MSG_NO_LANE_SAME_COST:
        return []
    out: list[tuple[str, str]] = []
    for segment in s.split(";"):
        segment = segment.strip()
        if not segment:
            continue
        m = re.search(
            r"Lane\s*#:\s*([^,]+?)\s*,\s*Weight\s*Bracket:\s*(.*)",
            segment,
            re.I,
        )
        if m:
            out.append((m.group(1).strip(), m.group(2).strip()))
    return out


def parse_ra_prefix_from_bracketed_rate_card_string(value: str) -> Optional[str]:
    """Leading ``[RA########]`` from ``Another_rate_card_Carrier_used`` style strings."""
    s = str(value).strip()
    m = re.match(r"^\[([^\]]+)\]\s*", s)
    if not m:
        return None
    cand = m.group(1).strip().upper()
    if re.match(r"^RA\d+$", cand):
        return cand
    return None


def parse_another_rate_card_ra_and_lane_numbers(value: str) -> tuple[Optional[str], list[str]]:
    """
    From ``[RA…] Lane #: 109, Weight Bracket: ; …`` return (RA id, unique lane numbers in order).
    """
    ra = parse_ra_prefix_from_bracketed_rate_card_string(value or "")
    pairs = parse_another_rate_card_lane_brackets(value or "")
    seen: set[str] = set()
    lanes: list[str] = []
    for ln, _br in pairs:
        k = str(ln).strip()
        if k and k not in seen:
            seen.add(k)
            lanes.append(k)
    return ra, lanes


def tightest_weight_bracket_for_weight(weight: float, brackets: list[str]) -> Optional[str]:
    """Among bracket strings that contain ``weight``, pick the tightest ``<=N`` (min N), else ``>`` logic."""
    fitting = [b for b in brackets if bracket_matches(weight, str(b))]
    if not fitting:
        return None
    le_br = [b for b in fitting if str(b).strip().upper().startswith("<=")]
    if le_br:

        def cap(b: str) -> float:
            return float(str(b).strip()[2:].strip())

        return min(le_br, key=cap)
    gt_br = [
        b
        for b in fitting
        if str(b).strip().startswith(">") and not str(b).strip().startswith(">=")
    ]
    if gt_br:

        def lo(b: str) -> float:
            return float(str(b).strip()[1:].strip())

        return min(gt_br, key=lo)
    return fitting[0]


def best_match_from_another_rate_card(
    another_crf_str: str,
    another_inv_str: str,
    rate_by: str,
    row: dict[str, Any],
) -> str:
    """
    When Rate_by is weight/chargeable kg or Weight/kg: merge lane/bracket pairs from the two
    ``Another_rate_card_Carrier_used`` columns, then per lane keep the tightest bracket
    that still fits the shipment weight column (chargeable or WEIGHT_ETOF).
    """
    if not _rate_by_uses_weight_tier_column(rate_by):
        return ""
    w = _weight_for_weight_rate_by(rate_by, row)
    if w is None:
        return ""
    weight = float(w)
    by_lane: dict[str, list[str]] = {}
    seen: set[tuple[str, str]] = set()
    for src in (another_crf_str or "", another_inv_str or ""):
        for lane, bracket in parse_another_rate_card_lane_brackets(src):
            key = (lane, bracket)
            if key in seen:
                continue
            seen.add(key)
            by_lane.setdefault(lane, []).append(bracket)
    if not by_lane:
        return ""

    def lane_sort_key(ln: str) -> tuple:
        s = str(ln).strip()
        return (0, int(s)) if s.isdigit() else (1, s)

    parts: list[str] = []
    for lane in sorted(by_lane.keys(), key=lane_sort_key):
        tb = tightest_weight_bracket_for_weight(weight, by_lane[lane])
        if tb:
            parts.append(f"Lane #: {lane}, Weight Bracket: {tb}")
    return "; ".join(parts)


def pick_tightest_weight_tier(
    weight: float, cost_lines: list[dict[str, Any]]
) -> Optional[dict[str, Any]]:
    """Among tiers whose Weight Bracket contains weight, prefer smallest <=N; else >N."""
    with_bracket = [c for c in cost_lines if str(c.get("Weight Bracket", "")).strip()]
    matching = [c for c in with_bracket if bracket_matches(weight, str(c.get("Weight Bracket")))]
    if not matching:
        return None
    le = [c for c in matching if str(c.get("Weight Bracket", "")).strip().startswith("<=")]
    if le:

        def cap(c: dict[str, Any]) -> float:
            s = str(c["Weight Bracket"]).strip()
            return float(s[2:].strip())

        return min(le, key=cap)
    gt = [c for c in matching if str(c.get("Weight Bracket", "")).strip().startswith(">")]
    if gt:

        def lo(c: dict[str, Any]) -> float:
            s = str(c["Weight Bracket"]).strip()
            return float(s[1:].strip())

        return min(gt, key=lo)
    return matching[0]


def _find_lane(rate_card_data: list[dict[str, Any]], lane_num: str) -> Optional[dict[str, Any]]:
    for L in rate_card_data:
        if str(L.get("Lane #", "")).strip() == lane_num:
            return L
    return None


def load_vocab_etof_mappings(partly_df: str) -> dict[str, str]:
    """Rate card column name -> shipment column name (``vocabulary_mapping.json`` ``etof_mappings``)."""
    path = os.path.join(partly_df, "vocabulary_mapping.json")
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        m = data.get("etof_mappings")
        return dict(m) if isinstance(m, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _row_value_for_vocab_shipment_key(row: dict[str, Any], vocab_shipment_key: str) -> Any:
    """Resolve mismatch-row value for a vocabulary shipment key (with ETOF/ISD / billing fallbacks)."""
    keys: list[str] = [vocab_shipment_key]
    if vocab_shipment_key == "Billing account":
        keys.extend(["CARRIER_ACCOUNT_NR_ETOF", "CARRIER_ACCOUNT_NR_ISD"])
    else:
        u = vocab_shipment_key.upper().replace(" ", "_")
        keys.extend([f"{u}_ETOF", f"{u}_ISD"])
    for k in keys:
        if k not in row:
            continue
        val = row[k]
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        s = str(val).strip()
        if s and s.lower() not in ("nan", "none"):
            return val
    return None


def shipment_view_for_rate_card_compare(
    row: dict[str, Any], etof_mappings: dict[str, str]
) -> dict[str, Any]:
    """Map mismatch row to rate-card column names so ``compare_shipment_to_lane`` can run."""
    view: dict[str, Any] = dict(row)
    for rc_col, ship_key in etof_mappings.items():
        view[rc_col] = _row_value_for_vocab_shipment_key(row, ship_key)
    return view


def _normalize_ship_date_for_matching(row: dict[str, Any]) -> Optional[str]:
    """SHIP_DATE as YYYY-MM-DD string for ``_lane_valid_for_shipment_date`` / parsing."""
    v = row.get("SHIP_DATE")
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, int):
        s = str(v)
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    s = str(v).strip()
    return s if s else None


def _display_val(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "n/a"
    s = str(v).strip()
    return s if s else "n/a"


def _lane_scalar_value_columns(lane: dict[str, Any]) -> list[str]:
    """Same as matching's lane value columns, but skip validity dates, ``Costs``, and nested values."""
    out: list[str] = []
    for c in _get_lane_value_columns(lane):
        if c in VALIDITY_DATE_COLUMNS or c == "Costs":
            continue
        v = lane.get(c)
        if isinstance(v, (list, dict)):
            continue
        out.append(c)
    return out


def parse_best_match_lane_numbers(best_match_str: str) -> list[str]:
    """Lane ids from ``Best match from another rate card`` (order preserved, unique)."""
    pairs = parse_another_rate_card_lane_brackets(best_match_str)
    out: list[str] = []
    seen: set[str] = set()
    for ln, _ in pairs:
        k = str(ln).strip()
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _format_lane_vs_shipment_messages(
    lane: dict[str, Any],
    ship_view: dict[str, Any],
    diffs: list[str],
) -> list[str]:
    """Human-readable phrases for columns that ``compare_shipment_to_lane`` flagged."""
    uniq: list[str] = []
    seen: set[str] = set()
    for d in diffs:
        if d not in seen:
            seen.add(d)
            uniq.append(d)
    msgs: list[str] = []
    for col in uniq:
        if col == "Service" or col == "Service Type":
            rc_val = lane.get(col) or lane.get("Service") or lane.get("Service Type") or ""
            tokens = [t.strip() for t in str(rc_val).split(",") if t.strip()]
            ship_v = _display_val(
                ship_view.get(col) or ship_view.get("Service Type") or ship_view.get("Service")
            )
            if len(tokens) > 1:
                opts = " or ".join(tokens)
                msgs.append(f"another service ({opts}) was used")
            else:
                lv = _display_val(rc_val)
                msgs.append(f"{col}: lane {lv} vs shipment {ship_v}")
        elif col == "Origin Country":
            msgs.append(
                "origin country differs (lane "
                f"{_display_val(lane.get('Origin Country'))} vs shipment {_display_val(ship_view.get('Origin Country'))})"
            )
        elif col == "Destination Country":
            msgs.append(
                "destination country differs (lane "
                f"{_display_val(lane.get('Destination Country'))} vs shipment {_display_val(ship_view.get('Destination Country'))})"
            )
        elif col == "Carrier Account Number":
            msgs.append(
                "carrier account differs (lane "
                f"{_display_val(lane.get('Carrier Account Number'))} vs shipment {_display_val(ship_view.get('Carrier Account Number'))})"
            )
        elif col == "Origin Postal Code":
            msgs.append(
                "origin postal code differs (lane "
                f"{_display_val(lane.get('Origin Postal Code'))} vs shipment {_display_val(ship_view.get('Origin Postal Code'))})"
            )
        elif col == "Destination Postal Code":
            msgs.append(
                "destination postal code differs (lane "
                f"{_display_val(lane.get('Destination Postal Code'))} vs shipment {_display_val(ship_view.get('Destination Postal Code'))})"
            )
        else:
            lv = _display_val(lane.get(col))
            sv = _display_val(ship_view.get(col))
            msgs.append(f"{col}: lane {lv} vs shipment {sv}")
    return msgs


def rate_card_lanes_vs_shipment_notes(
    row: dict[str, Any],
    filtered: dict[str, Any],
    lane_nums: list[str],
    ra_label: str,
    etof_mappings: dict[str, str],
) -> str:
    """
    Compare shipment (via ``etof_mappings``) to each lane in ``lane_nums`` on this rate card JSON.
    ``ra_label`` is only used in messages (e.g. RA id).
    """
    if not lane_nums or not etof_mappings:
        return ""
    rate_card_data = filtered.get("rate_card_data") or []
    conditions_list = filtered.get("conditions") or []
    business_rules_list = filtered.get("business_rules") or []
    ship_view = shipment_view_for_rate_card_compare(row, etof_mappings)
    ship_date_str = _normalize_ship_date_for_matching(row)
    parts: list[str] = []
    for lane_num in lane_nums:
        lane = _find_lane(rate_card_data, str(lane_num).strip())
        if not lane:
            parts.append(f"Lane {lane_num} ({ra_label}): lane not found in rate card")
            continue
        value_columns = _lane_scalar_value_columns(lane)
        _dc, diffs = compare_shipment_to_lane(
            ship_view, lane, conditions_list, business_rules_list, value_columns
        )
        msgs = _format_lane_vs_shipment_messages(lane, ship_view, diffs)
        if ship_date_str and not _lane_valid_for_shipment_date(lane, ship_date_str):
            msgs.append("shipment date is outside Valid from–Valid to")
        if msgs:
            parts.append(f"Lane {lane_num} ({ra_label}): " + "; ".join(msgs))
    return "\n".join(parts)


def amount_match_another_rate_card_lane_vs_shipment(
    row: dict[str, Any],
    another_crf_str: str,
    another_inv_str: str,
    agreement_ra: Optional[str],
    partly_df: str,
    rate_card_cache: dict[str, Any],
    etof_mappings: dict[str, str],
) -> str:
    """
    When ``Another_rate_card_Carrier_used`` lists lane(s) on a *different* RA (same price as
    invoice/CRF) but ``Best match from another rate card`` is empty — e.g. grouped Transport
    ``Rate_by`` is not weight/chargeable kg — still compare those lanes to the shipment so
    ``Service Type`` and other mismatches appear in ``Another rate card lane vs shipment``.
    """
    if not etof_mappings:
        return ""
    cur = (agreement_ra or "").strip().upper()
    by_ra: dict[str, list[str]] = {}
    for src in (another_crf_str or "", another_inv_str or ""):
        ra_p, lanes = parse_another_rate_card_ra_and_lane_numbers(src)
        if not ra_p or not lanes:
            continue
        if cur and ra_p.upper() == cur:
            continue
        for ln in lanes:
            if ln not in by_ra.setdefault(ra_p.upper(), []):
                by_ra[ra_p.upper()].append(ln)
    parts: list[str] = []
    for ra_key in sorted(by_ra.keys()):
        lane_nums = by_ra[ra_key]
        path = filtered_rate_card_json_path_for_ra_id(ra_key, partly_df)
        _cache_filtered_rate_card(path, rate_card_cache)
        filtered = rate_card_cache.get(path)
        if not isinstance(filtered, dict):
            continue
        body = rate_card_lanes_vs_shipment_notes(
            row, filtered, lane_nums, ra_key, etof_mappings
        )
        if body:
            parts.append(body)
    return "\n".join(parts)


def another_rate_card_lane_vs_shipment(
    row: dict[str, Any],
    best_match_str: str,
    agreement_ra: Optional[str],
    partly_df: str,
    rate_card_cache: dict[str, Any],
    etof_mappings: dict[str, str],
) -> str:
    """
    For each lane in ``Best match from another rate card``, compare the secondary
    ``Filtered_Rate_Card_with_Conditions_<RA>.json`` lane row to this shipment (via
    ``vocabulary_mapping.json`` ``etof_mappings``). Lists differences only.
    Lane blocks are separated by newlines (one lane per line in Excel when wrap text is on).
    """
    sec_ra = _secondary_ra_id_for_row(agreement_ra, partly_df)
    if not sec_ra or not best_match_str or not str(best_match_str).strip():
        return ""
    if not etof_mappings:
        return ""
    path = filtered_rate_card_json_path_for_ra_id(sec_ra, partly_df)
    _cache_filtered_rate_card(path, rate_card_cache)
    filtered = rate_card_cache.get(path)
    if not isinstance(filtered, dict):
        return ""
    lane_nums = parse_best_match_lane_numbers(best_match_str)
    if not lane_nums:
        return ""
    return rate_card_lanes_vs_shipment_notes(row, filtered, lane_nums, sec_ra, etof_mappings)


def _first_lane_number(best_lane_str: str) -> Optional[str]:
    if not best_lane_str or not str(best_lane_str).strip():
        return None
    return str(best_lane_str).split(",")[0].strip()


@dataclass
class GroupedTransportLaneResult:
    """Grouped ``Transport cost`` block: one validity window, multiple sub-costs (container lines)."""

    rate_cost_display: str
    calculated_total: float
    merged_applies_if: str
    merged_rate_by: str
    merged_rounding: str
    representative_tier: dict[str, Any]
    matched_sub_cost_names: list[str]
    grouped_cost_type: str
    sum_units: float
    rate_parts: list[float]
    unit_parts: list[float]
    rate_by_parts: list[str]


def _single_grouped_transport_definition(
    defs_for_type: list[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    """
    Single grouped block whose sub-costs use ``Container/…`` rate-by (Transport cost, Reefer buffer, etc.).
    """
    if len(defs_for_type) != 1:
        return None
    d = defs_for_type[0]
    if not d.get("grouped_cost"):
        return None
    ct = _norm(d.get("Cost_type"))
    if "grouped cost:" not in ct.lower():
        return None
    subs = d.get("sub_cost_definitions") or []
    for sub in subs:
        rb = _norm(sub.get("Rate_by") or "")
        if rb.lower().startswith("container/"):
            return d
    return None


def grouped_container_sub_rates_missing_measurement_comment(
    grouped_def: dict[str, Any], row: dict[str, Any]
) -> str:
    """
    When every ``Container/…`` sub-rate needs a MEASUREMENT segment + units but none match
    (same idea as Quantity/Container on non-grouped rows).
    """
    if not isinstance(grouped_def, dict) or not grouped_def.get("grouped_cost"):
        return ""
    subs = grouped_def.get("sub_cost_definitions") or []
    labels: list[str] = []
    for sub in subs:
        rb = _norm(sub.get("Rate_by") or "")
        if not rb.lower().startswith("container/"):
            continue
        u = _units_for_container_rate_by(row, rb)
        if u is not None:
            return ""
        lab = _container_rate_by_size_label_for_comment(rb)
        if lab and lab not in labels:
            labels.append(lab)
    if not labels:
        return ""
    return (
        f"Not provided {' / '.join(labels)} in MEASUREMENT "
        f"(required for grouped container rate)."
    )


def _primary_container_measurement_token(row: dict[str, Any]) -> str:
    meas = row.get("MEASUREMENT") or row.get("Measurement") or ""
    for p in str(meas).split(";"):
        p = p.strip().replace("\\/", "/")
        if "container/" in p.lower():
            return p
    return ""


def compute_grouped_transport_lane(
    row: dict[str, Any],
    filtered: dict[str, Any],
    best_lane_str: str,
    grouped_def: dict[str, Any],
) -> Optional[GroupedTransportLaneResult]:
    """
    Sum sub-costs whose ``Rate_by`` ``Container/…`` appears in ``MEASUREMENT`` with units,
    using lane ``Costs`` rows for ``Grouped under`` + shipment validity on each line.
    """
    lane_num = _first_lane_number(best_lane_str)
    if not lane_num:
        return None
    lane = _find_lane(filtered.get("rate_card_data") or [], lane_num)
    if not lane:
        return None
    grouped_title = _norm(grouped_def.get("Cost_type"))
    ship = _row_ship_date_as_date(row)
    subs = grouped_def.get("sub_cost_definitions") or []
    prices: list[float] = []
    units: list[float] = []
    rate_bys: list[str] = []
    rounds: list[str] = []
    tiers: list[dict[str, Any]] = []
    names: list[str] = []

    for sub in subs:
        sub_name = sub.get("sub_cost_name")
        rb = _norm(sub.get("Rate_by") or "")
        if not sub_name or not rb:
            continue
        if not rb.lower().startswith("container/"):
            continue
        u = _units_for_container_rate_by(row, rb)
        if u is None:
            continue
        line = _lane_cost_for_grouped_sub(lane, str(sub_name), grouped_title, ship)
        if line is None:
            continue
        p = line.get("Price")
        if p is None:
            continue
        try:
            pf = float(p)
        except (TypeError, ValueError):
            continue
        prices.append(pf)
        units.append(u)
        rate_bys.append(rb)
        rounds.append(_norm(sub.get("Rounding_rule") or ""))
        tiers.append(line)
        names.append(str(sub_name))

    if not prices or not tiers:
        return None

    calc = sum(p * u for p, u in zip(prices, units))
    rc_disp = "; ".join(_format_number_for_rate_comment(p) for p in prices)
    merged_applies = _norm(grouped_def.get("Applies_if") or "")
    merged_round = "; ".join(r for r in rounds if r) if any(rounds) else ""
    return GroupedTransportLaneResult(
        rate_cost_display=rc_disp,
        calculated_total=calc,
        merged_applies_if=merged_applies,
        merged_rate_by="; ".join(rate_bys),
        merged_rounding=merged_round,
        representative_tier=tiers[0],
        matched_sub_cost_names=names,
        grouped_cost_type=grouped_title,
        sum_units=float(sum(units)),
        rate_parts=prices,
        unit_parts=units,
        rate_by_parts=rate_bys,
    )


def format_grouped_transport_rate_cost_comment(result: GroupedTransportLaneResult) -> str:
    parts: list[str] = []
    for p, u, rb, name in zip(
        result.rate_parts,
        result.unit_parts,
        result.rate_by_parts,
        result.matched_sub_cost_names,
    ):
        line_total = p * u
        ps = _format_number_for_rate_comment(p)
        us = _format_number_for_rate_comment(u)
        ts = _format_number_for_rate_comment(line_total)
        parts.append(f"{ps} (p/unit) × {us} ({rb}) for {name} = {ts}")
    total_s = _format_number_for_rate_comment(result.calculated_total)
    return "Calculated according to the rate card. " + "; ".join(parts) + f"; total = {total_s}."


def compute_grouped_transport_rate_cost_file(
    row: dict[str, Any], result: GroupedTransportLaneResult
) -> Optional[float]:
    pc = _precalc_cost_inv_curr(row)
    if pc is None:
        return None
    su = result.sum_units
    if abs(su) < 1e-12:
        return None
    return _divide_by_exchange_rate(float(pc) / su, row)


def compute_grouped_transport_carrier_rate_file(
    row: dict[str, Any], result: GroupedTransportLaneResult
) -> Optional[float]:
    inv = _invoice_statement_cost_inv_curr(row)
    if inv is None:
        return None
    su = result.sum_units
    if abs(su) < 1e-12:
        return None
    return _divide_by_exchange_rate(float(inv) / su, row)


def _transport_tier_names_matching_invoice_on_lane(
    filtered: dict[str, Any],
    lane_num: str,
    ship: Any,
    invoice_amount: Optional[float],
) -> list[str]:
    """``Cost Type`` names on lane whose ``Price`` equals invoice (2dp) and line validity fits ``ship``."""
    if invoice_amount is None:
        return []
    try:
        target = ceil_two_decimals_up(float(invoice_amount))
    except (TypeError, ValueError):
        return []
    lane = _find_lane(filtered.get("rate_card_data") or [], str(lane_num).strip())
    if not lane:
        return []
    names: list[str] = []
    for c in lane.get("Costs") or []:
        ct = _norm(c.get("Cost Type"))
        if "transport cost" not in ct.lower():
            continue
        p = c.get("Price")
        if p is None:
            continue
        try:
            pf = float(p)
        except (TypeError, ValueError):
            continue
        if round(pf, 2) != round(target, 2):
            continue
        if not _cost_line_valid_for_ship_date(c, ship):
            continue
        names.append(ct)
    return names


def format_transport_grouped_possible_rate_card_value_used(
    row: dict[str, Any],
    filtered: dict[str, Any],
    lane_num: str,
    result: GroupedTransportLaneResult,
    rate_agreement_id: Optional[str],
) -> str:
    """
    Compare invoice amount to lane tier lines; explain when the same price applies to other
    container types (e.g. 40′) while calculation used MEASUREMENT (e.g. 20′).
    """
    inv = _invoice_statement_cost_inv_curr(row)
    ship = _row_ship_date_as_date(row)
    inv_names = _transport_tier_names_matching_invoice_on_lane(
        filtered, lane_num, ship, inv
    )
    matched_set = {_norm(n) for n in result.matched_sub_cost_names}
    alt = [n for n in inv_names if _norm(n) not in matched_set]
    if alt:
        body = (
            f"Lane #: {lane_num}, invoice amount matches rate line(s) for "
            f"{', '.join(alt)} (valid for shipment date); calculation uses "
            f"{', '.join(result.matched_sub_cost_names)} from MEASUREMENT."
        )
        return _prefix_rate_agreement_id(body, rate_agreement_id)
    body = _match_cost_tiers_body_for_target_price(inv, filtered, "Transport cost")
    if body == "":
        return ""
    return _prefix_rate_agreement_id(body, rate_agreement_id)


def transport_grouped_lane_vs_shipment_note(
    lane_num: str,
    result: GroupedTransportLaneResult,
    invoice_tier_names: list[str],
    row: dict[str, Any],
) -> str:
    """Narrative when invoice matches alternate container tier(s) vs calculated sub-cost(s)."""
    matched_set = {_norm(n) for n in result.matched_sub_cost_names}
    alt = [n for n in invoice_tier_names if _norm(n) not in matched_set]
    if not alt:
        return ""
    cont = _primary_container_measurement_token(row) or "(see MEASUREMENT)"
    alt_txt = " or ".join(alt)
    mat = ", ".join(result.matched_sub_cost_names)
    return (
        f"Lane {lane_num}: invoice amount matches rate card line(s) for {alt_txt} "
        f"(validity includes shipment date) while MEASUREMENT supports calculation with "
        f"{mat} ({cont})."
    )


def _json_rate_cost_output(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    try:
        return _json_numeric_output(float(v))
    except (TypeError, ValueError):
        return v


def compute_lane_rate_cost_and_tier(
    row: dict[str, Any],
    filtered: dict[str, Any],
    best_lane_str: str,
    cost_type: str,
    defs_for_type: list[dict[str, Any]],
) -> tuple[Optional[float], Optional[dict[str, Any]]]:
    """
    Lane price from rate_card_data.Costs and the selected cost line (for Measurement: Flat vs p/unit).
    """
    lane_num = _first_lane_number(best_lane_str)
    if not lane_num:
        return None, None
    rate_card_data = filtered.get("rate_card_data") or []
    lane = _find_lane(rate_card_data, lane_num)
    if not lane:
        return None, None

    ct = _norm(cost_type)
    costs = [
        c
        for c in (lane.get("Costs") or [])
        if _cost_type_matches_row_to_card(ct, c.get("Cost Type"))
    ]
    if not costs:
        return None, None

    costs_use = costs
    rate_by = ""
    rounding_rule = ""
    if defs_for_type:
        if not any(applies_if_allows(d.get("Applies_if") or "", row) for d in defs_for_type):
            return None, None
        d_sel, costs_use = _pick_lane_cost_definition_and_rows(defs_for_type, costs, row)
        if d_sel is None:
            return None, None
        rate_by = (d_sel.get("Rate_by") or "").strip()
        rounding_rule = d_sel.get("Rounding_rule") or ""
    else:
        if any(str(c.get("Weight Bracket", "")).strip() for c in costs):
            rate_by = "Weight/chargeable kg"

    rb = _normalize_rate_by_string(rate_by)
    rbl = rb.lower()
    if _rate_by_uses_weight_tier_column(rb):
        w = _weight_for_weight_rate_by(rb, row)
        if w is None:
            return None, None
        w_eff = apply_rounding_for_rate(w, rounding_rule)
        tier = pick_tightest_weight_tier(w_eff, costs_use)
        if tier is None:
            # One ``p/unit`` row with no weight bracket = per-kg rate for all weights (e.g. "Total per Kg rate").
            unbr = [
                c
                for c in costs_use
                if not str(c.get("Weight Bracket", "")).strip()
                and _measurement_is_p_unit(c.get("Measurement"))
            ]
            if len(unbr) == 1:
                tier = unbr[0]
            else:
                return None, None
        p = tier.get("Price")
        if p is None or (isinstance(p, float) and pd.isna(p)):
            return None, None
        try:
            return float(p), tier
        except (TypeError, ValueError):
            return None, None

    if "per shipment" in rbl or rbl.strip() == "per shipment":
        for c in costs_use:
            if _norm(c.get("Measurement")) == "Flat" or not str(c.get("Weight Bracket", "")).strip():
                p = c.get("Price")
                if p is not None:
                    try:
                        return float(p), c
                    except (TypeError, ValueError):
                        return None, None

    if "volume" in rbl and "cbm" in rbl:
        if _cbm_value(row) is None:
            return None, None
        for c in costs_use:
            if _measurement_is_p_unit(c.get("Measurement")):
                p = c.get("Price")
                if p is None or (isinstance(p, float) and pd.isna(p)):
                    continue
                try:
                    return float(p), c
                except (TypeError, ValueError):
                    return None, None
        return None, None

    if _rate_by_is_quantity_container(rb):
        q = quantity_container_units_from_row(row)
        if q is None or abs(q) < 1e-12:
            return None, None
        for c in costs_use:
            if _measurement_is_p_unit(c.get("Measurement")):
                p = c.get("Price")
                if p is None or (isinstance(p, float) and pd.isna(p)):
                    continue
                try:
                    return float(p), c
                except (TypeError, ValueError):
                    return None, None
        return None, None

    if _rate_by_is_cost_measurement_token(rb):
        u = _units_for_measurement_token_rate_by(row, rb)
        if u is None:
            return None, None
        for c in costs_use:
            if _measurement_is_p_unit(c.get("Measurement")):
                p = c.get("Price")
                if p is None or (isinstance(p, float) and pd.isna(p)):
                    continue
                try:
                    return float(p), c
                except (TypeError, ValueError):
                    return None, None
        return None, None

    return None, None


def accessorial_price_and_tier_for_lane(
    blocks: list[dict[str, Any]],
    lane_num: str,
    cost_type: str,
    row: dict[str, Any],
) -> tuple[Optional[float], Optional[dict[str, Any]]]:
    """
    First matching tier Price + tier row (Lane #, Cost type) where tier ``Applies if`` holds
    (accessorial rules: Cost/… is available, Service does not equal …, etc.).
    """
    ct = _norm(cost_type)
    for b in blocks:
        if _norm(b.get("Cost type")) != ct:
            continue
        for t in b.get("Tiers") or []:
            if str(t.get("Lane #", "")).strip() != lane_num:
                continue
            applies = t.get("Applies if") or t.get("Applies_if") or ""
            if not accessorial_tier_applies(row, applies):
                continue
            p = t.get("Price")
            if p is not None:
                try:
                    return float(p), t
                except (TypeError, ValueError):
                    return None, None
    return None, None


def build_match_index(matched_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = matched_payload.get("matched_shipments") or []
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        k = _norm_etof(r.get("ETOF"))
        if k:
            out[k] = r
    return out


def enrich_mismatch_rows(
    mismatch_rows: list[dict[str, Any]],
    match_by_etof: dict[str, dict[str, Any]],
    partly_df: str,
    rate_card_cache: dict[str, dict[str, Any]],
    accessorial_cache: dict[str, list[dict[str, Any]]],
    etof_mappings: dict[str, str],
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    etof_cost_row = _build_etof_cost_type_row_index(mismatch_rows)
    for row in mismatch_rows:
        ca = _carrier_agreement_value(row)
        ra_id = extract_ra_id_from_carrier_agreement(ca) if ca else None

        etof_key = _norm_etof(row.get("ETOF_NUMBER") or row.get("ETOF"))
        match_row = match_by_etof.get(etof_key) if etof_key else None
        best_lane = ""
        if match_row is not None:
            best_lane = _norm(match_row.get(COL_BEST_LANES))

        cost_type = row.get("Cost type")
        applies_if, rate_by, rounding_rule = "", "", ""
        defs_for_type: list[dict[str, Any]] = []
        filtered: Optional[dict[str, Any]] = None
        acc_list: list[dict[str, Any]] = []

        if ra_id and cost_type is not None:
            rc_path = filtered_rate_card_json_path_for_ra_id(ra_id, partly_df)
            _cache_filtered_rate_card(rc_path, rate_card_cache)
            filtered = rate_card_cache.get(rc_path)

            if isinstance(filtered, dict):
                defs_for_type = _cost_defs_from_filtered(filtered, str(cost_type), row)
                applies_if, rate_by, rounding_rule = _fields_from_filtered_definitions(defs_for_type)

            acc_path = _accessorial_json_path(partly_df, ra_id)
            if acc_path not in accessorial_cache and os.path.isfile(acc_path):
                data = _load_json(acc_path)
                accessorial_cache[acc_path] = data if isinstance(data, list) else []
            acc_list = accessorial_cache.get(acc_path) or []

            if not applies_if and not rate_by and not rounding_rule:
                blocks = _cost_blocks_from_accessorial(acc_list, str(cost_type))
                applies_if, rate_by, rounding_rule = _fields_from_accessorial_blocks(blocks)

        rate_by = _normalize_rate_by_string(rate_by)

        blocks_ct: list[dict[str, Any]] = []
        if acc_list and cost_type is not None:
            blocks_ct = _cost_blocks_from_accessorial(acc_list, str(cost_type))

        rate_cost_val: Any = None
        cost_tier: Optional[dict[str, Any]] = None
        grouped_tr: Optional[GroupedTransportLaneResult] = None
        # Matched ``best_lane(s)`` may be e.g. 14 for transport; accessorial tiers are often Lane # 1 — try both.
        ln_from_match = _first_lane_number(best_lane) or "1"
        if isinstance(filtered, dict) and best_lane and cost_type is not None:
            gd = _single_grouped_transport_definition(defs_for_type)
            if gd is not None:
                grouped_tr = compute_grouped_transport_lane(row, filtered, best_lane, gd)
                if grouped_tr is not None:
                    rate_cost_val = grouped_tr.rate_cost_display
                    cost_tier = grouped_tr.representative_tier
                    applies_if = grouped_tr.merged_applies_if
                    rate_by = _normalize_rate_by_string(grouped_tr.merged_rate_by)
                    rounding_rule = grouped_tr.merged_rounding
                else:
                    rate_cost_val, cost_tier = None, None
            else:
                rate_cost_val, cost_tier = compute_lane_rate_cost_and_tier(
                    row, filtered, best_lane, str(cost_type), defs_for_type
                )
        ln_acc = ln_from_match
        if rate_cost_val is None and ra_id and cost_type is not None and blocks_ct:
            rate_cost_val, cost_tier = accessorial_price_and_tier_for_lane(
                blocks_ct, ln_acc, str(cost_type), row
            )
            if rate_cost_val is None and ln_acc != "1":
                rate_cost_val, cost_tier = accessorial_price_and_tier_for_lane(
                    blocks_ct, "1", str(cost_type), row
                )
                if rate_cost_val is not None:
                    ln_acc = "1"

        tier_measurement = (cost_tier or {}).get("Measurement")

        new_row = dict(row)
        new_row[COL_AGREEMENT_RA] = ra_id or ""
        new_row[COL_BEST_LANES] = best_lane
        new_row[COL_APPLIES_IF] = applies_if
        new_row[COL_RATE_BY] = rate_by
        new_row[COL_ROUNDING_RULE] = rounding_rule
        new_row[COL_RATE_COST] = _json_rate_cost_output(rate_cost_val)
        if grouped_tr is not None:
            rcc = grouped_tr.calculated_total
            rcf = compute_grouped_transport_rate_cost_file(row, grouped_tr)
            crf = compute_grouped_transport_carrier_rate_file(row, grouped_tr)
            rcc_comment = format_grouped_transport_rate_cost_comment(grouped_tr)
            rc_for_pcu = grouped_tr.rate_parts[0] if grouped_tr.rate_parts else None
        else:
            rcc = compute_rate_cost_calculated(
                rate_cost_val, rate_by, row, tier_measurement, rounding_rule
            )
            base_ct_fuel = (
                _accessorial_applies_over_cost(blocks_ct) if blocks_ct else ""
            )
            etof_k = _norm_etof(row.get("ETOF_NUMBER") or row.get("ETOF"))
            base_row_fuel = (
                _row_for_etof_and_cost_type(etof_cost_row, etof_k, base_ct_fuel)
                if (
                    _is_fuel_surcharge_cost_type(cost_type)
                    and base_ct_fuel
                    and etof_k
                )
                else None
            )
            if base_row_fuel is not None:
                rcf = compute_fuel_surcharge_rate_cost_file(row, base_row_fuel)
                crf = compute_fuel_surcharge_carrier_rate_file(row, base_row_fuel)
            else:
                rcf = compute_rate_cost_file(rate_by, row, tier_measurement, rounding_rule)
                crf = compute_carrier_rate_file(rate_by, row, tier_measurement, rounding_rule)
            rcc_comment = format_rate_cost_comment(
                rate_cost_val,
                rate_by,
                row,
                tier_measurement,
                cost_tier,
                rcc,
                rounding_rule,
            )
            rc_for_pcu = rate_cost_val
        new_row[COL_RATE_COST_CALCULATED] = _json_numeric_output(rcc)
        if not rcc_comment and rate_cost_val is None and cost_type is not None:
            if blocks_ct:
                miss = accessorial_measurement_missing_comment(
                    blocks_ct, str(cost_type), ln_acc, row
                )
                if miss:
                    rcc_comment = miss
                else:
                    lane_try: list[str] = []
                    for x in (ln_acc, "1"):
                        xs = str(x).strip() if x else ""
                        if xs and xs not in lane_try:
                            lane_try.append(xs)
                    eqc = accessorial_equipment_type_not_met_comment(
                        blocks_ct, str(cost_type), lane_try, row
                    )
                    if eqc:
                        rcc_comment = eqc
            if not rcc_comment:
                qm = rate_card_quantity_container_missing_comment(
                    defs_for_type or [], row, rate_by
                )
                if qm:
                    rcc_comment = qm
            if not rcc_comment and len(defs_for_type) == 1:
                gm = grouped_container_sub_rates_missing_measurement_comment(
                    defs_for_type[0], row
                )
                if gm:
                    rcc_comment = gm
        new_row[COL_RATE_COST_COMMENT] = rcc_comment
        new_row[COL_RATE_COST_FILE] = _json_numeric_output(rcf)
        new_row[COL_CARRIER_RATE_FILE] = _json_money_two_dp(crf)
        if grouped_tr is not None and isinstance(filtered, dict):
            new_row[COL_POSSIBLE_RATE_CARD_VALUE_USED] = (
                format_transport_grouped_possible_rate_card_value_used(
                    row, filtered, ln_from_match, grouped_tr, ra_id
                )
            )
        else:
            new_row[COL_POSSIBLE_RATE_CARD_VALUE_USED] = format_possible_rate_card_value_used(
                crf,
                filtered if isinstance(filtered, dict) else None,
                cost_type,
                ra_id,
                _invoice_statement_cost_inv_curr(row),
            )
        pcer = compute_possible_carrier_exchange_rate(rate_by, row, rc_for_pcu)
        new_row[COL_POSSIBLE_CARRIER_EXCHANGE_RATE] = _json_numeric_output(pcer)
        pcu = compute_possible_carrier_used_units(rate_by, row, rc_for_pcu)
        new_row[COL_POSSIBLE_CARRIER_USED_UNITS] = _json_numeric_output(pcu)
        new_row[COL_POSSIBLE_CARRIER_USED_UNITS_COMMENT] = format_possible_carrier_used_units_comment(
            pcu
        )
        s_another_crf = another_rate_card_lane_match_for_amount(
            crf, ra_id, partly_df, cost_type, rate_card_cache
        )
        if not (s_another_crf or "").strip():
            s_another_crf = primary_rate_card_alternate_lane_tier_strings(
                crf, filtered if isinstance(filtered, dict) else None, cost_type, best_lane, ra_id
            )
        inv_stmt = _invoice_statement_cost_inv_curr(row)
        s_another_inv = another_rate_card_lane_match_for_amount(
            inv_stmt, ra_id, partly_df, cost_type, rate_card_cache
        )
        if not (s_another_inv or "").strip():
            s_another_inv = primary_rate_card_alternate_lane_tier_strings(
                inv_stmt, filtered if isinstance(filtered, dict) else None, cost_type, best_lane, ra_id
            )
        new_row[COL_ANOTHER_RATE_CARD_CARRIER_USED_CRF] = s_another_crf
        new_row[COL_ANOTHER_RATE_CARD_CARRIER_USED_INV] = s_another_inv
        new_row[COL_BEST_MATCH_ANOTHER_RATE_CARD] = best_match_from_another_rate_card(
            s_another_crf, s_another_inv, rate_by, row
        )
        note_transport = ""
        if grouped_tr is not None and isinstance(filtered, dict):
            inv_names = _transport_tier_names_matching_invoice_on_lane(
                filtered,
                ln_from_match,
                _row_ship_date_as_date(row),
                _invoice_statement_cost_inv_curr(row),
            )
            note_transport = transport_grouped_lane_vs_shipment_note(
                ln_from_match, grouped_tr, inv_names, row
            )
        note_other = another_rate_card_lane_vs_shipment(
            row,
            new_row[COL_BEST_MATCH_ANOTHER_RATE_CARD],
            ra_id,
            partly_df,
            rate_card_cache,
            etof_mappings,
        )
        note_amount_lanes = amount_match_another_rate_card_lane_vs_shipment(
            row,
            s_another_crf,
            s_another_inv,
            ra_id,
            partly_df,
            rate_card_cache,
            etof_mappings,
        )
        note_price_alt = ""
        if isinstance(filtered, dict) and etof_mappings:
            ref_amt: Optional[float] = None
            if crf is not None:
                ref_amt = crf
            elif inv_stmt is not None:
                ref_amt = inv_stmt
            if ref_amt is not None:
                note_price_alt = price_matched_alternate_lanes_vs_shipment_note(
                    row, filtered, ra_id, ref_amt, cost_type, best_lane, etof_mappings
                )
        new_row[COL_ANOTHER_RC_LANE_VS_SHIPMENT] = "\n".join(
            x for x in (note_transport, note_amount_lanes, note_price_alt, note_other) if x
        )
        enriched.append(new_row)
    return enriched


def run_processing(
    partly_df: Optional[str] = None,
    mismatch_json: Optional[str] = None,
    matched_json: Optional[str] = None,
    output_json: Optional[str] = None,
    output_xlsx: Optional[str] = None,
) -> tuple[str, str]:
    """
    Load mismatch + matching JSON, enrich rows, write JSON array then Excel.

    Returns (output_json_path, output_xlsx_path).
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    partly_df = partly_df or os.path.join(script_dir, "partly_df")
    mismatch_json = mismatch_json or os.path.join(partly_df, DEFAULT_MISMATCH_PROCESSED_JSON)
    matched_json = matched_json or os.path.join(partly_df, DEFAULT_MATCHED_JSON)
    output_json = output_json or os.path.join(partly_df, DEFAULT_OUTPUT_JSON)
    output_xlsx = output_xlsx or os.path.join(partly_df, DEFAULT_OUTPUT_XLSX)

    mismatch_rows = _load_json(mismatch_json)
    if not isinstance(mismatch_rows, list):
        raise ValueError(f"Expected a JSON array in {mismatch_json}")

    matched_payload = _load_json(matched_json)
    if not isinstance(matched_payload, dict):
        raise ValueError(f"Expected an object with matched_shipments in {matched_json}")
    match_by_etof = build_match_index(matched_payload)

    rate_card_cache: dict[str, dict[str, Any]] = {}
    accessorial_cache: dict[str, list[dict[str, Any]]] = {}
    etof_mappings = load_vocab_etof_mappings(partly_df)
    enriched = enrich_mismatch_rows(
        mismatch_rows,
        match_by_etof,
        partly_df,
        rate_card_cache,
        accessorial_cache,
        etof_mappings,
    )

    os.makedirs(os.path.dirname(output_json) or ".", exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)

    df = pd.DataFrame(enriched)
    with pd.ExcelWriter(output_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Enriched", index=False)

    return output_json, output_xlsx


if __name__ == "__main__":
    j, x = run_processing()
    print(f"Wrote: {j}")
    print(f"Wrote: {x}")
