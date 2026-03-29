"""
Read Advanced Export rate card Excel, sheet Accessorial costs, export structured JSON.

Default output is per workbook: ``partly_df/accessorial_costs_<RA id>.json`` (RA from filename).

**Layouts**

1. **Currency table** — header row ``Lane #``, ``Currency``, measurement column, then tier rows
   with currency, price, and trailing ``Applies if`` cells.

2. **% over costs** — header ``Lane #``, a multiline **price column** label (``% - Over costs``,
   ``Applied over:``, ``• <base cost>``), ``Applies if``, ``Valid From``, ``Valid To``. JSON block
   gets ``Applies over`` (first line of that label), ``Applies over cost`` (e.g. ``Transport cost``
   from the bullet), and each tier has ``Price``, ``Applies if``, validity dates (no currency).
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

import pandas as pd

from rate_card_input import extract_rate_agreement_id_from_filename

# Try common sheet name variants (workbook naming differs)
ACCESSORIAL_SHEET_CANDIDATES = (
    "Accessorial costs",
    "Accessorial Cost",
    "Accessorial Costs",
)


def _find_accessorial_sheet_name(xl: pd.ExcelFile) -> str:
    """Resolve sheet name: exact candidates first, then any tab containing 'accessorial' + 'cost'."""
    names_lower = {n.strip().lower(): n for n in xl.sheet_names}
    for cand in ACCESSORIAL_SHEET_CANDIDATES:
        key = cand.strip().lower()
        if key in names_lower:
            return names_lower[key]
    for raw in xl.sheet_names:
        low = raw.strip().lower()
        if "accessorial" in low and "cost" in low.replace(" ", ""):
            return raw
    raise ValueError(
        f"No accessorial sheet found; expected one of {ACCESSORIAL_SHEET_CANDIDATES} "
        f"or a name containing 'accessorial' and 'cost'. Found: {xl.sheet_names}"
    )

# Default: partly_df/accessorial_costs_<RA id>.json (RA from Excel filename, same idea as Filtered_Rate_Card_with_Conditions_<RA>.json)
ACCESSORIAL_JSON_BASENAME_TEMPLATE = "accessorial_costs_{ra_id}.json"


def default_accessorial_costs_json_basename(input_path: str) -> str:
    """e.g. accessorial_costs_RA20250911028.json — RA parsed from workbook filename."""
    ra_id = extract_rate_agreement_id_from_filename(input_path)
    return ACCESSORIAL_JSON_BASENAME_TEMPLATE.format(ra_id=ra_id)


def _norm(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def _read_accessorial_dataframe(path: str) -> pd.DataFrame:
    xl = pd.ExcelFile(path)
    sheet = _find_accessorial_sheet_name(xl)
    return pd.read_excel(path, sheet_name=sheet, header=None)


def _row_is_empty(row: list[str]) -> bool:
    return not any(x for x in row)


def _cell_is_lane_column_header(s: str) -> bool:
    """True for 'Lane #', 'Lane#', 'Lane  #', etc."""
    t = _norm(s).lower().replace(" ", "")
    return t == "lane#" or t.startswith("lane#")


def _cell_is_currency_header(s: str) -> bool:
    return _norm(s).lower() == "currency"


def _header_cell_is_applies_if(s: str) -> bool:
    t = _norm(s).lower().replace(" ", "")
    return t == "appliesif" or t.startswith("appliesif")


def _is_over_costs_lane_header(row: list[str]) -> bool:
    """
    Lane table without ``Currency``: column B is a multiline "% - Over costs / Applied over …" label,
    then ``Applies if``, ``Valid From``, ``Valid To``.
    """
    if len(row) < 3 or not _cell_is_lane_column_header(row[0]):
        return False
    b = _norm(row[1]).lower()
    if not b:
        return False
    looks_like_over = (
        ("%" in b and "over" in b)
        or "applied over" in b
        or ("over" in b and "cost" in b)
    )
    if not looks_like_over:
        return False
    if _header_cell_is_applies_if(row[2]):
        return True
    for x in row[2 : min(6, len(row))]:
        if _header_cell_is_applies_if(x):
            return True
    return False


def _parse_applies_over_header_cell(text: str) -> tuple[str, str]:
    """
    From e.g. ``% - Over costs`` / ``Applied over:`` / ``• Transport cost`` →
    (``Applies over`` label, ``Applies over cost``).
    """
    raw = _norm(text)
    if not raw:
        return "", ""
    lines = [ln.strip() for ln in re.split(r"[\r\n]+", raw) if ln.strip()]
    if not lines:
        return "", ""
    applies_over = lines[0]
    applies_over_cost = ""
    seen_applied = False
    for ln in lines[1:]:
        low = ln.lower()
        if low.startswith("applied over"):
            seen_applied = True
            rest = ln.split(":", 1)[1].strip() if ":" in ln else ""
            if rest:
                applies_over_cost = rest
            continue
        m = re.match(r"^[•\u2022\-\*]\s*(.+)$", ln)
        if m:
            applies_over_cost = m.group(1).strip()
            break
        if seen_applied and ln and not applies_over_cost:
            applies_over_cost = ln
            break
    return applies_over, applies_over_cost


def _lane_number_display(s: str) -> str:
    """Stable ``Lane #`` string for JSON (Excel may emit 1.0)."""
    t = _norm(s)
    if not t:
        return ""
    if t.isdigit():
        return t
    try:
        v = float(t)
        return str(int(round(v)))
    except ValueError:
        return t


def _is_lane_header(row: list[str]) -> bool:
    if len(row) < 2:
        return False
    if not _cell_is_lane_column_header(row[0]):
        return False
    if _cell_is_currency_header(row[1]):
        return True
    # Merged cell left column B empty; "Currency" may sit in C or D
    return any(_cell_is_currency_header(x) for x in row[2:8])


def _first_cell_is_lane_number(s: str) -> bool:
    """Lane index in column A: integer or Excel float like 1.0."""
    t = _norm(s)
    if not t:
        return False
    if t.isdigit():
        return True
    try:
        v = float(t)
        return v >= 0 and abs(v - int(round(v))) < 1e-9
    except ValueError:
        return False


def _is_lane_data_row(row: list[str]) -> bool:
    if not row or not row[0]:
        return False
    if not _first_cell_is_lane_number(row[0]):
        return False
    if len(row) < 2:
        return False
    # Standard: currency in column B; merged sheet may leave B blank and use C
    if row[1]:
        return True
    return len(row) > 2 and bool(row[2])


def _cost_title_text_in_col_b(row: list[str]) -> bool:
    return len(row) >= 2 and bool(row[1])


def _metadata_starts(s: str) -> bool:
    s = _norm(s)
    if not s:
        return False
    low = s.lower()
    return low.startswith("rate by") or low.startswith("multiplier")


def _looks_like_cost_title(row: list[str]) -> bool:
    if not row:
        return False
    if _cell_is_lane_column_header(row[0]):
        return False
    if _is_lane_header(row):
        return False
    if _is_lane_data_row(row):
        return False
    # Typical: cost name in column B (unmerged)
    if _cost_title_text_in_col_b(row):
        b = row[1]
        if _metadata_starts(b):
            return False
        return True
    # Merged title: only column A has text (column B empty in export)
    a = row[0]
    if not a:
        return False
    if _metadata_starts(a):
        return False
    return True


def _cost_type_from_title_row(row: list[str]) -> str:
    if _cost_title_text_in_col_b(row):
        return row[1]
    return row[0]


def _parse_rate_cell(text: str) -> tuple[Optional[str], Optional[str]]:
    """Split 'Rate by: …' and optional 'Regular rule' line."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    if not lines:
        return None, None
    rate_by = None
    rule = None
    first = lines[0]
    low = first.lower()
    if low.startswith("rate by:"):
        rate_by = first.split(":", 1)[1].strip()
    elif low.startswith("rate by"):
        rate_by = first.split(":", 1)[1].strip() if ":" in first else first
    for ln in lines[1:]:
        if re.search(r"rule", ln, re.I):
            rule = ln
            break
    return rate_by, rule


def _join_applies_if(row: list[str]) -> str:
    start = 3
    if len(row) > 2 and not row[1] and row[2]:
        start = 4
    parts = [row[i] for i in range(start, len(row)) if row[i]]
    return " ".join(parts).strip()


def _accessorial_currency_and_price(row: list[str]) -> tuple[str, Any]:
    """Currency and price cells; column B may be empty when the sheet uses merged cells."""
    if len(row) >= 2 and row[1]:
        return row[1], _parse_price(row[2] if len(row) > 2 else "")
    if len(row) >= 3 and row[2]:
        return row[2], _parse_price(row[3] if len(row) > 3 else "")
    return "", None


def _parse_price(cell: str) -> Any:
    s = _norm(cell)
    if not s:
        return None
    try:
        if "." in s:
            return float(s)
        return int(s)
    except ValueError:
        return s


def parse_accessorial_costs_dataframe(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Parse raw Accessorial costs sheet into a list of cost blocks."""
    nrows, ncols = df.shape
    rows: list[list[str]] = []
    for i in range(nrows):
        rows.append([_norm(df.iloc[i, j]) for j in range(ncols)])

    out: list[dict[str, Any]] = []
    i = 0
    while i < nrows:
        row = rows[i]
        if _row_is_empty(row):
            i += 1
            continue
        if not _looks_like_cost_title(row):
            i += 1
            continue

        title_i = i
        cost_type = _cost_type_from_title_row(row)
        i += 1
        multiplier: Optional[str] = None
        rate_lines: list[str] = []

        while i < nrows:
            r = rows[i]
            if _row_is_empty(r):
                i += 1
                continue
            if _is_lane_header(r):
                break
            if _is_over_costs_lane_header(r):
                break
            if _looks_like_cost_title(r) and _cost_type_from_title_row(r) != cost_type:
                # New block started without finishing previous — shouldn't happen
                break
            cell1 = r[1] if len(r) > 1 else ""
            cell0 = r[0] if r else ""
            mult_src = (
                cell1
                if cell1.startswith("Multiplier")
                else (cell0 if cell0.startswith("Multiplier") else "")
            )
            if mult_src:
                multiplier = mult_src
                i += 1
                continue
            rate_src = (
                cell1
                if (cell1.startswith("Rate by") or "Rate by" in cell1)
                else ""
            )
            if not rate_src and (cell0.startswith("Rate by") or "Rate by" in cell0):
                rate_src = cell0
            if rate_src:
                rate_lines.append(rate_src)
                i += 1
                continue
            i += 1

        if i >= nrows:
            i = title_i + 1
            continue

        hdr = rows[i]
        tiers: list[dict[str, Any]] = []

        if _is_over_costs_lane_header(hdr):
            applies_over, applies_over_cost = _parse_applies_over_header_cell(
                hdr[1] if len(hdr) > 1 else ""
            )
            i += 1
            while i < nrows:
                r = rows[i]
                if _row_is_empty(r):
                    i += 1
                    continue
                if _looks_like_cost_title(r):
                    break
                if _is_lane_header(r) or _is_over_costs_lane_header(r):
                    break
                if _is_lane_data_row(r):
                    tier_oc: dict[str, Any] = {
                        "Lane #": _lane_number_display(r[0]),
                        "Measurement": applies_over,
                        "Currency": "",
                        "Price": _parse_price(r[1] if len(r) > 1 else ""),
                        "Applies if": r[2] if len(r) > 2 else "",
                        "Valid From": r[3] if len(r) > 3 else "",
                        "Valid To": r[4] if len(r) > 4 else "",
                    }
                    tiers.append(tier_oc)
                    i += 1
                    continue
                i += 1

            rate_by: Optional[str] = None
            rule: Optional[str] = None
            for rl in rate_lines:
                rb, ru = _parse_rate_cell(rl)
                if rb:
                    rate_by = rb
                if ru:
                    rule = ru

            block_oc: dict[str, Any] = {"Cost type": cost_type}
            if multiplier:
                block_oc["Multiplier"] = multiplier
            if rate_by:
                block_oc["Rate by"] = rate_by
            if rule:
                block_oc["Rule"] = rule
            if applies_over:
                block_oc["Applies over"] = applies_over
            if applies_over_cost:
                block_oc["Applies over cost"] = applies_over_cost
            block_oc["Tiers"] = tiers
            out.append(block_oc)
            continue

        if not _is_lane_header(hdr):
            i = title_i + 1
            continue

        measurement_label = hdr[2] if len(hdr) > 2 else "p/unit"
        if len(hdr) > 3 and not hdr[2] and hdr[3]:
            measurement_label = hdr[3]
        i += 1

        while i < nrows:
            r = rows[i]
            if _row_is_empty(r):
                i += 1
                continue
            if _looks_like_cost_title(r):
                break
            if _is_lane_header(r) or _is_over_costs_lane_header(r):
                break
            if _is_lane_data_row(r):
                applies = _join_applies_if(r)
                cur, pr = _accessorial_currency_and_price(r)
                tier: dict[str, Any] = {
                    "Lane #": _lane_number_display(r[0]),
                    "Measurement": measurement_label,
                    "Currency": cur,
                    "Price": pr,
                    "Applies if": applies,
                }
                tiers.append(tier)
                i += 1
                continue
            i += 1

        rate_by = None
        rule = None
        for rl in rate_lines:
            rb, ru = _parse_rate_cell(rl)
            if rb:
                rate_by = rb
            if ru:
                rule = ru

        block: dict[str, Any] = {"Cost type": cost_type}
        if multiplier:
            block["Multiplier"] = multiplier
        if rate_by:
            block["Rate by"] = rate_by
        if rule:
            block["Rule"] = rule
        block["Tiers"] = tiers
        out.append(block)

    return out


def process_accessorial_costs_file(
    input_path: str,
    output_path: Optional[str] = None,
) -> str:
    """
    Read Excel accessorial sheet, write JSON (list of cost blocks).

    Default output: ``partly_df/accessorial_costs_<RA id>.json`` where ``RA id`` is taken
    from the workbook filename (e.g. ``...RA20250911028...xlsx``), so each rate card Excel
    gets its own JSON. Pass ``output_path`` to override.

    Returns path to written JSON.
    """
    df = _read_accessorial_dataframe(input_path)
    data = parse_accessorial_costs_dataframe(df)

    if output_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        out_dir = os.path.join(script_dir, "partly_df")
        os.makedirs(out_dir, exist_ok=True)
        basename = default_accessorial_costs_json_basename(input_path)
        output_path = os.path.join(out_dir, basename)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return output_path


def resolve_workbook_path(name_or_path: str) -> str:
    """
    For CLI convenience: bare filenames are looked up under ``input/``; existing paths are kept.

    Examples: ``"foo.xlsx"`` → ``input/foo.xlsx``; ``"input/foo.xlsx"`` or an absolute path unchanged if the file exists.
    """
    s = name_or_path.strip().strip('"').strip("'")
    if os.path.isfile(s):
        return os.path.normpath(os.path.abspath(s))
    base = os.path.basename(s)
    under = os.path.join("input", base if base else s)
    if os.path.isfile(under):
        return os.path.normpath(under)
    under2 = os.path.join("input", s)
    if os.path.isfile(under2):
        return os.path.normpath(under2)
    return os.path.normpath(under2)


def process_accessorial_costs_files(input_paths: list[str]) -> list[str]:
    """
    Run :func:`process_accessorial_costs_file` for each workbook; each file writes its own
    ``partly_df/accessorial_costs_<RA>.json``. Returns list of written paths.
    """
    out: list[str] = []
    for p in input_paths:
        out.append(process_accessorial_costs_file(p))
    return out


if __name__ == "__main__":
    import sys

    # Edit this list to run without CLI args — filenames only (files must sit in input/).
    _DEFAULT_FILENAMES = [
        "Advanced Export - RA20241217021 v.10 - MAERSK Consolidator.xlsx",
    ]

    # With args: pass bare filenames or full paths, e.g.
    #   python rate_card_accessorial_costs.py "Advanced Export - RA20250326009....xlsx" "Advanced Export - RA20250826013....xlsx"
    raw = sys.argv[1:] if len(sys.argv) > 1 else _DEFAULT_FILENAMES
    inputs = [resolve_workbook_path(x) for x in raw]
    for inp in inputs:
        outp = process_accessorial_costs_file(inp)
        print(f"Written: {outp}")
        print(f"Blocks: {len(json.load(open(outp, encoding='utf-8')))}")
