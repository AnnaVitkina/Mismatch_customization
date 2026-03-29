"""
Process mismatch export Excel: sort, drop columns, merge non-USD inv-currency rows, filter zero discrepancies.
"""
from __future__ import annotations

import os
from typing import Optional, Union

import pandas as pd

try:
    from shipment_input import DEFAULT_PROCESSED_SHIPMENT_JSON
except ImportError:
    DEFAULT_PROCESSED_SHIPMENT_JSON = "etof_processed_apple.json"

# Default outputs under partly_df (same paths for any input unless output_path is set)
DEFAULT_MISMATCH_PROCESSED_XLSX = "mismatch_processed.xlsx"
DEFAULT_MISMATCH_PROCESSED_JSON = "mismatch_processed.json"

# Sort / grouping key order (step 1)
GROUP_SORT_COLUMNS = [
    "SERVICE_ISD",
    "SHIP_COUNTRY_ISD",
    "SHIP_COUNTRY_ETOF",
    "CUST_COUNTRY_ETOF",
    "CUST_COUNTRY_ISD",
    "SHIP_CITY_ETOF",
    "SHIP_CITY_ISD",
    "CUST_CITY_ETOF",
    "CUST_CITY_ISD",
]

# Columns to remove (step 2)
COLUMNS_TO_DROP = [
    "Cost currency",
    "Pre-calc. cost value",
    "Pre-calc. cost adjusted",
    "Invoice statement cost value",
    "Invoice statement cost adjusted",
    "Discrepancy",
]

COL_PRECALC_INV = "Pre-calc. cost (in inv curr)"
COL_INV_STMT_INV = "Invoice statement cost  (in inv curr)"  # two spaces before '('
COL_DISC_INV = "Discrepancy in inv currency  (in inv curr)"  # two spaces before '('
COL_INV_CURR = "Invoice currency"

# Output column order: mismatch fields, then etof-only fields, then cost/invoice columns (fixed order)
TRAILING_COST_COLUMNS: list[str] = [
    "Cost type",
    COL_INV_CURR,
    COL_PRECALC_INV,
    COL_INV_STMT_INV,
    COL_DISC_INV,
    "Exchange rate",
]
TRAILING_COST_SET = frozenset(TRAILING_COST_COLUMNS)

# Join processed mismatch rows to shipment_input result (process_etof_file)
SHIPMENT_ETOF_COLUMN = "ETOF"  # renamed from "ETOF #" in shipment_input
MISMATCH_ETOF_COLUMN = "ETOF_NUMBER"

# ETOF file uses single columns (e.g. SHIP_COUNTRY); mismatch uses SHIP_COUNTRY_ETOF / _ISD — skip etof copy
# SERVICE is always merged from the etof file (see _skip_shipment_column_for_merge).
SEMANTIC_SHIPMENT_COVERAGE: dict[str, list[str]] = {
    "SHIP_COUNTRY": ["SHIP_COUNTRY_ETOF", "SHIP_COUNTRY_ISD"],
    "CUST_COUNTRY": ["CUST_COUNTRY_ETOF", "CUST_COUNTRY_ISD"],
    "SHIP_CITY": ["SHIP_CITY_ETOF", "SHIP_CITY_ISD"],
    "CUST_CITY": ["CUST_CITY_ETOF", "CUST_CITY_ISD"],
    "CUST_POST": ["CUST_POST_ETOF", "CUST_POST_ISD"],
    "SHIP_POST": ["SHIP_POST_ETOF", "SHIP_POST_ISD"],
    "SHIP_AIRPORT": ["SHIP_AIRPORT_ETOF", "SHIP_AIRPORT_ISD"],
    "CUST_AIRPORT": ["CUST_AIRPORT_ETOF", "CUST_AIRPORT_ISD"],
    "SHIP_SEAPORT": ["SHIP_SEAPORT_ETOF", "SHIP_SEAPORT_ISD", "SHIP_SEAPORT"],
    "CUST_SEAPORT": ["CUST_SEAPORT_ETOF", "CUST_SEAPORT_ISD", "CUST_SEAPORT"],
}

# Keys used to merge split non-USD rows (same cost line split across two rows)
MERGE_EXTRA_KEYS = ["ETOF_NUMBER", "Cost type"]


def _skip_shipment_column_for_merge(column: str, mismatch_cols: set[str]) -> bool:
    """
    True = do not add this etof column (already on mismatch under same or equivalent name).
    ``SERVICE`` is never skipped — always taken from the etof file.
    """
    if column == "_etof_join":
        return True
    if column in mismatch_cols:
        return True
    if column == "SERVICE":
        return False
    if column in ("ETOF", "ETOF_NUMBER"):
        if MISMATCH_ETOF_COLUMN in mismatch_cols or "ETOF" in mismatch_cols:
            return True
    if column in ("ISD", "ISD_NUMBER"):
        if "ISD_NUMBER" in mismatch_cols or "ISD" in mismatch_cols:
            return True
    equiv = SEMANTIC_SHIPMENT_COVERAGE.get(column)
    if equiv and any(x in mismatch_cols for x in equiv):
        return True
    return False


def _finalize_columns_order(
    df: pd.DataFrame,
    mismatch_order: list[str],
    etof_order: list[str],
) -> pd.DataFrame:
    """
    Columns order: mismatch (``mismatch_order`` minus trailing cost cols), then etof-only
    (``etof_order``), then any leftover, then trailing cost columns in ``TRAILING_COST_COLUMNS`` order.
    """
    if df.empty:
        return df
    trailing = [c for c in TRAILING_COST_COLUMNS if c in df.columns]
    first = [c for c in mismatch_order if c in df.columns and c not in TRAILING_COST_SET]
    second = [c for c in etof_order if c in df.columns]
    used = set(first + second + trailing)
    rest = [c for c in df.columns if c not in used]
    ordered = first + second + rest + trailing
    return df[ordered]


def _resolve_columns(df: pd.DataFrame, names: list) -> list:
    """Return names that exist on df (exact match)."""
    return [c for c in names if c in df.columns]


def _drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove columns that are entirely NaN or blank strings."""
    drop_cols = []
    for c in df.columns:
        s = df[c]
        if s.dtype == object or pd.api.types.is_string_dtype(s):
            empty = s.isna() | (s.astype(str).str.strip() == "") | (s.astype(str).str.lower() == "nan")
        else:
            empty = s.isna()
        if empty.all():
            drop_cols.append(c)
    if drop_cols:
        return df.drop(columns=drop_cols)
    return df


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _merge_non_usd_group(g: pd.DataFrame) -> pd.DataFrame:
    """
    Merge rows for the same ETOF + Cost type + group keys when Invoice currency != USD.
    Combine Pre-calc from the row with non-zero pre-calc (max) and Invoice statement from
    the row where pre-calc is 0 when such a row exists; otherwise use max invoice.
    """
    if len(g) == 1:
        row = g.iloc[0].copy()
        pc = _to_num(pd.Series([row[COL_PRECALC_INV]])).iloc[0]
        inv = _to_num(pd.Series([row[COL_INV_STMT_INV]])).iloc[0]
        if pd.notna(pc) and pd.notna(inv):
            row[COL_DISC_INV] = float(pc) - float(inv)
        return pd.DataFrame([row])

    g = g.copy()
    pc = _to_num(g[COL_PRECALC_INV]).fillna(0.0)
    inv = _to_num(g[COL_INV_STMT_INV]).fillna(0.0)

    precalc_final = float(pc.max())
    zero_pc = pc == 0
    if zero_pc.any():
        inv_pick = inv.loc[zero_pc]
        inv_final = float(inv_pick.iloc[0] if len(inv_pick) == 1 else inv_pick.max())
    else:
        inv_final = float(inv.max())

    out = g.iloc[[0]].copy()
    out[COL_PRECALC_INV] = precalc_final
    out[COL_INV_STMT_INV] = inv_final
    out[COL_DISC_INV] = precalc_final - inv_final
    return out


def _merge_non_usd_block(df_non: pd.DataFrame, merge_keys: list) -> pd.DataFrame:
    """Merge non-USD rows per merge_keys; avoid groupby.apply (pandas may drop non-numeric columns)."""
    if len(df_non) == 0:
        return df_non
    pieces: list[pd.DataFrame] = []
    for _, g in df_non.groupby(merge_keys, sort=False):
        pieces.append(_merge_non_usd_group(g))
    return pd.concat(pieces, ignore_index=True)


def add_shipment_columns_by_etof(
    df_mismatch: pd.DataFrame,
    df_shipment: pd.DataFrame,
) -> pd.DataFrame:
    """
    Keep only mismatch rows whose ``ETOF_NUMBER`` appears in the ETOF extract, then join
    columns from ``etof_processed_apple.json`` that are not already on the mismatch frame
    (same shipment values for every row with the same ETOF).
    """
    if df_mismatch.empty or df_shipment.empty:
        return _finalize_columns_order(df_mismatch, list(df_mismatch.columns), [])
    if MISMATCH_ETOF_COLUMN not in df_mismatch.columns:
        return _finalize_columns_order(df_mismatch, list(df_mismatch.columns), [])
    ship_etof = _resolve_shipment_etof_column(df_shipment)
    if ship_etof is None:
        return _finalize_columns_order(df_mismatch, list(df_mismatch.columns), [])

    df_s = df_shipment.copy()
    df_s["_etof_join"] = df_s[ship_etof].astype(str).str.strip()
    df_s = df_s.drop_duplicates(subset=["_etof_join"], keep="first")
    allowed = set(df_s["_etof_join"].dropna())

    mismatch_cols = set(df_mismatch.columns)
    ship_only = [
        c
        for c in df_s.columns
        if not _skip_shipment_column_for_merge(c, mismatch_cols)
    ]

    df_m = df_mismatch.copy()
    df_m["_etof_join"] = df_m[MISMATCH_ETOF_COLUMN].astype(str).str.strip()
    df_m = df_m.loc[df_m["_etof_join"].isin(allowed)].copy()
    mismatch_order = [c for c in df_m.columns if c != "_etof_join"]

    if not ship_only:
        out = df_m.drop(columns=["_etof_join"])
        return _finalize_columns_order(out, mismatch_order, [])

    right = df_s[["_etof_join"] + ship_only]
    df_out = df_m.merge(right, on="_etof_join", how="left")
    df_out = df_out.drop(columns=["_etof_join"])
    return _finalize_columns_order(df_out, mismatch_order, ship_only)


def _load_shipment_dataframe(path: str) -> pd.DataFrame:
    """Load shipment extract from .xlsx or .json (records)."""
    path = os.path.abspath(path)
    if path.lower().endswith(".json"):
        return pd.read_json(path)
    return pd.read_excel(path)


def _default_shipment_extract_path() -> str:
    """Default processed ETOF extract: ``partly_df/<DEFAULT_PROCESSED_SHIPMENT_JSON>``."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "partly_df", DEFAULT_PROCESSED_SHIPMENT_JSON)


def _resolve_shipment_etof_column(df: pd.DataFrame) -> Optional[str]:
    """Prefer ``ETOF`` from ``process_etof_file``; else ``ETOF_NUMBER`` if the extract uses mismatch-style names."""
    if SHIPMENT_ETOF_COLUMN in df.columns:
        return SHIPMENT_ETOF_COLUMN
    if MISMATCH_ETOF_COLUMN in df.columns:
        return MISMATCH_ETOF_COLUMN
    return None


def process_mismatch_dataframe(
    df: pd.DataFrame,
    shipment_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Apply steps 1–5 to a mismatch export DataFrame.

    1. Sort by GROUP_SORT_COLUMNS (only those present).
    2. Drop COLUMNS_TO_DROP (those present).
    3. Drop all-empty columns.
    4. If Invoice currency is not USD: merge rows per (group keys + ETOF_NUMBER + Cost type),
       then set Discrepancy in inv currency = Pre-calc (inv) - Invoice statement (inv).
    5. Drop rows where Discrepancy in inv currency (in inv curr) == 0.
    6. If ``shipment_df`` is given: drop mismatch rows whose ``ETOF_NUMBER`` is not in the ETOF
       extract, then merge in shipment-only columns (``ETOF_NUMBER`` = shipment ``ETOF``).
    7. Drop all-empty columns again (after merge / column order).
    """
    df = df.copy()

    sort_cols = _resolve_columns(df, GROUP_SORT_COLUMNS)
    if sort_cols:
        df = df.sort_values(by=sort_cols, kind="mergesort", na_position="last").reset_index(drop=True)

    to_drop = [c for c in COLUMNS_TO_DROP if c in df.columns]
    if to_drop:
        df = df.drop(columns=to_drop)

    df = _drop_empty_columns(df)

    if COL_INV_CURR not in df.columns or COL_DISC_INV not in df.columns:
        out = df
        if shipment_df is not None:
            out = add_shipment_columns_by_etof(out, shipment_df)
        else:
            out = _finalize_columns_order(out, list(out.columns), [])
        return _drop_empty_columns(out)

    inv_curr = df[COL_INV_CURR].astype(str).str.strip().str.upper()
    is_usd = inv_curr == "USD"

    merge_keys = _resolve_columns(df, GROUP_SORT_COLUMNS) + _resolve_columns(df, MERGE_EXTRA_KEYS)
    merge_keys = list(dict.fromkeys(merge_keys))  # unique, preserve order

    need_merge_cols = {COL_PRECALC_INV, COL_INV_STMT_INV, COL_DISC_INV}
    if not merge_keys or not need_merge_cols.issubset(df.columns):
        out = df
    else:
        df_usd = df.loc[is_usd].copy()
        df_non = df.loc[~is_usd].copy()

        if len(df_non) == 0:
            out = df_usd
        elif len(df_usd) == 0:
            out = _merge_non_usd_block(df_non, merge_keys)
        else:
            merged_non = _merge_non_usd_block(df_non, merge_keys)
            out = pd.concat([df_usd, merged_non], ignore_index=True)

    # Step 5: remove zero discrepancy (compare numerically)
    disc = _to_num(out[COL_DISC_INV])
    mask_nonzero = disc.isna() | (disc != 0)
    # Treat tiny float noise as zero
    mask_nonzero = mask_nonzero & ~(disc.fillna(1).abs() < 1e-9)
    out = out.loc[mask_nonzero].reset_index(drop=True)

    sort_cols_final = _resolve_columns(out, GROUP_SORT_COLUMNS)
    if sort_cols_final:
        out = out.sort_values(by=sort_cols_final, kind="mergesort", na_position="last").reset_index(drop=True)

    if shipment_df is not None:
        out = add_shipment_columns_by_etof(out, shipment_df)
    else:
        out = _finalize_columns_order(out, list(out.columns), [])

    return _drop_empty_columns(out)


def process_mismatch_file(
    input_path: str,
    output_path: Optional[str] = None,
    sheet_name: Union[str, int] = 0,
    shipment_df: Optional[pd.DataFrame] = None,
    shipment_path: Optional[str] = None,
) -> tuple[str, str]:
    """
    Read Excel, process, write result as .xlsx and .json.

    Args:
        input_path: Path to .xlsx (absolute or relative to cwd).
        output_path: Optional output .xlsx. Default: ``partly_df/mismatch_processed.xlsx``
            (and ``partly_df/mismatch_processed.json``).
        sheet_name: Sheet to read (name or index).
        shipment_df: Optional dataframe from ``shipment_input.process_etof_file`` (uses column ``ETOF``).
        shipment_path: Optional path to **processed** shipment .json/.xlsx (not raw ETOF input).
            If omitted, loads ``partly_df/<DEFAULT_PROCESSED_SHIPMENT_JSON>`` when present.

    Returns:
        (path_to_xlsx, path_to_json)
    """
    df = pd.read_excel(input_path, sheet_name=sheet_name)
    ship = shipment_df
    if ship is None:
        path = shipment_path
        if path is None:
            candidate = _default_shipment_extract_path()
            path = candidate if os.path.isfile(candidate) else None
        if path is not None:
            ship = _load_shipment_dataframe(path)
    out_df = process_mismatch_dataframe(df, shipment_df=ship)

    if output_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        out_dir = os.path.join(script_dir, "partly_df")
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, DEFAULT_MISMATCH_PROCESSED_XLSX)
        json_path = os.path.join(out_dir, DEFAULT_MISMATCH_PROCESSED_JSON)
    else:
        json_path = os.path.splitext(output_path)[0] + ".json"

    out_df.to_excel(output_path, index=False, sheet_name="data")
    out_df.to_json(
        json_path,
        orient="records",
        date_format="iso",
        indent=2,
        force_ascii=False,
    )

    return output_path, json_path


if __name__ == "__main__":
    import sys

    default_in = os.path.join("input", "mismatch (23).xlsx")
    inp = sys.argv[1] if len(sys.argv) > 1 else default_in
    rest = sys.argv[2:]
    no_shipment = "--no-shipment" in rest
    override = [a for a in rest if a != "--no-shipment"]
    ship_override = override[0] if override else None
    xlsx_out, json_out = process_mismatch_file(
        inp,
        shipment_path=None if no_shipment else ship_override,
        shipment_df=pd.DataFrame() if no_shipment else None,
    )
    print(f"Written: {xlsx_out}")
    print(f"Written: {json_out}")
