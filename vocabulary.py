

"""
Vocabulary Mapping Script

This script:
1. Collects column lists from rate_card_input (rate card) and shipment_input (ETOF/shipment result only).
2. Creates a vocabulary DataFrame mapping ETOF columns to standard names (rate card columns).
3. Uses regex-style direct mappings and fuzzy string matching only (no ML model, no per-client custom logic).
4. Filters to keep only relevant columns (rate card columns, key identifiers).

--------------------------------------------------------------------------------
HOW MAPPING WORKS NOW
--------------------------------------------------------------------------------

1. SOURCES
   - Rate card columns: from process_rate_card() — these are the "standard" names (target).
   - ETOF columns: from process_etof_file() — these are the shipment_input RESULT columns
     (e.g. ETOF, LC, CARRIER_NAME, SHIP_DATE, SHIP_COUNTRY, CUST_COUNTRY, SERVICE, ...).

2. WHO DRIVES THE MAPPING
   - Rate card columns drive. For each RATE CARD column we look for one matching ETOF column.
   - One-to-one: each ETOF column is used at most once.

3. USER MAPPING (optional)
   - If you set ETOF_TO_RATE_CARD_MAPPING below, it is applied first.
   - Keys = rate card column names (exact), values = ETOF result column names (exact).
   - Any rate card column not in the map (or whose ETOF column is missing) still uses
     the automatic logic (find_column_match).

4. AUTOMATIC LOGIC (find_column_match)
   - Input: (target_col = rate card column name, candidate_cols = list of ETOF column names).
   - Order of checks:
     a) Direct rules: postal (ship_post/cust_post), flow type (category), port (origin/destination airport), country (ship_country/cust_country). If target and a candidate match the same rule, return that candidate (score 0.95).
     b) Exact match: target_lower == cand_lower → score 1.0.
     c) Normalized match: normalize_for_semantics() on both; if equal or one contains the other and similarity > 0.7 → return candidate.
     d) Fuzzy: best SequenceMatcher ratio among candidates; if >= threshold (0.3) return it.

5. WHERE IT'S USED
   - create_vocabulary_dataframe(): builds vocabulary DataFrame (Source, Source_Column, Standard_Name, Mapping_Method, Confidence). Uses user map first, then find_column_match per rate card column.
   - map_and_rename_columns(): builds etof_mappings {rate_card_col: etof_col}, then create_output_dataframe() renames ETOF columns to rate card names and keeps key columns (ETOF, LC, CARRIER_NAME, SHIP_DATE, etc.).
"""

import json
import math
import pandas as pd
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from difflib import SequenceMatcher

# Import from Apple CANF customization: rate card + shipment (ETOF) result only
from rate_card_input import (
    process_rate_card,
    process_business_rules,
    transform_business_rules_to_conditions,
    find_business_rule_columns,
    get_required_geo_columns,
    extract_ra_id_from_carrier_agreement,
    find_rate_card_xlsx_basename_for_ra_id,
)
from shipment_input import process_etof_file


def _sanitize_for_json(obj):
    """Convert NaN/NaT and non-JSON-serializable values for json.dump."""
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(x) for x in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if pd.isna(obj):
        return None
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return obj


def save_vocabulary_to_json(
    etof_df_renamed: Optional[pd.DataFrame],
    mapping_results: List[Dict],
    etof_mappings: Dict[str, str],
    output_path: Optional[os.PathLike] = None,
) -> str:
    """
    Save vocabulary mapping output to a JSON file.

    Writes ETOF data (renamed to rate card columns), column mapping list, and
    etof_mappings dict to partly_df/vocabulary_mapping.json by default.

    Args:
        etof_df_renamed: ETOF dataframe with columns renamed to rate card names.
        mapping_results: List of dicts with keys Rate_Card_Column, ETOF_Column, Rule.
        etof_mappings: Dict rate_card_col -> etof_col.
        output_path: Optional path; default is partly_df/vocabulary_mapping.json.

    Returns:
        Path to the written JSON file.
    """
    if output_path is None:
        output_path = Path(__file__).parent / "partly_df" / "vocabulary_mapping.json"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    etof_data = []
    if etof_df_renamed is not None and not etof_df_renamed.empty:
        etof_data = etof_df_renamed.to_dict(orient="records")
        etof_data = _sanitize_for_json(etof_data)

    payload = {
        "etof_data": etof_data,
        "mapping": _sanitize_for_json(mapping_results),
        "etof_mappings": _sanitize_for_json(etof_mappings),
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return str(output_path)


def calculate_string_similarity(str1, str2):
    """Calculate similarity between two strings (0-1)."""
    return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()


# Part-based matching: SHIP = Origin/Loading, CUST = Destination/Entry;
# suffixes: COUNTRY, POST, SEAPORT, AIRPORT, CITY (see _rate_card_parts_to_etof_name).


def _rate_card_parts_to_etof_name(rate_card_col: str) -> Optional[str]:
    """
    Break rate card column into parts and build expected ETOF column name.
    ETOF uses: SHIP_* (origin/loading), CUST_* (destination/entry);
    suffixes: COUNTRY, POST, SEAPORT, AIRPORT, CITY.
    Examples: "Port of Entry" -> CUST_SEAPORT, "Origin Country" -> SHIP_COUNTRY.
    """
    if not rate_card_col or not isinstance(rate_card_col, str):
        return None
    raw = rate_card_col.strip()
    lower = raw.lower()
    # Normalize: replace spaces with underscore for phrase matching
    normalized = lower.replace(' ', '_').replace('-', '_')
    # Detect side (prefix): SHIP or CUST
    prefix = None
    if any(t in lower or t.replace(' ', '_') in normalized for t in ('origin', 'loading', 'pol', 'port of loading', 'portofloading')):
        prefix = 'SHIP'
    if any(t in lower or t.replace(' ', '_') in normalized for t in ('destination', 'entry', 'poe', 'port of entry', 'portofentry')):
        prefix = 'CUST'
    # If we have "port" without "airport", treat as seaport; "port of X" -> port = seaport
    if prefix is None:
        if 'origin' in lower or 'loading' in lower or 'ship' in lower or ('port' in lower and 'loading' in lower):
            prefix = 'SHIP'
        if 'destination' in lower or 'entry' in lower or 'cust' in lower or ('port' in lower and 'entry' in lower):
            prefix = 'CUST'
    if prefix is None:
        return None
    # Detect field (suffix): POST=postal code, PORT=seaport, AIRPORT=airport, COUNTRY, CITY
    suffix = None
    if 'country' in lower:
        suffix = 'COUNTRY'
    elif 'postal' in lower or 'zip' in lower or ' post' in lower or lower.startswith('post ') or normalized.endswith('_post'):
        suffix = 'POST'
    elif 'airport' in lower:
        suffix = 'AIRPORT'
    elif 'seaport' in lower or ('port' in lower and 'airport' not in lower):
        # "port" (e.g. "port of entry/loading") = seaport
        suffix = 'SEAPORT'
    elif 'city' in lower:
        suffix = 'CITY'
    if suffix is None:
        return None
    return f"{prefix}_{suffix}"


def normalize_for_semantics(text):
    """Normalize text by replacing semantic equivalents."""
    text = text.lower()
    text = text.replace('ship', 'origin')
    text = text.replace('cust', 'destination')
    text = text.replace('equipment type', 'cont_load')
    text = text.replace('equipmenttype', 'cont_load')
    text = text.replace('equipment', 'cont_load')
    # Postal code mappings
    text = text.replace('origin postal code', 'ship_post')
    text = text.replace('origin postal', 'ship_post')
    text = text.replace('destination postal code', 'cust_post')
    text = text.replace('destination postal', 'cust_post')
    text = text.replace('postal code', 'post')
    text = text.replace('zip code', 'post')
    text = text.replace('zip', 'post')
    # Country mappings
    text = text.replace('origin country', 'ship_country')
    text = text.replace('ship country', 'ship_country')
    text = text.replace('from country', 'ship_country')
    text = text.replace('destination country', 'cust_country')
    text = text.replace('cust country', 'cust_country')
    text = text.replace('to country', 'cust_country')
    # Flow Type / Category mappings
    text = text.replace('flow type', 'category')
    text = text.replace('flowtype', 'category')
    text = text.replace('flow_type', 'category')
    # Port / Seaport mappings
    text = text.replace('port of loading', 'origin airport')
    text = text.replace('port of entry', 'destination airport')
    return text


def find_column_match(target_col, candidate_cols, threshold=0.3):
    """Find the best column name match. Returns (match, confidence, rule) with rule in ('part_based', 'exact', 'normalized', 'fuzzy') or None."""
    if not candidate_cols:
        return None, 0.0, None

    target_lower = target_col.lower().strip()
    target_normalized = normalize_for_semantics(target_col)

    # 1) Part-based match
    expected_etof = _rate_card_parts_to_etof_name(target_col)
    if expected_etof:
        expected_lower = expected_etof.lower()
        for cand in candidate_cols:
            if cand.strip().lower() == expected_lower:
                return cand, 0.98, 'part_based'
        for cand in candidate_cols:
            cand_norm = cand.strip().lower().replace(' ', '_')
            if cand_norm == expected_lower or expected_lower in cand_norm or cand_norm in expected_lower:
                return cand, 0.95, 'part_based'

    # 2) Exact or normalized match
    for cand in candidate_cols:
        cand_lower = cand.lower().strip()
        cand_normalized = normalize_for_semantics(cand)

        if target_lower == cand_lower:
            return cand, 1.0, 'exact'

        if target_normalized == cand_normalized:
            return cand, 0.95, 'normalized'

        if target_normalized in cand_normalized or cand_normalized in target_normalized:
            similarity = calculate_string_similarity(target_col, cand)
            if similarity > 0.7:
                return cand, similarity, 'normalized'

    # 3) Fuzzy string matching
    best_match = None
    best_score = 0.0
    for cand in candidate_cols:
        similarity = calculate_string_similarity(target_col, cand)
        if similarity > best_score:
            best_score = similarity
            best_match = cand

    if best_score >= threshold:
        return best_match, best_score, 'fuzzy'

    return None, best_score, None


def is_date_column(column_name):
    """Check if column is related to SHIP_DATE."""
    date_keywords = ['date', 'ship_date', 'ship date', 'delivery_date', 'delivery date', 
                     'arrival_date', 'arrival date', 'invoice_date', 'invoice date']
    col_lower = column_name.lower()
    return any(keyword in col_lower for keyword in date_keywords)


def is_shipment_id_column(column_name):
    """Check if column is related to SHIPMENT_ID/delivery number/etof #/lc#."""
    shipment_keywords = ['shipment', 'shipment_id', 'shipment id', 'delivery', 'delivery number', 
                         'delivery_number', 'etof', 'etof #', 'etof#', 'lc', 'lc #', 'lc#', 
                         'order file', 'order_file', 'DELIVERY_NUMBER', 'DELIVERY NUMBER(s)', 'delivery number(s)']
    col_lower = column_name.lower()
    return any(keyword in col_lower for keyword in shipment_keywords)


# Columns to exclude from mapping (use shipment_input result names: ETOF, LC, CARRIER_NAME, etc.)
EXCLUDED_COLUMNS = [
    'ETOF',
    'ETOF #',
    'ETOF#',
    'LC',
    'LC #',
    'LC#',
    'ISD',
    'ISD #',
    'CARRIER_NAME',
    'Carrier',
    'Delivery Number',
    'DeliveryNumber',
    'Lane #',
    'DELIVERY_NUMBER',
    'DELIVERY NUMBER(s)',
    'SHIPMENT_ID',
    'Shipment ID',
    'ShipmentID',
    'shipment id',
    'shipmentid',
]

# Rate card columns that should not be mapped (kept as-is)
RATE_CARD_EXCLUDED_COLUMNS = [
    'Valid to',
    'Valid from',
    'Valid To',
    'Valid From'
]

# ---------------------------------------------------------------------------
# USER MAPPING: rate card column name -> ETOF (shipment result) column name
# ---------------------------------------------------------------------------
# Set this to override automatic matching. Keys = exact rate card column names,
# values = exact ETOF result column names (from shipment_input output).
# Columns not listed here (or with a missing ETOF column) still use automatic
# find_column_match. Leave empty {} to use only automatic logic.
# Example (adjust to your rate card and ETOF result columns):
#
# ETOF_TO_RATE_CARD_MAPPING = {
#     'Service Type': 'SERVICE',
#     'Port of Loading': 'SHIP_AIRPORT',
#     'Port of Entry': 'CUST_AIRPORT',
#     'Origin City': 'SHIP_CITY',
#     'Destination City': 'CUST_CITY',
#     'Origin Country': 'SHIP_COUNTRY',
#     'Destination Country': 'CUST_COUNTRY',
#     'Origin Postal Code': 'SHIP_POST',
#     'Destination Postal Code': 'CUST_POST',
# }
#
ETOF_TO_RATE_CARD_MAPPING = {
    'Invoice type': 'INV_TYPE',
    'Lane Type': 'ORIGINAL_SERVICE',
    'Carrier Account Number': 'Billing account',
    'Shipping Condition': 'INVOICE_ENTITY',
    'Carrier Name': 'CARRIER_NAME',
}


def _user_mapped_etof_column(rate_card_col: str) -> Optional[str]:
    """
    Resolve ETOF column from :data:`ETOF_TO_RATE_CARD_MAPPING`.
    Matches rate card keys with **strip** and **case-insensitive** comparison so
    ``Carrier Name`` overrides fuzzy mapping to ``Carrier agreement #`` reliably.
    """
    if not ETOF_TO_RATE_CARD_MAPPING or rate_card_col is None:
        return None
    rc = str(rate_card_col).strip()
    if rc in ETOF_TO_RATE_CARD_MAPPING:
        return ETOF_TO_RATE_CARD_MAPPING[rc]
    rcl = rc.lower()
    for k, v in ETOF_TO_RATE_CARD_MAPPING.items():
        if str(k).strip().lower() == rcl:
            return v
    return None


def is_excluded_column(column_name):
    """Check if a column name should be excluded (case-insensitive, handles variations)."""
    if not column_name:
        return False
    
    col_lower = str(column_name).lower().strip()
    
    # Check against excluded columns (case-insensitive)
    for excluded in EXCLUDED_COLUMNS:
        excluded_lower = str(excluded).lower().strip()
        # Exact match
        if col_lower == excluded_lower:
            return True
        # Check if column contains excluded keyword (for variations like "ETOF #" vs "ETOF#")
        if excluded_lower.replace(' ', '') in col_lower.replace(' ', '') or col_lower.replace(' ', '') in excluded_lower.replace(' ', ''):
            # Additional check: make sure it's not just a partial match
            if 'etof' in excluded_lower and 'etof' in col_lower:
                return True
            if 'lc' in excluded_lower and 'lc' in col_lower and '#' in col_lower:
                return True
            if excluded_lower == 'carrier' and col_lower == 'carrier':
                return True
            if 'delivery' in excluded_lower and 'delivery' in col_lower and 'number' in col_lower:
                return True
    
    return False


def create_vocabulary_dataframe(
    rate_card_file_path: str,
    etof_file_path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Create a vocabulary DataFrame mapping ETOF (shipment_input result) columns to standard names (rate card).
    
    Args:
        rate_card_file_path: Path to rate card file
        etof_file_path: Optional path to ETOF file (processed via shipment_input)
    
    Returns:
        DataFrame with vocabulary mappings
        Columns:
            - 'Source': Source of the column (ETOF only)
            - 'Source_Column': Original column name from source
            - 'Standard_Name': Standard column name (from rate card)
            - 'Mapping': Shows "Original_Column → Standard_Name" mapping
            - 'Mapping_Method': How it was mapped ('fuzzy' or 'regex')
            - 'Confidence': Confidence score (0-1)
    """
    print("\n" + "="*80)
    print("CREATING VOCABULARY DATAFRAME")
    print("="*80)
    
    # Step 1: Get rate card columns (these are the standard names)
    print("\n1. Processing Rate Card...")
    try:
        rate_card_df, rate_card_columns, rate_card_conditions = process_rate_card(rate_card_file_path)
        print(f"   Found {len(rate_card_columns)} rate card columns")
        
        # Filter out excluded columns from rate card (case-insensitive)
        excluded_found = [col for col in rate_card_columns if is_excluded_column(col)]
        rate_card_columns = [col for col in rate_card_columns if not is_excluded_column(col)]
        
        if excluded_found:
            print(f"   Excluded {len(excluded_found)} columns from mapping: {excluded_found}")
            print(f"   Remaining rate card columns for mapping: {len(rate_card_columns)}")
        
        # Find columns that contain business rule values - skip these from column matching
        business_rule_columns = set()
        try:
            business_rules = process_business_rules(rate_card_file_path)
            business_rules_conditions = transform_business_rules_to_conditions(business_rules)
            business_rule_cols_info = find_business_rule_columns(rate_card_df, business_rules_conditions)
            business_rule_columns = business_rule_cols_info.get('unique_columns', set())
            
            if business_rule_columns:
                print(f"   Found {len(business_rule_columns)} columns containing business rules (will skip matching):")
                for col in sorted(business_rule_columns):
                    print(f"      - {col}")
                # Remove business rule columns from rate_card_columns to skip matching
                rate_card_columns = [col for col in rate_card_columns if col not in business_rule_columns]
                print(f"   Remaining rate card columns for mapping: {len(rate_card_columns)}")
        except Exception as e:
            print(f"   Note: Could not process business rules: {e}")
            business_rule_columns = set()
        
        # Add required geographic columns to the standard columns for mapping
        geo_columns = get_required_geo_columns()
        for geo_col in geo_columns:
            if geo_col not in rate_card_columns:
                rate_card_columns.append(geo_col)
        print(f"   Added geographic columns for mapping: {geo_columns}")
        print(f"   Total standard columns for mapping: {len(rate_card_columns)}")
            
    except Exception as e:
        print(f"   Error processing rate card: {e}")
        return pd.DataFrame()
    
    # Step 2: Collect columns from all sources (excluding specified columns)
    all_source_columns = {}
    # Full ETOF column names (before excluding ``CARRIER_NAME`` etc.) for user overrides only.
    etof_columns_full_for_user_map: list[str] = []

    if etof_file_path:
        print("\n2. Processing ETOF file...")
        try:
            etof_df, etof_columns = process_etof_file(etof_file_path)
            etof_columns_full_for_user_map = list(etof_columns)
            # Filter out excluded columns for fuzzy matching — user map can still target excluded names.
            excluded_etof = [col for col in etof_columns if is_excluded_column(col)]
            etof_columns = [col for col in etof_columns if not is_excluded_column(col)]
            all_source_columns['ETOF'] = etof_columns
            print(f"   Found {len(etof_columns)} ETOF columns (excluded {len(excluded_etof)}: {excluded_etof})")
        except Exception as e:
            print(f"   Error processing ETOF: {e}")
    
    # Step 3: Print all columns explored from each source
    print("\n" + "="*80)
    print("COLUMNS EXPLORED FROM EACH SOURCE")
    print("="*80)
    print(f"\nRate Card ({len(rate_card_columns)} columns):")
    for i, col in enumerate(rate_card_columns, 1):
        print(f"  {i}. {col}")
    
    for source_name, source_columns in all_source_columns.items():
        print(f"\n{source_name} ({len(source_columns)} columns):")
        for i, col in enumerate(source_columns, 1):
            print(f"  {i}. {col}")
    
    # Step 4: Create vocabulary mappings (one-to-one mapping)
    print("\n" + "="*80)
    print("CREATING VOCABULARY MAPPINGS (ONE-TO-ONE)")
    print("="*80)
    vocabulary_data = []
    
    # Track which source columns have been used (for one-to-one mapping)
    # Format: {source_name: set of used source columns}
    used_source_columns = {source_name: set() for source_name in all_source_columns.keys()}
    
    # For each rate card column (standard name), find ONE match per source (ETOF only)
    for standard_col in rate_card_columns:
        for source_name, source_columns in all_source_columns.items():
            existing_mapping = [item for item in vocabulary_data 
                               if item['Standard_Name'] == standard_col and item['Source'] == source_name]
            if existing_mapping:
                continue
            
            match = None
            confidence = 0.0
            method = 'fuzzy'
            # User mapping first (only for ETOF); may target columns excluded from fuzzy pool (e.g. CARRIER_NAME)
            if source_name == 'ETOF' and ETOF_TO_RATE_CARD_MAPPING:
                user_etof_col = _user_mapped_etof_column(standard_col)
                if (
                    user_etof_col is not None
                    and user_etof_col in etof_columns_full_for_user_map
                    and user_etof_col not in used_source_columns[source_name]
                ):
                    match = user_etof_col
                    confidence = 1.0
                    method = 'user'
            if not match:
                available_columns = [col for col in source_columns if col not in used_source_columns[source_name]]
                if not available_columns:
                    continue
                match, confidence, method = find_column_match(standard_col, available_columns, threshold=0.3)
                method = method or 'fuzzy'
            if match:
                if is_excluded_column(standard_col):
                    continue
                # Fuzzy must not map *to* excluded ETOF columns; explicit user map may (e.g. CARRIER_NAME).
                if method != 'user' and is_excluded_column(match):
                    continue
                vocabulary_data.append({
                    'Standard_Name': standard_col,
                    'Source': source_name,
                    'Source_Column': match,
                    'Mapping_Method': method,
                    'Confidence': confidence
                })
                used_source_columns[source_name].add(match)
    
    # Step 6: Create DataFrame and identify unmapped columns
    print("\nCreating vocabulary DataFrame...")
    
    # Create DataFrame
    df_vocabulary = pd.DataFrame(vocabulary_data)
    
    if not df_vocabulary.empty:
        # Add a mapping column that shows Original → Standard clearly
        df_vocabulary['Mapping'] = df_vocabulary['Source_Column'] + ' → ' + df_vocabulary['Standard_Name']
        
        # Reorder columns to make it clearer: show original name, then what it maps to
        column_order = ['Source', 'Source_Column', 'Standard_Name', 'Mapping', 'Mapping_Method', 'Confidence']
        df_vocabulary = df_vocabulary[column_order]
        
        # Sort by Source, then Standard_Name
        df_vocabulary = df_vocabulary.sort_values(['Source', 'Standard_Name'])
    
    # Step 7: Identify and print unmapped columns
    print("\n" + "="*80)
    print("UNMAPPED COLUMNS ANALYSIS")
    print("="*80)
    
    # Find unmapped rate card columns
    if not df_vocabulary.empty:
        mapped_rate_cols = set(df_vocabulary['Standard_Name'].unique())
    else:
        mapped_rate_cols = set()
    
    unmapped_rate_cols = set(rate_card_columns) - mapped_rate_cols
    
    print(f"\nRate Card Columns:")
    print(f"  Total: {len(rate_card_columns)}")
    print(f"  Mapped: {len(mapped_rate_cols)}")
    print(f"  Unmapped: {len(unmapped_rate_cols)}")
    if unmapped_rate_cols:
        print(f"\n  Unmapped Rate Card Columns:")
        for col in sorted(unmapped_rate_cols):
            print(f"    - {col}")
    
    # Find unmapped source columns (columns that could have matched but didn't due to one-to-one constraint)
    print(f"\nSource Files Columns:")
    for source_name, source_columns in all_source_columns.items():
        used_cols = used_source_columns.get(source_name, set())
        unmapped_source_cols = set(source_columns) - used_cols
        print(f"\n  {source_name}:")
        print(f"    Total: {len(source_columns)}")
        print(f"    Mapped: {len(used_cols)}")
        print(f"    Unmapped: {len(unmapped_source_cols)}")
        if unmapped_source_cols:
            print(f"    Unmapped {source_name} Columns:")
            for col in sorted(unmapped_source_cols):
                print(f"      - {col}")
    
    print(f"\n   Created vocabulary with {len(df_vocabulary)} mappings")
    print(f"   Rate card columns mapped: {len(mapped_rate_cols)} out of {len(rate_card_columns)}")
    if not df_vocabulary.empty:
        print(f"   Sources: {df_vocabulary['Source'].unique().tolist()}")
    
    # Show mapping method breakdown
    if not df_vocabulary.empty:
        method_counts = df_vocabulary['Mapping_Method'].value_counts()
        print(f"\n   Mapping methods:")
        for method, count in method_counts.items():
            print(f"     {method}: {count}")
    
    return df_vocabulary


def _find_carrier_agreement_column(etof_df: pd.DataFrame) -> Optional[str]:
    """ETOF column holding values like 'RA20250326009 (v.13) - On Hold'."""
    for c in etof_df.columns:
        cl = str(c).lower()
        if "carrier" in cl and "agreement" in cl:
            return c
    return None


def map_and_rename_columns(
    rate_card_file_path: Optional[str] = None,
    etof_file_path: Optional[str] = None,
    output_txt_path: str = "column_mapping_results.txt",
    ignore_rate_card_columns: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Map rate card columns to ETOF (shipment_input result) only; rename columns and save results.

    Args:
        rate_card_file_path: Path to rate card file under ``input/``. If omitted, ``etof_file_path``
            is required; the rate card Excel is chosen by parsing the RA id from the ETOF column
            ``Carrier agreement #`` (value before ``(v...)``) and matching ``input/*{RA}*.xlsx``.
        etof_file_path: Path to ETOF file (processed via shipment_input). Required when
            ``rate_card_file_path`` is omitted.
        output_txt_path: Path to save the mapping results text file
        ignore_rate_card_columns: Optional list of rate card column names to ignore

    Returns:
        Tuple: (etof_dataframe_renamed, None, None) — LC and Origin are no longer used.
    """
    etof_df_preloaded: Optional[pd.DataFrame] = None
    input_folder = "input"

    if rate_card_file_path is None:
        if not etof_file_path:
            raise ValueError(
                "map_and_rename_columns: pass rate_card_file_path, or pass etof_file_path alone "
                "to resolve the rate card from ETOF 'Carrier agreement #'."
            )
        print("\nStep 0: Resolving rate card from ETOF Carrier agreement # ...")
        etof_df_preloaded, _ = process_etof_file(etof_file_path)
        carrier_col = _find_carrier_agreement_column(etof_df_preloaded)
        if not carrier_col:
            raise ValueError(
                "No ETOF column like 'Carrier agreement #' found; cannot pick rate card Excel."
            )
        series = etof_df_preloaded[carrier_col].dropna()
        ra_ids = set()
        for v in series.unique():
            rid = extract_ra_id_from_carrier_agreement(v)
            if rid:
                ra_ids.add(rid)
        if len(ra_ids) > 1:
            print(
                f"   WARNING: Multiple distinct RA ids in '{carrier_col}': {sorted(ra_ids)}. "
                "Using the first row's RA to select one rate card Excel for vocabulary mapping."
            )
        first_val = etof_df_preloaded[carrier_col].iloc[0]
        ra_id = extract_ra_id_from_carrier_agreement(first_val)
        if not ra_id:
            raise ValueError(
                f"Could not parse RA id from {carrier_col!r} (first row): {first_val!r}"
            )
        resolved = find_rate_card_xlsx_basename_for_ra_id(ra_id, input_folder)
        if not resolved:
            raise FileNotFoundError(
                f"No rate card .xlsx in '{input_folder}' whose filename contains {ra_id!r}. "
                "Add the Excel for this agreement or pass rate_card_file_path explicitly."
            )
        rate_card_file_path = resolved
        print(f"   Resolved RA id {ra_id} -> rate card file: {rate_card_file_path}")

    # Step 1: Get rate card columns
    try:
        print(f"\nStep 1: Processing rate card file: {rate_card_file_path}")
        
        # Check if file exists in input folder (process_rate_card expects files in "input" folder)
        import os
        
        # Check if input folder exists
        if not os.path.exists(input_folder):
            print(f"   WARNING: '{input_folder}' folder does not exist. Creating it...")
            os.makedirs(input_folder, exist_ok=True)
        
        expected_path = os.path.join(input_folder, rate_card_file_path)
        
        # Check if file exists in input folder
        if not os.path.exists(expected_path):
            # Try with just the filename
            filename = os.path.basename(rate_card_file_path)
            alt_path = os.path.join(input_folder, filename)
            if os.path.exists(alt_path):
                rate_card_file_path = filename
                print(f"   Using file: {alt_path}")
            else:
                error_msg = f"Rate card file not found at: {expected_path}"
                if os.path.exists(rate_card_file_path):
                    error_msg += f"\n   Found file at current location: {rate_card_file_path}"
                    error_msg += f"\n   Please move it to: {expected_path}"
                else:
                    error_msg += f"\n   Please ensure the file exists in the '{input_folder}' folder."
                raise FileNotFoundError(error_msg)
        else:
            print(f"   Found rate card at: {expected_path}")
        
        rate_card_df, rate_card_columns_all, rate_card_conditions = process_rate_card(rate_card_file_path)
        print(f"   Successfully loaded rate card: {len(rate_card_columns_all)} columns")
        
        # Filter out ignored columns
        if ignore_rate_card_columns is None:
            ignore_rate_card_columns = []
        
        # Remove ignored columns from rate card dataframe
        if ignore_rate_card_columns:
            columns_to_drop = [col for col in ignore_rate_card_columns if col in rate_card_df.columns]
            if columns_to_drop:
                rate_card_df = rate_card_df.drop(columns=columns_to_drop)
        
        # Update rate_card_columns_all to exclude ignored columns
        rate_card_columns_all = [col for col in rate_card_columns_all if col not in ignore_rate_card_columns]
        
        rate_card_columns_to_map = [
            col for col in rate_card_columns_all 
            if not is_excluded_column(col) and col not in RATE_CARD_EXCLUDED_COLUMNS
        ]
        rate_card_columns = rate_card_columns_to_map
        print(f"   Rate card columns to map: {len(rate_card_columns)}")
        
        # Find columns that contain business rule values - skip these from column matching
        business_rule_columns = set()
        try:
            business_rules = process_business_rules(rate_card_file_path)
            business_rules_conditions = transform_business_rules_to_conditions(business_rules)
            business_rule_cols_info = find_business_rule_columns(rate_card_df, business_rules_conditions)
            business_rule_columns = business_rule_cols_info.get('unique_columns', set())
            
            if business_rule_columns:
                print(f"   Found {len(business_rule_columns)} columns containing business rules (will skip matching):")
                for col in sorted(business_rule_columns):
                    print(f"      - {col}")
                # Remove business rule columns from rate_card_columns to skip matching
                rate_card_columns = [col for col in rate_card_columns if col not in business_rule_columns]
                print(f"   Remaining rate card columns for mapping: {len(rate_card_columns)}")
        except Exception as e:
            print(f"   Note: Could not process business rules: {e}")
            business_rule_columns = set()
        
        # Add required geographic columns to the standard columns for mapping
        geo_columns = get_required_geo_columns()
        for geo_col in geo_columns:
            if geo_col not in rate_card_columns:
                rate_card_columns.append(geo_col)
        print(f"   Added geographic columns for mapping: {geo_columns}")
        print(f"   Total standard columns for mapping: {len(rate_card_columns)}")
            
    except Exception as e:
        print(f"   ERROR processing rate card: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Step 2: Get ETOF dataframe only (shipment_input result)
    etof_df = None

    if etof_df_preloaded is not None:
        print(f"\nStep 2: Using ETOF already loaded (Step 0): {etof_file_path}")
        etof_df = etof_df_preloaded
        etof_columns = etof_df.columns.tolist()
        print(f"   ETOF: {len(etof_columns)} columns, {len(etof_df)} rows")
    elif etof_file_path:
        try:
            print(f"\nStep 2: Processing ETOF file: {etof_file_path}")
            etof_df, etof_columns = process_etof_file(etof_file_path)
            print(f"   Successfully loaded ETOF: {len(etof_columns)} columns, {len(etof_df)} rows")
        except Exception as e:
            print(f"   ERROR processing ETOF: {e}")
            import traceback
            traceback.print_exc()
            etof_df = None
    
    if etof_df is None or etof_df.empty:
        print("   ERROR: ETOF dataframe is None/Empty. Returning empty dataframes.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Columns to always keep (shipment_input result names: ETOF, LC, CARRIER_NAME, SHIP_DATE, etc.)
    keep_columns = [
        'ETOF', 'ETOF #', 'ETOF#', 'LC', 'LC #', 'LC#', 'ISD', 'ISD #',
        'CARRIER_NAME', 'Carrier', 'SHIP_DATE', 'Loading date', 'Loading Date',
        'DELIVERY_NUMBER', 'DELIVERY NUMBER(s)', 'Delivery Number', 'DeliveryNumber',
        'SHIPMENT_ID', 'Shipment ID', 'ShipmentID', 'shipment id', 'shipmentid',
        'Carrier agreement #',
    ]
    
    mapping_results = []
    etof_mappings = {}  # {rate_card_col: etof_col}
    used_etof_columns = set()
    
    for rate_card_col in rate_card_columns:
        etof_match = None
        rule = None
        # User mapping may target columns omitted from fuzzy pool (e.g. ``CARRIER_NAME`` in EXCLUDED_COLUMNS).
        user_col = _user_mapped_etof_column(rate_card_col)
        if user_col is not None and user_col not in used_etof_columns:
            if user_col in etof_df.columns:
                etof_match = user_col
                rule = 'user'
        etof_columns = [
            col
            for col in etof_df.columns
            if not is_excluded_column(col) and col not in used_etof_columns
        ]
        if etof_match is None and etof_columns:
            match, _, rule = find_column_match(rate_card_col, etof_columns, threshold=0.3)
            if match and not is_excluded_column(match) and match not in used_etof_columns:
                etof_match = match
            if rule is None and etof_match is None:
                rule = 'none'
        if etof_match is not None:
            etof_mappings[rate_card_col] = etof_match
            used_etof_columns.add(etof_match)

        mapping_results.append({
            'Rate_Card_Column': rate_card_col,
            'ETOF_Column': etof_match if etof_match else 'NONE',
            'Rule': rule or 'none',
        })
    
    # Step 4: Rename columns and include ALL rate card columns
    all_rate_card_cols_for_output = rate_card_columns_all.copy()
    
    # Add geo columns to output list as well (they were added to rate_card_columns for mapping)
    geo_columns = get_required_geo_columns()
    for geo_col in geo_columns:
        if geo_col not in all_rate_card_cols_for_output:
            all_rate_card_cols_for_output.append(geo_col)
    
    etof_df_renamed = None
    lc_df_renamed = None
    origin_df_renamed = None
    
    def create_output_dataframe(source_df, source_mappings, source_name, keep_cols_list, specific_keep_list, all_rate_card_cols):
        """Helper function to create output dataframe with rate card columns and key columns only."""
        if source_df is None or source_df.empty:
            return None

        output_df = source_df.copy()
        rename_dict = {}
        columns_to_keep = []
        
        # Step 1: Add rate card mapped columns (will be renamed to "RateCardColumn (OriginalColumn)")
        print(f"\n  [{source_name}] Step 1: Adding rate card mapped columns...")
        for rate_card_col, source_col in source_mappings.items():
            if source_col in output_df.columns:
                rename_dict[source_col] = f"{rate_card_col} ({source_col})"
                columns_to_keep.append(source_col)
        print(f"    After Step 1: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 2: Add columns to always keep (ETOF #, LC #, Carrier, Delivery Number)
        print(f"\n  [{source_name}] Step 2: Adding columns to always keep...")
        for keep_col in keep_cols_list:
            # Try to find the column (case-insensitive and handle variations)
            found = False
            for col in output_df.columns:
                col_normalized = col.lower().replace(' ', '').replace('#', '#')
                keep_normalized = keep_col.lower().replace(' ', '').replace('#', '#')
                if col_normalized == keep_normalized:
                    if col not in columns_to_keep:
                        columns_to_keep.append(col)
                    found = True
                    break
            if not found:
                # Also check if the column name itself matches (exact match)
                if keep_col in output_df.columns and keep_col not in columns_to_keep:
                    columns_to_keep.append(keep_col)
        print(f"    After Step 2: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns to keep so far: {columns_to_keep}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 3: Add source-specific columns to keep (Loading date for ETOF, SHIP_DATE for LC)
        print(f"\n  [{source_name}] Step 3: Adding source-specific columns to keep...")
        for keep_col in specific_keep_list:
            # Try to find the column (case-insensitive)
            for col in output_df.columns:
                if col.lower() == keep_col.lower():
                    if col not in columns_to_keep:
                        columns_to_keep.append(col)
                    break
        print(f"    After Step 3: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns to keep so far: {columns_to_keep}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 4: Rename columns first (before filtering)
        print(f"\n  [{source_name}] Step 4: Renaming columns...")
        output_df.rename(columns=rename_dict, inplace=True)
        print(f"    After Step 4: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 5: Now rename "RateCardColumn (OriginalColumn)" to just "RateCardColumn"
        print(f"\n  [{source_name}] Step 5: Renaming to standard column names...")
        rename_to_standard = {}
        for col in output_df.columns:
            if ' (' in col and col.endswith(')'):
                standard_name = col.split(' (')[0]
                # Only rename if it's a rate card column
                if standard_name in all_rate_card_cols:
                    rename_to_standard[col] = standard_name
        
        if rename_to_standard:
            output_df.rename(columns=rename_to_standard, inplace=True)
            # Update columns_to_keep list with renamed columns
            updated_columns_to_keep = []
            for col in columns_to_keep:
                if col in rename_to_standard:
                    updated_columns_to_keep.append(rename_to_standard[col])
                elif col in output_df.columns:
                    updated_columns_to_keep.append(col)
            columns_to_keep = updated_columns_to_keep
        print(f"    After Step 5: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 6: Add ALL rate card columns that are not yet in the dataframe (as empty columns)
        # Only add columns that don't have a mapping (were not mapped from this source)
        print(f"\n  [{source_name}] Step 6: Adding missing rate card columns as empty...")
        for rate_card_col in all_rate_card_cols:
            # Skip if this column was excluded from mapping
            if is_excluded_column(rate_card_col) or rate_card_col in RATE_CARD_EXCLUDED_COLUMNS:
                continue
            
            # Check if this column is already in the dataframe (was mapped)
            if rate_card_col not in output_df.columns:
                # Check if this rate card column has a mapping from this source
                # If it does, we should have already added it, so skip
                # If it doesn't, add it as empty
                if rate_card_col not in source_mappings:
                    # No mapping found - add as empty column
                    output_df[rate_card_col] = None
                    if rate_card_col not in columns_to_keep:
                        columns_to_keep.append(rate_card_col)
        print(f"    After Step 6: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 7: Build final column list - ONLY rate card columns + key columns (LC #, ETOF #, Carrier, Loading date/SHIP_DATE)
        print(f"\n  [{source_name}] Step 7: Building final column list...")
        final_columns = []
        
        # Add all rate card columns first (mapped or unmapped)
        for rate_card_col in all_rate_card_cols:
            # Skip excluded columns
            if is_excluded_column(rate_card_col) or rate_card_col in RATE_CARD_EXCLUDED_COLUMNS:
                continue
            
            # Add rate card column if it exists
            if rate_card_col in output_df.columns:
                final_columns.append(rate_card_col)
        
        # Add key columns (shipment_input result names: ETOF, LC, CARRIER_NAME, SHIP_DATE, DELIVERY_NUMBER, SHIPMENT_ID)
        key_columns_to_find = [
            'ETOF', 'ETOF #', 'ETOF#', 'LC', 'LC #', 'LC#',
            'CARRIER_NAME', 'carrier', 'carrier_name', 'Carrier',
            'SHIP_DATE', 'ship_date', 'ship date',
            'DELIVERY_NUMBER', 'delivery_number', 'Delivery Number', 'deliverynumber(s)',
            'SHIPMENT_ID', 'shipment_id', 'Shipment ID',
        ]
                              #'SHIPMENT_ID', 'DELIVERY_NUMBER','DELIVERY NUMBER(s)', 'delivery_number', 'delivery number(s)', 'deliverynumber', 'deliverynumber(s)']
        for key_col in key_columns_to_find:
            # Find the column (case-insensitive, handle variations)
            for col in output_df.columns:
                col_normalized = col.lower().replace(' ', '').replace('#', '#')
                key_normalized = key_col.lower().replace(' ', '').replace('#', '#')
                result = (col_normalized == key_normalized)
                print(f"Comparing '{col}' to '{key_col}': {col_normalized} == {key_normalized} -> {result}")
                if result:
                    if col not in final_columns:
                        final_columns.append(col)
                        print(f"Added '{col}' to final_columns")
                    break
        
        # Add source-specific columns: Loading date (ETOF) or SHIP_DATE (LC)
        for specific_col in specific_keep_list:
            # Find the column (case-insensitive)
            for col in output_df.columns:
                if col.lower() == specific_col.lower():
                    if col not in final_columns:
                        final_columns.append(col)
                    break
        # Add ALL remaining columns from the source (ETOF) so the result has every column from etof_processing
        for col in output_df.columns:
            if col not in final_columns:
                final_columns.append(col)
        print(f"    After Step 7: Final columns list (rate card + key + all ETOF columns): {len(final_columns)} columns")
        
        # Step 8: Filter to keep ONLY the final columns
        print(f"\n  [{source_name}] Step 8: Filtering to final columns...")
        output_df = output_df[final_columns]
        print(f"    After Step 8: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 9: Ensure key columns exist (shipment result names: CARRIER_NAME, DELIVERY_NUMBER, SHIPMENT_ID)
        print(f"\n  [{source_name}] Step 9: Ensuring required columns exist (Carrier/CARRIER_NAME, Delivery Number/DELIVERY_NUMBER, Shipment ID/SHIPMENT_ID)...")
        carrier_col_found = False
        carrier_variations = ['CARRIER_NAME', 'Carrier', 'carrier_name', 'CARRIER', 'carrier']
        for col in output_df.columns:
            if str(col).strip() in carrier_variations or str(col).strip().lower() in [v.lower() for v in carrier_variations]:
                carrier_col_found = True
                break
        if not carrier_col_found:
            output_df['CARRIER_NAME'] = None
            final_columns.append('CARRIER_NAME')

        delivery_col_found = False
        delivery_variations = ['DELIVERY_NUMBER', 'Delivery Number', 'DeliveryNumber', 'delivery_number', 'delivery number', 'deliverynumber', 'DELIVERY NUMBER(s)', 'delivery number(s)']
        for col in output_df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '').replace('_', '')
            if col_lower in ('delivery_number', 'deliverynumber', 'delivery_number(s)') or ('delivery' in col_lower and 'number' in col_lower):
                delivery_col_found = True
                break
        if not delivery_col_found:
            output_df['DELIVERY_NUMBER'] = None
            final_columns.append('DELIVERY_NUMBER')

        shipment_id_col_found = False
        shipment_id_variations = ['SHIPMENT_ID', 'Shipment ID', 'ShipmentID', 'shipment_id', 'shipment id', 'shipmentid']
        for col in output_df.columns:
            col_str = str(col).strip()
            col_lower = col_str.lower().replace(' ', '').replace('_', '')
            if col_lower in ('shipment_id', 'shipmentid') or ('shipment' in col_lower and 'id' in col_lower):
                shipment_id_col_found = True
                break
        if not shipment_id_col_found:
            output_df['SHIPMENT_ID'] = None
            final_columns.append('SHIPMENT_ID')
        
        print(f"    After Step 9: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Final columns: {list(output_df.columns)}")
        if not output_df.empty:
            print(f"    First 3 rows:\n{output_df.head(3).to_string()}")
        
        # Step 10: Ensure geographic columns exist (fallback - rename variations or add empty if not mapped)
        # These should already be mapped via semantic matching, but this ensures standard naming
        print(f"\n  [{source_name}] Step 10: Standardizing geographic columns (Country, Postal Code)...")
        
        geo_columns_mapping = {
            'Origin Country': ['Origin Country', 'origin country', 'OriginCountry', 'ORIGIN_COUNTRY', 
                              'Ship Country', 'ship country', 'ShipCountry', 'SHIP_COUNTRY',
                              'From Country', 'from country', 'FromCountry', 'FROM_COUNTRY'],
            'Origin Postal Code': ['Origin Postal Code', 'origin postal code', 'OriginPostalCode', 'ORIGIN_POSTAL_CODE',
                                   'Ship Postal', 'ship postal', 'ShipPostal', 'SHIP_POSTAL', 'SHIP_POST',
                                   'From Postal', 'from postal', 'FromPostal', 'FROM_POSTAL',
                                   'Origin Zip', 'origin zip', 'OriginZip', 'ORIGIN_ZIP'],
            'Destination Country': ['Destination Country', 'destination country', 'DestinationCountry', 'DESTINATION_COUNTRY',
                                   'Cust Country', 'cust country', 'CustCountry', 'CUST_COUNTRY',
                                   'To Country', 'to country', 'ToCountry', 'TO_COUNTRY'],
            'Destination Postal Code': ['Destination Postal Code', 'destination postal code', 'DestinationPostalCode', 'DESTINATION_POSTAL_CODE',
                                        'Cust Postal', 'cust postal', 'CustPostal', 'CUST_POSTAL', 'CUST_POST',
                                        'To Postal', 'to postal', 'ToPostal', 'TO_POSTAL',
                                        'Destination Zip', 'destination zip', 'DestinationZip', 'DESTINATION_ZIP']
        }
        
        for standard_geo_col, variations in geo_columns_mapping.items():
            geo_col_found = False
            found_col_name = None
            
            for col in output_df.columns:
                col_str = str(col).strip()
                col_lower = col_str.lower().replace(' ', '').replace('_', '')
                
                for variation in variations:
                    var_lower = variation.lower().replace(' ', '').replace('_', '')
                    if col_lower == var_lower:
                        geo_col_found = True
                        found_col_name = col
                        break
                if geo_col_found:
                    break
            
            if geo_col_found and found_col_name != standard_geo_col:
                # Rename to standard name
                output_df = output_df.rename(columns={found_col_name: standard_geo_col})
                print(f"    Renamed '{found_col_name}' -> '{standard_geo_col}'")
            elif not geo_col_found:
                # Add empty column
                output_df[standard_geo_col] = None
                final_columns.append(standard_geo_col)
                print(f"    Added empty column: '{standard_geo_col}'")
        
        print(f"    After Step 10: {len(output_df)} rows, {len(output_df.columns)} columns")
        print(f"    Final columns: {list(output_df.columns)}")
        
        return output_df
    
    # Process ETOF
    if etof_df is not None:
        etof_specific_keep = ['SHIP_DATE', 'Ship Date', 'ship_date', 'ship date', 'Loading date', 'Loading Date', 'loading date', 'LOADING DATE']
        etof_df_renamed = create_output_dataframe(
            etof_df, etof_mappings, 'ETOF', keep_columns, etof_specific_keep, all_rate_card_cols_for_output
        )
        print(f"\nStep 4a: After creating ETOF output dataframe")
        if etof_df_renamed is not None and not etof_df_renamed.empty:
            print(f"   ETOF DataFrame: {len(etof_df_renamed)} rows, {len(etof_df_renamed.columns)} columns")
            print(f"   Columns: {list(etof_df_renamed.columns)}")
            print(f"   First few rows:\n{etof_df_renamed.head(3).to_string()}")
        else:
            print(f"   ETOF DataFrame: Empty or None")
    
    # LC and Origin no longer used — lc_df_renamed and origin_df_renamed remain None
    
    # Step 6 removed: no Origin/LC merge (only ETOF is used)
    
    # Step 7: Save mapping to txt file
    output_folder = Path(__file__).parent / "partly_df"
    output_folder.mkdir(exist_ok=True)
    txt_output_path = output_folder / output_txt_path
    
    with open(txt_output_path, 'w', encoding='utf-8') as f:
        f.write("COLUMN MAPPING RESULTS\n")
        f.write("="*80 + "\n\n")
        f.write("MAPPINGS: Rate Card Column -> ETOF Column (Rule)\n")
        f.write("="*80 + "\n\n")
        for result in mapping_results:
            f.write(f"{result['Rate_Card_Column']} -> {result['ETOF_Column']}  (Rule: {result['Rule']})\n")
        f.write("\n" + "="*80 + "\n")
        f.write("ETOF Mappings:\n")
        for rate_card_col, etof_col in etof_mappings.items():
            f.write(f"  {rate_card_col} <- {etof_col}\n")
    
    print(f"\nStep 7: Final ETOF dataframe before return")
    if etof_df_renamed is not None and not etof_df_renamed.empty:
        print(f"   ETOF DataFrame: {len(etof_df_renamed)} rows, {len(etof_df_renamed.columns)} columns")
        print(f"   Columns: {list(etof_df_renamed.columns)}")
    else:
        print(f"   ETOF DataFrame: Empty or None")

    # Step 8: Save dataframes to Excel file
    excel_output_path = output_folder / "vocabulary_mapping.xlsx"
    with pd.ExcelWriter(excel_output_path, engine='openpyxl') as writer:
        if etof_df_renamed is not None and not etof_df_renamed.empty:
            etof_df_renamed.to_excel(writer, sheet_name='ETOF', index=False)
        # Save mapping DataFrame
        mapping_df = pd.DataFrame(mapping_results)
        if not mapping_df.empty:
            mapping_df.to_excel(writer, sheet_name='Mapping', index=False)

    # Step 9: Save vocabulary output to JSON (same folder as Excel)
    json_output_path = output_folder / "vocabulary_mapping.json"
    save_vocabulary_to_json(etof_df_renamed, mapping_results, etof_mappings, json_output_path)
    print(f"\nStep 9: Saved vocabulary JSON to {json_output_path}")

    return etof_df_renamed, None, None


# Example usage
if __name__ == "__main__":
    try:
        # Main function: Map and rename columns
        etof_renamed, lc_renamed, origin_renamed = map_and_rename_columns(
            rate_card_file_path=None,
            etof_file_path="etofs_03.04.2026 (GOR25 - Fastboat) 3.xlsx",
        )
    except Exception:
        pass  

 





