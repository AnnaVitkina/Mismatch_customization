import pandas as pd
import os
from pathlib import Path


# ============== CONFIGURATION ==============
# Set mismatch report path(s) once before calling process_etof_file to enable Service enrichment
MISMATCH_REPORT_PATHS = None  # e.g., "report.xlsx" or ["report1.xlsx", "report2.xlsx"]
# Default basename for save_dataframe_to_* outputs; mismatch_report loads this extract from partly_df/
DEFAULT_PROCESSED_SHIPMENT_JSON = "etof_processed_apple.json"
DEFAULT_PROCESSED_SHIPMENT_XLSX = "etof_processed_apple.xlsx"
# ===========================================


def configure_enrichment(mismatch_report_paths):
    """
    Configure the enrichment settings for process_etof_file.
    Call this once at the start to enable Service enrichment from mismatch report(s).

    Args:
        mismatch_report_paths (str or list): Single path or list of paths to mismatch_report xlsx files
    """
    global MISMATCH_REPORT_PATHS
    MISMATCH_REPORT_PATHS = mismatch_report_paths


def save_dataframe_to_excel(df, output_filename, folder_name="partly_df"):
    """Save DataFrame to Excel in the given folder (default: partly_df)."""
    output_folder = Path(__file__).parent / folder_name
    output_folder.mkdir(exist_ok=True)
    df.to_excel(output_folder / output_filename, index=False, engine='openpyxl')


def save_dataframe_to_json(df, output_filename, folder_name="partly_df"):
    """Save DataFrame to JSON in the given folder (default: partly_df). Creates JSON directly from the DataFrame."""
    output_folder = Path(__file__).parent / folder_name
    output_folder.mkdir(exist_ok=True)
    path = output_folder / output_filename
    if not str(path).lower().endswith('.json'):
        path = path.with_suffix('.json')
    # orient='records' -> list of row dicts; date_format='iso' for readable dates
    df.to_json(path, orient='records', date_format='iso', indent=2, default_handler=str)


def process_etof_file(file_path):
    """
    Process an ETOF Excel file from the input folder.
    
    Service enrichment is applied when configure_enrichment() has been called with mismatch report path(s).
    For rows where Transport mode contains "AIR", Service is replaced with SERVICE_ISD from the mismatch report.
    
    Args:
        file_path (str): Path to the file relative to the "input/" folder (e.g., "etof_file.xlsx")
    
    Returns:
        tuple: (dataframe, list of column names)
            - dataframe: Processed pandas DataFrame with specified columns removed
            - list: List of column names in the processed dataframe
    """
    # Use global configuration
    mismatch_report_paths = MISMATCH_REPORT_PATHS

    # Construct full path from input folder
    input_folder = "input"
    full_path = os.path.join(input_folder, file_path)
    
    # Read Excel file (skip first row)
    df_etofs = pd.read_excel(full_path, skiprows=1)
    
    # Rename duplicate columns
    new_column_names = {
        'Country code': 'Origin Country',
        'Postal code': 'Origin Postal Code',
        'Airport': 'Origin Airport',
        'City': 'Origin City',
        'Country code.1': 'Destination Country',
        'Postal code.1': 'Destination Postal Code',
        'Airport.1': 'Destination Airport',
        'City.1': 'Destination City',
        'Seaport': 'Origin Seaport',
        'Seaport.1': 'Destination Seaport'
    }
    df_etofs = df_etofs.rename(columns=new_column_names, inplace=False)
    

    columns_to_remove = [
        'Match', 'Approve', 'Calculation', 'State', 'Issue',
        'Currency', 'Value', 'Currency.1', 'Value.1', 'Currency.2', 'Value.2',
        'FI #', 'FID #', 'Data match', 'ISD items status', 'Close reason', 'Close date',
        'Invoice date', 'Reception date', 'SCAC', 'Carrier vendor', 'EDW vendor', 'Automatch',
        'MAP_ID', 'WEIGHT', 'CAPACITY WEIGHT',
        'CONTAINER_CM_VOLUME', 'VAT_REMARK',
        'ECR_NUMBER', 'CONSOLIDATION_ID', 'SHIPPING_CONDITION',
        'PROCESS_TYPE', 'DELIVERY_TYPE', 'DOC_NUMBER'
    ]
    # Remove specified columns
    # Only remove columns that actually exist in the dataframe
    columns_to_drop = [col for col in columns_to_remove if col in df_etofs.columns]
    if columns_to_drop:
        df_etofs = df_etofs.drop(columns=columns_to_drop)
    
    # Get list of column names
    column_names = df_etofs.columns.tolist()

    def extract_country_code(country_string):
        """Extract the two-letter country code from a country string."""
        if isinstance(country_string, str) and ' - ' in country_string:
            return country_string.split(' - ')[0]
        return country_string

    df_etofs['Origin Country'] = df_etofs['Origin Country'].apply(extract_country_code)
    df_etofs['Destination Country'] = df_etofs['Destination Country'].apply(extract_country_code)

    # Apply enrichments when mismatch report path(s) are configured
    if mismatch_report_paths is not None:
        df_etofs = enrich_etof_with_service(df_etofs, mismatch_report_paths)
        df_etofs = enrich_etof_with_isd_columns(df_etofs, mismatch_report_paths)
        column_names = df_etofs.columns.tolist()

    # Final result column renames (only rename columns that exist)
    result_column_renames = {
        'LC #': 'LC',
        'ETOF #': 'ETOF',
        'ISD #': 'ISD',
        'Carrier': 'CARRIER_NAME',
        'Loading date': 'SHIP_DATE',
        'Transport mode': 'TRANSPORT_MODE',
        'Equipment type': 'CONT_LOAD',
        'Service': 'SERVICE',
        'BU name': 'BU_NAME',
        'Invoice entity': 'INVOICE_ENTITY',
        'Origin Country': 'SHIP_COUNTRY',
        'Origin Postal Code': 'SHIP_POST',
        'Origin Airport': 'SHIP_AIRPORT',
        'Origin Seaport': 'SHIP_SEAPORT',
        'Origin City': 'SHIP_CITY',
        'Destination Country': 'CUST_COUNTRY',
        'Destination Postal Code': 'CUST_POST',
        'Destination Airport': 'CUST_AIRPORT',
        'Destination Seaport': 'CUST_SEAPORT',
        'Destination City': 'CUST_CITY',
        'DELIVERY NUMBER(s)': 'DELIVERY_NUMBER',
        'SHIPMENT ID(s)': 'SHIPMENT_ID',
        'Original service': 'ORIGINAL_SERVICE',
    }
    rename_map = {k: v for k, v in result_column_renames.items() if k in df_etofs.columns}
    if rename_map:
        df_etofs = df_etofs.rename(columns=rename_map)
        column_names = df_etofs.columns.tolist()

    return df_etofs, column_names


def load_mismatch_reports(mismatch_report_paths):
    """
    Load and combine one or multiple mismatch report files.
    
    Args:
        mismatch_report_paths (str or list): Single path or list of paths to mismatch_report xlsx files
                                              relative to "input/" folder
    
    Returns:
        pd.DataFrame: Combined dataframe from all mismatch reports
    """
    input_folder = "input"
    
    # Normalize to list
    if isinstance(mismatch_report_paths, str):
        mismatch_report_paths = [mismatch_report_paths]
    
    # Read and combine all mismatch reports
    dfs = []
    for path in mismatch_report_paths:
        full_path = os.path.join(input_folder, path)
        df = pd.read_excel(full_path)
        dfs.append(df)
    
    # Concatenate all dataframes
    df_combined = pd.concat(dfs, ignore_index=True)
    
    return df_combined


def enrich_etof_with_service(df_etofs, mismatch_report_paths):
    """
    Enrich ETOF dataframe by replacing Service with SERVICE_ISD from mismatch_report
    only for rows where Transport mode contains "AIR".

    Args:
        df_etofs (pd.DataFrame): The processed ETOF dataframe
        mismatch_report_paths (str or list): Single path or list of paths to mismatch_report xlsx files
                                              relative to "input/" folder

    Returns:
        pd.DataFrame: The enriched dataframe with updated Service column where Transport mode contains AIR
    """
    # Require Service and Transport mode in ETOF
    if 'Service' not in df_etofs.columns:
        return df_etofs
    transport_col = None
    for col in df_etofs.columns:
        if col.strip().lower() == 'transport mode':
            transport_col = col
            break
    if transport_col is None:
        return df_etofs

    # Rows where Transport mode contains "AIR" (case-insensitive)
    air_mask = df_etofs[transport_col].astype(str).str.contains('AIR', case=False, na=False)

    # Load mismatch report(s) and build ETOF_NUMBER -> SERVICE_ISD mapping
    df_mismatch = load_mismatch_reports(mismatch_report_paths)
    if 'SERVICE_ISD' not in df_mismatch.columns or 'ETOF_NUMBER' not in df_mismatch.columns:
        return df_etofs
    if 'ETOF #' not in df_etofs.columns:
        return df_etofs

    etof_to_service_mapping = dict(zip(
        df_mismatch['ETOF_NUMBER'].astype(str),
        df_mismatch['SERVICE_ISD']
    ))
    mapped_service = df_etofs['ETOF #'].astype(str).map(etof_to_service_mapping)

    # Only for AIR rows: replace Service with SERVICE_ISD from mapping; keep original if no mapping
    updated_service = mapped_service.fillna(df_etofs['Service'])
    df_etofs['Service'] = df_etofs['Service'].where(~air_mask, updated_service)

    return df_etofs


# Pairs (ISD column, ETOF column): add ISD column to result only where they don't match in mismatch report
ISD_ETOF_PAIRS = [
    ('SHIP_COUNTRY_ISD', 'SHIP_COUNTRY_ETOF'),
    ('CUST_COUNTRY_ISD', 'CUST_COUNTRY_ETOF'),
    ('SERVICE_ISD', 'SERVICE_ETOF'),
    ('SHIP_CITY_ISD', 'SHIP_CITY_ETOF'),
    ('CUST_CITY_ISD', 'CUST_CITY_ETOF'),
    ('CUST_POST_ISD', 'CUST_POST_ETOF'),
    ('SHIP_POST_ISD', 'SHIP_POST_ETOF'),
    ('SHIP_AIRPORT_ISD', 'SHIP_AIRPORT_ETOF'),
]


def enrich_etof_with_isd_columns(df_etofs, mismatch_report_paths):
    """
    Add result columns from the mismatch report using ETOF # as key.
    For each pair (ISD_col, ETOF_col): only where they don't match in the mismatch report,
    add the ISD column to the result with the value from the mismatch report.
    Columns that don't match are added at the end of the result.

    Args:
        df_etofs (pd.DataFrame): The processed ETOF dataframe
        mismatch_report_paths (str or list): Path(s) to mismatch_report xlsx files

    Returns:
        pd.DataFrame: The dataframe with ISD columns added at the end (only where ISD != ETOF)
    """
    if 'ETOF #' not in df_etofs.columns:
        return df_etofs

    df_mismatch = load_mismatch_reports(mismatch_report_paths)
    if 'ETOF_NUMBER' not in df_mismatch.columns:
        return df_etofs

    isd_columns_added = []

    for isd_col, etof_col in ISD_ETOF_PAIRS:
        if isd_col not in df_mismatch.columns or etof_col not in df_mismatch.columns:
            continue
        # Rows in mismatch where this pair does not match
        s_isd = df_mismatch[isd_col].astype(str).str.strip()
        s_etof = df_mismatch[etof_col].astype(str).str.strip()
        diff_mask = s_isd != s_etof
        df_diff = df_mismatch.loc[diff_mask][['ETOF_NUMBER', isd_col]].copy()
        if df_diff.empty:
            continue
        df_diff = df_diff.drop_duplicates(subset=['ETOF_NUMBER'], keep='first')
        mapping = dict(zip(df_diff['ETOF_NUMBER'].astype(str), df_diff[isd_col]))
        df_etofs[isd_col] = df_etofs['ETOF #'].astype(str).map(mapping)
        isd_columns_added.append(isd_col)

    # Move added ISD columns to the end
    if isd_columns_added:
        other_cols = [c for c in df_etofs.columns if c not in isd_columns_added]
        df_etofs = df_etofs[other_cols + isd_columns_added]

    return df_etofs


if __name__ == "__main__":
    configure_enrichment(mismatch_report_paths=["mismatch (23).xlsx"])
    etof_dataframe, etof_column_names = process_etof_file('etofs (24).xlsx')
    save_dataframe_to_excel(etof_dataframe, DEFAULT_PROCESSED_SHIPMENT_XLSX)
    save_dataframe_to_json(etof_dataframe, DEFAULT_PROCESSED_SHIPMENT_JSON)
    print(etof_dataframe.head())



