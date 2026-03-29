import os
import sys
import shutil
import gradio as gr
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Project root detection (for Colab exec/notebook when __file__ or cwd is wrong):
# CANF_PROJECT_ROOT, __file__, common clone paths, scan /content/*, cwd-relative
# repo names, then walk parents. A directory qualifies if it has shipment_input.py
# or vocabulary.py (this repo).
# ---------------------------------------------------------------------------

_PROJECT_MARKERS = (
    "shipment_input.py",
    "vocabulary.py",
)


def _is_project_dir(p: Path) -> bool:
    if not p.is_dir():
        return False
    return any((p / name).is_file() for name in _PROJECT_MARKERS)


def _known_repo_paths_colab() -> list:
    """Typical Google Colab clone locations for this repo (underscore or hyphen)."""
    extra = []
    for path in ("/content/CANF_customization", "/content/CANF-customization"):
        p = Path(path)
        if _is_project_dir(p):
            extra.append(p.resolve())
    cwd = Path.cwd()
    for rel in (Path("CANF_customization"), Path("CANF-customization")):
        p = (cwd / rel).resolve()
        if _is_project_dir(p):
            extra.append(p)
    return extra


def _colab_clone_candidates() -> list:
    """Likely repo locations when cwd is /content but code lives in /content/<repo>."""
    extra = []
    for p in _known_repo_paths_colab():
        if p not in extra:
            extra.append(p)
    content = Path("/content")
    if content.is_dir():
        try:
            for child in sorted(content.iterdir()):
                if child.is_dir() and _is_project_dir(child):
                    if child.resolve() not in extra:
                        extra.append(child.resolve())
        except OSError:
            pass
    cwd = Path.cwd()
    for folder_name in ("CANF_customization", "CANF-customization", "Apple CANF customization"):
        p = (cwd / folder_name).resolve()
        if _is_project_dir(p) and p not in extra:
            extra.append(p)
    return extra


def get_project_root() -> Optional[Path]:
    """
    Find the folder that contains this project's modules (see _PROJECT_MARKERS).
    Set env CANF_PROJECT_ROOT if auto-detection fails.
    """
    candidates = []
    env_root = os.environ.get("CANF_PROJECT_ROOT", "").strip()
    if env_root:
        candidates.append(Path(env_root).resolve())
    try:
        candidates.append(Path(__file__).resolve().parent)
    except NameError:
        pass
    # Prefer known Colab clone paths, then scan all /content/* subdirs that look like this repo
    _seen_norm = {c.resolve() for c in candidates if hasattr(c, "resolve")}
    for p in _colab_clone_candidates():
        try:
            pr = p.resolve()
        except OSError:
            pr = p
        if pr not in _seen_norm:
            _seen_norm.add(pr)
            candidates.insert(0, p)
    cwd = Path.cwd().resolve()
    candidates.append(cwd)
    # Walk up from cwd (user may launch from a subfolder)
    p = cwd
    for _ in range(8):
        candidates.append(p)
        if p.parent == p:
            break
        p = p.parent
    seen = set()
    for cand in candidates:
        try:
            c = cand.resolve()
        except OSError:
            continue
        if c in seen:
            continue
        seen.add(c)
        if _is_project_dir(c):
            return c
    return None


def ensure_project_on_syspath() -> Optional[str]:
    """Insert project root at front of sys.path so `import shipment_input` always works."""
    root = get_project_root()
    if root is None:
        return None
    s = str(root)
    if s not in sys.path:
        sys.path.insert(0, s)
    return s


def setup_python_path():
    """Setup Python path to include the project directory for imports."""
    try:
        added = ensure_project_on_syspath()
        if added:
            print(f"📁 Added project root to Python path: {added}")
        else:
            # Last resort: cwd
            cwd = os.getcwd()
            if cwd and cwd not in sys.path:
                sys.path.insert(0, cwd)
                print(f"📁 Added cwd to Python path (shipment_input.py not found): {cwd}")
    except Exception as e:
        print(f"⚠️ Warning: Could not set up Python path: {e}")


setup_python_path()

def run_full_workflow_gradio(rate_card_file, etof_file, mismatch_report_files=None):
    """
    Main workflow for use in Gradio.
    Accepts uploaded files; returns the final formatted workbook and status text.

    Pipeline (artifacts under ``partly_df/`` unless noted):
    1. Save uploads to ``input/`` (multiple rate cards and mismatch reports supported).
    2. For each rate card: ``rate_card_input.save_rate_card_output`` (JSON) and
       ``rate_card_accessorial_costs.process_accessorial_costs_file`` (per-RA JSON).
    3. ``shipment_input.configure_enrichment`` + ``process_etof_file``; save extract to ``partly_df/``.
    4. ``vocabulary.map_and_rename_columns`` → ``vocabulary_mapping.json``.
    5. ``matching.run_matching_from_json`` → ``Matched_Shipments_with.json`` (and .xlsx).
    6. ``mismatch_report.process_mismatch_file`` → ``mismatch_processed.json`` / .xlsx.
    7. ``processing.run_processing`` → ``mismatch_enriched.json`` / .xlsx.
    8. ``formatting.format_result_file`` (enrichment off) → ``output/mismatch_report.xlsx``.
    """
    status_messages = []
    errors = []
    warnings = []
    
    def log_status(msg, level="info"):
        """Log status messages with different levels"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_msg = f"[{timestamp}] {msg}"
        status_messages.append(formatted_msg)
        
        if level == "error":
            errors.append(msg)
        elif level == "warning":
            warnings.append(msg)
        
        print(formatted_msg)
    
    # Handle file input (Gradio may give strings or tempfile paths)
    def _handle_upload(uploaded, allow_multiple=False):
        if uploaded is None:
            return None if not allow_multiple else []
        if isinstance(uploaded, list):
            if not allow_multiple:
                return _handle_upload(uploaded[0] if uploaded else None, allow_multiple=False)
            result = []
            for item in uploaded:
                if item is None:
                    continue
                if hasattr(item, "name"):
                    result.append(item.name)
                elif isinstance(item, str):
                    result.append(item)
            return result if result else []
        if hasattr(uploaded, "name"):
            return uploaded.name
        if isinstance(uploaded, str):
            return uploaded
        return None if not allow_multiple else []
    
    # Convert all filepaths to correct types (rate card: multiple files)
    rate_card_path = _handle_upload(rate_card_file, allow_multiple=True)
    etof_path = _handle_upload(etof_file)
    mismatch_report_path = _handle_upload(mismatch_report_files, allow_multiple=True)
    
    if isinstance(rate_card_path, str):
        rate_card_paths = [rate_card_path]
    else:
        rate_card_paths = list(rate_card_path or [])
    
    # Validate required fields
    if not etof_path:
        error_msg = "❌ Error: ETOF File is required."
        log_status(error_msg, "error")
        return None, error_msg
    
    if not rate_card_paths:
        error_msg = "❌ Error: At least one Rate Card file is required."
        log_status(error_msg, "error")
        return None, error_msg
    
    if not mismatch_report_path:
        error_msg = "❌ Error: At least one Mismatch Report file is required."
        log_status(error_msg, "error")
        return None, error_msg
    
    log_status("✅ Validation passed. Starting workflow...", "info")
    
    # Resolve project root (folder with shipment_input.py) so imports and folders stay consistent
    ensure_project_on_syspath()
    project_path = get_project_root()
    if project_path:
        script_dir = str(project_path)
    else:
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            script_dir = os.getcwd()
    log_status(f"📁 Working project directory: {script_dir}", "info")

    # Create output and input directories
    input_dir = os.path.join(script_dir, "input")
    output_dir = os.path.join(script_dir, "output")
    partly_df_dir = os.path.join(script_dir, "partly_df")
    
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(partly_df_dir, exist_ok=True)
    
    log_status(f"📁 Input folder: {input_dir}", "info")
    log_status(f"📁 Output folder: {output_dir}", "info")
    log_status(f"📁 Intermediate files folder: {partly_df_dir}", "info")
    
    # Copy uploaded files to input directory
    rate_card_filenames: list[str] = []
    etof_filename = None
    mismatch_report_filenames = []
    
    for rate_card_path_one in rate_card_paths:
        fn = os.path.basename(rate_card_path_one)
        input_rc_path = os.path.join(input_dir, fn)
        shutil.copy2(rate_card_path_one, input_rc_path)
        rate_card_filenames.append(fn)
        log_status(f"✓ Rate Card file saved: {fn}", "info")
        if not os.path.exists(input_rc_path):
            error_msg = f"❌ Error: Failed to copy rate card file to {input_rc_path}"
            log_status(error_msg, "error")
            return None, error_msg
    
    # Copy ETOF file
    if etof_path:
        etof_filename = os.path.basename(etof_path)
        input_etof_path = os.path.join(input_dir, etof_filename)
        shutil.copy2(etof_path, input_etof_path)
        log_status(f"✓ ETOF file saved: {etof_filename}", "info")
        if not os.path.exists(input_etof_path):
            error_msg = f"❌ Error: Failed to copy ETOF file to {input_etof_path}"
            log_status(error_msg, "error")
            return None, error_msg
    
    # Copy mismatch report files (if provided)
    if mismatch_report_path:
        mismatch_files_list = mismatch_report_path if isinstance(mismatch_report_path, list) else [mismatch_report_path]
        for idx, mismatch_file_path in enumerate(mismatch_files_list):
            if mismatch_file_path:
                mismatch_filename = os.path.basename(mismatch_file_path)
                input_mismatch_path = os.path.join(input_dir, mismatch_filename)
                shutil.copy2(mismatch_file_path, input_mismatch_path)
                mismatch_report_filenames.append(mismatch_filename)
                log_status(f"✓ Mismatch Report file saved: {mismatch_filename}", "info")
    
    # Absolute paths so matching/formatting always find files even if cwd changes
    partly_df_abs = os.path.join(script_dir, "partly_df")
    os.makedirs(partly_df_abs, exist_ok=True)

    # Change to project directory so relative paths (input/, partly_df/) work
    original_cwd = os.getcwd()
    final_file_path = None
    try:
        os.chdir(script_dir)
        
        # --- STEP 1: Rate cards → partly_df (filtered JSON + accessorial JSON per file) ---
        try:
            from rate_card_input import save_rate_card_output
            from rate_card_accessorial_costs import process_accessorial_costs_file

            for rc_fn in rate_card_filenames:
                log_status(f"📄 Rate card (JSON): {rc_fn}", "info")
                save_rate_card_output(rc_fn, save_excel=False, save_json=True)
                acc_in = os.path.join("input", rc_fn)
                if os.path.isfile(acc_in):
                    acc_out = process_accessorial_costs_file(acc_in)
                    log_status(f"✓ Accessorial JSON: {acc_out}", "info")
        except Exception as e:
            error_msg = f"❌ Error processing rate card / accessorial: {str(e)}"
            log_status(error_msg, "error")
            import traceback
            log_status(traceback.format_exc(), "error")
            return None, "\n".join(status_messages)

        # --- STEP 2: ETOF enrichment config + process + save extract (shipment_input.py) ---
        try:
            from shipment_input import (
                configure_enrichment,
                process_etof_file,
                save_dataframe_to_excel,
                save_dataframe_to_json,
                DEFAULT_PROCESSED_SHIPMENT_JSON,
                DEFAULT_PROCESSED_SHIPMENT_XLSX,
            )

            mismatch_paths = (
                mismatch_report_filenames
                if len(mismatch_report_filenames) > 1
                else mismatch_report_filenames[0]
            )
            configure_enrichment(mismatch_report_paths=mismatch_paths)
            log_status(
                f"✓ Enrichment configured with {len(mismatch_report_filenames)} mismatch report(s)",
                "info",
            )

            log_status(f"📄 Processing ETOF file: {etof_filename}", "info")
            etof_df, etof_columns = process_etof_file(etof_filename)
            save_dataframe_to_excel(etof_df, DEFAULT_PROCESSED_SHIPMENT_XLSX)
            save_dataframe_to_json(etof_df, DEFAULT_PROCESSED_SHIPMENT_JSON)
            log_status(
                f"✓ ETOF processed: {etof_df.shape[0]} rows, {len(etof_columns)} columns; "
                f"saved {DEFAULT_PROCESSED_SHIPMENT_JSON}",
                "info",
            )
        except Exception as e:
            error_msg = f"❌ Error processing ETOF file: {str(e)}"
            log_status(error_msg, "error")
            import traceback
            log_status(traceback.format_exc(), "error")
            return None, "\n".join(status_messages)

        # --- STEP 3: Vocabulary (vocabulary.py) ---
        try:
            from vocabulary import map_and_rename_columns

            log_status("🔤 Vocabulary mapping → vocabulary_mapping.json", "info")
            if len(rate_card_filenames) == 1:
                vocab_result = map_and_rename_columns(
                    rate_card_file_path=rate_card_filenames[0],
                    etof_file_path=etof_filename,
                    output_txt_path="column_mapping_results.txt",
                    ignore_rate_card_columns=None,
                )
            else:
                vocab_result = map_and_rename_columns(
                    rate_card_file_path=None,
                    etof_file_path=etof_filename,
                    output_txt_path="column_mapping_results.txt",
                    ignore_rate_card_columns=None,
                )

            if vocab_result is None:
                error_msg = "❌ Error: Vocabulary mapping returned None"
                log_status(error_msg, "error")
                return None, "\n".join(status_messages)

            etof_renamed, _, _ = vocab_result
            if etof_renamed is not None and not etof_renamed.empty:
                log_status(f"✓ Vocabulary mapping: {etof_renamed.shape[0]} rows", "info")
            else:
                log_status("⚠️ Warning: Vocabulary mapping produced no data", "warning")
            log_status(
                f"   Created: {os.path.join(partly_df_abs, 'vocabulary_mapping.json')}",
                "info",
            )
        except Exception as e:
            error_msg = f"❌ Error in vocabulary mapping: {str(e)}"
            log_status(error_msg, "error")
            import traceback
            log_status(traceback.format_exc(), "error")
            return None, "\n".join(status_messages)

        # --- STEP 4: Matching (matching.py) ---
        try:
            from matching import run_matching_from_json

            log_status("🔍 Matching → Matched_Shipments_with.json", "info")
            matching_result = run_matching_from_json(
                rate_card_json_path=os.path.join(
                    partly_df_abs, "Filtered_Rate_Card_with_Conditions.json"
                ),
                vocabulary_json_path=os.path.join(partly_df_abs, "vocabulary_mapping.json"),
                output_dir=partly_df_abs,
            )
            if matching_result and matching_result[0]:
                log_status("✓ Matching completed", "info")
            else:
                log_status("⚠️ Warning: Matching did not produce output", "warning")
        except Exception as e:
            error_msg = f"❌ Error in matching: {str(e)}"
            log_status(error_msg, "error")
            import traceback
            log_status(traceback.format_exc(), "error")
            return None, "\n".join(status_messages)

        # --- STEP 5: Mismatch report (mismatch_report.py) ---
        try:
            from shipment_input import load_mismatch_reports
            from mismatch_report import process_mismatch_file

            if len(mismatch_report_filenames) > 1:
                merged = load_mismatch_reports(mismatch_report_filenames)
                merge_name = "_workflow_merged_mismatch.xlsx"
                merged.to_excel(os.path.join("input", merge_name), index=False)
                mismatch_basename = merge_name
                log_status(
                    f"📄 Merged {len(mismatch_report_filenames)} mismatch files → {merge_name}",
                    "info",
                )
            else:
                mismatch_basename = mismatch_report_filenames[0]

            log_status(f"📄 Mismatch report pipeline: {mismatch_basename}", "info")
            mx_xlsx, mx_json = process_mismatch_file(
                os.path.join("input", mismatch_basename),
                shipment_df=etof_df,
            )
            log_status(f"✓ Mismatch processed: {mx_json}", "info")
        except Exception as e:
            error_msg = f"❌ Error in mismatch_report: {str(e)}"
            log_status(error_msg, "error")
            import traceback
            log_status(traceback.format_exc(), "error")
            return None, "\n".join(status_messages)

        # --- STEP 6: Enrichment / rate logic (processing.py) ---
        try:
            from processing import run_processing

            log_status("📊 processing.run_processing → mismatch_enriched.*", "info")
            out_j, out_x = run_processing(partly_df=partly_df_abs)
            log_status(f"✓ Enriched: {out_j}", "info")
        except Exception as e:
            error_msg = f"❌ Error in processing: {str(e)}"
            log_status(error_msg, "error")
            import traceback
            log_status(traceback.format_exc(), "error")
            return None, "\n".join(status_messages)

        # --- STEP 7: Formatted export (formatting.py → output/) ---
        try:
            from formatting import format_result_file

            log_status("📝 format_result_file → output/mismatch_report.xlsx", "info")
            final_path = format_result_file(
                run_enrichment=False,
                file_path=os.path.join(partly_df_abs, "mismatch_enriched.xlsx"),
                partly_df=partly_df_abs,
            )
            final_file_path = os.fspath(final_path)
            log_status(f"✓ Final workbook: {final_file_path}", "info")
        except Exception as e:
            error_msg = f"❌ Error in formatting: {str(e)}"
            log_status(error_msg, "error")
            import traceback
            log_status(traceback.format_exc(), "error")
            return None, "\n".join(status_messages)
        
    finally:
        os.chdir(original_cwd)
    
    # Prepare status summary
    status_summary = []
    status_summary.append("=" * 60)
    status_summary.append("WORKFLOW SUMMARY")
    status_summary.append("=" * 60)
    status_summary.append("")
    
    if final_file_path and os.path.exists(final_file_path):
        status_summary.append(f"✅ SUCCESS: Output file created")
        status_summary.append(f"   Location: {final_file_path}")
    else:
        status_summary.append(f"❌ Workflow did not complete successfully")
    
    status_summary.append("")
    
    if errors:
        status_summary.append(f"❌ ERRORS ({len(errors)}):")
        for i, error in enumerate(errors[:5], 1):
            status_summary.append(f"  {i}. {error}")
        if len(errors) > 5:
            status_summary.append(f"  ... and {len(errors) - 5} more errors")
        status_summary.append("")
    
    if warnings:
        status_summary.append(f"⚠️  WARNINGS ({len(warnings)}):")
        for i, warning in enumerate(warnings[:5], 1):
            status_summary.append(f"  {i}. {warning}")
        if len(warnings) > 5:
            status_summary.append(f"  ... and {len(warnings) - 5} more warnings")
        status_summary.append("")
    
    # Add key status messages
    key_messages = [msg for msg in status_messages if any(keyword in msg for keyword in 
                    ['✓', '❌', '⚠️', 'Error', 'Warning', 'SUCCESS', 'completed', 'failed'])]
    
    if key_messages:
        status_summary.append("Key Steps:")
        status_summary.append("-" * 60)
        status_summary.extend(key_messages[-15:])
    
    status_text = "\n".join(status_summary)
    return (final_file_path, status_text) if final_file_path and os.path.exists(final_file_path) else (None, status_text)


# ---- Gradio UI definition ----
with gr.Blocks(title="Mismatch Analyzer", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 📊 Mismatch Analyzer")
    gr.Markdown("### Process and match shipment data with rate card lanes")
    
    with gr.Accordion("📖 Instructions & Information", open=False):
        gr.Markdown("""
        ## How to Use This Workflow

        ### Google Colab (recommended)
        Do **not** `chdir` to `/content` before loading this app — stay inside the cloned repo, or set `CANF_PROJECT_ROOT`.
        ```text
        !git clone https://github.com/YOUR_ORG/CANF_customization.git  # or pull if already cloned
        %cd /content/CANF_customization
        !pip install -q gradio pandas openpyxl nest_asyncio
        !python result.py
        ```
        If you use `exec(open(...).read())` from `/content`, the app still tries to auto-detect `/content/CANF_customization`.
        Optional: `os.environ["CANF_PROJECT_ROOT"] = "/content/CANF_customization"` before `exec`.
        
        ### Step 1: Upload Required Files
        - **Rate Card file(s)** (Required): one or more rate card workbooks (.xlsx); each produces
          `partly_df/Filtered_Rate_Card_with_Conditions_<RA>.json` and `partly_df/accessorial_costs_<RA>.json`
        - **ETOF File** (Required): shipment extract (.xlsx)
        - **Mismatch Report file(s)** (Required): one or more mismatch exports (.xlsx); used for ETOF enrichment
          and for `mismatch_report.process_mismatch_file` (multiple files are concatenated for the mismatch step)
        
        ### Step 2: Run Workflow
        - Click **Run Analyzer**
        - Download **output/mismatch_report.xlsx** (Gradio file widget)
        - Intermediate artifacts remain under `partly_df/` (enriched JSON, matched shipments, etc.)
        
        ## Workflow Steps
        1. Save uploads under `input/`
        2. **Rate cards**: `rate_card_input.save_rate_card_output` + `rate_card_accessorial_costs.process_accessorial_costs_file` per workbook
        3. **ETOF**: `shipment_input.configure_enrichment` + `process_etof_file`; save `partly_df/etof_processed_apple.json` (and .xlsx)
        4. **Vocabulary**: `vocabulary.map_and_rename_columns` → `partly_df/vocabulary_mapping.json` (with multiple rate cards, RA is resolved from ETOF)
        5. **Matching**: `matching.run_matching_from_json` → `partly_df/Matched_Shipments_with.json`
        6. **Mismatch**: `mismatch_report.process_mismatch_file` → `partly_df/mismatch_processed.json`
        7. **Processing**: `processing.run_processing` → `partly_df/mismatch_enriched.json` / `.xlsx`
        8. **Formatting**: `formatting.format_result_file` → **`output/mismatch_report.xlsx`**
        
        ## Output Files
        - **`output/mismatch_report.xlsx`**: final styled report (download from the app)
        - Raw enriched data: `partly_df/mismatch_enriched.xlsx` / `.json`
        
        ## Troubleshooting
        - **Errors are shown in red** in the Status/Errors section
        - **Warnings are shown in yellow** - these may not prevent completion
        - Check that all required files are uploaded
        - Verify file formats are correct (.xlsx)
        - Ensure Rate Card and ETOF files have the expected structure
        """)
    
    gr.Markdown("---")
    gr.Markdown("### 📁 File Upload")
    gr.Markdown("**Required:** one or more Rate Card files, ETOF file, and one or more Mismatch Report files.")
    
    with gr.Row():
        rate_card_input = gr.File(
            label="Rate Card file(s) (.xlsx) *Required",
            file_types=[".xlsx", ".xls"],
            file_count="multiple",
        )
        etof_input = gr.File(label="ETOF File (.xlsx) *Required", file_types=[".xlsx", ".xls"])
    
    with gr.Row():
        mismatch_report_input = gr.File(
            label="Mismatch Report file(s) (.xlsx) *Required — ETOF enrichment + mismatch pipeline",
            file_types=[".xlsx", ".xls"],
            file_count="multiple",
        )
    
    gr.Markdown("---")
    launch_button = gr.Button("🚀 Run Analyzer", variant="primary", size="lg")
    
    with gr.Row():
        out = gr.File(label="📥 Result Files (Download Final Output)")
        status_output = gr.Textbox(
            label="📋 Status & Errors",
            lines=20,
            max_lines=30,
            interactive=False,
            placeholder="Workflow status and error messages will appear here..."
        )
    
    def launch_workflow(rate_card_file, etof_file, mismatch_report_files):
        try:
            result_file, status_text = run_full_workflow_gradio(
                rate_card_file=rate_card_file,
                etof_file=etof_file,
                mismatch_report_files=mismatch_report_files,
            )
            return result_file, status_text
        except Exception as e:
            import traceback
            error_details = f"❌ CRITICAL ERROR:\n{str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            return None, error_details
    
    launch_button.click(
        launch_workflow,
        inputs=[
            rate_card_input, etof_input, mismatch_report_input,
        ],
        outputs=[out, status_output]
    )

if __name__ == "__main__":
    import sys
    
    # Resolve project root only — do NOT mkdir here (avoids duplicate /content/input when cwd≠repo).
    # Folders are created when you run the workflow with the correct script_dir.
    ensure_project_on_syspath()
    _root = get_project_root()
    if _root:
        script_dir = str(_root)
    else:
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            script_dir = os.getcwd()
    
    input_dir = os.path.join(script_dir, "input")
    output_dir = os.path.join(script_dir, "output")
    partly_df_dir = os.path.join(script_dir, "partly_df")
    
    print(f"📁 Project root: {script_dir}")
    print(f"📁 Workflow will use: {input_dir} | {output_dir} | {partly_df_dir}")
    
    # Check if running in Colab
    in_colab = 'google.colab' in sys.modules
    
    if in_colab:
        print("🚀 Launching Gradio interface for Google Colab...")
        demo.launch(server_name="0.0.0.0", share=False, debug=False, show_error=True)
    else:
        print("🚀 Launching Gradio interface locally...")
        print(f"💡 Input files will be saved to: {input_dir}")
        print(f"💡 Output files will be saved to: {output_dir}")
        print(f"💡 Intermediate files will be saved to: {partly_df_dir}")
        demo.launch(server_name="127.0.0.1", share=False)
