"""
Upload CANF Project Files to Google Drive

This script:
1. Prompts user for Name, Rate case (e.g. GAR25), and optional comment
2. Creates a folder named "Name GAR25 dd.mm.yyyy" (name + rate case + date) on Google Drive
3. Uploads files from partly_df, input, and output folders
4. Saves the comment as a txt file in the created folder
"""

import os
import shutil
from datetime import datetime

# ============================================================
# CONFIGURATION - Hardcoded Google Drive path
# ============================================================
# Examples:
#   "My Drive/CANF Reports"                    - Personal Google Drive
#   "Shared drives/Team Drive Name/Folder"     - Shared Drive
#   "Shareddrives/Team Drive Name/Folder"      - Shared Drive (alternative)
# ============================================================
GOOGLE_DRIVE_PATH = "Shared drives/FA Operations Europe/AT/Generic_AA/Mismatch Analyzer/Apple Mismatch Analyzer/Files for analysis or issues" # Change this to your desired path
# ============================================================


def get_user_input():
    """
    Get user input for folder naming and comment.
    
    Returns:
        tuple: (name, rate_case, date_str, comment)
    """
    print("\n" + "="*60)
    print("📤 UPLOAD CANF FILES TO GOOGLE DRIVE")
    print("="*60)
    
    # Get Name
    print("\n1. Enter your name (e.g., Anna Vitkina):")
    name = input("   Name: ").strip()
    while not name:
        print("   ❌ Name cannot be empty. Please enter your name:")
        name = input("   Name: ").strip()
    
    # Get Rate case (e.g. GAR25)
    print("\n2. Enter rate case (e.g., GAR25):")
    rate_case = input("   Rate case: ").strip()
    while not rate_case:
        print("   ❌ Rate case cannot be empty. Please enter rate case:")
        rate_case = input("   Rate case: ").strip()
    
    # Auto-generate date
    date_str = datetime.now().strftime("%d.%m.%Y")
    print(f"\n3. Date (auto-generated): {date_str}")
    
    # Get comment (optional)
    print("\n4. Enter a brief comment (optional, press Enter twice to finish):")
    print("   You can enter multiple lines. Press Enter twice when finished.")
    
    comment_lines = []
    empty_line_count = 0
    
    while True:
        line = input("   > ")
        if line.strip() == '':
            empty_line_count += 1
            if empty_line_count >= 2:
                # Two consecutive empty lines - stop
                break
            if not comment_lines:
                # First line is empty - skip comment entirely
                break
            comment_lines.append(line)  # Keep single empty line as paragraph break
        else:
            empty_line_count = 0  # Reset counter when non-empty line entered
            comment_lines.append(line)
    
    # Remove trailing empty lines
    while comment_lines and comment_lines[-1].strip() == '':
        comment_lines.pop()
    
    comment = '\n'.join(comment_lines) if comment_lines else None
    
    # Preview folder name (preserve casing for codes like GAR25)
    folder_name = f"{name} {rate_case} {date_str}"
    print(f"\n📁 Folder will be created: {folder_name}")
    
    if comment:
        print(f"📝 Comment preview:\n   {comment[:100]}{'...' if len(comment) > 100 else ''}")
    
    return name, rate_case, date_str, comment


def upload_to_google_drive(
    google_drive_base_path: str,
    name: str = None,
    rate_case: str = None,
    date_str: str = None,
    comment: str = None,
    local_base_folder: str = None
):
    """
    Upload files from partly_df, input, and output folders to Google Drive.
    
    Args:
        google_drive_base_path: Base path on Google Drive where folder will be created
                               (e.g., "My Drive/CANF Reports")
        name: User name for folder naming. If None, will prompt for input.
        rate_case: Rate case identifier for folder naming (e.g. GAR25). If None, will prompt for input.
        date_str: Date string in dd.mm.yyyy format. If None, uses current date.
        comment: Optional comment to save as txt file.
        local_base_folder: Local folder containing partly_df, input, output. 
                          If None, uses script directory.
    
    Returns:
        str: Path to the created folder on Google Drive, or None if failed
    """
    
    # Check if running in Google Colab
    try:
        from google.colab import drive
        in_colab = True
        print("\n📁 Running in Google Colab - mounting Google Drive...")
        drive.mount('/content/drive')
        drive_mount_point = "/content/drive"
    except ImportError:
        in_colab = False
        print("\n⚠️ Not running in Google Colab.")
        print("   Using local file system (Google Drive Desktop app sync assumed).")
        drive_mount_point = ""
    
    # Get user input if not provided
    if name is None or rate_case is None:
        name, rate_case, date_str, comment = get_user_input()
        if name is None:
            return None
    
    # Auto-generate date if not provided
    if date_str is None:
        date_str = datetime.now().strftime("%d.%m.%Y")
    
    # Determine local base folder
    if local_base_folder is None:
        try:
            local_base_folder = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            local_base_folder = os.getcwd()
    
    # Create folder name
    folder_name = f"{name} {rate_case} {date_str}"
    
    # Construct full Google Drive path
    if in_colab:
        # Handle different Google Drive path formats:
        # - "My Drive/folder" -> /content/drive/My Drive/folder
        # - "Shared drives/TeamDrive/folder" -> /content/drive/Shareddrives/TeamDrive/folder
        # - "Shareddrives/TeamDrive/folder" -> /content/drive/Shareddrives/TeamDrive/folder
        
        path_lower = google_drive_base_path.lower()
        
        if path_lower.startswith("my drive"):
            # Personal Google Drive
            full_drive_path = os.path.join(drive_mount_point, google_drive_base_path, folder_name)
        elif path_lower.startswith("shared drives") or path_lower.startswith("shareddrives"):
            # Shared Drive - normalize to "Shareddrives" (Colab mount format)
            # "Shared drives/X" or "Shareddrives/X" -> "Shareddrives/X"
            if path_lower.startswith("shared drives"):
                normalized_path = "Shareddrives" + google_drive_base_path[13:]  # Remove "Shared drives"
            else:
                normalized_path = google_drive_base_path
            full_drive_path = os.path.join(drive_mount_point, normalized_path, folder_name)
        else:
            # Assume it's a path inside My Drive if not specified
            full_drive_path = os.path.join(drive_mount_point, "My Drive", google_drive_base_path, folder_name)
    else:
        full_drive_path = os.path.join(google_drive_base_path, folder_name)
    
    print(f"\n📁 Creating folder: {full_drive_path}")
    
    # Create the main folder
    try:
        os.makedirs(full_drive_path, exist_ok=True)
        print(f"   ✅ Folder created successfully")
    except Exception as e:
        print(f"   ❌ Error creating folder: {e}")
        return None
    
    # Define source folders to upload
    source_folders = ['CANF-test-updated/partly_df', 'input', 'output']
    
    total_files_copied = 0
    
    for folder_name_src in source_folders:
        source_path = os.path.join(local_base_folder, folder_name_src)
        dest_path = os.path.join(full_drive_path, folder_name_src)
        
        if not os.path.exists(source_path):
            continue  # Skip folders that don't exist
        
        print(f"\n📂 Uploading {folder_name_src}/...")
        
        # Create destination folder
        os.makedirs(dest_path, exist_ok=True)
        
        # Copy files
        files_copied = 0
        for item in os.listdir(source_path):
            item_source = os.path.join(source_path, item)
            item_dest = os.path.join(dest_path, item)
            
            try:
                if os.path.isfile(item_source):
                    shutil.copy2(item_source, item_dest)
                    files_copied += 1
                    print(f"   ✓ {item}")
                elif os.path.isdir(item_source):
                    # Copy subdirectory
                    shutil.copytree(item_source, item_dest, dirs_exist_ok=True)
                    subfiles = sum([len(files) for _, _, files in os.walk(item_source)])
                    files_copied += subfiles
                    print(f"   ✓ {item}/ ({subfiles} files)")
            except Exception as e:
                print(f"   ❌ Error copying {item}: {e}")
        
        print(f"   📊 {files_copied} files copied from {folder_name_src}/")
        total_files_copied += files_copied
    
    # Save comment as txt file if provided
    if comment:
        comment_file_path = os.path.join(full_drive_path, "comment.txt")
        try:
            with open(comment_file_path, 'w', encoding='utf-8') as f:
                f.write(f"CANF Analysis Comment\n")
                f.write(f"{'='*40}\n\n")
                f.write(f"Name: {name}\n")
                f.write(f"Rate case: {rate_case}\n")
                f.write(f"Date: {date_str}\n")
                f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"Comment:\n")
                f.write(f"{'-'*40}\n")
                f.write(comment)
            print(f"\n📝 Comment saved to: comment.txt")
        except Exception as e:
            print(f"\n❌ Error saving comment: {e}")
    
    # Summary
    print("\n" + "="*60)
    print("📊 UPLOAD SUMMARY")
    print("="*60)
    print(f"   Folder created: {folder_name}")
    print(f"   Location: {full_drive_path}")
    print(f"   Total files uploaded: {total_files_copied}")
    if comment:
        print(f"   Comment file: ✅ Saved")
    print("="*60)
    
    return full_drive_path


def upload_from_colab(google_drive_folder_path: str = None):
    """
    Convenience function for Google Colab usage.
    Prompts user for name, rate case, and comment interactively.
    
    Args:
        google_drive_folder_path: Path to folder on Google Drive where subfolder will be created.
                                  If None, uses hardcoded GOOGLE_DRIVE_PATH.
    
    Example:
        upload_from_colab()  # Uses hardcoded path
        
        # This will:
        # 1. Prompt for Name, Rate case, Comment
        # 2. Create folder like "Anna Vitkina GAR25 16.12.2024"
        # 3. Upload partly_df, input, output folders
        # 4. Save comment.txt
    """
    if google_drive_folder_path is None:
        google_drive_folder_path = GOOGLE_DRIVE_PATH
    return upload_to_google_drive(google_drive_base_path=google_drive_folder_path)


def upload_with_params(
    google_drive_folder_path: str,
    name: str,
    rate_case: str,
    comment: str = None
):
    """
    Upload with pre-defined parameters (no user prompts).
    
    Args:
        google_drive_folder_path: Path to folder on Google Drive
        name: User name (e.g., "Anna Vitkina")
        rate_case: Rate case code (e.g., "GAR25")
        comment: Optional comment text
    
    Example:
        upload_with_params(
            google_drive_folder_path="My Drive/CANF Reports",
            name="Anna Vitkina",
            rate_case="GAR25",
            comment="Initial analysis for December shipments"
        )
    """
    return upload_to_google_drive(
        google_drive_base_path=google_drive_folder_path,
        name=name,
        rate_case=rate_case,
        comment=comment
    )


# Example usage
if __name__ == "__main__":
    print("="*60)
    print("UPLOAD CANF FILES TO GOOGLE DRIVE")
    print("="*60)
    print(f"\n📁 Destination: {GOOGLE_DRIVE_PATH}")
    print("")
    
    # Run upload with hardcoded path
    upload_to_google_drive(google_drive_base_path=GOOGLE_DRIVE_PATH)
