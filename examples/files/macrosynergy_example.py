from pathlib import Path

from dotenv import load_dotenv
from macrosynergy.download import DataQueryFileAPIClient
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# Hardcoded file-group-ids for UAT testing
FILE_GROUP_IDS = ["JPMAQS_GENERIC_RETURNS"]

client = DataQueryFileAPIClient("./jpmaqs_data")

# Download files for each file group
for file_group_id in FILE_GROUP_IDS:
    print(f"Processing {file_group_id}...")
    try:
        files_df = client.list_available_files(file_group_id=file_group_id)
        if files_df is not None and not files_df.empty:
            print(f"  Found {len(files_df)} files")
            # Download each file
            for _, row in files_df.iterrows():
                filename = row.get("filename") or row.get("file-name")
                if filename:
                    print(f"  Downloading {filename}...")
                    try:
                        file_path = client.download_file(filename=filename)
                        print(f"    Saved to {file_path}")
                    except Exception as e:
                        print(f"    Download error: {e}")
        else:
            print(f"  No files found for {file_group_id}")
    except Exception as e:
        print(f"  Error: {e}")

print("Done.")
