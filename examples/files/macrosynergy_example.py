#!/usr/bin/env python3
"""Download via the third-party macrosynergy.download client (compat reference)."""

from macrosynergy.download import DataQueryFileAPIClient

FILE_GROUP_IDS = ["JPMAQS_GENERIC_RETURNS"]

client = DataQueryFileAPIClient("./jpmaqs_data")

for file_group_id in FILE_GROUP_IDS:
    files_df = client.list_available_files(file_group_id=file_group_id)
    for _, row in files_df.iterrows():
        filename = row.get("filename") or row.get("file-name")
        if filename:
            print(client.download_file(filename=filename))
