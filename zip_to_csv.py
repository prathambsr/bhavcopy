#!/usr/bin/env python
import os
import zipfile
import re
from pathlib import Path

# Path to the zips directory
zips_dir = Path('samco_bhav_data/zips')
# Path to the raw data directory
raw_dir = Path('samco_bhav_data/raw')

# Regex to extract date range from filename
date_pattern = re.compile(r'bhav_(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\\.zip')

def get_zip_files_sorted(zips_dir):
    zip_files = []
    for file in os.listdir(zips_dir):
        match = date_pattern.match(file)
        if match:
            start_date = match.group(1)
            end_date = match.group(2)
            zip_files.append((file, start_date, end_date))
    # Sort by start_date
    zip_files.sort(key=lambda x: x[1])
    return zip_files

def extract_zip(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

def main():
    zip_files = get_zip_files_sorted(zips_dir)
    for file, start_date, end_date in zip_files:
        folder_name = f"bhav_{start_date}_to_{end_date}"
        target_dir = raw_dir / folder_name
        target_dir.mkdir(parents=True, exist_ok=True)
        zip_path = zips_dir / file
        print(f"Extracting {file} to {target_dir}")
        extract_zip(zip_path, target_dir)
    print("Extraction and arrangement complete.")

if __name__ == "__main__":
    main()
