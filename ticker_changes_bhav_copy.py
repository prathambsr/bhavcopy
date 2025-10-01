import os
import pandas as pd
from datetime import datetime

def find_ticker_changes_from_bhavcopies(base_folder):
    """
    Analyzes NSE Bhavcopy files to identify changes in ticker symbols for the same ISIN.

    Args:
        base_folder (str): The path to the main folder containing date-ranged subfolders of Bhavcopies.

    Returns:
        pandas.DataFrame: A DataFrame detailing the ticker changes, including ISIN,
                          company name, old symbol, new symbol, and detection date.
    """
    print("--- Starting Analysis of Bhavcopy Archives ---")
    
    # Use ISIN as the unique key to track securities over time.
    # Structure: {isin: {'symbols': {'TICKER1', 'TICKER2'}, 'last_seen_symbol': 'TICKER2', 'company_name': '...', 'first_seen': 'YYYY-MM-DD', 'last_seen': 'YYYY-MM-DD'}}
    isin_tracker = {}
    
    # A list to store the detected changes.
    changes_log = []

    # 1. Systematically Discover all relevant Bhavcopy files
    # --------------------------------------------------------
    all_files = []
    print(f"Scanning for Bhavcopy files in '{base_folder}'...")
    
    # Walk through the directory structure
    for root, dirs, files in os.walk(base_folder):
        for file in files:
            # We assume files are named like 'YYYYMMDD_NSE.csv' or similar common formats
            if file.endswith(('.csv', '.CSV')):
                all_files.append(os.path.join(root, file))

    if not all_files:
        print(f"Error: No CSV files found in '{base_folder}' or its subdirectories. Please check the path.")
        return pd.DataFrame()

    # Sort files chronologically based on the filename
    all_files.sort()
    print(f"Found {len(all_files)} total files to process.")

    # 2. Process Files Chronologically and Track Changes
    # --------------------------------------------------
    processed_files = 0
    for file_path in all_files:
        try:
            # Extract date from filename for logging
            file_name = os.path.basename(file_path)
            current_date_str = file_name.split('_')[0]
            current_date = datetime.strptime(current_date_str, '%Y%m%d').date()

            # Read the Bhavcopy file. We only care about stocks traded on the main board ('EQ' series).
            # Columns based on the provided image: SYMBOL, SERIES, ISIN. We'll also grab company name if available.
            df = pd.read_csv(file_path, usecols=['SYMBOL', 'SERIES', 'ISIN'])
            
            # Filter for Equity series to avoid derivatives, debentures etc.
            equity_df = df[df['SERIES'] == 'EQ'].copy()

            for _, row in equity_df.iterrows():
                isin = row['ISIN']
                symbol = row['SYMBOL'].strip()

                if pd.isna(isin):
                    continue

                # If we've never seen this ISIN before, initialize its record.
                if isin not in isin_tracker:
                    isin_tracker[isin] = {
                        'symbols': {symbol},
                        'last_seen_symbol': symbol,
                        'first_seen': current_date
                    }
                else:
                    # We have seen this ISIN. Check if the symbol is new.
                    if symbol not in isin_tracker[isin]['symbols']:
                        old_symbol = isin_tracker[isin]['last_seen_symbol']
                        
                        # Log the change
                        change_record = {
                            'ISIN': isin,
                            'Old_Symbol': old_symbol,
                            'New_Symbol': symbol,
                            'Date_Detected': current_date
                        }
                        changes_log.append(change_record)
                        
                        print(f"Change Detected! ISIN: {isin}, Old: {old_symbol}, New: {symbol}, Date: {current_date}")
                        
                        # Update the tracker with the new symbol information
                        isin_tracker[isin]['symbols'].add(symbol)
                        isin_tracker[isin]['last_seen_symbol'] = symbol
            
            processed_files += 1
            if processed_files % 100 == 0:
                print(f"Processed {processed_files}/{len(all_files)} files...")

        except (FileNotFoundError, pd.errors.EmptyDataError):
            print(f"Warning: Could not read or found empty file: {file_path}")
            continue
        except KeyError:
            print(f"Warning: File {file_path} is missing required columns (SYMBOL, SERIES, ISIN). Skipping.")
            continue

    print(f"\n--- Analysis Complete ---")
    print(f"Processed a total of {processed_files} files.")

    if not changes_log:
        print("No ticker symbol changes were detected in the provided data.")
        return pd.DataFrame()

    return pd.DataFrame(changes_log)

if __name__ == "__main__":
    # --- Configuration ---
    # IMPORTANT: Update this path to the location of your main Bhavcopy folder.
    # This script assumes a structure like:
    # ./bhavcopy_data/bhav_2016-04-01_to_2016-04-15/20160401_NSE.csv
    BASE_DATA_FOLDER = os.path.join(os.path.dirname(__file__), 'samco_bhav_data', 'raw')

    # --- Run Analysis ---
    ticker_change_df = find_ticker_changes_from_bhavcopies(BASE_DATA_FOLDER)

    # --- Display Results ---
    if not ticker_change_df.empty:
        print("\n--- Summary of Ticker Symbol Changes Found ---")
        # Sort results for clarity
        ticker_change_df = ticker_change_df.sort_values(by='Date_Detected').reset_index(drop=True)
        print(ticker_change_df.to_string())
    else:
        print("\nNo changes found that match the criteria.")
