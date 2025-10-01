"""
Script 2: Samco BHAV Copy Downloader (Automated)
Downloads historical BHAV copy data from Samco using the session cookie.

This script:
- Uses the ci_session cookie to authenticate
- Downloads monthly chunks as ZIP files
- Extracts and processes the data
- Includes 5-second delays to respect rate limits
- Handles errors gracefully with retries

Author: Quant Analyst
Date: 2025-10-01
"""

import os
import time
import zipfile
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Tuple
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('samco_download.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SamcoBhavDownloader:
    """Automated BHAV copy downloader for Samco with session-based authentication."""
    
    def __init__(self, session_cookie: str, output_dir: str = "samco_bhav_data"):
        """
        Initialize the downloader.
        
        Args:
            session_cookie: The ci_session cookie value from Samco
            output_dir: Directory to save downloaded files
        """
        self.session_cookie = session_cookie
        self.output_dir = Path(output_dir)
        
        # Create directory structure
        self.output_dir.mkdir(exist_ok=True)
        self.raw_dir = self.output_dir / "raw"
        self.processed_dir = self.output_dir / "processed"
        self.zip_dir = self.output_dir / "zips"
        
        self.raw_dir.mkdir(exist_ok=True)
        self.processed_dir.mkdir(exist_ok=True)
        self.zip_dir.mkdir(exist_ok=True)
        
        # API Configuration
        self.base_url = "https://www.samco.in/bse_nse_mcx/getBhavcopy"
        self.page_url = "https://www.samco.in/bhavcopy-nse-bse-mcx"
        
        # Request headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www.samco.in',
            'Referer': 'https://www.samco.in/bhavcopy-nse-bse-mcx',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'Cookie': f'ci_session={self.session_cookie}'
        }
        
        # Rate limiting and retry configuration
        self.delay_between_requests = 5  # 5 seconds between requests
        self.max_retries = 3
        self.retry_delay = 10  # 10 seconds on retry
        
        # Statistics
        self.stats = {
            'successful': 0,
            'failed': 0,
            'total_records': 0,
            'files_downloaded': []
        }
        
        logger.info(f"Initialized Samco BHAV Downloader")
        logger.info(f"Output directory: {self.output_dir.absolute()}")
        logger.info(f"Rate limit: {self.delay_between_requests}s between requests")
    
    def generate_15day_chunks(self, start_date: str, end_date: str) -> List[Tuple[datetime, datetime]]:
        """
        Generate 15-day date chunks (1st-15th, 16th-end of month pattern).
        Handles different month lengths (30, 31 days) and leap years (Feb 28/29).
        
        Args:
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            
        Returns:
            List of (start, end) datetime tuples for each 15-day chunk
        """
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        chunks = []
        current = start
        
        while current <= end:
            # Determine if we're in first half (1-15) or second half (16-end) of month
            if current.day <= 15:
                # First half: day 1 to day 15
                chunk_end = current.replace(day=15)
            else:
                # Second half: day 16 to last day of month
                # Calculate last day of current month
                if current.month == 12:
                    next_month = current.replace(year=current.year + 1, month=1, day=1)
                else:
                    next_month = current.replace(month=current.month + 1, day=1)
                
                last_day_of_month = next_month - timedelta(days=1)
                chunk_end = last_day_of_month
            
            # Don't exceed the overall end date
            chunk_end = min(chunk_end, end)
            
            chunks.append((current, chunk_end))
            
            # Move to next chunk (next day after chunk_end)
            current = chunk_end + timedelta(days=1)
        
        logger.info(f"Generated {len(chunks)} 15-day chunks from {start_date} to {end_date}")
        logger.info(f"  Pattern: 1st-15th, 16th-end of month (handles 28/29/30/31 day months)")
        return chunks
    
    def generate_monthly_chunks(self, start_date: str, end_date: str) -> List[Tuple[datetime, datetime]]:
        """
        Generate monthly date chunks (kept for backwards compatibility).
        
        Args:
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            
        Returns:
            List of (start, end) datetime tuples for each month
        """
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        chunks = []
        current = start
        
        while current < end:
            # Calculate month end (last day of current month or end_date, whichever is earlier)
            next_month = current + relativedelta(months=1)
            month_end = min(next_month - relativedelta(days=1), end)
            
            chunks.append((current, month_end))
            current = next_month
        
        logger.info(f"Generated {len(chunks)} monthly chunks from {start_date} to {end_date}")
        return chunks
    
    def download_bhav_zip(self, start_date: datetime, end_date: datetime) -> Optional[Path]:
        """
        Download BHAV copy ZIP file for a date range.
        
        Args:
            start_date: Start datetime
            end_date: End datetime
            
        Returns:
            Path to downloaded ZIP file or None if failed
        """
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        logger.info(f"Downloading BHAV copy: {start_str} to {end_str}")
        
        # Prepare form data (IMPORTANT: bhavcopy_data[] has square brackets!)
        form_data = {
            'start_date': start_str,
            'end_date': end_str,
            'bhavcopy_data[]': 'NSE',  # NSE exchange (field name has [])
            'show_or_down': '2'  # 2 = download (1 = preview)
        }
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"  Attempt {attempt + 1}/{self.max_retries}")
                
                # Make POST request
                response = requests.post(
                    self.base_url,
                    data=form_data,
                    headers=self.headers,
                    timeout=30,
                    allow_redirects=True
                )
                
                logger.info(f"  Status: {response.status_code}, Content-Type: {response.headers.get('Content-Type', 'unknown')}")
                
                if response.status_code == 200:
                    # Check if we got a ZIP file
                    content_type = response.headers.get('Content-Type', '')
                    content_disposition = response.headers.get('Content-Disposition', '')
                    
                    if 'application/octet-stream' in content_type or 'attachment' in content_disposition:
                        # Save ZIP file
                        zip_filename = f"bhav_{start_str}_to_{end_str}.zip"
                        zip_path = self.zip_dir / zip_filename
                        
                        with open(zip_path, 'wb') as f:
                            f.write(response.content)
                        
                        file_size = len(response.content) / (1024 * 1024)  # Size in MB
                        logger.info(f"  ✓ Downloaded: {zip_filename} ({file_size:.2f} MB)")
                        
                        self.stats['files_downloaded'].append(zip_path)
                        return zip_path
                    else:
                        logger.warning(f"  Unexpected content type: {content_type}")
                        logger.warning(f"  Response size: {len(response.content)} bytes")
                        
                        # Save for debugging
                        debug_file = self.zip_dir / f"debug_{start_str}_{end_str}.html"
                        with open(debug_file, 'wb') as f:
                            f.write(response.content)
                        logger.warning(f"  Saved response to: {debug_file}")
                        
                elif response.status_code == 302 or response.status_code == 301:
                    logger.warning(f"  Redirect detected. Cookie might be expired.")
                    
                else:
                    logger.error(f"  HTTP {response.status_code}")
                
                # Retry delay
                if attempt < self.max_retries - 1:
                    logger.info(f"  Retrying in {self.retry_delay}s...")
                    time.sleep(self.retry_delay)
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"  Request error: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"  Unexpected error: {str(e)}")
                break
        
        logger.error(f"  ✗ Failed to download: {start_str} to {end_str}")
        return None
    
    def extract_and_process_zip(self, zip_path: Path) -> Optional[pd.DataFrame]:
        """
        Extract ZIP file and process the data.
        
        Args:
            zip_path: Path to ZIP file
            
        Returns:
            DataFrame with processed data or None if failed
        """
        try:
            logger.info(f"Extracting: {zip_path.name}")
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # List files in ZIP
                file_list = zip_ref.namelist()
                logger.info(f"  Files in ZIP: {file_list}")
                
                # Extract all files
                extract_dir = self.raw_dir / zip_path.stem
                extract_dir.mkdir(exist_ok=True)
                zip_ref.extractall(extract_dir)
                
                # Process each file
                dataframes = []
                for file_name in file_list:
                    file_path = extract_dir / file_name
                    
                    if file_path.suffix.lower() in ['.csv', '.txt']:
                        df = pd.read_csv(file_path)
                        dataframes.append(df)
                        logger.info(f"  ✓ Loaded CSV: {file_name} ({len(df)} records)")
                    elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                        df = pd.read_excel(file_path, engine='openpyxl')
                        dataframes.append(df)
                        logger.info(f"  ✓ Loaded Excel: {file_name} ({len(df)} records)")
                
                if dataframes:
                    # Combine all dataframes
                    combined_df = pd.concat(dataframes, ignore_index=True)
                    logger.info(f"  ✓ Total records: {len(combined_df)}")
                    
                    self.stats['total_records'] += len(combined_df)
                    return combined_df
                else:
                    logger.warning(f"  No processable files found in ZIP")
                    return None
                    
        except zipfile.BadZipFile:
            logger.error(f"  ✗ Invalid ZIP file: {zip_path}")
            return None
        except Exception as e:
            logger.error(f"  ✗ Error processing ZIP: {str(e)}")
            return None
    
    def download_all(self, start_date: str = "2016-04-01", end_date: str = "2025-09-28"):
        """
        Download all BHAV copy data for the date range.
        
        Args:
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
        """
        logger.info("="*80)
        logger.info("SAMCO BHAV COPY AUTOMATED DOWNLOADER (15-DAY INTERVALS)")
        logger.info("="*80)
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Exchange: NSE (Cash Market)")
        logger.info(f"Chunk Pattern: 1st-15th, 16th-end of month")
        logger.info(f"Rate Limit: {self.delay_between_requests} seconds between requests")
        logger.info("="*80)
        
        # Generate 15-day chunks (1st-15th, 16th-end pattern)
        chunks = self.generate_15day_chunks(start_date, end_date)
        
        all_dataframes = []
        
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            logger.info(f"\n[Chunk {i}/{len(chunks)}] Processing...")
            logger.info(f"  Date range: {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
            logger.info(f"  Days: {(chunk_end - chunk_start).days + 1}")
            
            # Download ZIP
            zip_path = self.download_bhav_zip(chunk_start, chunk_end)
            
            if zip_path:
                # Extract and process
                df = self.extract_and_process_zip(zip_path)
                
                if df is not None and not df.empty:
                    all_dataframes.append(df)
                    self.stats['successful'] += 1
                else:
                    self.stats['failed'] += 1
            else:
                self.stats['failed'] += 1
            
            # Rate limiting delay (except for last request)
            if i < len(chunks):
                logger.info(f"  Waiting {self.delay_between_requests}s before next request...")
                time.sleep(self.delay_between_requests)
        
        # Combine and save all data
        if all_dataframes:
            logger.info("\n" + "="*80)
            logger.info("Combining all downloaded data...")
            
            final_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Remove duplicates if applicable
            if 'TIMESTAMP' in final_df.columns and 'SYMBOL' in final_df.columns:
                before = len(final_df)
                final_df = final_df.drop_duplicates(subset=['TIMESTAMP', 'SYMBOL'])
                after = len(final_df)
                if before > after:
                    logger.info(f"Removed {before - after} duplicate records")
            
            # Sort by date
            date_cols = [col for col in final_df.columns if 'DATE' in col.upper() or 'TIME' in col.upper()]
            if date_cols:
                final_df = final_df.sort_values(date_cols[0])
            
            # Save combined file
            output_file = self.processed_dir / f"bhav_combined_{start_date}_to_{end_date}.xlsx"
            final_df.to_excel(output_file, index=False)
            
            logger.info(f"✓ Combined file saved: {output_file}")
            logger.info(f"  Total records: {len(final_df):,}")
            
            if 'SYMBOL' in final_df.columns:
                logger.info(f"  Unique symbols: {final_df['SYMBOL'].nunique():,}")
            
            logger.info(f"  Columns: {', '.join(final_df.columns.tolist())}")
        
        # Print final statistics
        logger.info("\n" + "="*80)
        logger.info("DOWNLOAD COMPLETE - SUMMARY")
        logger.info("="*80)
        logger.info(f"Successful downloads: {self.stats['successful']}/{len(chunks)}")
        logger.info(f"Failed downloads: {self.stats['failed']}/{len(chunks)}")
        logger.info(f"Total records processed: {self.stats['total_records']:,}")
        logger.info(f"Files downloaded: {len(self.stats['files_downloaded'])}")
        logger.info(f"Chunk pattern: 15-day intervals (1st-15th, 16th-end of month)")
        logger.info("="*80)


def main():
    """Main execution function."""
    
    print("="*80)
    print("SAMCO BHAV COPY DOWNLOADER")
    print("="*80)
    
    # Get session cookie from user
    session_cookie = input("\nEnter your Samco session cookie (ci_session): ").strip()
    
    if not session_cookie:
        print("\n✗ ERROR: No cookie provided!")
        print("\nPlease run: python get_samco_cookie.py")
        print("Copy the cookie and paste it here when prompted.")
        return
    
    print(f"\n✓ Cookie received (length: {len(session_cookie)})")
    
    # Get date range (optional)
    print("\nDate Range Configuration:")
    print("Press Enter to use defaults or provide custom dates")
    
    start_date = input("Start date [2016-04-01]: ").strip() or "2016-04-01"
    end_date = input("End date [2025-09-28]: ").strip() or "2025-09-28"
    
    # Initialize downloader
    downloader = SamcoBhavDownloader(
        session_cookie=session_cookie,
        output_dir="samco_bhav_data"
    )
    
    # Start download
    print("\n" + "="*80)
    print("Starting download process...")
    print("This will take some time due to rate limiting (5s between requests)")
    print("="*80 + "\n")
    
    downloader.download_all(start_date=start_date, end_date=end_date)
    
    print("\n✓ Process completed!")
    print(f"Check the 'samco_bhav_data' folder for downloaded files")


if __name__ == "__main__":
    main()

