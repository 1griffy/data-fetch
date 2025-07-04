import pandas as pd
import requests
import time
import os
import glob
from datetime import datetime, timedelta
import json
from typing import List, Dict, Tuple
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import concurrent.futures

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BinanceDataFiller:
    def __init__(self, base_url: str = "https://fapi.binance.com", max_workers: int = 4):
        self.base_url = base_url
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        # Thread-local storage for sessions
        self._local = threading.local()
        
    def get_session(self):
        """Get thread-local session"""
        if not hasattr(self._local, 'session'):
            self._local.session = requests.Session()
            self._local.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
        return self._local.session
        
    def get_klines(self, symbol: str, start_time: int, end_time: int, interval: str = "1m") -> List[List]:
        """
        Fetch kline data from Binance API
        """
        url = f"{self.base_url}/fapi/v1/klines"
        params = {
            'symbol': symbol.upper(),
            'interval': interval,
            'startTime': start_time,
            'endTime': end_time,
            'limit': 1000
        }
        
        try:
            session = self.get_session()
            response = session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching klines for {symbol}: {e}")
            return []
    
    def get_funding_rates(self, symbol: str, start_time: int, end_time: int) -> List[Dict]:
        """
        Fetch funding rates from Binance API
        """
        url = f"{self.base_url}/fapi/v1/fundingRate"
        params = {
            'symbol': symbol.upper(),
            'startTime': start_time,
            'endTime': end_time,
            'limit': 1000
        }
        
        try:
            session = self.get_session()
            response = session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching funding rates for {symbol}: {e}")
            return []
    
    def find_missing_intervals(self, df: pd.DataFrame) -> List[Dict]:
        """
        Find missing 1-minute intervals in the data
        """
        if 'OpenTime' not in df.columns:
            logger.error("OpenTime column not found in dataframe")
            return []
        
        # Convert OpenTime to datetime
        df['OpenTime_dt'] = pd.to_datetime(df['OpenTime'], unit='ms')
        df = df.sort_values('OpenTime_dt').reset_index(drop=True)
        
        missing_intervals = []
        expected_interval = timedelta(minutes=1)
        
        for i in range(1, len(df)):
            current_time = df.iloc[i]['OpenTime_dt']
            previous_time = df.iloc[i-1]['OpenTime_dt']
            
            time_diff = current_time - previous_time
            
            # If the difference is more than 1 minute, we have missing data
            if time_diff > expected_interval:
                missing_intervals.append({
                    'missing_from': previous_time,
                    'missing_to': current_time,
                    'missing_minutes': int(time_diff.total_seconds() / 60),
                    'gap_duration': time_diff
                })
        
        return missing_intervals
    
    def fetch_interval_data(self, symbol: str, interval: Dict) -> List[Dict]:
        """
        Fetch data for a single interval (thread-safe)
        """
        start_time = interval['missing_from']
        end_time = interval['missing_to']
        
        # Convert to milliseconds
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        
        logger.info(f"Fetching missing data for {symbol} from {start_time} to {end_time}")
        
        # Fetch klines
        klines = self.get_klines(symbol, start_ms, end_ms)
        
        # Fetch funding rates for this period
        funding_rates = self.get_funding_rates(symbol, start_ms, end_ms)
        
        # Create a mapping of funding rates by time
        funding_map = {}
        for fr in funding_rates:
            funding_time = int(fr['fundingTime'])
            funding_map[funding_time] = {
                'FundingRate': float(fr['fundingRate']),
                'FundingTime': funding_time,
                'FundingTimeFormatted': pd.to_datetime(funding_time, unit='ms')
            }
        
        # Process klines and add funding rates
        interval_data = []
        for kline in klines:
            open_time = int(kline[0])
            close_time = int(kline[6])
            
            # Find applicable funding rate
            applicable_funding = None
            for funding_time, funding_data in funding_map.items():
                if funding_time <= close_time:
                    applicable_funding = funding_data
                else:
                    break
            
            row_data = {
                'Symbol': symbol,
                'OpenTime': open_time,
                'OpenTimeFormatted': pd.to_datetime(open_time, unit='ms'),
                'OpenPrice': float(kline[1]),
                'HighPrice': float(kline[2]),
                'LowPrice': float(kline[3]),
                'ClosePrice': float(kline[4]),
                'Volume': float(kline[5]),
                'CloseTime': close_time,
                'CloseTimeFormatted': pd.to_datetime(close_time, unit='ms'),
                'QuoteAssetVolume': float(kline[7]),
                'NumberOfTrades': int(kline[8]),
                'TakerBuyBase': float(kline[9]),
                'TakerBuyQuote': float(kline[10])
            }
            
            if applicable_funding:
                row_data.update(applicable_funding)
            else:
                row_data.update({
                    'FundingRate': None,
                    'FundingTime': None,
                    'FundingTimeFormatted': None
                })
            
            interval_data.append(row_data)
        
        return interval_data
    
    def fetch_missing_data(self, symbol: str, missing_intervals: List[Dict]) -> List[Dict]:
        """
        Fetch missing data for given intervals using multithreading
        """
        all_missing_data = []
        
        # Use ThreadPoolExecutor to fetch intervals concurrently
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(missing_intervals))) as executor:
            # Submit all interval fetching tasks
            future_to_interval = {
                executor.submit(self.fetch_interval_data, symbol, interval): interval 
                for interval in missing_intervals
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_interval):
                interval = future_to_interval[future]
                try:
                    interval_data = future.result()
                    all_missing_data.extend(interval_data)
                    logger.info(f"Completed fetching interval: {interval['missing_from']} to {interval['missing_to']}")
                except Exception as e:
                    logger.error(f"Error fetching interval {interval['missing_from']} to {interval['missing_to']}: {e}")
                
                # Rate limiting - be respectful to Binance API
                time.sleep(0.05)  # Reduced sleep time due to concurrent processing
        
        return all_missing_data
    
    def merge_and_save_data(self, original_file: str, missing_data: List[Dict], output_file: str = None):
        """
        Merge missing data with original data and save to new file
        """
        if not missing_data:
            logger.info("No missing data to merge")
            return
        
        # Read original data
        original_df = pd.read_csv(original_file)
        
        # Create dataframe from missing data
        missing_df = pd.DataFrame(missing_data)
        
        # Combine dataframes
        combined_df = pd.concat([original_df, missing_df], ignore_index=True)
        
        # Sort by OpenTime
        combined_df = combined_df.sort_values('OpenTime').reset_index(drop=True)
        
        # Remove duplicates based on OpenTime
        combined_df = combined_df.drop_duplicates(subset=['OpenTime'], keep='first')
        
        # Determine output filename
        if output_file is None:
            base_name = os.path.splitext(original_file)[0]
            output_file = f"{base_name}.csv"
        
        # Save to file
        combined_df.to_csv(output_file, index=False)
        logger.info(f"Saved filled data to {output_file}")
        
        return output_file

def process_single_file(args):
    """
    Process a single file (for multithreading)
    """
    csv_file, filler, create_backup = args
    
    try:
        file_name = os.path.basename(csv_file)
        logger.info(f"Processing file: {file_name}")
        
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Extract symbol from filename
        symbol = file_name.split('_')[0]
        
        # Find missing intervals
        missing_intervals = filler.find_missing_intervals(df)
        
        if not missing_intervals:
            logger.info(f"No missing data found in {file_name}")
            return {
                'file': file_name,
                'symbol': symbol,
                'missing_intervals': 0,
                'missing_minutes': 0,
                'added_records': 0,
                'status': 'no_missing_data'
            }
        
        missing_minutes = sum(interval['missing_minutes'] for interval in missing_intervals)
        logger.info(f"Found {len(missing_intervals)} missing intervals ({missing_minutes} minutes) in {file_name}")
        
        # Create backup if requested
        if create_backup:
            backup_file = csv_file.replace('.csv', '_backup.csv')
            df.to_csv(backup_file, index=False)
            logger.info(f"Created backup: {backup_file}")
        
        # Fetch missing data
        missing_data = filler.fetch_missing_data(symbol, missing_intervals)
        
        if missing_data:
            logger.info(f"Fetched {len(missing_data)} missing records for {symbol}")
            
            # Merge and save
            output_file = filler.merge_and_save_data(csv_file, missing_data)
            
            # Replace original file with filled version
            if output_file and output_file != csv_file:
                os.replace(output_file, csv_file)
                logger.info(f"Updated original file: {csv_file}")
            
            return {
                'file': file_name,
                'symbol': symbol,
                'missing_intervals': len(missing_intervals),
                'missing_minutes': missing_minutes,
                'added_records': len(missing_data),
                'status': 'success'
            }
        else:
            logger.warning(f"No missing data could be fetched for {symbol}")
            return {
                'file': file_name,
                'symbol': symbol,
                'missing_intervals': len(missing_intervals),
                'missing_minutes': missing_minutes,
                'added_records': 0,
                'status': 'no_data_fetched'
            }
            
    except Exception as e:
        logger.error(f"Error processing {csv_file}: {e}")
        return {
            'file': os.path.basename(csv_file),
            'symbol': 'unknown',
            'missing_intervals': 0,
            'missing_minutes': 0,
            'added_records': 0,
            'status': 'error',
            'error': str(e)
        }

def process_all_files(data_folder: str = "data", create_backup: bool = True, max_workers: int = 4):
    """
    Process all CSV files in the data folder to fill missing data using multithreading
    """
    filler = BinanceDataFiller(max_workers=max_workers)
    csv_files = glob.glob(os.path.join(data_folder, "*.csv"))
    
    total_files = len(csv_files)
    logger.info(f"Processing {total_files} CSV files with {max_workers} workers...")
    
    # Prepare arguments for each file
    file_args = [(csv_file, filler, create_backup) for csv_file in csv_files]
    
    # Process files concurrently
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all file processing tasks
        future_to_file = {executor.submit(process_single_file, args): args[0] for args in file_args}
        
        # Collect results as they complete
        for future in as_completed(future_to_file):
            csv_file = future_to_file[future]
            try:
                result = future.result()
                results.append(result)
                logger.info(f"Completed processing: {result['file']} - {result['status']}")
            except Exception as e:
                logger.error(f"Error processing {csv_file}: {e}")
                results.append({
                    'file': os.path.basename(csv_file),
                    'symbol': 'unknown',
                    'missing_intervals': 0,
                    'missing_minutes': 0,
                    'added_records': 0,
                    'status': 'error',
                    'error': str(e)
                })
    
    # Summary
    logger.info(f"Processing completed!")
    
    files_with_missing = sum(1 for r in results if r['missing_intervals'] > 0)
    total_missing_minutes = sum(r['missing_minutes'] for r in results)
    total_added_records = sum(r['added_records'] for r in results)
    successful_files = sum(1 for r in results if r['status'] == 'success')
    error_files = sum(1 for r in results if r['status'] == 'error')
    
    logger.info(f"Summary:")
    logger.info(f"  Total files processed: {total_files}")
    logger.info(f"  Files with missing data: {files_with_missing}")
    logger.info(f"  Successfully filled: {successful_files}")
    logger.info(f"  Files with errors: {error_files}")
    logger.info(f"  Total missing minutes filled: {total_missing_minutes}")
    logger.info(f"  Total records added: {total_added_records}")
    
    # Show top files with most missing data
    files_with_data = [r for r in results if r['missing_minutes'] > 0]
    if files_with_data:
        files_with_data.sort(key=lambda x: x['missing_minutes'], reverse=True)
        logger.info(f"\nTop 10 files with most missing data:")
        for i, result in enumerate(files_with_data[:10], 1):
            logger.info(f"  {i}. {result['file']}: {result['missing_minutes']} minutes ({result['added_records']} records added)")

def main():
    """
    Main function to run the missing data filler
    """
    print("Binance Missing Data Filler (Multithreaded)")
    print("=" * 50)
    
    # Check if data folder exists
    if not os.path.exists("data"):
        print("Error: 'data' folder not found!")
        return
    
    # Get number of workers
    try:
        max_workers = int(input("Enter number of concurrent workers (default 4): ") or "4")
        max_workers = max(1, min(max_workers, 10))  # Limit between 1 and 10
    except ValueError:
        max_workers = 4
    
    # Ask user for confirmation
    print("This script will:")
    print("1. Scan all CSV files in the 'data' folder")
    print("2. Identify missing 1-minute intervals")
    print("3. Fetch missing data from Binance API (using multithreading)")
    print("4. Merge missing data with existing data")
    print("5. Create backups of original files")
    print(f"6. Use {max_workers} concurrent workers")
    print()
    
    response = input("Do you want to continue? (y/N): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Process all files
    process_all_files(max_workers=max_workers)

if __name__ == "__main__":
    main() 