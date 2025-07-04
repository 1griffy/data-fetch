import pandas as pd
import os
import glob
from datetime import datetime, timedelta

def check_missing_minutes():
    """
    Read all CSV files from the data folder and check for missing minutes in 1-minute candle data.
    Returns a dictionary with file names and their missing time intervals information.
    """
    data_folder = "data"
    csv_files = glob.glob(os.path.join(data_folder, "*.csv"))
    
    missing_minutes_report = {}
    
    for csv_file in csv_files:
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Get file info
            file_name = os.path.basename(csv_file)
            total_rows = len(df)
            
            # Check if we have the required columns
            if 'OpenTime' not in df.columns:
                print(f"  ERROR: OpenTime column not found")
                missing_minutes_report[file_name] = {
                    'error': 'OpenTime column not found',
                    'total_rows': total_rows
                }
                print("-" * 50)
                continue
            
            # Convert OpenTime to datetime for easier analysis
            df['OpenTime_dt'] = pd.to_datetime(df['OpenTime'], unit='ms')
            
            # Sort by time to ensure chronological order
            df = df.sort_values('OpenTime_dt').reset_index(drop=True)
            
            # Check for missing minutes
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
            
            # Calculate statistics
            total_missing_minutes = sum(interval['missing_minutes'] for interval in missing_intervals)
            expected_total_minutes = (df['OpenTime_dt'].max() - df['OpenTime_dt'].min()).total_seconds() / 60
            data_coverage = ((expected_total_minutes - total_missing_minutes) / expected_total_minutes * 100) if expected_total_minutes > 0 else 0
            
            missing_minutes_report[file_name] = {
                'total_rows': total_rows,
                'start_time': df['OpenTime_dt'].min(),
                'end_time': df['OpenTime_dt'].max(),
                'expected_total_minutes': int(expected_total_minutes),
                'total_missing_minutes': total_missing_minutes,
                'data_coverage_percent': round(data_coverage, 2),
                'missing_intervals': missing_intervals,
                'has_missing_minutes': len(missing_intervals) > 0
            }
            
            print(f"  Time range: {df['OpenTime_dt'].min()} to {df['OpenTime_dt'].max()}")
            print(f"  Expected total minutes: {int(expected_total_minutes)}")
            print(f"  Total missing minutes: {total_missing_minutes}")
            print(f"  Data coverage: {round(data_coverage, 2)}%")
            
            if missing_intervals:
                print(f"  Missing intervals found: {len(missing_intervals)}")
                print("  Missing time periods:")
                for i, interval in enumerate(missing_intervals[:5]):  # Show first 5 missing intervals
                    print(f"    {i+1}. {interval['missing_from']} to {interval['missing_to']} ({interval['missing_minutes']} minutes)")
                if len(missing_intervals) > 5:
                    print(f"    ... and {len(missing_intervals) - 5} more intervals")
            else:
                print("  No missing minutes found - data is complete!")
            
            print("-" * 50)
            
        except Exception as e:
            print(f"Error reading {csv_file}: {str(e)}")
            missing_minutes_report[os.path.basename(csv_file)] = {
                'error': str(e)
            }
            print("-" * 50)

    return missing_minutes_report

if __name__ == "__main__":
    print("Scanning CSV files for missing minutes in 1-minute candle data...")
    print("=" * 70)
    report = check_missing_minutes()
    
    # Summary
    print("\nSUMMARY:")
    print("=" * 70)
    files_with_missing = sum(1 for info in report.values() if isinstance(info, dict) and info.get('has_missing_minutes', False))
    total_files = len(report)
    files_with_errors = sum(1 for info in report.values() if isinstance(info, dict) and 'error' in info)
    
    print(f"Total files scanned: {total_files}")
    print(f"Files with missing minutes: {files_with_missing}")
    print(f"Files with complete data: {total_files - files_with_missing - files_with_errors}")
    print(f"Files with errors: {files_with_errors}")
    
    # Show files with worst coverage
    print("\nFiles with missing data (sorted by missing minutes):")
    print("-" * 70)
    
    files_with_missing_data = []
    for file_name, info in report.items():
        if isinstance(info, dict) and 'total_missing_minutes' in info:
            files_with_missing_data.append((file_name, info['total_missing_minutes'], info['data_coverage_percent']))
    
    # Sort by missing minutes (descending)
    files_with_missing_data.sort(key=lambda x: x[1], reverse=True)
    
    for file_name, missing_minutes, coverage in files_with_missing_data[:10]:  # Top 10
        print(f"{file_name}: {missing_minutes} missing minutes ({coverage}% coverage)")