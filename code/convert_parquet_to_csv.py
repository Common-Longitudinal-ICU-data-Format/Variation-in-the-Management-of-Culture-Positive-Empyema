#!/usr/bin/env python3
"""
Convert all parquet files in PHI_DATA directory to CSV format.

This script finds all .parquet files in the PHI_DATA directory and converts
them to .csv files in the same location.

Usage:
    python code/convert_parquet_to_csv.py
"""

import pandas as pd
from pathlib import Path


def convert_parquet_to_csv(phi_data_dir='PHI_DATA'):
    """
    Convert all parquet files in PHI_DATA directory to CSV.

    Args:
        phi_data_dir (str): Path to PHI_DATA directory (default: 'PHI_DATA')
    """
    print("=" * 80)
    print("PHI_DATA Parquet to CSV Converter")
    print("=" * 80)

    # Get PHI_DATA directory
    data_dir = Path(phi_data_dir)

    if not data_dir.exists():
        print(f"\n❌ Error: Directory '{phi_data_dir}' does not exist!")
        return

    # Find all parquet files
    parquet_files = list(data_dir.glob('*.parquet'))

    if not parquet_files:
        print(f"\n⚠ No parquet files found in '{phi_data_dir}' directory.")
        return

    print(f"\n✓ Found {len(parquet_files)} parquet file(s) in '{phi_data_dir}/'")
    print()

    # Convert each parquet file to CSV
    converted_count = 0
    failed_files = []

    for parquet_file in parquet_files:
        try:
            # Read parquet file
            print(f"Reading: {parquet_file.name}")
            df = pd.read_parquet(parquet_file)

            # Generate CSV filename
            csv_file = parquet_file.with_suffix('.csv')

            # Save as CSV
            df.to_csv(csv_file, index=False)

            # Print summary
            file_size_mb = csv_file.stat().st_size / (1024 ** 2)
            print(f"  ✓ Saved: {csv_file.name}")
            print(f"    Rows: {len(df):,}")
            print(f"    Columns: {len(df.columns)}")
            print(f"    Size: {file_size_mb:.2f} MB")
            print()

            converted_count += 1

        except Exception as e:
            print(f"  ❌ Error converting {parquet_file.name}: {str(e)}")
            print()
            failed_files.append(parquet_file.name)

    # Print final summary
    print("=" * 80)
    print("Conversion Summary")
    print("=" * 80)
    print(f"✓ Successfully converted: {converted_count} file(s)")

    if failed_files:
        print(f"❌ Failed conversions: {len(failed_files)} file(s)")
        for failed_file in failed_files:
            print(f"  - {failed_file}")

    print(f"\n📁 CSV files saved to: {data_dir.absolute()}/")
    print()


if __name__ == "__main__":
    convert_parquet_to_csv()
