import os
import sys

import pandas as pd


def load_bulk_data(bulk_data_path: str) -> pd.DataFrame:
    """Load the bulk dataset."""
    try:
        df = pd.read_csv(bulk_data_path)
        print(f"Loaded bulk data with {len(df)} records.")
        return df
    except Exception as e:
        print(f"Error loading bulk data: {e}")
        sys.exit(1)


def get_timestamp_range(df: pd.DataFrame) -> None:
    """Get the timestamp range in epoch and human-readable format."""
    min_timestamp = df["timestamp"].min()
    max_timestamp = df["timestamp"].max()
    min_datetime = pd.to_datetime(min_timestamp, unit="s", utc=True)
    max_datetime = pd.to_datetime(max_timestamp, unit="s", utc=True)
    print("Timestamp Range:")
    print(f"  From: {min_timestamp} ({min_datetime})")
    print(f"  To:   {max_timestamp} ({max_datetime})")


def print_data_schema(df: pd.DataFrame) -> None:
    """Print the data schema."""
    print("\nData Schema:")
    print(df.dtypes)


def check_missing_values(df: pd.DataFrame) -> None:
    """Check and print missing values per column."""
    missing = df.isnull().sum()
    print("\nMissing Values per Column:")
    print(missing)


def check_duplicates(df: pd.DataFrame) -> None:
    """Check and print the number of duplicate timestamps."""
    duplicates = df.duplicated(subset="timestamp").sum()
    if duplicates > 0:
        print(f"\nNumber of Duplicate Timestamps: {duplicates}")
    else:
        print("\nNo duplicate timestamps found.")


def print_descriptive_statistics(df: pd.DataFrame) -> None:
    """Print descriptive statistics of the dataset."""
    print("\nDescriptive Statistics:")
    print(df.describe())


def print_sample_rows(df: pd.DataFrame) -> None:
    """Print first and last few rows of the dataset."""
    print("\nFirst 5 rows:")
    print(df.head())

    print("\nMost recent 5 rows:")
    print(df.tail())


def main() -> None:
    # Define the path to the bulk data
    bulk_data_path = os.path.join(
        "data", "historical", "btcusd_bitstamp_1min_2012-2025.csv"
    )

    # Load the bulk data
    df = load_bulk_data(bulk_data_path)

    # Get and print the timestamp range
    get_timestamp_range(df)

    # Print the data schema
    print_data_schema(df)

    # Check for missing values
    check_missing_values(df)

    # Check for duplicate timestamps
    check_duplicates(df)

    # Print descriptive statistics
    print_descriptive_statistics(df)

    # Print sample rows
    print_sample_rows(df)


if __name__ == "__main__":
    main()
