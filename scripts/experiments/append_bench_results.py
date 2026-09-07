from pathlib import Path
import pandas as pd


def print_processed_logs(root_dir: str):
    """
    Recursively search root_dir for 'processed_logs.parquet',
    load each one, and print its contents.
    """
    root = Path(root_dir)

    if not root.exists():
        raise FileNotFoundError(f"Directory does not exist: {root_dir}")
    if not root.is_dir():
        raise NotADirectoryError(f"Not a directory: {root_dir}")

    found = False

    for parquet_file in root.rglob("processed_logs.parquet"):
        found = True
        print(f"\n=== {parquet_file} ===")
        df = pd.read_parquet(parquet_file)
        print(df.to_string(index=False))

    if not found:
        print(f"No 'processed_logs.parquet' files found under: {root_dir}")


def main(args):
    print_processed_logs(args.directory)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("directory", help="Root directory to search")
    args = parser.parse_args()
    main(args)
