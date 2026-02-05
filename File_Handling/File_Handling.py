#File Handling (CSV, JSON, Parquet)
# ============================================================
# 1. Read a CSV File and Display First N Records
# ============================================================

# This function reads a CSV file and returns the first N records.
# Useful for validating incoming data during ingestion.

import csv

def read_csv_head(file_path, n=5):
    records = []
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader):
            if i >= n:
                break
            records.append(row)
    return records

# Example usage:
# print(read_csv_head("customers.csv", 5))


# ============================================================
# 2. Count Total Records in a CSV File
# ============================================================

# Counts the total number of records in a CSV file excluding header.
# Commonly used to verify ingestion completeness.

def count_csv_records(file_path):
    with open(file_path, "r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header
        return sum(1 for _ in reader)


# ============================================================
# 3. Write Data to a CSV File
# ============================================================

# Writes a list of dictionaries to a CSV file.
# Used after transformations to persist processed data.

def write_csv(file_path, data, fieldnames):
    with open(file_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


# ============================================================
# 4. Read a Large CSV File Line by Line
# ============================================================

# Reads a large CSV file in a memory-efficient way.
# Prevents loading the entire file into memory.

def read_large_csv(file_path):
    with open(file_path, "r") as file:
        for line in file:
            yield line.strip()


# ============================================================
# 5. Merge Multiple CSV Files with Same Schema
# ============================================================

# Merges multiple CSV files into a single output file.
# Common requirement when processing partitioned data.

def merge_csv_files(input_files, output_file, fieldnames):
    with open(output_file, "w", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()

        for file_path in input_files:
            with open(file_path, "r") as f:
                reader = csv.DictReader(f)
                writer.writerows(reader)


# ============================================================
# 6. Read a JSON File into Python Object
# ============================================================

# Reads a JSON file and converts it into a Python dictionary or list.
# Useful for API responses or semi-structured data.

import json

def read_json(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


# ============================================================
# 7. Write Python Object to JSON File
# ============================================================

# Writes a Python dictionary or list into a JSON file.
# Commonly used after data transformations.

def write_json(file_path, data):
    with open(file_path, "w") as file:
        json.dump(data, file, indent=4)


# ============================================================
# 8. Flatten a Nested JSON Object
# ============================================================

# Flattens a nested JSON structure into a single-level dictionary.
# Useful before loading JSON data into tabular systems.

def flatten_json(data):
    flat = {}
    for key, value in data.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat[f"{key}_{sub_key}"] = sub_value
        else:
            flat[key] = value
    return flat


# ============================================================
# 9. Convert CSV File to JSON File
# ============================================================

# Converts CSV data into JSON format.
# Useful when moving data between systems.

def csv_to_json(csv_path, json_path):
    with open(csv_path, "r") as csv_file:
        reader = csv.DictReader(csv_file)
        data = list(reader)

    write_json(json_path, data)


# ============================================================
# 10. Validate File Exists Before Processing
# ============================================================

# Checks if a file exists before attempting to read it.
# Prevents pipeline failures.

import os

def validate_file_exists(file_path):
    return os.path.exists(file_path)


# ============================================================
# 11. Read a Compressed GZIP File
# ============================================================

# Reads a .gz compressed file.
# Common when handling archived logs or datasets.

import gzip

def read_gzip_file(file_path):
    with gzip.open(file_path, "rt") as file:
        return file.read()


# ============================================================
# 12. Detect File Type Based on Extension
# ============================================================

# Identifies file type using file extension.
# Useful for dynamic file processing pipelines.

def detect_file_type(file_path):
    return file_path.split(".")[-1]


# ============================================================
# 13. Read Parquet File Using Pandas
# ============================================================

# Reads a Parquet file into a Pandas DataFrame.
# Parquet is widely used in big data ecosystems.

import pandas as pd

def read_parquet(file_path):
    return pd.read_parquet(file_path)


# ============================================================
# 14. Write DataFrame to Parquet File
# ============================================================

# Writes a Pandas DataFrame to Parquet format.
# Efficient for analytical workloads.

def write_parquet(file_path, dataframe):
    dataframe.to_parquet(file_path, index=False)


# ============================================================
# 15. Count Records in Multiple Files
# ============================================================

# Counts total records across multiple CSV files.
# Useful when data is split across partitions.

def count_records_multiple_files(file_paths):
    total = 0
    for path in file_paths:
        total += count_csv_records(path)
    return total
