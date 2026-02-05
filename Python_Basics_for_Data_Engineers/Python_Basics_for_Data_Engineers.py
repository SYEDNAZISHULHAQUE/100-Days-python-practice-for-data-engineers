#Python Basics for Data Engineers 
# ============================================================
# 1. Read a Text File and Count the Number of Lines
# ============================================================

# This function reads a text file and returns the total number of lines.
# Useful when analyzing log files or raw ingestion data.

def count_lines(file_path):
    # Open the file in read mode
    with open(file_path, "r") as file:
        # Read all lines into a list and return its length
        return len(file.readlines())

# Example usage:
# print(count_lines("logs.txt"))


# ============================================================
# 2. Count Word Frequency in a Text File
# ============================================================

# This function counts how many times each word appears in a text file.
# Commonly used for log analysis and text-based data processing.

from collections import Counter

def top_word_frequency(file_path, top_n=5):
    # Read the entire file
    with open(file_path, "r") as file:
        # Convert text to lowercase and split into words
        words = file.read().lower().split()

    # Count word occurrences and return top N
    return Counter(words).most_common(top_n)

# Example usage:
# print(top_word_frequency("events.txt"))


# ============================================================
# 3. Read a CSV File and Count Total Records
# ============================================================

# This function reads a CSV file and counts the number of data records.
# Helpful for validating ingestion completeness.

import csv

def count_csv_records(file_path):
    with open(file_path, "r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        return sum(1 for _ in reader)

# Example usage:
# print(count_csv_records("customers.csv"))


# ============================================================
# 4. Filter Transactions Based on Amount Threshold
# ============================================================

# Filters transactions where amount exceeds a given threshold.
# Common in financial and transaction-based pipelines.

def filter_transactions(transactions, threshold):
    return [t for t in transactions if t["amount"] > threshold]


# ============================================================
# 5. Replace Missing Values with Zero
# ============================================================

# Replaces None values in a numeric list with 0.
# Used during data cleaning to avoid calculation errors.

def replace_none_with_zero(values):
    return [v if v is not None else 0 for v in values]


# ============================================================
# 6. Remove Duplicate Records Based on Unique ID
# ============================================================

# Removes duplicate records using customer_id as the unique key.
# Very common during deduplication steps in ETL pipelines.

def remove_duplicate_records(records):
    seen_ids = set()
    unique_records = []

    for record in records:
        if record["customer_id"] not in seen_ids:
            seen_ids.add(record["customer_id"])
            unique_records.append(record)

    return unique_records


# ============================================================
# 7. Convert Date Format from YYYY-MM-DD to DD-MM-YYYY
# ============================================================

# Converts date formats for reporting or downstream system compatibility.

from datetime import datetime

def convert_date_format(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")


# ============================================================
# 8. Read Configuration from Environment Variables
# ============================================================

# Reads configuration values from environment variables.
# Useful for environment-based deployments (DEV, QA, PROD).

import os

def get_config_value(key, default=None):
    return os.getenv(key, default)


# ============================================================
# 9. Count Records by Category
# ============================================================

# Aggregates records by category.
# Similar to GROUP BY operations in SQL.

from collections import defaultdict

def count_by_category(records):
    category_count = defaultdict(int)

    for record in records:
        category_count[record["category"]] += 1

    return dict(category_count)


# ============================================================
# 10. Sort Records by Timestamp
# ============================================================

# Sorts records by timestamp field.
# Useful when ordering events or logs.

def sort_records_by_timestamp(records):
    return sorted(records, key=lambda x: x["timestamp"])


# ============================================================
# 11. Safely Read a File with Exception Handling
# ============================================================

# Reads a file safely and handles file-not-found errors.
# Prevents pipeline failure due to missing files.

def safe_file_read(file_path):
    try:
        with open(file_path, "r") as file:
            return file.read()
    except FileNotFoundError:
        return "File not found"


# ============================================================
# 12. Validate Mandatory Fields in Records
# ============================================================

# Ensures all records contain required fields.
# Important for schema validation.

def validate_records(records):
    required_fields = {"id", "name", "created_date"}

    for record in records:
        if not required_fields.issubset(record.keys()):
            return False
    return True


# ============================================================
# 13. Clean a String Column
# ============================================================

# Cleans text data by removing extra spaces, converting to lowercase,
# and removing special characters.

import re

def clean_string(text):
    text = text.strip().lower()
    return re.sub(r"[^a-z0-9 ]", "", text)


# ============================================================
# 14. Generate Summary Statistics for Numeric Data
# ============================================================

# Calculates basic summary statistics.
# Frequently used in exploratory data analysis.

def summary_statistics(numbers):
    return {
        "min": min(numbers),
        "max": max(numbers),
        "sum": sum(numbers),
        "average": sum(numbers) / len(numbers) if numbers else 0
    }


# ============================================================
# 15. Modularize Data Processing Code
# ============================================================

# Demonstrates modular coding style for ETL pipelines.

def read_data(data):
    return data

def process_data(data):
    return [d * 2 for d in data]

def write_data(data):
    print(data)

# Example workflow:
# data = read_data([1, 2, 3])
# processed = process_data(data)
# write_data(processed)
