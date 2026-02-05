# MINI ETL TASKS – 10
# ============================================================

# ============================================================
# 1. Extract Data from CSV and Load into Memory
# ============================================================

# Extracts data from a CSV file into a list of dictionaries.
# Represents the Extract phase of ETL.

import csv

def extract_csv(file_path):
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        return list(reader)


# ============================================================
# 2. Clean Extracted Data by Removing Null Records
# ============================================================

# Cleans extracted data by removing records with null values.
# Part of the Transform phase.

def clean_data(records, field):
    return [r for r in records if r.get(field) not in (None, "")]


# ============================================================
# 3. Transform Data by Adding Derived Column
# ============================================================

# Adds a derived column to the dataset.
# Example: calculating tax amount.

def transform_add_tax(records):
    for record in records:
        record["tax"] = float(record.get("amount", 0)) * 0.1
    return records


# ============================================================
# 4. Aggregate Data for Reporting
# ============================================================

# Aggregates total amount by category.
# Common reporting transformation.

from collections import defaultdict

def aggregate_by_category(records):
    totals = defaultdict(float)

    for record in records:
        totals[record["category"]] += float(record.get("amount", 0))

    return dict(totals)


# ============================================================
# 5. Load Transformed Data into CSV
# ============================================================

# Loads processed data into a CSV file.
# Represents the Load phase.

def load_to_csv(file_path, data, fieldnames):
    with open(file_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


# ============================================================
# 6. End-to-End ETL Pipeline Function
# ============================================================

# Combines extract, transform, and load steps.
# Simulates a complete ETL pipeline.

def run_etl(input_file, output_file):
    data = extract_csv(input_file)
    cleaned = clean_data(data, "amount")
    transformed = transform_add_tax(cleaned)
    load_to_csv(output_file, transformed, transformed[0].keys())


# ============================================================
# 7. Validate Data Before Loading
# ============================================================

# Performs data validation checks before loading.
# Prevents bad data from reaching target systems.

def validate_before_load(records, required_fields):
    for record in records:
        if not required_fields.issubset(record.keys()):
            return False
    return True


# ============================================================
# 8. Log ETL Pipeline Steps
# ============================================================

# Logs ETL execution steps for monitoring.

import logging
logging.basicConfig(level=logging.INFO)

def log_step(step_name):
    logging.info(f"ETL Step: {step_name}")


# ============================================================
# 9. Handle Errors During ETL Execution
# ============================================================

# Handles exceptions to prevent pipeline crashes.

def safe_etl_execution(input_file, output_file):
    try:
        run_etl(input_file, output_file)
    except Exception as error:
        logging.error(f"ETL failed: {error}")


# ============================================================
# 10. Track Record Counts Through ETL Stages
# ============================================================

# Tracks record counts at each ETL stage.
# Helps detect data loss issues.

def track_record_counts(input_records, output_records):
    logging.info(f"Input record count: {len(input_records)}")
    logging.info(f"Output record count: {len(output_records)}")
