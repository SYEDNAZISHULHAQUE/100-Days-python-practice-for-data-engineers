#Data Cleaning & Validation
# ============================================================
# 1. Remove Records with Null Values in a Specific Field
# ============================================================

# This function removes records where a specified field contains None.
# Useful for filtering incomplete records before transformations.

def remove_null_records(records, field):
    cleaned_records = []

    for record in records:
        if record.get(field) is not None:
            cleaned_records.append(record)

    return cleaned_records

# Example usage:
# remove_null_records(data, "customer_id")


# ============================================================
# 2. Replace Missing Numeric Values with Zero
# ============================================================

# Replaces None values in a numeric list with zero.
# Prevents errors during aggregations and calculations.

def replace_missing_with_zero(values):
    return [value if value is not None else 0 for value in values]


# ============================================================
# 3. Trim Whitespace from All String Fields
# ============================================================

# Removes leading and trailing spaces from all string fields in a record.
# Ensures consistent text formatting.

def trim_string_fields(record):
    cleaned_record = {}

    for key, value in record.items():
        if isinstance(value, str):
            cleaned_record[key] = value.strip()
        else:
            cleaned_record[key] = value

    return cleaned_record


# ============================================================
# 4. Standardize Column Names
# ============================================================

# Converts column names to lowercase and replaces spaces with underscores.
# Helps enforce naming standards across datasets.

def standardize_column_names(record):
    return {key.lower().replace(" ", "_"): value for key, value in record.items()}


# ============================================================
# 5. Validate Numeric Fields
# ============================================================

# Checks whether a value is numeric (integer or float).
# Used during schema validation.

def is_numeric(value):
    return isinstance(value, (int, float))


# ============================================================
# 6. Remove Duplicate Records Based on Unique Key
# ============================================================

# Removes duplicate records using a unique identifier.
# Common in raw ingestion deduplication.

def remove_duplicates(records, unique_key):
    seen_keys = set()
    unique_records = []

    for record in records:
        key_value = record.get(unique_key)

        if key_value not in seen_keys:
            seen_keys.add(key_value)
            unique_records.append(record)

    return unique_records


# ============================================================
# 7. Validate Date Format (YYYY-MM-DD)
# ============================================================

# Validates whether a date string matches the expected format.
# Prevents downstream date parsing errors.

from datetime import datetime

def validate_date_format(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


# ============================================================
# 8. Fill Missing Fields with Default Values
# ============================================================

# Fills missing keys in a record with default values.
# Ensures schema consistency.

def fill_missing_fields(record, defaults):
    for key, default_value in defaults.items():
        record.setdefault(key, default_value)

    return record


# ============================================================
# 9. Remove Empty Strings from a List
# ============================================================

# Removes empty string values from a list.
# Useful for cleaning text-based columns.

def remove_empty_strings(values):
    return [value for value in values if value != ""]


# ============================================================
# 10. Validate Email Address Format
# ============================================================

# Validates whether an email address follows a basic format.
# Common validation in customer datasets.

import re

def validate_email(email):
    pattern = r"[^@]+@[^@]+\.[^@]+"
    return bool(re.match(pattern, email))


# ============================================================
# 11. Check Schema Consistency Across Records
# ============================================================

# Verifies that all records contain the same set of fields.
# Prevents schema drift issues.

def validate_schema(records):
    if not records:
        return True

    base_schema = set(records[0].keys())

    for record in records:
        if set(record.keys()) != base_schema:
            return False

    return True


# ============================================================
# 12. Remove Records with Invalid ID Values
# ============================================================

# Removes records where the ID field is not an integer.
# Helps enforce data type consistency.

def remove_invalid_ids(records, id_field):
    return [record for record in records if isinstance(record.get(id_field), int)]


# ============================================================
# 13. Normalize Boolean Values
# ============================================================

# Converts various boolean representations into True or False.
# Useful when ingesting data from multiple sources.

def normalize_boolean(value):
    return str(value).lower() in ["true", "1", "yes"]


# ============================================================
# 14. Detect Outliers Using Threshold
# ============================================================

# Identifies values greater than a specified threshold.
# Simple outlier detection method.

def detect_outliers(values, threshold):
    return [value for value in values if value > threshold]


# ============================================================
# 15. Validate Mandatory Fields in a Record
# ============================================================

# Checks whether all required fields are present in a record.
# Essential before loading data into curated layers.

def validate_mandatory_fields(record, required_fields):
    return required_fields.issubset(record.keys())
