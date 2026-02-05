# TRANSFORMATIONS & AGGREGATIONS – 15
# ============================================================

# ============================================================
# 1. Calculate Total Sales from Transaction Records
# ============================================================

# Calculates the total sales amount from a list of transaction records.
# Common metric in financial and e-commerce analytics.

def calculate_total_sales(transactions):
    total = 0

    for transaction in transactions:
        total += transaction.get("amount", 0)

    return total


# ============================================================
# 2. Calculate Average Value from Numeric Records
# ============================================================

# Calculates the average value from a list of numbers.
# Used in KPI and performance analysis.

def calculate_average(values):
    if not values:
        return 0

    return sum(values) / len(values)


# ============================================================
# 3. Group Records by a Specific Key
# ============================================================

# Groups records based on a given key.
# Similar to GROUP BY operation in SQL.

from collections import defaultdict

def group_records_by_key(records, key):
    grouped = defaultdict(list)

    for record in records:
        grouped[record[key]].append(record)

    return dict(grouped)


# ============================================================
# 4. Aggregate Sum by Group
# ============================================================

# Aggregates total amount per group.
# Frequently used in reporting pipelines.

def aggregate_sum_by_key(records, group_key, value_key):
    aggregated = defaultdict(int)

    for record in records:
        aggregated[record[group_key]] += record.get(value_key, 0)

    return dict(aggregated)


# ============================================================
# 5. Count Records per Group
# ============================================================

# Counts the number of records per group.
# Useful for distribution analysis.

def count_records_by_group(records, group_key):
    counts = defaultdict(int)

    for record in records:
        counts[record[group_key]] += 1

    return dict(counts)


# ============================================================
# 6. Sort Records by Numeric Field
# ============================================================

# Sorts records in ascending order based on a numeric field.
# Used for ranking and ordered reporting.

def sort_records_by_field(records, field):
    return sorted(records, key=lambda x: x.get(field, 0))


# ============================================================
# 7. Filter Records Based on Condition
# ============================================================

# Filters records where a numeric field exceeds a threshold.
# Common transformation during data enrichment.

def filter_records_by_threshold(records, field, threshold):
    return [record for record in records if record.get(field, 0) > threshold]


# ============================================================
# 8. Create Derived Column in Records
# ============================================================

# Adds a derived field based on existing values.
# Example: calculating tax or discount.

def add_derived_column(records):
    for record in records:
        record["tax_amount"] = record.get("amount", 0) * 0.1
    return records


# ============================================================
# 9. Normalize Numeric Values
# ============================================================

# Normalizes numeric values between 0 and 1.
# Useful in scoring and ML feature preparation.

def normalize_values(values):
    min_val = min(values)
    max_val = max(values)

    return [(v - min_val) / (max_val - min_val) if max_val != min_val else 0 for v in values]


# ============================================================
# 10. Pivot Data from Rows to Columns
# ============================================================

# Transforms row-based data into pivoted structure.
# Common in analytics transformations.

def pivot_data(records, row_key, column_key, value_key):
    pivot = defaultdict(dict)

    for record in records:
        pivot[record[row_key]][record[column_key]] = record[value_key]

    return dict(pivot)


# ============================================================
# 11. Join Two Datasets on a Common Key
# ============================================================

# Performs an inner join between two datasets.
# Fundamental transformation in data engineering.

def inner_join(left, right, key):
    right_lookup = {r[key]: r for r in right}
    joined = []

    for l in left:
        if l[key] in right_lookup:
            joined_record = {**l, **right_lookup[l[key]]}
            joined.append(joined_record)

    return joined


# ============================================================
# 12. Calculate Running Total
# ============================================================

# Calculates cumulative sum from a list of numbers.
# Used in trend and time-series analysis.

def running_total(values):
    result = []
    total = 0

    for value in values:
        total += value
        result.append(total)

    return result


# ============================================================
# 13. Rank Records Based on Value
# ============================================================

# Assigns rank to records based on a numeric field.
# Useful for top-N reporting.

def rank_records(records, field):
    sorted_records = sorted(records, key=lambda x: x[field], reverse=True)

    for index, record in enumerate(sorted_records, start=1):
        record["rank"] = index

    return sorted_records


# ============================================================
# 14. Bucketize Numeric Values
# ============================================================

# Groups numeric values into buckets.
# Used for segmentation and categorization.

def bucketize_values(values, bucket_size):
    buckets = defaultdict(list)

    for value in values:
        bucket_key = (value // bucket_size) * bucket_size
        buckets[bucket_key].append(value)

    return dict(buckets)


# ============================================================
# 15. Aggregate Multiple Metrics at Once
# ============================================================

# Aggregates multiple metrics such as sum, min, max, and average.
# Common in summary reporting layers.

def aggregate_metrics(values):
    if not values:
        return {}

    return {
        "sum": sum(values),
        "min": min(values),
        "max": max(values),
        "average": sum(values) / len(values)
    }
