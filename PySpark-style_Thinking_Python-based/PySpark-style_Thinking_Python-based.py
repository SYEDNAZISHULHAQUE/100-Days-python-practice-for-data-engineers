# PYSPARK-STYLE THINKING (PYTHON-BASED) – 10
# ============================================================

# ============================================================
# 1. Select Columns from Records (DataFrame-style)
# ============================================================

# Mimics Spark DataFrame select() operation.
# Equivalent to: df.select("col1", "col2")

def df_select(records, columns):
    return [{col: record.get(col) for col in columns} for record in records]


# ============================================================
# 2. Filter Rows Based on Condition
# ============================================================

# Mimics Spark DataFrame filter() or where().
# Equivalent to: df.filter(col("age") > 30)

def df_filter(records, field, threshold):
    return [record for record in records if record.get(field, 0) > threshold]


# ============================================================
# 3. Add a Derived Column
# ============================================================

# Mimics Spark withColumn().
# Adds a new derived column to each record.

def df_with_column(records, new_column):
    for record in records:
        record[new_column] = record.get("amount", 0) * 0.1
    return records


# ============================================================
# 4. Drop a Column
# ============================================================

# Mimics Spark drop() operation.
# Removes an unwanted column.

def df_drop(records, column):
    for record in records:
        record.pop(column, None)
    return records


# ============================================================
# 5. Group By and Aggregate
# ============================================================

# Mimics Spark groupBy().agg().
# Aggregates sum by group key.

from collections import defaultdict

def df_groupby_sum(records, group_key, agg_key):
    aggregated = defaultdict(int)

    for record in records:
        aggregated[record[group_key]] += record.get(agg_key, 0)

    return dict(aggregated)


# ============================================================
# 6. Rename Columns
# ============================================================

# Mimics Spark withColumnRenamed().
# Renames a column in all records.

def df_rename_column(records, old_name, new_name):
    for record in records:
        record[new_name] = record.pop(old_name)
    return records


# ============================================================
# 7. Handle Null Values (Fill NA)
# ============================================================

# Mimics Spark fillna().
# Replaces None values with a default.

def df_fill_na(records, field, default_value):
    for record in records:
        if record.get(field) is None:
            record[field] = default_value
    return records


# ============================================================
# 8. Sort Records (Order By)
# ============================================================

# Mimics Spark orderBy().
# Sorts records by a field.

def df_order_by(records, field, ascending=True):
    return sorted(records, key=lambda x: x.get(field), reverse=not ascending)


# ============================================================
# 9. Join Two Datasets
# ============================================================

# Mimics Spark join().
# Performs an inner join.

def df_join(left, right, join_key):
    right_lookup = {r[join_key]: r for r in right}
    result = []

    for record in left:
        key = record.get(join_key)
        if key in right_lookup:
            joined_record = {**record, **right_lookup[key]}
            result.append(joined_record)

    return result


# ============================================================
# 10. Cache-like Behavior
# ============================================================

# Mimics Spark cache() concept.
# Stores intermediate result for reuse.

def df_cache(records):
    cached_data = records.copy()
    return cached_data
