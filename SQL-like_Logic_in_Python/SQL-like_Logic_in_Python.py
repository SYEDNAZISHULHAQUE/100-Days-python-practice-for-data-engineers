# SQL-LIKE LOGIC IN PYTHON – 10
# ============================================================

# ============================================================
# 1. SELECT Specific Columns from Records
# ============================================================

# Simulates a SQL SELECT statement by extracting specific columns.
# Equivalent to: SELECT col1, col2 FROM table

def select_columns(records, columns):
    result = []

    for record in records:
        selected = {}
        for column in columns:
            selected[column] = record.get(column)
        result.append(selected)

    return result


# ============================================================
# 2. WHERE Clause Filtering
# ============================================================

# Filters records based on a condition.
# Equivalent to: SELECT * FROM table WHERE condition

def where_filter(records, field, value):
    return [record for record in records if record.get(field) == value]


# ============================================================
# 3. WHERE Clause with Greater Than Condition
# ============================================================

# Filters records where a numeric field is greater than a threshold.
# Equivalent to: WHERE amount > 1000

def where_greater_than(records, field, threshold):
    return [record for record in records if record.get(field, 0) > threshold]


# ============================================================
# 4. GROUP BY with COUNT
# ============================================================

# Groups records by a field and counts records in each group.
# Equivalent to: SELECT field, COUNT(*) FROM table GROUP BY field

from collections import defaultdict

def group_by_count(records, group_field):
    counts = defaultdict(int)

    for record in records:
        counts[record[group_field]] += 1

    return dict(counts)


# ============================================================
# 5. GROUP BY with SUM
# ============================================================

# Groups records by a field and calculates sum of another field.
# Equivalent to: SELECT field, SUM(amount) FROM table GROUP BY field

def group_by_sum(records, group_field, sum_field):
    sums = defaultdict(int)

    for record in records:
        sums[record[group_field]] += record.get(sum_field, 0)

    return dict(sums)


# ============================================================
# 6. ORDER BY Clause
# ============================================================

# Sorts records based on a field.
# Equivalent to: ORDER BY field ASC/DESC

def order_by(records, field, ascending=True):
    return sorted(records, key=lambda x: x.get(field), reverse=not ascending)


# ============================================================
# 7. INNER JOIN Between Two Tables
# ============================================================

# Performs an inner join on two datasets.
# Equivalent to: SELECT * FROM A INNER JOIN B ON key

def inner_join(left, right, join_key):
    right_lookup = {r[join_key]: r for r in right}
    result = []

    for record in left:
        key_value = record.get(join_key)
        if key_value in right_lookup:
            joined_record = {**record, **right_lookup[key_value]}
            result.append(joined_record)

    return result


# ============================================================
# 8. LEFT JOIN Between Two Tables
# ============================================================

# Performs a left join between two datasets.
# Equivalent to: LEFT JOIN in SQL

def left_join(left, right, join_key):
    right_lookup = {r[join_key]: r for r in right}
    result = []

    for record in left:
        joined = record.copy()
        joined.update(right_lookup.get(record.get(join_key), {}))
        result.append(joined)

    return result


# ============================================================
# 9. HAVING Clause After GROUP BY
# ============================================================

# Filters grouped data based on aggregate condition.
# Equivalent to: HAVING COUNT(*) > N

def having_filter(grouped_data, threshold):
    return {k: v for k, v in grouped_data.items() if v > threshold}


# ============================================================
# 10. DISTINCT Operation
# ============================================================

# Returns distinct values from a column.
# Equivalent to: SELECT DISTINCT column FROM table

def distinct_values(records, field):
    return list({record.get(field) for record in records})
