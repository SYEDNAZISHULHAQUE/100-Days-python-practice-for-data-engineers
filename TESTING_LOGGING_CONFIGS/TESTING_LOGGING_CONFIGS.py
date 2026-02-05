# ============================================================
# TESTING, LOGGING, CONFIGS – 10
# ============================================================

# ============================================================
# 1. Write a Simple Unit Test for a Function
# ============================================================

# Demonstrates how to test a simple function using assert.
# Used for validating transformation logic.

def add_numbers(a, b):
    return a + b

def test_add_numbers():
    assert add_numbers(2, 3) == 5
    assert add_numbers(-1, 1) == 0


# ============================================================
# 2. Validate Output Using Assertions
# ============================================================

# Uses assertions to validate data quality conditions.
# Helps catch issues early in the pipeline.

def validate_positive_numbers(values):
    for value in values:
        assert value >= 0, "Negative value detected"


# ============================================================
# 3. Log Pipeline Execution Steps
# ============================================================

# Logs execution steps for observability.
# Essential for debugging production pipelines.

import logging

logging.basicConfig(level=logging.INFO)

def log_pipeline_step(step_name):
    logging.info(f"Executing step: {step_name}")


# ============================================================
# 4. Log Errors Using Exception Handling
# ============================================================

# Logs error messages when exceptions occur.
# Helps with troubleshooting failures.

def safe_division(a, b):
    try:
        return a / b
    except ZeroDivisionError as error:
        logging.error("Division by zero error occurred")
        return None


# ============================================================
# 5. Read Configuration from Environment Variables
# ============================================================

# Reads config values from environment variables.
# Supports environment-specific behavior (DEV, QA, PROD).

import os

def get_env_config(key, default=None):
    return os.getenv(key, default)


# ============================================================
# 6. Read Configuration from a JSON File
# ============================================================

# Reads application configuration from a JSON file.
# Centralizes pipeline configuration.

import json

def read_config_file(file_path):
    with open(file_path, "r") as file:
        return json.load(file)


# ============================================================
# 7. Parameterize Functions Using Config Values
# ============================================================

# Uses configuration values to control function behavior.
# Improves flexibility and reusability.

def threshold_filter(values, threshold):
    return [v for v in values if v > threshold]


# ============================================================
# 8. Log Record Counts Before and After Processing
# ============================================================

# Logs record counts for monitoring data loss or growth.
# Important for data quality checks.

def log_record_count(stage, records):
    logging.info(f"{stage} record count: {len(records)}")


# ============================================================
# 9. Validate Mandatory Configuration Keys
# ============================================================

# Ensures required configuration keys are present.
# Prevents runtime failures.

def validate_config(config, required_keys):
    for key in required_keys:
        assert key in config, f"Missing config key: {key}"


# ============================================================
# 10. Enable Debug Logging for Development
# ============================================================

# Switches logging level based on environment.
# Useful during development and troubleshooting.

def set_logging_level(env):
    if env == "DEV":
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
