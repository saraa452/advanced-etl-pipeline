"""
Configuration module for the ETL pipeline.
Contains configuration files and sample data.
"""

import os

# Base directory paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

# Sample data path
SAMPLE_DATA_PATH = os.path.join(CONFIG_DIR, 'sample_data.csv')

# Database settings
DATABASE_NAME = 'etl_database.db'
DATABASE_PATH = os.path.join(OUTPUT_DIR, DATABASE_NAME)

# API settings
DEFAULT_API_TIMEOUT = 30
DEFAULT_API_RETRIES = 3
