"""
Extract module for the ETL pipeline.
Contains modules for extracting data from various sources.
"""

from .api import extract_from_api
from .csv_extractor import extract_from_csv

__all__ = ['extract_from_api', 'extract_from_csv']
