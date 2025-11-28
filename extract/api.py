"""
API Extraction Module

This module provides functions to extract data from REST APIs.
Supports various authentication methods and data formats.
"""

import requests
import pandas as pd
from typing import Optional, Dict, Any, List
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_from_api(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    auth: Optional[tuple] = None,
    timeout: int = 30,
    retries: int = 3,
    retry_delay: int = 1
) -> pd.DataFrame:
    """
    Extract data from a REST API endpoint.
    
    Args:
        url: The API endpoint URL
        headers: Optional HTTP headers
        params: Optional query parameters
        auth: Optional authentication tuple (username, password)
        timeout: Request timeout in seconds
        retries: Number of retry attempts
        retry_delay: Delay between retries in seconds
    
    Returns:
        pandas DataFrame with the extracted data
    
    Raises:
        requests.RequestException: If the API request fails after all retries
    """
    logger.info(f"Extracting data from API: {url}")
    
    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                auth=auth,
                timeout=timeout
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different JSON structures
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Check for common data wrapper keys
                for key in ['data', 'results', 'items', 'records']:
                    if key in data and isinstance(data[key], list):
                        df = pd.DataFrame(data[key])
                        break
                else:
                    # Single record or nested structure
                    df = pd.DataFrame([data])
            else:
                raise ValueError(f"Unexpected data format: {type(data)}")
            
            logger.info(f"Successfully extracted {len(df)} records from API")
            return df
            
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt + 1}/{retries} failed: {str(e)}")
            if attempt < retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"All {retries} attempts failed for URL: {url}")
                raise
    
    return pd.DataFrame()


def extract_paginated_api(
    base_url: str,
    page_param: str = 'page',
    limit_param: str = 'limit',
    limit: int = 100,
    max_pages: Optional[int] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30
) -> pd.DataFrame:
    """
    Extract data from a paginated API endpoint.
    
    Args:
        base_url: The base API endpoint URL
        page_param: Name of the page parameter
        limit_param: Name of the limit parameter
        limit: Number of records per page
        max_pages: Maximum number of pages to fetch (None for all)
        headers: Optional HTTP headers
        timeout: Request timeout in seconds
    
    Returns:
        pandas DataFrame with all extracted data
    """
    logger.info(f"Extracting paginated data from API: {base_url}")
    
    all_data: List[pd.DataFrame] = []
    page = 1
    
    while max_pages is None or page <= max_pages:
        params = {page_param: page, limit_param: limit}
        
        try:
            df = extract_from_api(
                url=base_url,
                headers=headers,
                params=params,
                timeout=timeout,
                retries=2
            )
            
            if df.empty:
                logger.info(f"No more data at page {page}, stopping pagination")
                break
            
            all_data.append(df)
            logger.info(f"Extracted page {page} with {len(df)} records")
            
            if len(df) < limit:
                logger.info("Last page reached (fewer records than limit)")
                break
            
            page += 1
            
        except requests.RequestException:
            logger.error(f"Failed to fetch page {page}, stopping pagination")
            break
    
    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total records extracted: {len(result)}")
        return result
    
    return pd.DataFrame()


def extract_mock_api_data() -> pd.DataFrame:
    """
    Generate mock API data for testing and demonstration purposes.
    
    Returns:
        pandas DataFrame with mock sales data
    """
    logger.info("Generating mock API data")
    
    mock_data = [
        {"id": 1, "product": "Laptop", "category": "Electronics", "price": 999.99, "quantity": 50, "region": "North"},
        {"id": 2, "product": "Smartphone", "category": "Electronics", "price": 699.99, "quantity": 150, "region": "South"},
        {"id": 3, "product": "Tablet", "category": "Electronics", "price": 449.99, "quantity": 75, "region": "East"},
        {"id": 4, "product": "Headphones", "category": "Electronics", "price": 149.99, "quantity": 200, "region": "West"},
        {"id": 5, "product": "Monitor", "category": "Electronics", "price": 299.99, "quantity": 80, "region": "North"},
        {"id": 6, "product": "Keyboard", "category": "Accessories", "price": 79.99, "quantity": 300, "region": "South"},
        {"id": 7, "product": "Mouse", "category": "Accessories", "price": 39.99, "quantity": 500, "region": "East"},
        {"id": 8, "product": "Webcam", "category": "Accessories", "price": 89.99, "quantity": 120, "region": "West"},
        {"id": 9, "product": "USB Hub", "category": "Accessories", "price": 29.99, "quantity": 250, "region": "North"},
        {"id": 10, "product": "Desk Lamp", "category": "Office", "price": 49.99, "quantity": 180, "region": "South"},
    ]
    
    df = pd.DataFrame(mock_data)
    logger.info(f"Generated {len(df)} mock records")
    return df
