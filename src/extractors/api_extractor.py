"""API Extractor - Extract data from REST APIs."""
from typing import Any, Dict, List, Optional
import requests
import pandas as pd

from .base_extractor import BaseExtractor


class APIExtractor(BaseExtractor):
    """Extractor for REST API data sources."""

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 30,
        name: str = "APIExtractor"
    ):
        """Initialize the API extractor.
        
        Args:
            base_url: Base URL for the API
            headers: Optional HTTP headers
            timeout: Request timeout in seconds
            name: Name identifier for the extractor
        """
        super().__init__(name)
        self.base_url = base_url.rstrip('/')
        self.headers = headers or {}
        self.timeout = timeout
        self.session: Optional[requests.Session] = None

    def connect(self) -> bool:
        """Create a requests session.
        
        Returns:
            bool: True if session created successfully
        """
        try:
            self.session = requests.Session()
            self.session.headers.update(self.headers)
            self.logger.info(f"Connected to API: {self.base_url}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to create session: {e}")
            return False

    def extract(
        self,
        endpoint: str = "",
        params: Optional[Dict[str, Any]] = None,
        method: str = "GET"
    ) -> pd.DataFrame:
        """Extract data from the API.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            method: HTTP method (GET, POST)
            
        Returns:
            pd.DataFrame: Extracted data
        """
        if self.session is None:
            self.connect()

        url = f"{self.base_url}/{endpoint.lstrip('/')}" if endpoint else self.base_url
        self.logger.info(f"Extracting data from: {url}")

        try:
            if method.upper() == "GET":
                response = self.session.get(url, params=params, timeout=self.timeout)
            elif method.upper() == "POST":
                response = self.session.post(url, json=params, timeout=self.timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            data = response.json()

            # Handle different JSON structures
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict):
                # Try to find a list within the dict
                for key, value in data.items():
                    if isinstance(value, list):
                        df = pd.DataFrame(value)
                        break
                else:
                    df = pd.DataFrame([data])
            else:
                df = pd.DataFrame()

            self.logger.info(f"Extracted {len(df)} records from API")
            return df

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            return pd.DataFrame()
        except ValueError as e:
            self.logger.error(f"Failed to parse API response: {e}")
            return pd.DataFrame()

    def disconnect(self) -> bool:
        """Close the requests session.
        
        Returns:
            bool: True if session closed successfully
        """
        try:
            if self.session:
                self.session.close()
                self.session = None
            self.logger.info("Disconnected from API")
            return True
        except Exception as e:
            self.logger.error(f"Failed to close session: {e}")
            return False
