"""Tests for the Flask API"""

import pytest
from app import app


class TestFlaskAPI:
    """Tests for Flask API endpoints"""
    
    @pytest.fixture
    def client(self):
        """Fixture that creates a Flask test client"""
        app.config['TESTING'] = True
        with app.test_client() as client:
            yield client
    
    def test_index_endpoint(self, client):
        """Test the root endpoint"""
        response = client.get('/')
        assert response.status_code == 200
        data = response.get_json()
        assert data['name'] == 'ETL Pipeline API'
        assert 'endpoints' in data
    
    def test_health_check(self, client):
        """Test the health check endpoint"""
        response = client.get('/health')
        assert response.status_code == 200
        data = response.get_json()
        assert data['status'] == 'healthy'
        assert 'timestamp' in data
        assert data['service'] == 'ETL Pipeline API'
    
    def test_pipeline_status(self, client):
        """Test the pipeline status endpoint"""
        response = client.get('/pipeline/status')
        assert response.status_code == 200
        data = response.get_json()
        assert 'pipeline' in data
        assert 'timestamp' in data
    
    def test_run_pipeline(self, client):
        """Test the pipeline execution endpoint"""
        response = client.post('/pipeline/run')
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert 'records_processed' in data
        assert 'timestamp' in data
