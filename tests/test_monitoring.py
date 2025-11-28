"""Tests for Pipeline Monitor."""
import pytest
import time

from src.utils.monitoring import PipelineMonitor


class TestPipelineMonitor:
    """Test suite for PipelineMonitor."""

    @pytest.fixture
    def monitor(self):
        """Create a pipeline monitor instance."""
        return PipelineMonitor("test_pipeline")

    def test_init(self, monitor):
        """Test monitor initialization."""
        assert monitor.pipeline_name == "test_pipeline"
        assert monitor.metrics["status"] == "not_started"

    def test_start_pipeline(self, monitor):
        """Test pipeline start."""
        monitor.start_pipeline()
        
        assert monitor.metrics["status"] == "running"
        assert monitor.metrics["start_time"] is not None

    def test_end_pipeline_success(self, monitor):
        """Test successful pipeline end."""
        monitor.start_pipeline()
        metrics = monitor.end_pipeline(success=True)
        
        assert metrics["status"] == "success"
        assert metrics["end_time"] is not None
        assert metrics["duration_seconds"] is not None

    def test_end_pipeline_failure(self, monitor):
        """Test failed pipeline end."""
        monitor.start_pipeline()
        metrics = monitor.end_pipeline(success=False)
        
        assert metrics["status"] == "failed"

    def test_stage_tracking(self, monitor):
        """Test stage start and end."""
        monitor.start_pipeline()
        
        monitor.start_stage("extract")
        monitor.end_stage(records_processed=100)
        
        assert len(monitor.metrics["stages"]) == 1
        stage = monitor.metrics["stages"][0]
        assert stage["name"] == "extract"
        assert stage["records_processed"] == 100
        assert stage["status"] == "success"

    def test_multiple_stages(self, monitor):
        """Test multiple stages."""
        monitor.start_pipeline()
        
        monitor.start_stage("extract")
        monitor.end_stage(records_processed=100)
        
        monitor.start_stage("transform")
        monitor.end_stage(records_processed=95)
        
        monitor.start_stage("load")
        monitor.end_stage(records_processed=95)
        
        assert len(monitor.metrics["stages"]) == 3

    def test_error_recording(self, monitor):
        """Test error recording."""
        monitor.start_pipeline()
        monitor.start_stage("extract")
        monitor.record_error("Connection timeout")
        
        assert len(monitor.metrics["errors"]) == 1
        error = monitor.metrics["errors"][0]
        assert error["error"] == "Connection timeout"
        assert error["stage"] == "extract"

    def test_get_metrics(self, monitor):
        """Test getting metrics."""
        monitor.start_pipeline()
        metrics = monitor.get_metrics()
        
        assert metrics["pipeline_name"] == "test_pipeline"
        assert metrics["status"] == "running"

    def test_get_summary(self, monitor):
        """Test getting summary."""
        monitor.start_pipeline()
        monitor.start_stage("extract")
        monitor.end_stage(records_processed=100)
        monitor.end_pipeline(success=True)
        
        summary = monitor.get_summary()
        
        assert "test_pipeline" in summary
        assert "success" in summary
        assert "Stages: 1" in summary
        assert "Errors: 0" in summary
