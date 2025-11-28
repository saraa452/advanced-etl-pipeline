"""Pipeline Monitor - Monitoring and metrics for ETL pipelines."""
from datetime import datetime
from typing import Any, Dict, List, Optional
import time

from .logger import get_logger


class PipelineMonitor:
    """Monitor ETL pipeline execution and collect metrics."""

    def __init__(self, pipeline_name: str):
        """Initialize the pipeline monitor.
        
        Args:
            pipeline_name: Name of the pipeline being monitored
        """
        self.pipeline_name = pipeline_name
        self.logger = get_logger(f"Monitor:{pipeline_name}")
        self.metrics: Dict[str, Any] = {
            "pipeline_name": pipeline_name,
            "start_time": None,
            "end_time": None,
            "duration_seconds": None,
            "status": "not_started",
            "stages": [],
            "errors": []
        }
        self._stage_start_time: Optional[float] = None
        self._current_stage: Optional[str] = None

    def start_pipeline(self) -> None:
        """Mark pipeline start."""
        self.metrics["start_time"] = datetime.now().isoformat()
        self.metrics["status"] = "running"
        self._pipeline_start = time.time()
        self.logger.info(f"Pipeline '{self.pipeline_name}' started")

    def end_pipeline(self, success: bool = True) -> Dict[str, Any]:
        """Mark pipeline end and return metrics.
        
        Args:
            success: Whether pipeline completed successfully
            
        Returns:
            Dict containing pipeline metrics
        """
        end_time = time.time()
        self.metrics["end_time"] = datetime.now().isoformat()
        self.metrics["duration_seconds"] = round(end_time - self._pipeline_start, 2)
        self.metrics["status"] = "success" if success else "failed"
        
        status = "completed successfully" if success else "failed"
        self.logger.info(
            f"Pipeline '{self.pipeline_name}' {status} "
            f"in {self.metrics['duration_seconds']}s"
        )
        return self.metrics

    def start_stage(self, stage_name: str) -> None:
        """Mark stage start.
        
        Args:
            stage_name: Name of the stage
        """
        self._current_stage = stage_name
        self._stage_start_time = time.time()
        self.logger.info(f"Stage '{stage_name}' started")

    def end_stage(self, records_processed: int = 0, success: bool = True) -> None:
        """Mark stage end.
        
        Args:
            records_processed: Number of records processed in this stage
            success: Whether stage completed successfully
        """
        if self._stage_start_time is None:
            return

        duration = round(time.time() - self._stage_start_time, 2)
        stage_metrics = {
            "name": self._current_stage,
            "duration_seconds": duration,
            "records_processed": records_processed,
            "status": "success" if success else "failed"
        }
        self.metrics["stages"].append(stage_metrics)
        
        status = "completed" if success else "failed"
        self.logger.info(
            f"Stage '{self._current_stage}' {status} "
            f"in {duration}s - {records_processed} records"
        )
        
        self._current_stage = None
        self._stage_start_time = None

    def record_error(self, error: str, stage: Optional[str] = None) -> None:
        """Record an error.
        
        Args:
            error: Error message
            stage: Stage where error occurred
        """
        error_record = {
            "timestamp": datetime.now().isoformat(),
            "stage": stage or self._current_stage,
            "error": error
        }
        self.metrics["errors"].append(error_record)
        self.logger.error(f"Error in stage '{error_record['stage']}': {error}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics.
        
        Returns:
            Dict containing current pipeline metrics
        """
        return self.metrics

    def get_summary(self) -> str:
        """Get a human-readable summary.
        
        Returns:
            String summary of pipeline execution
        """
        summary_lines = [
            f"Pipeline: {self.pipeline_name}",
            f"Status: {self.metrics['status']}",
            f"Duration: {self.metrics.get('duration_seconds', 'N/A')}s",
            f"Stages: {len(self.metrics['stages'])}",
            f"Errors: {len(self.metrics['errors'])}"
        ]
        return "\n".join(summary_lines)
