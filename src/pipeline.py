"""ETL Pipeline - Main pipeline orchestrator."""
from typing import Any, Dict, List, Optional
import pandas as pd

from src.extractors.base_extractor import BaseExtractor
from src.transformers.data_transformer import DataTransformer
from src.loaders.base_loader import BaseLoader
from src.utils.logger import get_logger
from src.utils.monitoring import PipelineMonitor


class ETLPipeline:
    """Orchestrate ETL pipeline execution."""

    def __init__(self, name: str = "ETLPipeline"):
        """Initialize the ETL pipeline.
        
        Args:
            name: Pipeline name
        """
        self.name = name
        self.logger = get_logger(name)
        self.monitor = PipelineMonitor(name)
        self.extractors: List[BaseExtractor] = []
        self.transformer: Optional[DataTransformer] = None
        self.loaders: List[BaseLoader] = []

    def add_extractor(self, extractor: BaseExtractor) -> "ETLPipeline":
        """Add an extractor to the pipeline.
        
        Args:
            extractor: Extractor instance
            
        Returns:
            ETLPipeline: Self for method chaining
        """
        self.extractors.append(extractor)
        self.logger.info(f"Added extractor: {extractor.name}")
        return self

    def set_transformer(self, transformer: DataTransformer) -> "ETLPipeline":
        """Set the transformer for the pipeline.
        
        Args:
            transformer: Transformer instance
            
        Returns:
            ETLPipeline: Self for method chaining
        """
        self.transformer = transformer
        self.logger.info(f"Set transformer: {transformer.name}")
        return self

    def add_loader(self, loader: BaseLoader) -> "ETLPipeline":
        """Add a loader to the pipeline.
        
        Args:
            loader: Loader instance
            
        Returns:
            ETLPipeline: Self for method chaining
        """
        self.loaders.append(loader)
        self.logger.info(f"Added loader: {loader.name}")
        return self

    def run(
        self,
        extract_kwargs: Optional[List[Dict[str, Any]]] = None,
        load_kwargs: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Run the ETL pipeline.
        
        Args:
            extract_kwargs: List of kwargs for each extractor
            load_kwargs: List of kwargs for each loader
            
        Returns:
            Dict containing pipeline metrics
        """
        self.monitor.start_pipeline()
        success = True
        
        try:
            # Extract
            self.monitor.start_stage("extract")
            all_data = []
            extract_kwargs = extract_kwargs or [{}] * len(self.extractors)
            
            for i, extractor in enumerate(self.extractors):
                kwargs = extract_kwargs[i] if i < len(extract_kwargs) else {}
                extractor.connect()
                data = extractor.extract(**kwargs)
                if not data.empty:
                    all_data.append(data)
                extractor.disconnect()
            
            if all_data:
                combined_data = pd.concat(all_data, ignore_index=True)
            else:
                combined_data = pd.DataFrame()
            
            self.monitor.end_stage(records_processed=len(combined_data))
            
            # Transform
            if self.transformer and not combined_data.empty:
                self.monitor.start_stage("transform")
                transformed_data = self.transformer.transform(combined_data)
                self.monitor.end_stage(records_processed=len(transformed_data))
            else:
                transformed_data = combined_data
            
            # Load
            self.monitor.start_stage("load")
            load_kwargs = load_kwargs or [{}] * len(self.loaders)
            
            for i, loader in enumerate(self.loaders):
                kwargs = load_kwargs[i] if i < len(load_kwargs) else {}
                loader.connect()
                loader.load(transformed_data, **kwargs)
                loader.disconnect()
            
            self.monitor.end_stage(records_processed=len(transformed_data))
            
        except Exception as e:
            self.monitor.record_error(str(e))
            success = False
            self.logger.error(f"Pipeline failed: {e}")
        
        return self.monitor.end_pipeline(success=success)
