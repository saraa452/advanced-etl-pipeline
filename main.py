"""
Main ETL Pipeline Orchestrator

Coordena o fluxo de dados através das etapas:
Extract → Transform → Load
"""

import logging
from pathlib import Path
from extract.extractor import DataExtractor
from transform.transformer import DataTransformer
from load.loader import DataLoader
from config.settings import load_config

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """Executa o pipeline ETL completo"""
    try:
        # Carregar configurações
        config = load_config()
        logger.info("Configurações carregadas com sucesso")
        
        # Etapa 1: Extração
        logger.info("Iniciando extração de dados...")
        extractor = DataExtractor(config)
        raw_data = extractor.extract()
        logger.info(f"Dados extraídos: {len(raw_data)} registros")
        
        # Etapa 2: Transformação
        logger.info("Iniciando transformação de dados...")
        transformer = DataTransformer(config)
        transformed_data = transformer.transform(raw_data)
        logger.info(f"Dados transformados: {len(transformed_data)} registros")
        
        # Etapa 3: Carregamento
        logger.info("Iniciando carregamento de dados...")
        loader = DataLoader(config)
        loader.load(transformed_data)
        logger.info("Dados carregados com sucesso")
        
        logger.info("Pipeline ETL finalizado com sucesso!")
        return True
        
    except Exception as e:
        logger.error(f"Erro durante execução do pipeline: {str(e)}")
        return False


if __name__ == "__main__":
    success = run_etl_pipeline()
    exit(0 if success else 1)
