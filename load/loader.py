import logging
from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class BaseLoader(ABC):
    """Classe abstrata para implementar diferentes tipos de carregamento"""
    
    def __init__(self, config):
        """Inicializa o loader com configurações
        
        Args:
            config: Dicionário com configurações de carregamento
        """
        self.config = config
    
    @abstractmethod
    def load(self, data):
        """Método abstrato para carregamento de dados
        
        Args:
            data: pd.DataFrame com dados transformados
        """
        pass


class CSVLoader(BaseLoader):
    """Loader para exportar dados em formato CSV"""
    
    def load(self, data):
        """Salva dados em arquivo CSV
        
        Args:
            data: pd.DataFrame com dados transformados
        """
        try:
            dest_path = Path(self.config.get('load', {}).get('destination_path', './output'))
            dest_path.mkdir(parents=True, exist_ok=True)
            
            csv_file = dest_path / 'results.csv'
            
            logger.info(f"Salvando dados em CSV: {csv_file}")
            data.to_csv(csv_file, index=False, encoding='utf-8')
            
            logger.info(f"CSV salvo com sucesso: {len(data)} registros")
            
        except Exception as e:
            logger.error(f"Erro ao salvar CSV: {str(e)}")


class SQLiteLoader(BaseLoader):
    """Loader para exportar dados em banco SQLite"""
    
    def load(self, data):
        """Salva dados em banco de dados SQLite
        
        Args:
            data: pd.DataFrame com dados transformados
        """
        try:
            dest_path = Path(self.config.get('load', {}).get('destination_path', './output'))
            dest_path.mkdir(parents=True, exist_ok=True)
            
            db_path = dest_path / 'results.db'
            db_url = f'sqlite:///{db_path.absolute()}'
            
            logger.info(f"Conectando ao banco SQLite: {db_path}")
            engine = create_engine(db_url)
            
            # Salvar em tabela COVID
            logger.info(f"Inserindo {len(data)} registros na tabela 'covid_data'")
            data.to_sql('covid_data', con=engine, if_exists='replace', index=False)
            
            logger.info(f"Banco SQLite salvo com sucesso: {db_path}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar SQLite: {str(e)}")


class DataLoader(BaseLoader):
    """Loader de dados - suporte a múltiplos destinos"""
    
    def load(self, data):
        """Carrega dados em múltiplos formatos conforme configuração
        
        Args:
            data: pd.DataFrame com dados transformados
        """
        if data.empty:
            logger.warning("Dados vazios recebidos para carregamento")
            return
        
        logger.info(f"Carregando {len(data)} registros...")
        
        destination_type = self.config.get('load', {}).get('destination_type', 'csv')
        
        if destination_type == 'csv':
            loader = CSVLoader(self.config)
            loader.load(data)
        
        elif destination_type == 'sqlite':
            loader = SQLiteLoader(self.config)
            loader.load(data)
        
        elif destination_type == 'both':
            # Carregar em ambos os formatos
            logger.info("Carregando em múltiplos formatos...")
            
            csv_loader = CSVLoader(self.config)
            csv_loader.load(data)
            
            sqlite_loader = SQLiteLoader(self.config)
            sqlite_loader.load(data)
            
            logger.info("Dados carregados em CSV e SQLite")
        
        else:
            logger.warning(f"Tipo de destino desconhecido: {destination_type}")
        
        logger.info("Carregamento concluído com sucesso")
