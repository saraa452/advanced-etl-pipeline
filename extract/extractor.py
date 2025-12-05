import logging
from abc import ABC, abstractmethod
import pandas as pd
import requests

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    """Classe abstrata para implementar diferentes tipos de extração"""
    
    def __init__(self, config):
        """Inicializa o extrator com configurações
        
        Args:
            config: Dicionário com configurações de extração
        """
        self.config = config
    
    @abstractmethod
    def extract(self):
        """Método abstrato para extração de dados
        
        Returns:
            pd.DataFrame: Dados extraídos
        """
        pass


class APIExtractor(BaseExtractor):
    """Extractor de API COVID-19"""
    
    def extract(self):
        """Extrai dados da API disease.sh COVID-19
        
        Returns:
            pd.DataFrame: Dados da API
        """
        try:
            api_url = self.config.get('extract', {}).get('api_url', 
                'https://disease.sh/v3/covid-19/countries')
            
            logger.info(f"Fazendo requisição para {api_url}...")
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data)
            
            # Selecionar e renomear colunas relevantes
            df = df[['country', 'cases', 'deaths', 'recovered', 'population', 'continent']]
            
            logger.info(f"Extraídos {len(df)} registros da API")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao fazer requisição à API: {str(e)}")
            return pd.DataFrame()


class CSVExtractor(BaseExtractor):
    """Extractor de dados CSV"""
    
    def extract(self):
        """Extrai dados de arquivo CSV
        
        Returns:
            pd.DataFrame: Dados do CSV
        """
        try:
            csv_path = self.config.get('extract', {}).get('source_path', './config/sample_data.csv')
            
            logger.info(f"Lendo arquivo CSV: {csv_path}")
            df = pd.read_csv(csv_path)
            
            logger.info(f"Extraídos {len(df)} registros do CSV")
            return df
            
        except FileNotFoundError:
            logger.error(f"Arquivo CSV não encontrado: {csv_path}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Erro ao ler arquivo CSV: {str(e)}")
            return pd.DataFrame()


class DataExtractor(BaseExtractor):
    """Extrator de dados - implementação com suporte a múltiplas fontes"""
    
    def extract(self):
        """Extrai dados de múltiplas fontes (API e CSV) e combina
        
        Returns:
            pd.DataFrame: Dados extraídos combinados
        """
        logger.info("Extraindo dados de múltiplas fontes...")
        
        source_type = self.config.get('extract', {}).get('source_type', 'csv')
        
        if source_type == 'api':
            extractor = APIExtractor(self.config)
            return extractor.extract()
        
        elif source_type == 'csv':
            extractor = CSVExtractor(self.config)
            return extractor.extract()
        
        elif source_type == 'hybrid':
            # Combina dados de ambas as fontes
            logger.info("Modo híbrido: combinando dados de API e CSV")
            
            csv_extractor = CSVExtractor(self.config)
            csv_data = csv_extractor.extract()
            
            api_extractor = APIExtractor(self.config)
            api_data = api_extractor.extract()
            
            if not csv_data.empty and not api_data.empty:
                # Combinar e remover duplicatas (priorizando dados da API)
                combined = pd.concat([api_data, csv_data], ignore_index=True)
                combined = combined.drop_duplicates(subset=['country'], keep='first')
                logger.info(f"Dados combinados: {len(combined)} registros únicos")
                return combined
            elif not csv_data.empty:
                return csv_data
            else:
                return api_data
        
        else:
            logger.warning(f"Tipo de fonte desconhecido: {source_type}")
            return pd.DataFrame()
