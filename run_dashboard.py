#!/usr/bin/env python3
import logging
import webbrowser
from pathlib import Path
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info('Iniciando Dashboard ETL...')
    try:
        from extract.extractor import DataExtractor
        from transform.transformer import DataTransformer
        from load.loader import DataLoader
        from visualize import DataVisualizer
        from config.settings import load_config
        
        logger.info('Executando pipeline ETL...')
        config = load_config()
        extractor = DataExtractor(config)
        raw_data = extractor.extract()
        
        if raw_data.empty:
            logger.error('Nenhum dado foi extraído!')
            return False
        
        transformer = DataTransformer(config)
        transformed_data = transformer.transform(raw_data)
        loader = DataLoader(config)
        loader.load(transformed_data)
        
        logger.info('Gerando graficos e relatorio...')
        visualizer = DataVisualizer(output_path='./output')
        visualizer.generate_report(transformed_data)
        
        report_path = Path('./output/report.html').resolve()
        report_path_str = str(report_path).replace('\\', '/')
        report_url = f'file:///{report_path_str}'
        
        logger.info(f'Relatorio salvo em: {report_path}')
        logger.info(f'Abrindo no navegador: {report_url}')
        
        try:
            webbrowser.open(report_url)
            logger.info('Dashboard aberto no navegador!')
        except Exception as e:
            logger.warning(f'Nao foi possivel abrir automaticamente. Acesse: {report_url}')
        
        logger.info('Dashboard pronto!')
        return True
    except Exception as e:
        logger.error(f'Erro: {str(e)}')
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
