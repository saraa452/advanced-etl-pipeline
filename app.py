"""
Flask API for the ETL Pipeline

Available endpoints:
- GET /health - Check if API is online
- POST /pipeline/run - Execute the ETL pipeline
- GET /pipeline/status - Return current pipeline status
"""

import logging
from datetime import datetime, timezone
from flask import Flask, jsonify

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

app = Flask(__name__)

# Estado do pipeline
pipeline_status = {
    'status': 'idle',
    'last_run': None,
    'last_result': None,
    'records_processed': 0
}


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint to verify if the API is online"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'service': 'ETL Pipeline API'
    })


@app.route('/pipeline/run', methods=['POST'])
def run_pipeline():
    """Execute the complete ETL pipeline"""
    global pipeline_status
    
    try:
        pipeline_status['status'] = 'running'
        pipeline_status['last_run'] = datetime.now(timezone.utc).isoformat()
        
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
        
        # Atualizar status
        pipeline_status['status'] = 'completed'
        pipeline_status['last_result'] = 'success'
        pipeline_status['records_processed'] = len(transformed_data)
        
        logger.info("Pipeline ETL finalizado com sucesso!")
        
        return jsonify({
            'success': True,
            'message': 'Pipeline executado com sucesso',
            'records_processed': len(transformed_data),
            'timestamp': datetime.now(timezone.utc).isoformat()
        })
        
    except Exception as e:
        logger.error(f"Erro durante execução do pipeline: {str(e)}")
        pipeline_status['status'] = 'error'
        pipeline_status['last_result'] = 'failed'
        
        return jsonify({
            'success': False,
            'message': f'Erro durante execução: {str(e)}',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }), 500


@app.route('/pipeline/status', methods=['GET'])
def get_status():
    """Return the current status of the ETL pipeline"""
    return jsonify({
        'pipeline': pipeline_status,
        'timestamp': datetime.now(timezone.utc).isoformat()
    })


@app.route('/', methods=['GET'])
def index():
    """Root endpoint with API information"""
    return jsonify({
        'name': 'ETL Pipeline API',
        'version': '1.0.0',
        'description': 'API for ETL pipeline management',
        'endpoints': {
            'GET /': 'API information',
            'GET /health': 'Health check',
            'POST /pipeline/run': 'Execute the ETL pipeline',
            'GET /pipeline/status': 'Pipeline status'
        }
    })


if __name__ == '__main__':
    import os
    debug_mode = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=5000, debug=debug_mode)
