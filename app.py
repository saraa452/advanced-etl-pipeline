import logging
from flask import Flask, send_from_directory, jsonify
from pathlib import Path
import pandas as pd
import threading

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

APP = Flask(__name__)

OUTPUT_DIR = Path('./output').resolve()
REPORT_FILE = OUTPUT_DIR / 'report.html'
DATA_FILE = OUTPUT_DIR / 'results.csv'


def regenerate_report():
    """Executa o pipeline mínimo para regenerar report.html usando os módulos existentes."""
    try:
        from extract.extractor import DataExtractor
        from transform.transformer import DataTransformer
        from load.loader import DataLoader
        from visualize import DataVisualizer
        from config.settings import load_config

        config = load_config()
        extractor = DataExtractor(config)
        raw = extractor.extract()
        if raw.empty:
            logger.error("Nenhum dado extraído durante refresh.")
            return False

        transformer = DataTransformer(config)
        transformed = transformer.transform(raw)

        loader = DataLoader(config)
        loader.load(transformed)

        visualizer = DataVisualizer(output_path=str(OUTPUT_DIR))
        visualizer.generate_report(transformed)
        logger.info("Relatório regenerado com sucesso.")
        return True
    except Exception as e:
        logger.exception("Falha ao regenerar relatório: %s", e)
        return False


@APP.route('/')
def index():
    if not REPORT_FILE.exists():
        return ("Relatório não encontrado. Regenerate via /refresh", 404)
    return send_from_directory(str(OUTPUT_DIR), REPORT_FILE.name)


@APP.route('/api/data')
def api_data():
    if not DATA_FILE.exists():
        return jsonify({"error": "data file not found"}), 404
    try:
        df = pd.read_csv(DATA_FILE)
        records = df.fillna('').to_dict(orient='records')
        return jsonify(records)
    except Exception as e:
        logger.exception("Erro ao ler results.csv: %s", e)
        return jsonify({"error": "failed to read data"}), 500


@APP.route('/refresh', methods=['POST', 'GET'])
def refresh():
    """Regenera o relatório. Retorna 202 se iniciado, 200 quando concluído."""
    thread = threading.Thread(target=regenerate_report, daemon=True)
    thread.start()
    return jsonify({"status": "regeneration started"}), 202


if __name__ == '__main__':
    APP.run(host='0.0.0.0', port=5000, debug=False)
