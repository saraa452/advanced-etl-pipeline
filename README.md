# Advanced ETL Pipeline

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](LICENSE)

Um pipeline ETL avançado e modular para processamento de dados.

## 🚀 Funcionalidades

- Extração de múltiplas fontes (API, Banco de Dados)
- Transformação e enriquecimento de dados
- Carregamento em múltiplos destinos
- Logging e monitoramento
- Testes automatizados
- Dockerização

## 📦 Instalação

```bash
git clone https://github.com/seu-usuario/advanced-etl-pipeline.git
cd advanced-etl-pipeline
pip install -r requirements.txt
```

## 🏗️ Estrutura do Projeto

```
advanced-etl-pipeline/
├── src/
│   ├── extractors/          # Módulos de extração de dados
│   │   ├── api_extractor.py     # Extrator de APIs REST
│   │   ├── database_extractor.py # Extrator de bancos de dados
│   │   └── base_extractor.py    # Classe base abstrata
│   ├── transformers/        # Módulos de transformação
│   │   └── data_transformer.py  # Transformador de dados
│   ├── loaders/             # Módulos de carregamento
│   │   ├── database_loader.py   # Carregador para banco de dados
│   │   ├── file_loader.py       # Carregador para arquivos
│   │   └── base_loader.py       # Classe base abstrata
│   ├── utils/               # Utilitários
│   │   ├── logger.py            # Configuração de logging
│   │   └── monitoring.py        # Monitoramento de pipeline
│   └── pipeline.py          # Orquestrador do pipeline ETL
├── tests/                   # Testes automatizados
├── config/                  # Arquivos de configuração
├── Dockerfile               # Configuração Docker
├── docker-compose.yml       # Composição de serviços
└── requirements.txt         # Dependências Python
```

## 📖 Uso

### Exemplo Básico

```python
from src.pipeline import ETLPipeline
from src.extractors import APIExtractor
from src.transformers import DataTransformer
from src.loaders import FileLoader

# Criar pipeline
pipeline = ETLPipeline("meu_pipeline")

# Configurar extrator
extractor = APIExtractor(
    base_url="https://api.exemplo.com",
    headers={"Authorization": "Bearer token"}
)

# Configurar transformador
transformer = DataTransformer()
transformer.add_transformation(lambda df: df.drop_duplicates())
transformer.add_transformation(lambda df: df.fillna(0))

# Configurar carregador
loader = FileLoader(output_dir="./output")

# Montar e executar pipeline
metrics = (
    pipeline
    .add_extractor(extractor)
    .set_transformer(transformer)
    .add_loader(loader)
    .run(
        extract_kwargs=[{"endpoint": "/dados"}],
        load_kwargs=[{"filename": "dados_processados", "file_format": "csv"}]
    )
)

print(f"Pipeline concluído: {metrics['status']}")
```

### Extração de Banco de Dados

```python
from src.extractors import DatabaseExtractor

extractor = DatabaseExtractor(
    connection_string="postgresql://user:password@localhost:5432/database"
)
extractor.connect()
data = extractor.extract(table="usuarios", columns=["id", "nome", "email"])
extractor.disconnect()
```

### Transformações Disponíveis

```python
from src.transformers import DataTransformer

transformer = DataTransformer()

# Limpeza de dados
transformer.clean_data(data, drop_duplicates=True, fill_na=0)

# Renomear colunas
transformer.rename_columns(data, {"old_name": "new_name"})

# Filtrar linhas
transformer.filter_rows(data, "valor > 100")

# Adicionar coluna
transformer.add_column(data, "nova_coluna", lambda df: df["valor"] * 2)

# Converter tipos
transformer.convert_types(data, {"data": "datetime", "valor": "float"})

# Agregar dados
transformer.aggregate(data, group_by=["categoria"], aggregations={"valor": "sum"})
```

### Carregamento para Múltiplos Destinos

```python
from src.loaders import DatabaseLoader, FileLoader

# Carregador de banco de dados
db_loader = DatabaseLoader(
    connection_string="postgresql://user:password@localhost:5432/database"
)
db_loader.connect()
db_loader.load(data, table="tabela_destino", if_exists="append")
db_loader.disconnect()

# Carregador de arquivos (CSV, JSON, Parquet)
file_loader = FileLoader(output_dir="./output")
file_loader.connect()
file_loader.load(data, filename="dados", file_format="csv")
file_loader.disconnect()
```

## 🧪 Testes

Executar todos os testes:

```bash
pytest tests/ -v
```

Executar com cobertura:

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

## 🐳 Docker

### Construir imagem

```bash
docker build -t etl-pipeline .
```

### Executar com Docker Compose

```bash
# Iniciar todos os serviços
docker-compose up -d

# Executar testes no container
docker-compose run test

# Parar serviços
docker-compose down
```

## 📊 Monitoramento

O pipeline inclui monitoramento integrado:

```python
from src.utils.monitoring import PipelineMonitor

monitor = PipelineMonitor("meu_pipeline")
monitor.start_pipeline()

monitor.start_stage("extract")
# ... operações de extração
monitor.end_stage(records_processed=1000)

monitor.start_stage("transform")
# ... operações de transformação
monitor.end_stage(records_processed=950)

metrics = monitor.end_pipeline(success=True)
print(monitor.get_summary())
```

## 📝 Configuração

Edite o arquivo `config/config.yaml` para personalizar:

- Configurações de banco de dados
- Parâmetros de API
- Diretórios de saída
- Níveis de logging

## 📄 Licença

Este projeto está licenciado sob a Apache License 2.0 - veja o arquivo [LICENSE](LICENSE) para detalhes.
