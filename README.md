# 🔄 Advanced ETL Pipeline

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-pytest-orange.svg)](tests/)
[![Dashboard](https://img.shields.io/badge/Dashboard-Dash-purple.svg)](visualize/)
[![Deploy](https://img.shields.io/badge/Deploy-Render-blue.svg)](https://render.com)

Um pipeline ETL (Extract, Transform, Load) avançado e modular para processamento de dados profissional, com dashboard interativo para visualização.

## 🌐 Demo

🚀 **[Acesse o Dashboard ao Vivo](https://etl-dashboard.onrender.com)** *(Deploy no Render.com)*

## 🚀 Funcionalidades

### Extract (Extração)
- ✅ Extração de dados via **REST API** com retry e paginação
- ✅ Leitura de arquivos **CSV** com validação de estrutura
- ✅ Suporte a múltiplos formatos e encodings
- ✅ Dados de demonstração incluídos

### Transform (Transformação)
- ✅ **Limpeza de dados**: remoção de duplicatas, tratamento de valores nulos
- ✅ **Padronização**: normalização de nomes de colunas e tipos de dados
- ✅ **Agregações**: soma, média, contagem por grupos
- ✅ **Join de DataFrames**: inner, left, right, outer joins
- ✅ **Cálculo de métricas**: receita total, percentuais, rankings

### Load (Carga)
- ✅ Carregamento em banco de dados **SQLite**
- ✅ Exportação para arquivos **CSV**
- ✅ Geração de relatórios de resumo
- ✅ Validação de output

### Dashboard
- ✅ Interface web profissional com **Dash/Plotly**
- ✅ **Filtros interativos**: categoria, região, faixa de preço
- ✅ **KPIs em tempo real**: receita, quantidade, preço médio
- ✅ **Gráficos dinâmicos**: pizza, barras, scatter
- ✅ **Tabela de dados** com ordenação e filtro
- ✅ Layout responsivo e profissional

## 📁 Estrutura do Projeto

```
advanced-etl-pipeline/
├── config/                 # Configurações e dados de exemplo
│   ├── __init__.py
│   └── sample_data.csv     # Dados de demonstração
├── extract/                # Módulos de extração
│   ├── __init__.py
│   ├── api.py              # Extração via API REST
│   └── csv_extractor.py    # Extração de arquivos CSV
├── transform/              # Módulos de transformação
│   ├── __init__.py
│   ├── clean.py            # Limpeza e padronização
│   └── join.py             # Joins e agregações
├── load/                   # Módulos de carga
│   ├── __init__.py
│   ├── to_sqlite.py        # Carga para SQLite
│   └── to_csv.py           # Carga para CSV
├── visualize/              # Dashboard web
│   ├── __init__.py
│   └── dash_app.py         # Aplicação Dash
├── tests/                  # Testes unitários
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── output/                 # Dados processados (gerado)
├── main.py                 # Ponto de entrada principal
├── requirements.txt        # Dependências Python
├── render.yaml             # Configuração de deploy Render.com
├── LICENSE                 # Licença Apache 2.0
└── README.md               # Este arquivo
```

## 📦 Instalação

### Pré-requisitos
- Python 3.9 ou superior
- pip (gerenciador de pacotes Python)

### Instalação Local

```bash
# Clone o repositório
git clone https://github.com/saraa452/advanced-etl-pipeline.git
cd advanced-etl-pipeline

# Crie um ambiente virtual (recomendado)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# Instale as dependências
pip install -r requirements.txt
```

## 🎯 Como Usar

### Executar o Pipeline ETL

```bash
# Executa o pipeline completo
python main.py --run

# ou simplesmente
python main.py
```

### Iniciar o Dashboard

```bash
# Inicia o servidor de dashboard
python main.py --dashboard

# Com porta personalizada
python main.py --dashboard --port 8080
```

Acesse: http://localhost:8050

### Usar como Biblioteca

```python
# Importar módulos
from extract.csv_extractor import extract_from_csv
from transform.clean import clean_data
from transform.join import aggregate_data
from load.to_sqlite import load_to_sqlite
from load.to_csv import load_to_csv

# Extrair dados
df = extract_from_csv('config/sample_data.csv')

# Transformar
df_clean = clean_data(df, remove_duplicates=True)
df_agg = aggregate_data(df_clean, group_by='category', aggregations={'price': 'sum'})

# Carregar
load_to_sqlite(df_agg, 'sales_summary', 'output/data.db')
load_to_csv(df_agg, 'output/sales_summary.csv')
```

## 🧪 Testes

```bash
# Executar todos os testes
pytest

# Com cobertura de código
pytest --cov=. --cov-report=html

# Testes específicos
pytest tests/test_extract.py
pytest tests/test_transform.py
pytest tests/test_load.py

# Modo verboso
pytest -v
```

## 🚀 Deploy no Render.com

O projeto está configurado para deploy automático no Render.com:

1. Faça fork deste repositório
2. Crie uma conta no [Render.com](https://render.com)
3. Clique em "New +" → "Blueprint"
4. Conecte seu repositório GitHub
5. O deploy será feito automaticamente usando o `render.yaml`

### Configuração Manual

```yaml
# render.yaml já está configurado com:
services:
  - type: web
    name: etl-dashboard
    runtime: python
    buildCommand: pip install -r requirements.txt
    startCommand: gunicorn visualize.dash_app:server --bind 0.0.0.0:$PORT
```

## 📊 Dados de Exemplo

O arquivo `config/sample_data.csv` contém dados de vendas simulados com:

| Coluna | Descrição |
|--------|-----------|
| id | Identificador único |
| product | Nome do produto |
| category | Categoria (Electronics, Accessories, Office) |
| price | Preço unitário |
| quantity | Quantidade vendida |
| region | Região (North, South, East, West) |
| date | Data da venda |
| customer_id | ID do cliente |
| sales_rep | Representante de vendas |

## 🛠️ Tecnologias

- **Python 3.9+** - Linguagem principal
- **Pandas** - Manipulação de dados
- **SQLAlchemy** - ORM e conexão com banco de dados
- **Dash/Plotly** - Dashboard interativo
- **pytest** - Framework de testes
- **Gunicorn** - Servidor WSGI para produção

## 📈 Métricas Calculadas

O pipeline calcula automaticamente:

- **Total Value**: Receita total (price × quantity)
- **Percentuais**: Distribuição por categoria/região
- **Rankings**: Produtos mais vendidos
- **Estatísticas**: Média, mínimo, máximo por grupo

## 🤝 Contribuição

Contribuições são bem-vindas! Por favor:

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -m 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

## 📝 Licença

Este projeto está licenciado sob a Licença Apache 2.0 - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 👤 Autor

**Sara** - [GitHub](https://github.com/saraa452)

---

⭐ Se este projeto foi útil para você, considere dar uma estrela!
