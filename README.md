# 🚀 Advanced ETL Pipeline

Um pipeline ETL avançado e modular para processamento de dados COVID-19 com visualização interativa via Flask.

## ✨ Funcionalidades

- **Extração de múltiplas fontes**: API REST (disease.sh) e arquivos CSV locais
- **Transformação e enriquecimento**: Cálculo de métricas (taxas de mortalidade, recuperação, casos por milhão)
- **Carregamento em múltiplos destinos**: CSV, banco de dados (SQLAlchemy)
- **Visualização interativa**: Dashboard Flask com gráficos Plotly
- **API REST**: Endpoints para acessar dados processados em JSON
- **Logging e monitoramento**: Sistema completo de logs
- **Testes automatizados**: Suite de testes com pytest
- **Arquitetura modular**: Fácil extensão e manutenção

## 📦 Instalação

### Pré-requisitos
- Python 3.8+
- Git

### Passos

```bash
# Clone o repositório
git clone https://github.com/saraa452/advanced-etl-pipeline.git
cd advanced-etl-pipeline

# Crie e ative um ambiente virtual (recomendado)
python -m venv .venv
.venv\Scripts\Activate.ps1  # Windows PowerShell
# ou
source .venv/bin/activate    # Linux/Mac

# Instale as dependências
pip install -r requirements.txt
```

##  Uso Rápido

### Opção 1: Pipeline ETL + Relatório HTML Estático

Execute o pipeline completo e gere um relatório HTML estático:

```bash
python main.py
```

Ou abra o relatório automaticamente no navegador:

```bash
python run_dashboard.py
```

### Opção 2: Dashboard Flask Interativo (Recomendado)

Inicie o servidor Flask para visualização web interativa:

```bash
python app.py
```

O servidor estará disponível em: **http://localhost:5000**

#### Endpoints disponíveis:

- `GET /` - Dashboard interativo com todos os gráficos
- `GET /api/data` - Retorna dados processados em JSON
- `GET /refresh` ou `POST /refresh` - Regenera o relatório executando o pipeline ETL

#### Exemplo de uso da API:

```bash
# Ver dados em JSON
curl http://localhost:5000/api/data

# Regenerar relatório
curl -X POST http://localhost:5000/refresh
```

##  Estrutura do Projeto

```
advanced-etl-pipeline/
 config/              # Configurações
    __init__.py
    settings.py      # Configurações centralizadas
 extract/             # Módulo de extração
    __init__.py
    extractor.py     # Extrai dados de API e CSV
 transform/           # Módulo de transformação
    __init__.py
    transformer.py   # Transforma e enriquece dados
 load/                # Módulo de carregamento
    __init__.py
    loader.py        # Salva em CSV e banco de dados
 tests/               # Testes automatizados
    __init__.py
    test_extractor.py
    test_transformer.py
    test_loader.py
 output/              # Arquivos gerados
    report.html      # Relatório visual
    results.csv      # Dados processados
 app.py               # Servidor Flask (dashboard web)
 main.py              # Pipeline ETL principal
 run_dashboard.py     # Executa pipeline + abre navegador
 visualize.py         # Gera gráficos e relatório HTML
 requirements.txt     # Dependências Python
 LICENSE              # Licença MIT
 README.md            # Este arquivo
```

##  Testes

Execute a suite completa de testes:

```bash
pytest tests/ -v
```

Execute testes com cobertura:

```bash
pytest tests/ --cov=. --cov-report=html
```

##  Tecnologias Utilizadas

- **Python 3.8+**: Linguagem principal
- **Pandas**: Manipulação e análise de dados
- **Plotly**: Visualização interativa de dados
- **Flask**: Framework web para dashboard
- **SQLAlchemy**: ORM para banco de dados
- **Requests**: Cliente HTTP para APIs
- **Pytest**: Framework de testes
- **Python-dotenv**: Gerenciamento de variáveis de ambiente

##  Visualizações Disponíveis

O dashboard inclui os seguintes gráficos interativos:

1. **Top 15 Países por Casos** - Gráfico de barras horizontal
2. **Top 15 Países por Óbitos** - Gráfico de barras horizontal
3. **Top 10 Taxa de Mortalidade** - Ranking de países
4. **Top 10 Taxa de Recuperação** - Ranking de países
5. **Distribuição por Continente** - Gráfico de pizza
6. **Casos por Milhão vs População** - Scatter plot com bolhas
7. **Taxa de Mortalidade vs Recuperação** - Scatter plot
8. **Heatmap de Métricas** - Matriz de correlação

##  Configuração

Edite `config/settings.py` para personalizar:

- URLs de APIs
- Caminhos de arquivos CSV
- Configurações de banco de dados
- Diretório de saída

##  Deploy

### Opção 1: Executar localmente

```bash
python app.py
```

### Opção 2: Docker (futuro)

```bash
docker build -t etl-pipeline .
docker run -p 5000:5000 etl-pipeline
```

##  Contribuindo

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/MinhaFeature`)
3. Commit suas mudanças (`git commit -m 'Adiciona MinhaFeature'`)
4. Push para a branch (`git push origin feature/MinhaFeature`)
5. Abra um Pull Request

##  Comandos Git Úteis

```bash
# Verificar status
git status

# Adicionar todos os arquivos
git add .

# Commit com mensagem
git commit -m "Adiciona dashboard Flask com visualização interativa"

# Push para o repositório
git push origin main

# Criar nova branch
git checkout -b nome-da-branch
```

##  Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

##  Autor

**Sara**
- GitHub: [@saraa452](https://github.com/saraa452)
- Repositório: [advanced-etl-pipeline](https://github.com/saraa452/advanced-etl-pipeline)

##  Suporte

Se você encontrar algum problema ou tiver sugestões, por favor abra uma [issue](https://github.com/saraa452/advanced-etl-pipeline/issues).

---

 Se este projeto foi útil, considere dar uma estrela no GitHub!
