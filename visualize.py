"""Módulo de visualização - gera gráficos interativos e página HTML"""

import logging
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class DataVisualizer:
    """Classe responsável pela criação de visualizações e relatórios"""
    
    def __init__(self, output_path='./output'):
        """Inicializa o visualizador
        
        Args:
            output_path: Caminho para salvar os gráficos e HTML
        """
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.html_file = self.output_path / 'report.html'
    
    def generate_report(self, data):
        """Gera um relatório HTML completo com todos os gráficos
        
        Args:
            data: pd.DataFrame com dados transformados
        """
        if data.empty:
            logger.warning("Dados vazios para visualização")
            return
        
        logger.info("Gerando relatório visual...")
        
        try:
            # Criar gráficos
            figs = self._create_all_charts(data)
            
            # Gerar HTML
            html_content = self._generate_html(data, figs)
            
            # Salvar arquivo
            with open(self.html_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"Relatório salvo em: {self.html_file}")
            
        except Exception as e:
            logger.error(f"Erro ao gerar relatório: {str(e)}")
    
    def _create_all_charts(self, data):
        """Cria todos os gráficos
        
        Args:
            data: pd.DataFrame
            
        Returns:
            dict: Dicionário com gráficos HTML
        """
        logger.info("Criando gráficos...")
        
        figs = {
            'casos_por_pais': self._chart_casos_por_pais(data),
            'mortes_por_pais': self._chart_mortes_por_pais(data),
            'top_10_mortalidade': self._chart_top_mortalidade(data),
            'top_10_recuperacao': self._chart_top_recuperacao(data),
            'casos_vs_populacao': self._chart_casos_vs_populacao(data),
            'distribuicao_continentes': self._chart_distribuicao_continentes(data),
            'heatmap_metricas': self._chart_heatmap(data),
            'scatter_taxa_mortalidade': self._chart_scatter_mortalidade(data),
        }
        
        return figs
    
    def _chart_casos_por_pais(self, data):
        """Gráfico de barras: top 15 países por casos"""
        top_15 = data.nlargest(15, 'cases')[['country', 'cases']].sort_values('cases')
        
        fig = go.Figure(data=[
            go.Bar(
                y=top_15['country'],
                x=top_15['cases'],
                orientation='h',
                marker=dict(
                    color=top_15['cases'],
                    colorscale='Blues',
                    showscale=True,
                    colorbar=dict(title="Casos")
                ),
                hovertemplate='<b>%{y}</b><br>Casos: %{x:,.0f}<extra></extra>'
            )
        ])
        
        fig.update_layout(
            title='Top 15 Países por Número de Casos',
            xaxis_title='Número de Casos',
            yaxis_title='País',
            height=500,
            template='plotly_white',
            hovermode='closest'
        )
        
        return fig.to_html(include_plotlyjs='cdn', div_id='chart_casos')
    
    def _chart_mortes_por_pais(self, data):
        """Gráfico de barras: top 15 países por mortes"""
        top_15 = data.nlargest(15, 'deaths')[['country', 'deaths']].sort_values('deaths')
        
        fig = go.Figure(data=[
            go.Bar(
                y=top_15['country'],
                x=top_15['deaths'],
                orientation='h',
                marker=dict(
                    color=top_15['deaths'],
                    colorscale='Reds',
                    showscale=True,
                    colorbar=dict(title="Óbitos")
                ),
                hovertemplate='<b>%{y}</b><br>Óbitos: %{x:,.0f}<extra></extra>'
            )
        ])
        
        fig.update_layout(
            title='Top 15 Países por Número de Óbitos',
            xaxis_title='Número de Óbitos',
            yaxis_title='País',
            height=500,
            template='plotly_white',
            hovermode='closest'
        )
        
        return fig.to_html(include_plotlyjs=False, div_id='chart_mortes')
    
    def _chart_top_mortalidade(self, data):
        """Gráfico de barras: países com maior taxa de mortalidade"""
        # Filtrar apenas países com casos > 0
        data_filtered = data[data['cases'] > 0].copy()
        top_10 = data_filtered.nlargest(10, 'mortality_rate')[['country', 'mortality_rate']].sort_values('mortality_rate')
        
        fig = go.Figure(data=[
            go.Bar(
                y=top_10['country'],
                x=top_10['mortality_rate'],
                orientation='h',
                marker=dict(
                    color=top_10['mortality_rate'],
                    colorscale='YlOrRd',
                    showscale=True,
                    colorbar=dict(title="Taxa (%)")
                ),
                hovertemplate='<b>%{y}</b><br>Taxa: %{x:.2f}%<extra></extra>'
            )
        ])
        
        fig.update_layout(
            title='Top 10 Países com Maior Taxa de Mortalidade (%)',
            xaxis_title='Taxa de Mortalidade (%)',
            yaxis_title='País',
            height=450,
            template='plotly_white',
            hovermode='closest'
        )
        
        return fig.to_html(include_plotlyjs=False, div_id='chart_mortalidade')
    
    def _chart_top_recuperacao(self, data):
        """Gráfico de barras: países com maior taxa de recuperação"""
        data_filtered = data[data['cases'] > 0].copy()
        top_10 = data_filtered.nlargest(10, 'recovery_rate')[['country', 'recovery_rate']].sort_values('recovery_rate')
        
        fig = go.Figure(data=[
            go.Bar(
                y=top_10['country'],
                x=top_10['recovery_rate'],
                orientation='h',
                marker=dict(
                    color=top_10['recovery_rate'],
                    colorscale='Greens',
                    showscale=True,
                    colorbar=dict(title="Taxa (%)")
                ),
                hovertemplate='<b>%{y}</b><br>Taxa: %{x:.2f}%<extra></extra>'
            )
        ])
        
        fig.update_layout(
            title='Top 10 Países com Maior Taxa de Recuperação (%)',
            xaxis_title='Taxa de Recuperação (%)',
            yaxis_title='País',
            height=450,
            template='plotly_white',
            hovermode='closest'
        )
        
        return fig.to_html(include_plotlyjs=False, div_id='chart_recuperacao')
    
    def _chart_casos_vs_populacao(self, data):
        """Gráfico scatter: casos por população"""
        data_filtered = data[data['population'] > 0].copy()
        top_20 = data_filtered.nlargest(20, 'cases_per_million')
        
        fig = go.Figure(data=[
            go.Scatter(
                x=top_20['population'],
                y=top_20['cases_per_million'],
                mode='markers+text',
                marker=dict(
                    size=top_20['cases']/1000000,
                    color=top_20['mortality_rate'],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="Mortalidade (%)"),
                    line=dict(width=1, color='white')
                ),
                text=top_20['country'],
                textposition='top center',
                textfont=dict(size=8),
                hovertemplate='<b>%{text}</b><br>População: %{x:,.0f}<br>Casos/Milhão: %{y:.2f}<extra></extra>'
            )
        ])
        
        fig.update_layout(
            title='Casos por Milhão de Habitantes vs População<br><sub>Tamanho da bolha = Total de Casos</sub>',
            xaxis_title='População',
            yaxis_title='Casos por Milhão',
            xaxis_type='log',
            height=500,
            template='plotly_white',
            hovermode='closest'
        )
        
        return fig.to_html(include_plotlyjs=False, div_id='chart_casos_populacao')
    
    def _chart_distribuicao_continentes(self, data):
        """Gráfico pizza: distribuição por continentes"""
        if 'continent' not in data.columns:
            return '<div></div>'
        
        continent_data = data.groupby('continent')['cases'].sum().sort_values(ascending=False)
        
        fig = go.Figure(data=[
            go.Pie(
                labels=continent_data.index,
                values=continent_data.values,
                hole=0.3,
                hovertemplate='<b>%{label}</b><br>Casos: %{value:,.0f}<br>%{percent}<extra></extra>'
            )
        ])
        
        fig.update_layout(
            title='Distribuição de Casos por Continente',
            height=450,
            template='plotly_white'
        )
        
        return fig.to_html(include_plotlyjs=False, div_id='chart_continentes')
    
    def _chart_heatmap(self, data):
        """Gráfico heatmap: matriz de correlação entre métricas"""
        # Selecionar top 20 países
        top_20 = data.nlargest(20, 'cases')[['country', 'cases', 'deaths', 'recovered', 'mortality_rate', 'recovery_rate']].set_index('country')
        
        # Normalizar dados para melhor visualização
        normalized = (top_20 - top_20.min()) / (top_20.max() - top_20.min())
        
        fig = go.Figure(data=go.Heatmap(
            z=normalized.T.values,
            x=normalized.index,
            y=normalized.columns,
            colorscale='RdYlGn_r',
            hovertemplate='País: %{x}<br>Métrica: %{y}<br>Valor: %{z:.2f}<extra></extra>'
        ))
        
        fig.update_layout(
            title='Heatmap de Métricas - Top 20 Países (Normalizado)',
            xaxis_title='País',
            yaxis_title='Métrica',
            height=450,
            template='plotly_white'
        )
        
        return fig.to_html(include_plotlyjs=False, div_id='chart_heatmap')
    
    def _chart_scatter_mortalidade(self, data):
        """Gráfico scatter: mortalidade vs recuperação"""
        data_filtered = data[(data['cases'] > 0) & (data['population'] > 0)].copy()
        
        fig = go.Figure()
        
        # Adicionar pontos por continente (se disponível)
        if 'continent' in data_filtered.columns:
            continentes = data_filtered['continent'].unique()
            colors = px.colors.qualitative.Set2
            
            for i, cont in enumerate(continentes):
                cont_data = data_filtered[data_filtered['continent'] == cont]
                fig.add_trace(go.Scatter(
                    x=cont_data['mortality_rate'],
                    y=cont_data['recovery_rate'],
                    mode='markers+text',
                    name=cont,
                    marker=dict(size=8, color=colors[i % len(colors)]),
                    text=cont_data['country'],
                    textposition='top center',
                    textfont=dict(size=7),
                    hovertemplate='<b>%{text}</b><br>Mortalidade: %{x:.2f}%<br>Recuperação: %{y:.2f}%<extra></extra>'
                ))
        else:
            fig.add_trace(go.Scatter(
                x=data_filtered['mortality_rate'],
                y=data_filtered['recovery_rate'],
                mode='markers',
                marker=dict(
                    size=data_filtered['cases']/1000000,
                    color=data_filtered['cases_per_million'],
                    colorscale='Viridis',
                    showscale=True
                ),
                text=data_filtered['country'],
                hovertemplate='<b>%{text}</b><br>Mortalidade: %{x:.2f}%<br>Recuperação: %{y:.2f}%<extra></extra>'
            ))
        
        fig.update_layout(
            title='Taxa de Mortalidade vs Taxa de Recuperação',
            xaxis_title='Taxa de Mortalidade (%)',
            yaxis_title='Taxa de Recuperação (%)',
            height=500,
            template='plotly_white',
            hovermode='closest'
        )
        
        return fig.to_html(include_plotlyjs=False, div_id='chart_scatter')
    
    def _generate_html(self, data, figs):
        """Gera o arquivo HTML completo
        
        Args:
            data: pd.DataFrame
            figs: dict com gráficos HTML
            
        Returns:
            str: Conteúdo HTML
        """
        # Calcular estatísticas
        stats = self._calculate_stats(data)
        
        html = f"""
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Relatório ETL - Dados COVID-19</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
            padding: 20px;
            min-height: 100vh;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 10px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }}
        
        header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px 20px;
            text-align: center;
        }}
        
        header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        
        header p {{
            font-size: 1.1em;
            opacity: 0.9;
        }}
        
        .timestamp {{
            font-size: 0.9em;
            opacity: 0.8;
            margin-top: 10px;
        }}
        
        .stats-container {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 30px 20px;
            background: #f8f9fa;
            border-bottom: 1px solid #e0e0e0;
        }}
        
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            text-align: center;
            border-top: 4px solid #667eea;
        }}
        
        .stat-card h3 {{
            font-size: 0.9em;
            color: #666;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .stat-card .value {{
            font-size: 1.8em;
            font-weight: bold;
            color: #667eea;
        }}
        
        .content {{
            padding: 30px 20px;
        }}
        
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
            gap: 30px;
            margin-bottom: 30px;
        }}
        
        .full-width {{
            grid-column: 1 / -1;
        }}
        
        .chart-container {{
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 20px;
            transition: transform 0.2s, box-shadow 0.2s;
        }}
        
        .chart-container:hover {{
            transform: translateY(-5px);
            box-shadow: 0 5px 20px rgba(0,0,0,0.15);
        }}
        
        .chart-container h2 {{
            font-size: 1.3em;
            margin-bottom: 15px;
            color: #667eea;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }}
        
        footer {{
            background: #f8f9fa;
            padding: 20px;
            text-align: center;
            color: #999;
            border-top: 1px solid #e0e0e0;
            margin-top: 30px;
        }}
        
        .refresh-info {{
            background: #e3f2fd;
            border-left: 4px solid #2196F3;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 4px;
            color: #1976D2;
        }}
        
        @media (max-width: 768px) {{
            .charts-grid {{
                grid-template-columns: 1fr;
            }}
            
            header h1 {{
                font-size: 1.8em;
            }}
            
            .stats-container {{
                grid-template-columns: repeat(2, 1fr);
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1> Relatório de Análise COVID-19</h1>
            <p>Pipeline ETL Avançado - Visualização Automática de Dados</p>
            <div class="timestamp">Gerado em {datetime.now().strftime('%d/%m/%Y às %H:%M:%S')}</div>
        </header>
        
        <div class="stats-container">
            <div class="stat-card">
                <h3>Total de Países</h3>
                <div class="value">{stats['total_paises']}</div>
            </div>
            <div class="stat-card">
                <h3>Total de Casos</h3>
                <div class="value">{stats['total_casos']:,.0f}</div>
            </div>
            <div class="stat-card">
                <h3>Total de Óbitos</h3>
                <div class="value">{stats['total_mortes']:,.0f}</div>
            </div>
            <div class="stat-card">
                <h3>Total Recuperados</h3>
                <div class="value">{stats['total_recuperados']:,.0f}</div>
            </div>
            <div class="stat-card">
                <h3>Taxa Média de Mortalidade</h3>
                <div class="value">{stats['taxa_mortalidade_media']:.2f}%</div>
            </div>
            <div class="stat-card">
                <h3>Taxa Média de Recuperação</h3>
                <div class="value">{stats['taxa_recuperacao_media']:.2f}%</div>
            </div>
        </div>
        
        <div class="content">
            <div class="refresh-info">
                 Dados atualizados automaticamente. Esta página foi gerada pelo pipeline ETL e pode ser atualizada reexecutando main.py
            </div>
            
            <div class="charts-grid">
                <div class="chart-container">
                    <h2> Casos por País</h2>
                    {figs['casos_por_pais']}
                </div>
                
                <div class="chart-container">
                    <h2> Óbitos por País</h2>
                    {figs['mortes_por_pais']}
                </div>
                
                <div class="chart-container">
                    <h2> Taxa de Mortalidade</h2>
                    {figs['top_10_mortalidade']}
                </div>
                
                <div class="chart-container">
                    <h2> Taxa de Recuperação</h2>
                    {figs['top_10_recuperacao']}
                </div>
                
                <div class="chart-container full-width">
                    <h2> Distribuição por Continente</h2>
                    {figs['distribuicao_continentes']}
                </div>
                
                <div class="chart-container full-width">
                    <h2> Casos por Milhão vs População</h2>
                    {figs['casos_vs_populacao']}
                </div>
                
                <div class="chart-container full-width">
                    <h2> Taxa de Mortalidade vs Recuperação</h2>
                    {figs['scatter_taxa_mortalidade']}
                </div>
                
                <div class="chart-container full-width">
                    <h2> Heatmap de Métricas</h2>
                    {figs['heatmap_metricas']}
                </div>
            </div>
        </div>
        
        <footer>
            <p> Relatório gerado automaticamente pelo Pipeline ETL Avançado</p>
            <p>Dados de origem: API disease.sh COVID-19 + Dados Locais CSV</p>
            <p>Licença MIT  2024</p>
        </footer>
    </div>
</body>
</html>
"""
        return html
    
    def _calculate_stats(self, data):
        """Calcula estatísticas para o relatório
        
        Args:
            data: pd.DataFrame
            
        Returns:
            dict: Estatísticas calculadas
        """
        stats = {
            'total_paises': len(data),
            'total_casos': data['cases'].sum(),
            'total_mortes': data['deaths'].sum(),
            'total_recuperados': data['recovered'].sum(),
            'taxa_mortalidade_media': data['mortality_rate'].mean(),
            'taxa_recuperacao_media': data['recovery_rate'].mean(),
        }
        return stats
