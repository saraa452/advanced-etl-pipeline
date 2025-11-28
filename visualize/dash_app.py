"""
Professional ETL Dashboard with Dash

This module provides a professional web dashboard for visualizing ETL pipeline data.
Features interactive filters, charts, and real-time data exploration.
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime

from config import SAMPLE_DATA_PATH, OUTPUT_DIR, DATABASE_PATH
from extract.csv_extractor import extract_from_csv
from extract.api import extract_mock_api_data
from transform.clean import clean_data
from transform.join import aggregate_data, calculate_metrics
from load.to_sqlite import load_to_sqlite, query_sqlite

# Initialize the Dash app
app = dash.Dash(
    __name__,
    title='ETL Pipeline Dashboard',
    meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}]
)

# For Render.com deployment
server = app.server

# Professional color scheme
COLORS = {
    'primary': '#2C3E50',
    'secondary': '#3498DB',
    'success': '#27AE60',
    'warning': '#F39C12',
    'danger': '#E74C3C',
    'light': '#ECF0F1',
    'dark': '#1A252F',
    'background': '#F8F9FA',
    'card': '#FFFFFF'
}

# Chart template
CHART_TEMPLATE = 'plotly_white'


def load_data():
    """Load and prepare data for the dashboard."""
    try:
        # Try to load from CSV first
        if os.path.exists(SAMPLE_DATA_PATH):
            df = extract_from_csv(SAMPLE_DATA_PATH)
            df = clean_data(df, handle_missing='keep', remove_duplicates=True)
            
            # Ensure date column is datetime
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
            
            # Calculate total_value if not present
            if 'total_value' not in df.columns and 'price' in df.columns and 'quantity' in df.columns:
                df['total_value'] = df['price'] * df['quantity']
            
            return df
    except Exception as e:
        print(f"Error loading CSV: {e}")
    
    # Fallback to mock data
    df = extract_mock_api_data()
    if 'total_value' not in df.columns:
        df['total_value'] = df['price'] * df['quantity']
    return df


# Load initial data
df = load_data()

# App layout
app.layout = html.Div([
    # Header
    html.Div([
        html.Div([
            html.H1('📊 ETL Pipeline Dashboard', 
                   style={'margin': '0', 'color': COLORS['card'], 'fontWeight': '600'}),
            html.P('Advanced Data Pipeline Visualization & Analytics',
                  style={'margin': '5px 0 0 0', 'color': COLORS['light'], 'fontSize': '14px'})
        ], style={'flex': '1'}),
        html.Div([
            html.Span(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                     style={'color': COLORS['light'], 'fontSize': '12px'})
        ])
    ], style={
        'display': 'flex',
        'justifyContent': 'space-between',
        'alignItems': 'center',
        'padding': '20px 30px',
        'backgroundColor': COLORS['primary'],
        'marginBottom': '20px'
    }),
    
    # Main content
    html.Div([
        # Filters Section
        html.Div([
            html.H3('🔍 Filters', style={'marginBottom': '15px', 'color': COLORS['primary']}),
            
            html.Div([
                html.Div([
                    html.Label('Category', style={'fontWeight': '600', 'marginBottom': '5px'}),
                    dcc.Dropdown(
                        id='category-filter',
                        options=[{'label': 'All Categories', 'value': 'all'}] + 
                                [{'label': c, 'value': c} for c in df['category'].unique()],
                        value='all',
                        clearable=False,
                        style={'width': '100%'}
                    )
                ], style={'flex': '1', 'marginRight': '15px'}),
                
                html.Div([
                    html.Label('Region', style={'fontWeight': '600', 'marginBottom': '5px'}),
                    dcc.Dropdown(
                        id='region-filter',
                        options=[{'label': 'All Regions', 'value': 'all'}] + 
                                [{'label': r, 'value': r} for r in df['region'].unique()],
                        value='all',
                        clearable=False,
                        style={'width': '100%'}
                    )
                ], style={'flex': '1', 'marginRight': '15px'}),
                
                html.Div([
                    html.Label('Price Range', style={'fontWeight': '600', 'marginBottom': '5px'}),
                    dcc.RangeSlider(
                        id='price-filter',
                        min=df['price'].min(),
                        max=df['price'].max(),
                        step=10,
                        value=[df['price'].min(), df['price'].max()],
                        marks={int(v): f'${int(v)}' for v in [df['price'].min(), df['price'].max()]},
                        tooltip={'placement': 'bottom', 'always_visible': False}
                    )
                ], style={'flex': '2'})
            ], style={'display': 'flex', 'alignItems': 'flex-end'})
        ], style={
            'backgroundColor': COLORS['card'],
            'padding': '20px',
            'borderRadius': '10px',
            'marginBottom': '20px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        }),
        
        # KPI Cards
        html.Div([
            html.Div([
                html.Div([
                    html.Span('💰', style={'fontSize': '30px'}),
                    html.Div([
                        html.H4('Total Revenue', style={'margin': '0', 'color': COLORS['dark'], 'fontSize': '14px'}),
                        html.H2(id='total-revenue', style={'margin': '5px 0', 'color': COLORS['success']})
                    ], style={'marginLeft': '15px'})
                ], style={'display': 'flex', 'alignItems': 'center'})
            ], style={
                'backgroundColor': COLORS['card'],
                'padding': '20px',
                'borderRadius': '10px',
                'flex': '1',
                'marginRight': '15px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            }),
            
            html.Div([
                html.Div([
                    html.Span('📦', style={'fontSize': '30px'}),
                    html.Div([
                        html.H4('Total Products', style={'margin': '0', 'color': COLORS['dark'], 'fontSize': '14px'}),
                        html.H2(id='total-products', style={'margin': '5px 0', 'color': COLORS['secondary']})
                    ], style={'marginLeft': '15px'})
                ], style={'display': 'flex', 'alignItems': 'center'})
            ], style={
                'backgroundColor': COLORS['card'],
                'padding': '20px',
                'borderRadius': '10px',
                'flex': '1',
                'marginRight': '15px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            }),
            
            html.Div([
                html.Div([
                    html.Span('📊', style={'fontSize': '30px'}),
                    html.Div([
                        html.H4('Avg Price', style={'margin': '0', 'color': COLORS['dark'], 'fontSize': '14px'}),
                        html.H2(id='avg-price', style={'margin': '5px 0', 'color': COLORS['warning']})
                    ], style={'marginLeft': '15px'})
                ], style={'display': 'flex', 'alignItems': 'center'})
            ], style={
                'backgroundColor': COLORS['card'],
                'padding': '20px',
                'borderRadius': '10px',
                'flex': '1',
                'marginRight': '15px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            }),
            
            html.Div([
                html.Div([
                    html.Span('🛒', style={'fontSize': '30px'}),
                    html.Div([
                        html.H4('Total Quantity', style={'margin': '0', 'color': COLORS['dark'], 'fontSize': '14px'}),
                        html.H2(id='total-quantity', style={'margin': '5px 0', 'color': COLORS['danger']})
                    ], style={'marginLeft': '15px'})
                ], style={'display': 'flex', 'alignItems': 'center'})
            ], style={
                'backgroundColor': COLORS['card'],
                'padding': '20px',
                'borderRadius': '10px',
                'flex': '1',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            })
        ], style={'display': 'flex', 'marginBottom': '20px'}),
        
        # Charts Row 1
        html.Div([
            html.Div([
                html.H4('Revenue by Category', style={'marginBottom': '10px', 'color': COLORS['primary']}),
                dcc.Graph(id='category-chart', config={'displayModeBar': False})
            ], style={
                'backgroundColor': COLORS['card'],
                'padding': '20px',
                'borderRadius': '10px',
                'flex': '1',
                'marginRight': '15px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            }),
            
            html.Div([
                html.H4('Revenue by Region', style={'marginBottom': '10px', 'color': COLORS['primary']}),
                dcc.Graph(id='region-chart', config={'displayModeBar': False})
            ], style={
                'backgroundColor': COLORS['card'],
                'padding': '20px',
                'borderRadius': '10px',
                'flex': '1',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            })
        ], style={'display': 'flex', 'marginBottom': '20px'}),
        
        # Charts Row 2
        html.Div([
            html.Div([
                html.H4('Top 10 Products by Revenue', style={'marginBottom': '10px', 'color': COLORS['primary']}),
                dcc.Graph(id='products-chart', config={'displayModeBar': False})
            ], style={
                'backgroundColor': COLORS['card'],
                'padding': '20px',
                'borderRadius': '10px',
                'flex': '2',
                'marginRight': '15px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            }),
            
            html.Div([
                html.H4('Price vs Quantity', style={'marginBottom': '10px', 'color': COLORS['primary']}),
                dcc.Graph(id='scatter-chart', config={'displayModeBar': False})
            ], style={
                'backgroundColor': COLORS['card'],
                'padding': '20px',
                'borderRadius': '10px',
                'flex': '1',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            })
        ], style={'display': 'flex', 'marginBottom': '20px'}),
        
        # Data Table
        html.Div([
            html.H4('📋 Data Table', style={'marginBottom': '15px', 'color': COLORS['primary']}),
            dash_table.DataTable(
                id='data-table',
                columns=[{'name': col, 'id': col} for col in df.columns if col != 'total_value'] + 
                        [{'name': 'total_value', 'id': 'total_value', 'type': 'numeric', 
                          'format': {'specifier': ',.2f'}}],
                page_size=10,
                sort_action='native',
                filter_action='native',
                style_table={'overflowX': 'auto'},
                style_header={
                    'backgroundColor': COLORS['primary'],
                    'color': COLORS['card'],
                    'fontWeight': 'bold',
                    'textAlign': 'center'
                },
                style_cell={
                    'textAlign': 'left',
                    'padding': '10px',
                    'minWidth': '80px',
                    'maxWidth': '180px',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis'
                },
                style_data_conditional=[
                    {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': COLORS['light']
                    }
                ]
            )
        ], style={
            'backgroundColor': COLORS['card'],
            'padding': '20px',
            'borderRadius': '10px',
            'marginBottom': '20px',
            'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
        }),
        
        # Footer
        html.Div([
            html.P([
                '🚀 Advanced ETL Pipeline Dashboard | ',
                html.A('GitHub Repository', href='https://github.com/saraa452/advanced-etl-pipeline', 
                      target='_blank', style={'color': COLORS['secondary']}),
                ' | Built with Dash & Plotly'
            ], style={'textAlign': 'center', 'color': COLORS['dark'], 'margin': '0'})
        ], style={'padding': '20px'})
        
    ], style={'padding': '0 30px 30px 30px', 'backgroundColor': COLORS['background'], 'minHeight': '100vh'})
], style={'fontFamily': "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif", 'backgroundColor': COLORS['background']})


@app.callback(
    [Output('total-revenue', 'children'),
     Output('total-products', 'children'),
     Output('avg-price', 'children'),
     Output('total-quantity', 'children'),
     Output('category-chart', 'figure'),
     Output('region-chart', 'figure'),
     Output('products-chart', 'figure'),
     Output('scatter-chart', 'figure'),
     Output('data-table', 'data')],
    [Input('category-filter', 'value'),
     Input('region-filter', 'value'),
     Input('price-filter', 'value')]
)
def update_dashboard(category, region, price_range):
    """Update all dashboard components based on filters."""
    # Filter data
    filtered_df = df.copy()
    
    if category != 'all':
        filtered_df = filtered_df[filtered_df['category'] == category]
    
    if region != 'all':
        filtered_df = filtered_df[filtered_df['region'] == region]
    
    if price_range:
        filtered_df = filtered_df[
            (filtered_df['price'] >= price_range[0]) & 
            (filtered_df['price'] <= price_range[1])
        ]
    
    # Calculate KPIs
    total_revenue = f"${filtered_df['total_value'].sum():,.2f}"
    total_products = f"{len(filtered_df):,}"
    avg_price = f"${filtered_df['price'].mean():,.2f}" if len(filtered_df) > 0 else "$0.00"
    total_quantity = f"{filtered_df['quantity'].sum():,}"
    
    # Category chart
    cat_data = filtered_df.groupby('category')['total_value'].sum().reset_index()
    cat_chart = px.pie(
        cat_data, 
        values='total_value', 
        names='category',
        color_discrete_sequence=px.colors.qualitative.Set2,
        hole=0.4
    )
    cat_chart.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        template=CHART_TEMPLATE,
        showlegend=True,
        legend=dict(orientation='h', yanchor='bottom', y=-0.2)
    )
    
    # Region chart
    reg_data = filtered_df.groupby('region')['total_value'].sum().reset_index()
    reg_chart = px.bar(
        reg_data,
        x='region',
        y='total_value',
        color='region',
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    reg_chart.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        template=CHART_TEMPLATE,
        showlegend=False,
        xaxis_title='',
        yaxis_title='Revenue ($)'
    )
    
    # Products chart
    prod_data = filtered_df.groupby('product')['total_value'].sum().reset_index()
    prod_data = prod_data.nlargest(10, 'total_value')
    prod_chart = px.bar(
        prod_data,
        x='total_value',
        y='product',
        orientation='h',
        color='total_value',
        color_continuous_scale='Blues'
    )
    prod_chart.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        template=CHART_TEMPLATE,
        showlegend=False,
        xaxis_title='Revenue ($)',
        yaxis_title='',
        yaxis={'categoryorder': 'total ascending'},
        coloraxis_showscale=False
    )
    
    # Scatter chart
    scatter_chart = px.scatter(
        filtered_df,
        x='price',
        y='quantity',
        color='category',
        size='total_value',
        hover_data=['product'],
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    scatter_chart.update_layout(
        margin=dict(l=20, r=20, t=20, b=20),
        template=CHART_TEMPLATE,
        xaxis_title='Price ($)',
        yaxis_title='Quantity',
        legend=dict(orientation='h', yanchor='bottom', y=-0.3)
    )
    
    # Table data
    table_data = filtered_df.to_dict('records')
    
    return (
        total_revenue, 
        total_products, 
        avg_price, 
        total_quantity,
        cat_chart, 
        reg_chart, 
        prod_chart, 
        scatter_chart,
        table_data
    )


if __name__ == '__main__':
    # Get port from environment variable (for Render.com deployment)
    port = int(os.environ.get('PORT', 8050))
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    app.run(debug=debug, host='0.0.0.0', port=port)
