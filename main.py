"""
Main ETL Pipeline Entry Point

This module orchestrates the complete ETL (Extract, Transform, Load) pipeline.
It demonstrates the full workflow from data extraction to final output.
"""

import os
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import ETL modules
from config import SAMPLE_DATA_PATH, OUTPUT_DIR, DATABASE_PATH
from extract.csv_extractor import extract_from_csv
from extract.api import extract_mock_api_data
from transform.clean import clean_data, fill_missing_values
from transform.join import aggregate_data, calculate_metrics, calculate_percentages
from load.to_sqlite import load_to_sqlite, create_database
from load.to_csv import load_to_csv, export_summary_report


def run_etl_pipeline():
    """
    Execute the complete ETL pipeline.
    
    Steps:
    1. Extract - Load data from CSV and API sources
    2. Transform - Clean, aggregate, and calculate metrics
    3. Load - Save to SQLite database and CSV files
    
    Returns:
        Dictionary with pipeline execution results
    """
    logger.info("=" * 60)
    logger.info("Starting ETL Pipeline")
    logger.info("=" * 60)
    
    results = {
        'start_time': datetime.now(),
        'status': 'running',
        'steps': {}
    }
    
    try:
        # ============================================================
        # EXTRACT PHASE
        # ============================================================
        logger.info("\n--- EXTRACT PHASE ---")
        
        # Extract from CSV
        logger.info(f"Extracting data from CSV: {SAMPLE_DATA_PATH}")
        csv_df = extract_from_csv(SAMPLE_DATA_PATH)
        results['steps']['extract_csv'] = {
            'status': 'success',
            'rows': len(csv_df),
            'columns': list(csv_df.columns)
        }
        logger.info(f"Extracted {len(csv_df)} rows from CSV")
        
        # Extract from mock API (for demonstration)
        logger.info("Extracting data from mock API")
        api_df = extract_mock_api_data()
        results['steps']['extract_api'] = {
            'status': 'success',
            'rows': len(api_df),
            'columns': list(api_df.columns)
        }
        logger.info(f"Extracted {len(api_df)} rows from API")
        
        # ============================================================
        # TRANSFORM PHASE
        # ============================================================
        logger.info("\n--- TRANSFORM PHASE ---")
        
        # Clean CSV data
        logger.info("Cleaning CSV data")
        cleaned_df = clean_data(
            csv_df,
            remove_duplicates=True,
            handle_missing='keep',
            strip_whitespace=True,
            standardize_columns=True
        )
        results['steps']['clean_data'] = {
            'status': 'success',
            'rows_before': len(csv_df),
            'rows_after': len(cleaned_df)
        }
        logger.info(f"Cleaned data: {len(csv_df)} -> {len(cleaned_df)} rows")
        
        # Calculate metrics
        logger.info("Calculating business metrics")
        metrics_df = calculate_metrics(cleaned_df)
        results['steps']['calculate_metrics'] = {
            'status': 'success',
            'new_columns': [col for col in metrics_df.columns if col not in cleaned_df.columns]
        }
        
        # Aggregate by category
        logger.info("Aggregating data by category")
        category_agg = aggregate_data(
            metrics_df,
            group_by='category',
            aggregations={
                'total_value': 'sum',
                'price': ['mean', 'min', 'max'],
                'quantity': 'sum'
            }
        )
        results['steps']['aggregate_category'] = {
            'status': 'success',
            'groups': len(category_agg)
        }
        logger.info(f"Created {len(category_agg)} category aggregations")
        
        # Aggregate by region
        logger.info("Aggregating data by region")
        region_agg = aggregate_data(
            metrics_df,
            group_by='region',
            aggregations={
                'total_value': 'sum',
                'price': 'mean',
                'quantity': 'sum'
            }
        )
        results['steps']['aggregate_region'] = {
            'status': 'success',
            'groups': len(region_agg)
        }
        logger.info(f"Created {len(region_agg)} region aggregations")
        
        # Calculate percentages
        logger.info("Calculating percentage distributions")
        pct_df = calculate_percentages(metrics_df, value_column='total_value')
        
        # ============================================================
        # LOAD PHASE
        # ============================================================
        logger.info("\n--- LOAD PHASE ---")
        
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Load to SQLite
        logger.info(f"Loading data to SQLite: {DATABASE_PATH}")
        
        # Main data table
        rows_loaded = load_to_sqlite(metrics_df, 'sales_data', DATABASE_PATH)
        results['steps']['load_sqlite_main'] = {
            'status': 'success',
            'table': 'sales_data',
            'rows': rows_loaded
        }
        logger.info(f"Loaded {rows_loaded} rows to 'sales_data' table")
        
        # Category aggregation table
        load_to_sqlite(category_agg, 'category_summary', DATABASE_PATH)
        results['steps']['load_sqlite_category'] = {
            'status': 'success',
            'table': 'category_summary',
            'rows': len(category_agg)
        }
        logger.info(f"Loaded {len(category_agg)} rows to 'category_summary' table")
        
        # Region aggregation table
        load_to_sqlite(region_agg, 'region_summary', DATABASE_PATH)
        results['steps']['load_sqlite_region'] = {
            'status': 'success',
            'table': 'region_summary',
            'rows': len(region_agg)
        }
        logger.info(f"Loaded {len(region_agg)} rows to 'region_summary' table")
        
        # Load to CSV
        logger.info("Exporting data to CSV files")
        
        # Main data CSV
        main_csv_path = os.path.join(OUTPUT_DIR, 'processed_data.csv')
        load_to_csv(metrics_df, main_csv_path)
        results['steps']['load_csv_main'] = {
            'status': 'success',
            'file': main_csv_path,
            'rows': len(metrics_df)
        }
        logger.info(f"Exported main data to {main_csv_path}")
        
        # Category summary CSV
        category_csv_path = os.path.join(OUTPUT_DIR, 'category_summary.csv')
        load_to_csv(category_agg, category_csv_path)
        results['steps']['load_csv_category'] = {
            'status': 'success',
            'file': category_csv_path
        }
        logger.info(f"Exported category summary to {category_csv_path}")
        
        # Region summary CSV
        region_csv_path = os.path.join(OUTPUT_DIR, 'region_summary.csv')
        load_to_csv(region_agg, region_csv_path)
        results['steps']['load_csv_region'] = {
            'status': 'success',
            'file': region_csv_path
        }
        logger.info(f"Exported region summary to {region_csv_path}")
        
        # Export summary report
        logger.info("Generating summary reports")
        reports = export_summary_report(metrics_df, OUTPUT_DIR, 'etl_report')
        results['steps']['summary_reports'] = {
            'status': 'success',
            'reports': list(reports.keys())
        }
        logger.info(f"Generated {len(reports)} summary reports")
        
        # ============================================================
        # COMPLETION
        # ============================================================
        results['end_time'] = datetime.now()
        results['duration'] = str(results['end_time'] - results['start_time'])
        results['status'] = 'success'
        
        logger.info("\n" + "=" * 60)
        logger.info("ETL Pipeline Completed Successfully!")
        logger.info(f"Duration: {results['duration']}")
        logger.info("=" * 60)
        
        # Print summary
        print("\n📊 ETL Pipeline Summary:")
        print("-" * 40)
        print(f"  Records Processed: {len(metrics_df)}")
        print(f"  Database: {DATABASE_PATH}")
        print(f"  Output Directory: {OUTPUT_DIR}")
        print(f"  Tables Created: sales_data, category_summary, region_summary")
        print(f"  CSV Files: processed_data.csv, category_summary.csv, region_summary.csv")
        print("-" * 40)
        
        return results
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {str(e)}")
        results['status'] = 'failed'
        results['error'] = str(e)
        results['end_time'] = datetime.now()
        raise


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Advanced ETL Pipeline - Extract, Transform, Load'
    )
    parser.add_argument(
        '--run', 
        action='store_true',
        help='Run the ETL pipeline'
    )
    parser.add_argument(
        '--dashboard',
        action='store_true',
        help='Start the Dash dashboard'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=8050,
        help='Port for the dashboard (default: 8050)'
    )
    
    args = parser.parse_args()
    
    if args.dashboard:
        print("Starting ETL Dashboard...")
        from visualize.dash_app import app
        app.run(debug=True, host='0.0.0.0', port=args.port)
    elif args.run:
        run_etl_pipeline()
    else:
        # Default: run ETL pipeline
        run_etl_pipeline()


if __name__ == '__main__':
    main()
