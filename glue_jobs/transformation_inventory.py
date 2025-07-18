import sys
import json
import boto3
from datetime import datetime, timedelta, date
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
# Define all possible arguments. Those not provided will raise KeyError if accessed directly.
all_expected_args = [
    'JOB_NAME',
    'BUCKET_NAME',
    'DATABASE_NAME',
    'REDSHIFT_CONNECTION',
    'REDSHIFT_SCHEMA',
    'REDSHIFT_PROCESSED_SCHEMA',
    #'PROCESSING_DATE', # This will be the *current* date for daily runs, or the specific date for single-day backfill
    'start_processing_date', # Optional: for backfill range (format YYYY-MM-DD)
    'end_processing_date',   # Optional: for backfill range (format YYYY-MM-DD)
    'TempDir' # Required for Redshift writes
]

args = getResolvedOptions(sys.argv, all_expected_args)

# Configuration parameters
BUCKET_NAME = args['BUCKET_NAME']
DATABASE_NAME = args['DATABASE_NAME']
REDSHIFT_CONNECTION = args['REDSHIFT_CONNECTION']
REDSHIFT_KPI_SCHEMA = args['REDSHIFT_SCHEMA']
REDSHIFT_PROCESSED_SCHEMA = args['REDSHIFT_PROCESSED_SCHEMA']

# --- AUTOMATIC GENERATION OF RUN_TIMESTAMP ---
# Use UTC for consistency across AWS services
current_utc_time = datetime.utcnow()
# RUN_TIMESTAMP: Format: YYYYMMDD_HHMMSS (for unique job run identification)
RUN_TIMESTAMP = current_utc_time.strftime('%Y%m%d_%H%M%S')
# --- END AUTOMATIC GENERATION ---

# S3 paths configuration
S3_PATHS = {
    'silver_inventory': f's3://{BUCKET_NAME}/processed/inventory/',
    'silver_pos': f's3://{BUCKET_NAME}/processed/pos/',
    'kpi_errors': f's3://{BUCKET_NAME}/errors/kpi/',
    'logs': f's3://{BUCKET_NAME}/logs/kpi/'
}

# Redshift table names
REDSHIFT_KPI_TABLES = {
    'sales_kpi': f'{REDSHIFT_KPI_SCHEMA}.sales_kpi_daily',
    'inventory_kpi': f'{REDSHIFT_KPI_SCHEMA}.inventory_kpi_daily',
    'regional_kpi': f'{REDSHIFT_KPI_SCHEMA}.regional_kpi_daily'
}

# Redshift processed source data table names (for Option 2, these are daily appends/upserts)
REDSHIFT_PROCESSED_SOURCE_TABLES = {
    'inventory': f'{REDSHIFT_PROCESSED_SCHEMA}.inventory_daily',
    'pos': f'{REDSHIFT_PROCESSED_SCHEMA}.pos_daily'
}

# Initialize job
job.init(args['JOB_NAME'], args)

# Initialize S3 client
s3_client = boto3.client('s3')

def setup_logging():
    """
    Setup structured logging for the job
    """
    log_data = {
        'job_name': args['JOB_NAME'],
        'run_timestamp': RUN_TIMESTAMP,
        'processing_date': args.get('PROCESSING_DATE'), # Initial processing_date for the run
        'logs': []
    }
    return log_data

def log_message(log_data, level, message, details=None):
    """
    Add structured log message to log data
    
    Args:
        log_data: Dictionary containing log information
        level: Log level (INFO, WARN, ERROR)
        message: Log message
        details: Optional additional details
    """
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'level': level,
        'message': message
    }
    if details:
        log_entry['details'] = details
    
    log_data['logs'].append(log_entry)
    print(f"[{level}] {message}")

def save_logs_to_s3(log_data):
    """
    Save structured logs to S3
    
    Args:
        log_data: Dictionary containing all log information
    """
    try:
        log_path = f"{S3_PATHS['logs']}{RUN_TIMESTAMP}/kpi_logs.json"
        log_json = json.dumps(log_data, indent=2)
        
        # Write to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=log_path.replace(f's3://{BUCKET_NAME}/', ''),
            Body=log_json,
            ContentType='application/json'
        )
        print(f"Logs saved to: {log_path}")
    except Exception as e:
        print(f"ERROR: Failed to save logs to S3: {str(e)}")

def read_silver_data(source_type, current_processing_date_str, log_data):
    """
    Read data for a specific processing_date from Silver layer.
    
    Args:
        source_type: 'inventory' or 'pos'
        current_processing_date_str: The specific date string (YYYY-MM-DD) to read for.
        log_data: Logging data structure
        
    Returns:
        Spark DataFrame or None if failed
    """
    try:
        if source_type == 'inventory':
            silver_path = S3_PATHS['silver_inventory']
        elif source_type == 'pos':
            silver_path = S3_PATHS['silver_pos']
        else:
            raise ValueError(f"Unsupported source_type: {source_type}")

        log_message(log_data, "INFO", f"Reading {source_type} data for PROCESSING_DATE={current_processing_date_str} from Silver layer: {silver_path}")
        
        # Read only the partition for the current_processing_date_str
        # Assumes S3 path is partitioned by processing_date like: s3://bucket/silver/inventory/processing_date=YYYY-MM-DD/
        df = spark.read.parquet(f"{silver_path}processing_date={current_processing_date_str}/")
        
        row_count = df.count()
        log_message(log_data, "INFO", f"Successfully read {row_count} rows from {source_type} Silver layer for processing_date={current_processing_date_str}")
        
        return df
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to read {source_type} Silver data for {current_processing_date_str}", str(e))
        return None

def join_inventory_pos_data(inventory_df, pos_df, current_processing_date_str, log_data):
    """
    Join inventory and POS data on product_id and nearest timestamp using 2-hour buckets.
    This function expects data for a single processing date as defined by the current iteration.

    Args:
        inventory_df: Inventory DataFrame
        pos_df: POS DataFrame
        current_processing_date_str: The specific date string (YYYY-MM-DD) for KPI calculation.
        log_data: Logging data structure

    Returns:
        Tuple of (joined_df, failed_joins_df)
    """
    try:
        log_message(log_data, "INFO", f"Starting inventory-POS data join with 2-hour time buckets for KPI calculation for date {current_processing_date_str}.")

        # Prepare inventory data with 2-hour time buckets
        inventory_prep = inventory_df.withColumn(
            'time_bucket',
            floor(hour(col('last_updated_timestamp')) / 2) * 2
        ).withColumn(
            'inv_date',
            to_date(col('last_updated_timestamp'))
        )

        # Prepare POS data with 2-hour time buckets
        # --- FIX 1: Use 'timestamp_timestamp' instead of 'transaction_timestamp' ---
        pos_prep = pos_df.withColumn(
            'time_bucket',
            floor(hour(col('timestamp_timestamp')) / 2) * 2
        ).withColumn(
            'pos_date',
            to_date(col('timestamp_timestamp'))
        )

        # Aggregate inventory data by product, date, and time bucket (latest stock level)
        inventory_agg = inventory_prep.groupBy('product_id', 'inv_date', 'time_bucket') \
            .agg(
                last('stock_level', True).alias('stock_level'),
                last('stock_status', True).alias('stock_status'),
                last('warehouse_id', True).alias('warehouse_id'),
                max('last_updated_timestamp').alias('inv_timestamp')
            )

        # Aggregate POS data by product, date, and time bucket
        pos_agg = pos_prep.groupBy('product_id', 'pos_date', 'time_bucket') \
            .agg(
                sum('quantity').alias('total_quantity'),
                # --- FIX 2: Use 'revenue' instead of 'net_revenue' ---
                sum('revenue').alias('total_revenue'),
                # --- FIX 3: Use 'discount_applied' instead of 'discount_amount' ---
                sum('discount_applied').alias('total_discount'),
                count('transaction_id').alias('transaction_count'),
                collect_set('store_id').alias('store_list'),
                max('timestamp_timestamp').alias('pos_timestamp') # Also ensure this uses the correct column
            )

        # The rest of your join logic remains the same
        joined_df = inventory_agg.alias('inv').join(
            pos_agg.alias('pos'),
            (col('inv.product_id') == col('pos.product_id')) &
            (col('inv.inv_date') == col('pos.pos_date')) &
            (col('inv.time_bucket') == col('pos.time_bucket')),
            'inner'
        ).select(
            col('inv.product_id'),
            col('inv.inv_date').alias('analysis_date'),
            col('inv.time_bucket'),
            col('inv.stock_level'),
            col('inv.stock_status'),
            col('inv.warehouse_id'),
            lit(10.0).alias('cost_price'), # Placeholder: Replace with actual column or join
            lit('General').alias('category'), # Placeholder: Replace with actual column or join
            col('pos.total_quantity'),
            col('pos.total_revenue'),
            col('pos.total_discount'),
            col('pos.transaction_count'),
            col('pos.store_list'),
            col('inv.inv_timestamp'),
            col('pos.pos_timestamp')
        )

        # Find failed joins (inventory without matching POS data for the same product, date, and time bucket)
        failed_joins_df = inventory_agg.alias('inv').join(
            pos_agg.alias('pos'),
            (col('inv.product_id') == col('pos.product_id')) &
            (col('inv.inv_date') == col('pos.pos_date')) &
            (col('inv.time_bucket') == col('pos.time_bucket')),
            'left_anti'
        ).select(
            col('inv.product_id'),
            col('inv.inv_date').alias('analysis_date'),
            col('inv.time_bucket'),
            col('inv.stock_level'),
            col('inv.warehouse_id'),
            lit('No matching POS data for product, date, and time bucket').alias('error_reason')
        )

        joined_count = joined_df.count()
        failed_count = failed_joins_df.count()

        log_message(log_data, "INFO", f"KPI Join completed for {current_processing_date_str} - Successful: {joined_count}, Failed: {failed_count}")

        return joined_df, failed_joins_df

    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to join inventory and POS data for KPI calculation for {current_processing_date_str}", str(e))
        return None, None