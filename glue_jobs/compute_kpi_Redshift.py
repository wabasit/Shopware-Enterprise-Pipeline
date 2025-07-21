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
    'DATABASE_NAME', # This is for Glue Catalog
    'REDSHIFT_CONNECTION',
    'REDSHIFT_SCHEMA', # This is for KPI schema
    'REDSHIFT_PROCESSED_SCHEMA', # This is for the source data schema in Redshift (still needed for KPI table DDL)
    'start_processing_date', # Optional: for backfill range (format YYYY-MM-DD)
    'end_processing_date',   # Optional: for backfill range (format YYYY-MM-DD)
    'TempDir', # Required for Redshift writes
    'PROCESSING_DATE' # Added explicitly to all_expected_args for clarity, even if optional
]

args = getResolvedOptions(sys.argv, all_expected_args)

# Configuration parameters
BUCKET_NAME = args['BUCKET_NAME']
GLUE_CATALOG_DATABASE_NAME = args['DATABASE_NAME'] # Renamed for clarity
REDSHIFT_CONNECTION = args['REDSHIFT_CONNECTION']
REDSHIFT_KPI_SCHEMA = args['REDSHIFT_SCHEMA']
REDSHIFT_PROCESSED_SCHEMA = args['REDSHIFT_PROCESSED_SCHEMA'] # Still used for KPI table DDL
TEMP_DIR = args['TempDir']

# --- AUTOMATIC GENERATION OF RUN_TIMESTAMP ---
# Use UTC for consistency across AWS services
current_utc_time = datetime.utcnow()
# RUN_TIMESTAMP: Format: YYYYMMDD_HHMMSS (for unique job run identification)
RUN_TIMESTAMP = current_utc_time.strftime('%Y%m%d_%H%M%S')
# --- END AUTOMATIC GENERATION ---

# S3 paths configuration (for Silver layer, logs and errors)
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
        'processing_date': None, # This will be set dynamically in main()
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
    
    # Add current processing_date to log entry if available
    if log_data.get('processing_date'):
        log_entry['processing_date'] = log_data['processing_date']

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
        log_json = json.dumps(log_data, indent=2, default=str) # Use default=str for datetime objects
        
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
        
        # No explicit cast here. Spark will infer the schema.
        # The join_inventory_pos_data function will now directly use 'last_updated' as a TimestampType.

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
        inventory_df: Inventory DataFrame (read from Silver)
        pos_df: POS DataFrame (read from Silver)
        current_processing_date_str: The specific date string (YYYY-MM-DD) for KPI calculation.
        log_data: Logging data structure

    Returns:
        Tuple of (joined_df, failed_joins_df)
    """
    try:
        log_message(log_data, "INFO", f"Starting inventory-POS data join with 2-hour time buckets for KPI calculation for date {current_processing_date_str}.")

        # --- DEBUGGING: Log schemas of input DataFrames ---
        log_message(log_data, "INFO", "Schema of inventory_df before join:", details=inventory_df._jdf.schema().treeString())
        log_message(log_data, "INFO", "Schema of pos_df before join:", details=pos_df._jdf.schema().treeString())
        # --- END DEBUGGING ---

        # Prepare inventory data with 2-hour time buckets
        # Use 'last_updated' directly as it is confirmed to be TIMESTAMP in the input DataFrame schema.
        inventory_prep = inventory_df.withColumn(
            'time_bucket',
            floor(hour(col('last_updated')) / 2) * 2
        ).withColumn(
            'inv_date',
            to_date(col('last_updated'))
        )

        # Prepare POS data with 2-hour time buckets
        # Use 'transaction_timestamp' directly as it is already TIMESTAMP type
        pos_prep = pos_df.withColumn(
            'time_bucket',
            floor(hour(col('transaction_timestamp')) / 2) * 2
        ).withColumn(
            'pos_date',
            to_date(col('transaction_timestamp'))
        )

        # Aggregate inventory data by product, date, and time bucket (latest stock level)
        inventory_agg = inventory_prep.groupBy('product_id', 'inv_date', 'time_bucket') \
            .agg(
                last('stock_level', True).alias('stock_level'),
                last('stock_status', True).alias('stock_status'),
                last('warehouse_id', True).alias('warehouse_id'),
                max('last_updated').alias('inv_timestamp') # Use 'last_updated' directly
            )

        # Aggregate POS data by product, date, and time bucket
        pos_agg = pos_prep.groupBy('product_id', 'pos_date', 'time_bucket') \
            .agg(
                sum('quantity').alias('total_quantity'),
                sum('revenue').alias('total_revenue'),
                sum('discount_applied').alias('total_discount'),
                count('transaction_id').alias('transaction_count'),
                collect_set('store_id').alias('store_list'),
                max('transaction_timestamp').alias('pos_timestamp')
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
    
def compute_sales_kpis(joined_df, current_processing_date_str, log_data):
    """
    Computes daily sales KPIs.
    
    Args:
        joined_df: Joined DataFrame containing inventory and POS data
        current_processing_date_str: The specific date string (YYYY-MM-DD) for which KPIs are being calculated.
        log_data: Logging data structure
        
    Returns:
        DataFrame with sales KPIs or None if input is empty/invalid
    """
    try:
        if joined_df is None or joined_df.count() == 0:
            log_message(log_data, "WARN", f"Joined DataFrame is empty for {current_processing_date_str}, cannot compute sales KPIs.")
            return None

        log_message(log_data, "INFO", f"Computing sales KPIs for {current_processing_date_str}.")
        
        sales_kpis = joined_df.groupBy('analysis_date') \
            .agg(
                # Explicit casting for DECIMAL columns
                sum('total_revenue').cast(DecimalType(18, 4)).alias('total_daily_revenue'),
                sum('total_quantity').alias('total_daily_quantity_sold'),
                sum('total_discount').cast(DecimalType(18, 4)).alias('total_daily_discount_given'),
                count_distinct('product_id').alias('distinct_products_sold_daily'),
                avg(col('total_revenue') / col('total_quantity')).cast(DecimalType(18, 4)).alias('average_price_per_item')
            ) \
            .withColumn('job_run_id', lit(RUN_TIMESTAMP)) \
            .withColumn('kpi_creation_timestamp', current_timestamp()) \
            .withColumn('processing_date', lit(current_processing_date_str))
        
        # Ensure NOT NULL columns are indeed not null (analysis_date, processing_date)
        sales_kpis = sales_kpis.filter(
            col('analysis_date').isNotNull() &
            col('processing_date').isNotNull()
        )

        log_message(log_data, "INFO", f"Sales KPIs computed for {current_processing_date_str}: {sales_kpis.count()} rows.")
        return sales_kpis
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to compute sales KPIs for {current_processing_date_str}", str(e))
        return None
    
def compute_inventory_kpis(joined_df, current_processing_date_str, log_data):
    """
    Computes daily inventory KPIs.
    
    Args:
        joined_df: Joined DataFrame containing inventory and POS data
        current_processing_date_str: The specific date string (YYYY-MM-DD) for which KPIs are being calculated.
        log_data: Logging data structure
        
    Returns:
        DataFrame with inventory KPIs or None if input is empty/invalid
    """
    try:
        if joined_df is None or joined_df.count() == 0:
            log_message(log_data, "WARN", f"Joined DataFrame is empty for {current_processing_date_str}, cannot compute inventory KPIs.")
            return None

        log_message(log_data, "INFO", f"Computing inventory KPIs for {current_processing_date_str}.")
        
        inventory_kpis = joined_df.groupBy('analysis_date', 'warehouse_id') \
            .agg(
                sum('stock_level').alias('total_stock_level'),
                count_distinct('product_id').alias('distinct_products_in_stock'),
                sum(when(col('stock_status') == 'IN_STOCK', 1).otherwise(0)).alias('products_in_stock'),
                sum(when(col('stock_status') == 'OUT_OF_STOCK', 1).otherwise(0)).alias('products_out_of_stock'),
                # Explicit casting for DECIMAL columns
                sum(col('stock_level') * col('cost_price')).cast(DecimalType(18, 4)).alias('total_stock_value')
            ) \
            .withColumn('job_run_id', lit(RUN_TIMESTAMP)) \
            .withColumn('kpi_creation_timestamp', current_timestamp()) \
            .withColumn('processing_date', lit(current_processing_date_str))
        
        # Ensure NOT NULL columns are indeed not null (analysis_date, warehouse_id, processing_date)
        inventory_kpis = inventory_kpis.filter(
            col('analysis_date').isNotNull() &
            col('warehouse_id').isNotNull() & # Corrected from warehouse_date to warehouse_id
            col('processing_date').isNotNull()
        )

        log_message(log_data, "INFO", f"Inventory KPIs computed for {current_processing_date_str}: {inventory_kpis.count()} rows.")
        return inventory_kpis
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to compute inventory KPIs for {current_processing_date_str}", str(e))
        return None
    
def compute_regional_kpis(joined_df, current_processing_date_str, log_data):
    """
    Computes daily regional KPIs.
    
    Args:
        joined_df: Joined DataFrame containing inventory and POS data
        current_processing_date_str: The specific date string (YYYY-MM-DD) for which KPIs are being calculated.
        log_data: Logging data structure
        
    Returns:
        DataFrame with regional KPIs or None if input is empty/invalid
    """
    try:
        if joined_df is None or joined_df.count() == 0:
            log_message(log_data, "WARN", f"Joined DataFrame is empty for {current_processing_date_str}, cannot compute regional KPIs.")
            return None

        log_message(log_data, "INFO", f"Computing regional KPIs for {current_processing_date_str}.")

        regional_kpis = joined_df.withColumn('store_id', explode('store_list')) \
            .groupBy('analysis_date', 'store_id') \
            .agg(
                # Explicit casting for DECIMAL columns
                sum('total_revenue').cast(DecimalType(18, 4)).alias('store_daily_revenue'),
                sum('total_quantity').alias('store_daily_quantity_sold'),
                sum('transaction_count').alias('store_daily_transactions')
            ) \
            .withColumn('job_run_id', lit(RUN_TIMESTAMP)) \
            .withColumn('kpi_creation_timestamp', current_timestamp()) \
            .withColumn('processing_date', lit(current_processing_date_str))
        
        # Ensure NOT NULL columns are indeed not null (analysis_date, store_id, processing_date)
        regional_kpis = regional_kpis.filter(
            col('analysis_date').isNotNull() &
            col('store_id').isNotNull() &
            col('processing_date').isNotNull()
        )

        log_message(log_data, "INFO", f"Regional KPIs computed for {current_processing_date_str}: {regional_kpis.count()} rows.")
        return regional_kpis
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to compute regional KPIs for {current_processing_date_str}", str(e))
        return None
    
def write_to_redshift(df, table_name, current_processing_date_str, log_data):
    """
    Write DataFrame to Redshift (for KPI tables, using upsert logic based on analysis_date).
    
    Args:
        df: DataFrame to write
        table_name: Target Redshift table name (e.g., shopware_KPIs.sales_kpi_daily)
        current_processing_date_str: The date for which the data is being written.
        log_data: Logging data structure
    """
    try:
        log_message(log_data, "INFO", f"Writing data to Redshift KPI table: {table_name} for date {current_processing_date_str} with upsert.")
        
        # Convert DataFrame to DynamicFrame
        df_dynamic = DynamicFrame.fromDF(df, glueContext, "df_dynamic_kpi_redshift")
        
        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=df_dynamic,
            catalog_connection=REDSHIFT_CONNECTION,
            connection_options={
                # Delete by analysis_date to handle re-runs (upsert logic)
                #"preactions": f"DELETE FROM {table_name} WHERE analysis_date = '{current_processing_date_str}';",
                "dbtable": table_name,
                "database": "dev" # Use the actual Redshift database name
            },
            redshift_tmp_dir=args["TempDir"], 
            transformation_ctx=f"write_redshift_kpi_{table_name.split('.')[-1]}_{current_processing_date_str}"
        )
        
        row_count = df.count()
        log_message(log_data, "INFO", f"Successfully wrote {row_count} rows to {table_name} for date {current_processing_date_str}")
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to write to Redshift KPI table {table_name} for date {current_processing_date_str}", str(e))
        raise

def write_errors_to_s3(df, error_type, current_processing_date_str, log_data):
    """
    Write failed records to S3 errors location
    
    Args:
        df: DataFrame with failed records
        error_type: Type of error (e.g., 'failed_joins')
        current_processing_date_str: The date for which the errors occurred.
        log_data: Logging data structure
    """
    try:
        if df.count() == 0:
            log_message(log_data, "INFO", f"No {error_type} errors to write for {current_processing_date_str}")
            return
        
        error_path = f"{S3_PATHS['kpi_errors']}{error_type}/{RUN_TIMESTAMP}/{current_processing_date_str}/"
        
        log_message(log_data, "INFO", f"Writing {error_type} errors for {current_processing_date_str} to: {error_path}")
        
        df_with_metadata = df \
            .withColumn('error_timestamp', current_timestamp()) \
            .withColumn('job_run_id', lit(RUN_TIMESTAMP)) \
            .withColumn('error_type', lit(error_type)) \
            .withColumn('processing_date', lit(current_processing_date_str)) # Add processing_date for audit
        
        df_with_metadata.write \
            .mode('overwrite') \
            .option('path', error_path) \
            .format('json') \
            .save()
        
        error_count = df.count()
        log_message(log_data, "INFO", f"Successfully wrote {error_count} {error_type} error records for {current_processing_date_str}")
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to write {error_type} error records for {current_processing_date_str}", str(e))

def get_processing_metrics(inventory_df, pos_df, joined_df, current_processing_date_str, log_data):
    """
    Calculate and log processing metrics
    
    Args:
        inventory_df: Inventory DataFrame (for current PROCESSING_DATE)
        pos_df: POS DataFrame (for current PROCESSING_DATE)
        joined_df: Joined DataFrame (for current PROCESSING_DATE)
        current_processing_date_str: The date for which metrics are calculated.
        log_data: Logging data structure
    """
    try:
        inventory_count = inventory_df.count()
        pos_count = pos_df.count()
        joined_count = joined_df.count()

        metrics = {
            'inventory_records_read_for_date': inventory_count,
            'pos_records_read_for_date': pos_count,
            'joined_records_for_date': joined_count,
            'join_success_rate': (joined_count / inventory_count) * 100 if inventory_count > 0 else 0,
            'processing_date': current_processing_date_str,
            'run_timestamp': RUN_TIMESTAMP
        }
        
        log_message(log_data, "INFO", f"Processing metrics calculated for {current_processing_date_str}", metrics)
        
    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to calculate processing metrics for {current_processing_date_str}", str(e))

def main():
    """
    Main execution function
    """
    # Setup logging early as it's used in date parsing
    log_data = setup_logging()
    
    overall_status = "SUCCESS" # Track overall job status
    processing_dates = []
    log_mode = "Unknown"

    try:
        start_date_param = args.get('start_processing_date')
        end_date_param = args.get('end_processing_date')
        processing_date_param = args.get('PROCESSING_DATE') # Get the single processing date argument

        if start_date_param and end_date_param:
            # Backfill mode: use the provided date range
            try:
                start_date = datetime.strptime(start_date_param, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date_param, '%Y-%m-%d').date()
                # Ensure start_date is not after end_date
                if start_date > end_date:
                    raise ValueError("start_processing_date cannot be after end_processing_date.")
                
                delta = timedelta(days=1)
                current_date_iter = start_date # Initialize current_date_iter
                while current_date_iter <= end_date:
                    processing_dates.append(current_date_iter.strftime('%Y-%m-%d'))
                    current_date_iter += timedelta(days=1)
                
                log_mode = "Backfill"
                log_message(log_data, "INFO", f"Job running in Backfill mode for dates from {start_date_param} to {end_date_param}")
            except ValueError as e:
                log_message(log_data, "ERROR", f"Invalid date format for --start_processing_date or --end_processing_date. Expected YYYY-MM-DD. Aborting.", str(e))
                raise Exception("Date parsing error.")
        elif processing_date_param:
            # Daily run mode: process only the single specified PROCESSING_DATE
            processing_dates = [processing_date_param]
            log_mode = "Daily"
            log_message(log_data, "INFO", f"Job running in Daily mode for date {processing_date_param}")
        else:
            # Default to current UTC date if no specific date or range is provided
            default_date = current_utc_time.strftime('%Y-%m-%d')
            processing_dates.append(default_date)
            log_mode = "Continuous/Default Daily"
            log_message(log_data, "INFO", f"No specific processing date or range provided. Defaulting to current UTC date: {default_date}")


        log_message(log_data, "INFO", f"Starting KPI Computation & Redshift Load job ({log_mode} mode)")
        
        for current_processing_date_str in processing_dates:
            # Update the processing_date in log_data for the current iteration
            log_data['processing_date'] = current_processing_date_str

            log_message(log_data, "INFO", f"--- Processing date: {current_processing_date_str} ---")
            
            # Read data from S3 Silver layer
            inventory_df = read_silver_data('inventory', current_processing_date_str, log_data)
            pos_df = read_silver_data('pos', current_processing_date_str, log_data)
            
            if inventory_df is None or pos_df is None or inventory_df.count() == 0 or pos_df.count() == 0:
                log_message(log_data, "WARN", f"Skipping KPI computation for {current_processing_date_str} due to missing or empty Silver layer data.")
                overall_status = "PARTIAL_FAILURE"
                continue # Move to the next date in the backfill range

            # Join inventory and POS data for KPI computation
            joined_df, failed_joins_df = join_inventory_pos_data(inventory_df, pos_df, current_processing_date_str, log_data)
            
            if joined_df is None or joined_df.count() == 0:
                log_message(log_data, "WARN", f"No data after joining inventory and POS for {current_processing_date_str}. No KPIs will be computed for this date.")
                if failed_joins_df is not None and failed_joins_df.count() > 0:
                    write_errors_to_s3(failed_joins_df, 'failed_joins', current_processing_date_str, log_data)
                overall_status = "PARTIAL_FAILURE"
                continue # Move to the next date
            
            # Write failed joins to errors location
            if failed_joins_df is not None and failed_joins_df.count() > 0:
                write_errors_to_s3(failed_joins_df, 'failed_joins', current_processing_date_str, log_data)
            
            # Get processing metrics
            get_processing_metrics(inventory_df, pos_df, joined_df, current_processing_date_str, log_data)
            
            # Compute different types of KPIs
            sales_kpis = compute_sales_kpis(joined_df, current_processing_date_str, log_data)
            inventory_kpis = compute_inventory_kpis(joined_df, current_processing_date_str, log_data)
            regional_kpis = compute_regional_kpis(joined_df, current_processing_date_str, log_data)
            
            # Write KPIs to Redshift
            kpi_results = {}
            
            if sales_kpis is not None and sales_kpis.count() > 0:
                try:
                    write_to_redshift(sales_kpis, REDSHIFT_KPI_TABLES['sales_kpi'], current_processing_date_str, log_data)
                    kpi_results['sales_kpi'] = True
                except Exception as e:
                    log_message(log_data, "ERROR", f"Failed to write sales KPIs to Redshift for {current_processing_date_str}: {str(e)}")
                    kpi_results['sales_kpi'] = False
            else:
                log_message(log_data, "INFO", f"No sales KPIs generated or found to write for {current_processing_date_str}.")
                kpi_results['sales_kpi'] = False
            
            if inventory_kpis is not None and inventory_kpis.count() > 0:
                try:
                    write_to_redshift(inventory_kpis, REDSHIFT_KPI_TABLES['inventory_kpi'], current_processing_date_str, log_data)
                    kpi_results['inventory_kpi'] = True
                except Exception as e:
                    log_message(log_data, "ERROR", f"Failed to write inventory KPIs to Redshift for {current_processing_date_str}: {str(e)}")
                    kpi_results['inventory_kpi'] = False
            else:
                log_message(log_data, "INFO", f"No inventory KPIs generated or found to write for {current_processing_date_str}.")
                kpi_results['inventory_kpi'] = False

            if regional_kpis is not None and regional_kpis.count() > 0:
                try:
                    write_to_redshift(regional_kpis, REDSHIFT_KPI_TABLES['regional_kpi'], current_processing_date_str, log_data)
                    kpi_results['regional_kpi'] = True
                except Exception as e:
                    log_message(log_data, "ERROR", f"Failed to write regional KPIs to Redshift for {current_processing_date_str}: {str(e)}")
                    kpi_results['regional_kpi'] = False
            else:
                log_message(log_data, "INFO", f"No regional KPIs generated or found to write for {current_processing_date_str}.")
                kpi_results['regional_kpi'] = False

            # Check if all KPIs for the current date were successful
            if not all(kpi_results.values()):
                overall_status = "PARTIAL_FAILURE"
                log_message(log_data, "WARN", f"Some KPI loads failed for date {current_processing_date_str}.")
            else:
                log_message(log_data, "INFO", f"All KPIs for date {current_processing_date_str} processed and loaded successfully.")

        log_message(log_data, "INFO", f"Job finished with overall status: {overall_status}")
        if overall_status == "PARTIAL_FAILURE":
            raise Exception("Job finished with partial failures. Please review logs for details.")
        
    except Exception as e:
        log_message(log_data, "ERROR", "Job failed with unexpected error", str(e))
        overall_status = "FAILURE"
        raise # Re-raise to mark the Glue job as failed in the console
    
    finally:
        # Always attempt to save logs to S3, regardless of job success or failure
        save_logs_to_s3(log_data)
        
        # Commit the Glue job. This is crucial for Glue to mark the job as succeeded.
        # If an exception is re-raised before this, the job will be marked as failed.
        job.commit()

# Execute the main function when the script runs
if __name__ == "__main__":
    main()

