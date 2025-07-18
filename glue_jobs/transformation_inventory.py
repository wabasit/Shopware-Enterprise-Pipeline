import sys
import json
import boto3
from datetime import datetime, timedelta, date
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import re
from pyspark.sql.window import Window # Import for window function
from awsglue.dynamicframe import DynamicFrame # Import DynamicFrame for Redshift write

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_NAME',
    'GLUE_CATALOG_DATABASE_NAME', # Renamed from DATABASE_NAME for clarity
    'REDSHIFT_DATABASE_NAME',     # New: Explicit Redshift database name
    'REDSHIFT_CONNECTION',        # Redshift connection name
    'REDSHIFT_PROCESSED_SCHEMA',  # Target schema in Redshift
    'TempDir',                    # S3 temporary directory for Redshift
    'start_processing_date',      # Optional: for backfill range (format YYYY-MM-DD)
    'end_processing_date'         # Optional: for backfill range (format YYYY-MM-DD)
])

# Configuration parameters
BUCKET_NAME = args['BUCKET_NAME']
GLUE_CATALOG_DATABASE_NAME = args['GLUE_CATALOG_DATABASE_NAME'] # For Glue Data Catalog
REDSHIFT_DATABASE_NAME = args['REDSHIFT_DATABASE_NAME']         # For the actual Redshift database
REDSHIFT_CONNECTION = args['REDSHIFT_CONNECTION']               # Redshift connection name from Glue Catalog
REDSHIFT_PROCESSED_SCHEMA = args['REDSHIFT_PROCESSED_SCHEMA']   # Target schema in Redshift

# --- AUTOMATIC GENERATION OF RUN_TIMESTAMP ---
# Use UTC for consistency across AWS services
current_utc_time = datetime.utcnow()
# RUN_TIMESTAMP: Format: YYYYMMDD_HHMMSS (for unique job run identification)
RUN_TIMESTAMP = current_utc_time.strftime('%Y%m%d_%H%M%S')
# --- END AUTOMATIC GENERATION ---

# S3 paths configuration (only for inventory)
S3_PATHS = {
    'inventory_raw': f's3://{BUCKET_NAME}/raw/batch/inventory-raw/',
    'silver_inventory': f's3://{BUCKET_NAME}/processed/inventory/',
    'validation_errors': f's3://{BUCKET_NAME}/errors/validation/', # Generic path for errors
    'archive': f's3://{BUCKET_NAME}/archive/', # Generic path for archive
    'logs': f's3://{BUCKET_NAME}/logs/validation/' # Generic path for logs
}

# Redshift table names (for clarity and consistency)
REDSHIFT_PROCESSED_TABLES = {
    'inventory_daily': f'{REDSHIFT_PROCESSED_SCHEMA}.inventory_daily'
}

# Initialize job
job.init(args['JOB_NAME'], args)

# Initialize S3 client for archival operations
s3_client = boto3.client('s3')

# PROCESSING_DATE is now a global variable that will be set dynamically in main()
# It needs an initial placeholder value for `setup_logging` when it's first called,
# but it will be updated per iteration in `main()`.
PROCESSING_DATE = current_utc_time.strftime('%Y-%m-%d')


def setup_logging():
    """
    Setup structured logging for the job.
    Note: 'processing_date' in log_data will be updated dynamically in main()
          when processing multiple dates. This initial value is a placeholder.
    """
    log_data = {
        'job_name': args['JOB_NAME'],
        'run_timestamp': RUN_TIMESTAMP,
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
        'timestamp': datetime.now().isoformat(), # Use local time for log timestamp for readability in console
        'level': level,
        'message': message
    }
    if details:
        log_entry['details'] = details

    # Add PROCESSING_DATE to each log entry for multi-day runs if it's set globally
    if 'PROCESSING_DATE' in globals():
        log_entry['processing_date'] = globals()['PROCESSING_DATE']

    log_data['logs'].append(log_entry)
    print(f"[{level}] {message}") # Also print to console for real-time monitoring

def save_logs_to_s3(log_data):
    """
    Save structured logs to S3

    Args:
        log_data: Dictionary containing all log information
    """
    try:
        log_path = f"{S3_PATHS['logs']}{RUN_TIMESTAMP}/validation_logs.json"
        log_json = json.dumps(log_data, indent=2, default=str) # Use default=str for datetime objects

        # Write to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=log_path.replace(f's3://{BUCKET_NAME}/', ''), # Remove s3://bucket-name/ prefix
            Body=log_json,
            ContentType='application/json'
        )
        print(f"Logs saved to: {log_path}")
    except Exception as e:
        print(f"ERROR: Failed to save logs to S3: {str(e)}")

def get_schema_for_source(source_type):
    """
    Define expected schemas for 'inventory' data source based on the provided schema.
    Raises ValueError if source_type is not 'inventory'.

    Args:
        source_type: 'inventory'

    Returns:
        Dictionary with schema definition and validation rules
    """
    if source_type == 'inventory':
        return {
            'required_fields': ['inventory_id', 'product_id', 'warehouse_id', 'stock_level', 'last_updated'],
            'schema': StructType([
                StructField('inventory_id', IntegerType(), False),
                StructField('product_id', IntegerType(), False),
                StructField('warehouse_id', IntegerType(), False),
                StructField('stock_level', IntegerType(), False),
                StructField('restock_threshold', IntegerType(), True), # Nullable
                StructField('last_updated', LongType(), False) # Changed to LongType for epoch seconds
            ]),
            'validation_rules': {
                'stock_level': lambda col: col >= 0, # Stock level must be non-negative
                'restock_threshold': lambda col: (col.isNull()) | (col >= 0), # Restock threshold can be null or non-negative
                'last_updated': lambda col: (col.isNotNull()) & (col > 0) # last_updated must not be null and be a positive epoch
            }
        }
    else:
        # This branch should ideally not be reached as only 'inventory' is processed
        raise ValueError(f"Unsupported source type: {source_type}. This job only processes 'inventory' data.")

def read_data_from_catalog(source_type, log_data):
    """
    Read data from Glue Data Catalog for the 'inventory' source.

    Args:
        source_type: 'inventory'
        log_data: Logging data structure

    Returns:
        Spark DataFrame or None if failed
    """
    try:
        table_name = f"{source_type}_raw" # This will be 'inventory_raw'

        log_message(log_data, "INFO", f"Reading {source_type} data from catalog table: {table_name}")

        # Create dynamic frame from catalog
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=GLUE_CATALOG_DATABASE_NAME, # Updated: Use specific Glue Catalog DB name
            table_name=table_name,
            transformation_ctx=f"read_{source_type}"
        )

        # Convert to Spark DataFrame
        df = dynamic_frame.toDF()

        # Filter for current processing date partitions if the raw data is partitioned by 'partition_date'
        # Important: This assumes raw data is partitioned. If not, the 'archive_processed_files'
        # logic will still attempt to filter by filename date for archival.
        if 'partition_date' in df.columns:
            log_message(log_data, "INFO", f"Filtering {source_type} data for partition_date = {PROCESSING_DATE}")
            df = df.filter(col('partition_date') == PROCESSING_DATE)
        else:
            log_message(log_data, "WARN", f"Table {table_name} does not have 'partition_date' column. Reading all data.")

        row_count = df.count()
        log_message(log_data, "INFO", f"Successfully read {row_count} rows from {source_type} table")

        return df

    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to read {source_type} data from catalog", str(e))
        return None

def validate_data(df, source_type, log_data):
    """
    Validate data against defined schema and rules for 'inventory' data.

    Args:
        df: Spark DataFrame
        source_type: 'inventory'
        log_data: Logging data structure

    Returns:
        Tuple of (valid_df, invalid_df, validation_summary)
    """
    try:
        schema_config = get_schema_for_source(source_type) # This will get inventory schema
        required_fields = schema_config['required_fields']
        validation_rules = schema_config['validation_rules']
        expected_schema = schema_config['schema']

        log_message(log_data, "INFO", f"Starting validation for {source_type} data")

        # 1. Check for required fields existence in the DataFrame
        # Important: Check against the original DF before casting, as casting might drop columns
        missing_fields = [field for field in required_fields if field not in df.columns]
        if missing_fields:
            error_msg = f"Missing critical required fields in input DataFrame: {missing_fields}"
            log_message(log_data, "ERROR", error_msg)
            # If critical fields are missing, the entire DF is considered unprocessable
            return None, df.withColumn('error_reason', lit(error_msg)), {'status': 'failed', 'reason': error_msg}

        # 2. Cast to expected schema (this handles type mismatches by turning them into nulls if incompatible)
        # It's important to select and reorder columns to match the target schema structure
        selected_cols = [col(field.name).cast(field.dataType).alias(field.name) for field in expected_schema.fields]
        df_casted = df.select(*selected_cols)

        # 3. Build validation conditions and error reasons
        invalid_conditions = []
        error_reason_expr = lit(None) # Default to None, will be set for invalid rows

        # Check for null values in required fields *after* casting
        for field in required_fields:
            if field in df_casted.columns: # Ensure field exists after casting/selection
                null_check = col(field).isNull()
                invalid_conditions.append(null_check)
                error_reason_expr = when(null_check, f"Null in required field: {field}").otherwise(error_reason_expr)

        # Apply validation rules
        for field, rule in validation_rules.items():
            if field in df_casted.columns: # Ensure field exists after casting/selection
                rule_violation_check = ~rule(col(field))
                invalid_conditions.append(rule_violation_check)
                error_reason_expr = when(rule_violation_check, f"Rule violation for {field}").otherwise(error_reason_expr)

        # Combine all invalid conditions using OR
        combined_invalid_condition = lit(False)
        if invalid_conditions:
            for condition in invalid_conditions:
                combined_invalid_condition = combined_invalid_condition | condition

        df_with_validation = df_casted.withColumn('is_valid', ~combined_invalid_condition) \
                                     .withColumn('error_reason', error_reason_expr) # This column will have reason for invalid rows

        # Split into valid and invalid DataFrames
        valid_df = df_with_validation.filter(col('is_valid') == True).drop('error_reason', 'is_valid')
        invalid_df = df_with_validation.filter(col('is_valid') == False).drop('is_valid')

        # Get validation summary
        total_rows = df.count() # Use count from original DF for total
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        validation_summary = {
            'total_rows': total_rows,
            'valid_rows': valid_count,
            'invalid_rows': invalid_count,
            'validation_rate': (valid_count / total_rows) * 100 if total_rows > 0 else 0,
            'status': 'success' if invalid_count == 0 else 'partial_success'
        }

        log_message(log_data, "INFO", f"Validation completed for {source_type}", validation_summary)

        return valid_df, invalid_df, validation_summary

    except Exception as e:
        log_message(log_data, "ERROR", f"Validation failed for {source_type}", str(e))
        # Return original DF with a generic error reason if validation setup itself fails
        return None, df.withColumn('error_reason', lit(f"Validation setup error: {str(e)}")), {'status': 'error', 'reason': str(e)}


def transform_data(df, source_type, log_data):
    """
    Apply transformations to validated 'inventory' data and prepare for Redshift.
    This includes adding derived columns, audit columns, and ensuring column names
    and types are suitable for the target Redshift 'shopware_processed.inventory_daily' schema.

    Args:
        df: Spark DataFrame with valid data
        source_type: 'inventory'
        log_data: Logging data structure

    Returns:
        Transformed DataFrame ready for Redshift.
    """
    try:
        log_message(log_data, "INFO", f"Starting transformation for {source_type} data")

        # Convert 'last_updated' (Long epoch seconds) to TimestampType
        df_transformed = df.withColumn('last_updated_timestamp',
                                       from_unixtime(col('last_updated')).cast(TimestampType()))

        # Add derived columns specific to inventory
        df_transformed = df_transformed \
            .withColumn('stock_status',
                        when(col('stock_level') == 0, 'OUT_OF_STOCK')
                        .when(col('stock_level') < 10, 'LOW_STOCK')
                        .otherwise('IN_STOCK')) \
            .withColumn('processing_date', lit(PROCESSING_DATE)) # Add processing_date for partitioning

        # Add audit columns (using UTC for consistency)
        df_transformed = df_transformed \
            .withColumn('redshift_load_timestamp', current_timestamp()) \
            .withColumn('job_run_id', lit(RUN_TIMESTAMP))

        # Select, rename, and reorder columns to match the Redshift
        # 'shopware_processed.inventory_daily' DDL for a direct push
        df_final_for_redshift = df_transformed.select(
            col('inventory_id').cast(IntegerType()).alias('inventory_id'),
            col('product_id').cast(IntegerType()).alias('product_id'),
            col('warehouse_id').cast(IntegerType()).alias('warehouse_id'),
            col('stock_level').cast(IntegerType()).alias('stock_level'),
            col('restock_threshold').cast(IntegerType()).alias('restock_threshold'),
            col('last_updated_timestamp').cast(TimestampType()).alias('last_updated'), # Rename for Redshift target
            col('stock_status').cast(StringType()).alias('stock_status'),
            col('processing_date').cast(StringType()).alias('processing_date'),
            col('job_run_id').cast(StringType()).alias('job_run_id'),
            col('redshift_load_timestamp').cast(TimestampType()).alias('redshift_load_timestamp')
        )

        row_count = df_final_for_redshift.count()
        log_message(log_data, "INFO", f"Transformation completed for {source_type}: {row_count} rows. Schema prepared for Redshift.")

        return df_final_for_redshift

    except Exception as e:
        log_message(log_data, "ERROR", f"Transformation failed for {source_type}", str(e))
        return None

def deduplicate_data(df, source_type, log_data):
    """
    De-duplicates the inventory data, keeping the latest record based on last_updated
    for each unique inventory_id. Assumes 'last_updated' is now the TIMESTAMP column for Redshift target.

    Args:
        df: Spark DataFrame with transformed data (already includes 'last_updated' as TIMESTAMP).
        source_type: 'inventory'
        log_data: Logging data structure

    Returns:
        Deduplicated Spark DataFrame.
    """
    try:
        log_message(log_data, "INFO", f"Starting de-duplication for {source_type} data based on 'inventory_id' and 'last_updated'.")

        # Define window specification: partition by inventory_id, order by last_updated descending
        window_spec = Window.partitionBy("inventory_id").orderBy(col("last_updated").desc())

        # Apply window function to assign row numbers, then filter to keep only the latest record
        deduplicated_df = df.withColumn("rn", row_number().over(window_spec)) \
                            .filter(col("rn") == 1) \
                            .drop("rn") # Drop the row_number column as it's no longer needed

        original_count = df.count()
        deduplicated_count = deduplicated_df.count()

        if original_count > deduplicated_count:
            log_message(log_data, "INFO", f"De-duplication completed: {original_count - deduplicated_count} duplicates removed. Remaining records: {deduplicated_count}")
        else:
            log_message(log_data, "INFO", f"De-duplication completed: No duplicates found.")

        return deduplicated_df

    except Exception as e:
        log_message(log_data, "ERROR", f"De-duplication failed for {source_type}", str(e))
        # Re-raise to indicate a critical failure if de-duplication itself fails
        raise

def write_to_silver(df, source_type, log_data):
    """
    Write transformed 'inventory' data to Silver layer.
    Uses 'append' mode to add new data to existing partitions.

    Args:
        df: Transformed and deduplicated DataFrame
        source_type: 'inventory'
        log_data: Logging data structure
    """
    try:
        output_path = f"{S3_PATHS['silver_inventory']}" # Base Silver path for inventory

        log_message(log_data, "INFO", f"Writing {source_type} data to Silver layer base path: {output_path}")

        df.write \
          .mode('append') \
          .option('path', output_path) \
          .partitionBy('processing_date') \
          .format('parquet') \
          .save()

        log_message(log_data, "INFO", f"Successfully wrote {source_type} data to Silver layer for PROCESSING_DATE={PROCESSING_DATE}")

    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to write {source_type} data to Silver layer", str(e))
        raise # Re-raise to indicate a critical failure

def write_inventory_to_redshift(df, current_processing_date_str, log_data):
    """
    Write the final transformed and deduplicated Inventory data to the Redshift
    shopware_processed.inventory_daily table.

    Args:
        df: Spark DataFrame to write (already in target Redshift schema).
        current_processing_date_str: The date for which the data is being written.
        log_data: Logging data structure
    """
    try:
        table_name = REDSHIFT_PROCESSED_TABLES['inventory_daily']
        log_message(log_data, "INFO", f"Writing processed Inventory data to Redshift table: {table_name} for date {current_processing_date_str} with upsert.")

        # Convert DataFrame to DynamicFrame
        df_dynamic = DynamicFrame.fromDF(df, glueContext, f"df_dynamic_inventory_daily_redshift_{current_processing_date_str}")

        glueContext.write_dynamic_frame.from_jdbc_conf(
            frame=df_dynamic,
            catalog_connection=REDSHIFT_CONNECTION,
            connection_options={
                # Delete by processing_date to handle re-runs (upsert logic)
                "preactions": f"DELETE FROM {table_name} WHERE processing_date = '{current_processing_date_str}';",
                "dbtable": table_name,
                "database": REDSHIFT_DATABASE_NAME # Updated to use the specific Redshift DB name
            },
            redshift_tmp_dir=args["TempDir"],
            transformation_ctx=f"write_redshift_inventory_daily_{current_processing_date_str}"
        )

        row_count = df.count()
        log_message(log_data, "INFO", f"Successfully wrote {row_count} rows to {table_name} for date {current_processing_date_str}")

    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to write processed Inventory data to Redshift table {table_name} for date {current_processing_date_str}", str(e))
        raise

def write_errors_to_s3(df, source_type, log_data):
    """
    Write invalid rows to errors location for 'inventory' data.

    Args:
        df: DataFrame with invalid rows (should contain 'error_reason' column)
        source_type: 'inventory'
        log_data: Logging data structure
    """
    try:
        if df.count() == 0:
            log_message(log_data, "INFO", f"No errors to write for {source_type}")
            return

        # Errors are stored under source_type/run_timestamp for traceability
        error_path = f"{S3_PATHS['validation_errors']}{source_type}/{RUN_TIMESTAMP}/"

        log_message(log_data, "INFO", f"Writing {df.count()} error records for {source_type} to: {error_path}")

        # Add metadata columns for error analysis (using UTC for consistency)
        df_with_metadata = df \
            .withColumn('error_timestamp_utc', lit(datetime.utcnow())) \
            .withColumn('job_run_id', lit(RUN_TIMESTAMP)) \
            .withColumn('processing_date_of_error', lit(PROCESSING_DATE)) \
            .withColumn('source_type', lit(source_type))

        # Write as JSON for easier error analysis and readability
        df_with_metadata.write \
            .mode('overwrite') \
            .option('path', error_path) \
            .format('json') \
            .save()

        log_message(log_data, "INFO", f"Successfully wrote error records for {source_type}")

    except Exception as e:
        log_message(log_data, "ERROR", f"Failed to write error records for {source_type}", str(e))

def archive_processed_files(source_type, log_data):
    """
    Move processed 'inventory' files from raw to archive location for the current processing date.
    Assumes raw files contain the date in YYYYMMDD format in their filename and are JSON.

    Args:
        source_type: 'inventory'
        log_data: Logging data structure
    """
    try:
        log_message(log_data, "INFO", f"Starting archival process for {source_type} for date {PROCESSING_DATE}")

        source_prefix = f"raw/batch/inventory-raw/" # Always 'inventory/' for this job
        archive_prefix = f"archive/inventory/{PROCESSING_DATE}/" # Specific archive path for inventory

        # Convert PROCESSING_DATE to YYYYMMDD for filename matching
        processing_date_yyyymmdd = PROCESSING_DATE.replace('-', '')

        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=source_prefix)

        archived_count = 0
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    source_key = obj['Key'] # e.g., 'raw/batch/inventory/data_20240715_100000.json'

                    # Ensure it's a JSON file and not a directory or other type
                    if source_key.endswith('/') or not source_key.lower().endswith('.json'):
                        continue

                    filename = source_key.split('/')[-1]

                    # Heuristic to find YYYYMMDD in filename to match with PROCESSING_DATE
                    # Example: inventory_20240715_123456.json
                    file_date_match = re.search(r'(\d{8})', filename)

                    if not file_date_match:
                        log_message(log_data, "INFO", f"Skipping {filename}: No YYYYMMDD date found in filename for archival.")
                        continue

                    file_date_yyyymmdd = file_date_match.group(1)

                    if file_date_yyyymmdd == processing_date_yyyymmdd:
                        # Move to archive
                        archive_key = archive_prefix + filename

                        log_message(log_data, "INFO", f"Archiving {source_key} to {archive_key}")
                        s3_client.copy_object(
                            Bucket=BUCKET_NAME,
                            CopySource={'Bucket': BUCKET_NAME, 'Key': source_key},
                            Key=archive_key
                        )

                        s3_client.delete_object(Bucket=BUCKET_NAME, Key=source_key)
                        archived_count += 1
                    else:
                        log_message(log_data, "INFO", f"Skipping {filename}: File date {file_date_yyyymmdd} does not match PROCESSING_DATE {processing_date_yyyymmdd}.")

        log_message(log_data, "INFO", f"Archived {archived_count} files for {source_type} for date {PROCESSING_DATE}")

    except Exception as e:
        log_message(log_data, "ERROR", f"Archival failed for {source_type}", str(e))
        # Do not raise here, as archival is often a best-effort operation and shouldn't fail the whole job.

def process_source(source_type, log_data):
    """
    Main processing function for 'inventory' data: read, validate, transform, deduplicate, write, archive.

    Args:
        source_type: Should always be 'inventory' for this script.
        log_data: Logging data structure

    Returns:
        Boolean indicating success for this specific source.
    """
    try:
        if source_type != 'inventory':
            log_message(log_data, "ERROR", f"Invalid source_type '{source_type}' passed to process_source. Only 'inventory' is supported.")
            return False

        log_message(log_data, "INFO", f"Starting processing for {source_type} for date {PROCESSING_DATE}")

        # Read data from catalog
        raw_df = read_data_from_catalog(source_type, log_data)
        if raw_df is None or raw_df.count() == 0:
            log_message(log_data, "WARN", f"No raw data found or read for {source_type} for {PROCESSING_DATE}. Skipping further processing for this source.")
            return True # Consider it a success if no data to process (e.g., no files for date)

        # Validate data
        valid_df, invalid_df, validation_summary = validate_data(raw_df, source_type, log_data)

        if valid_df is None and validation_summary.get('status') == 'failed':
            log_message(log_data, "ERROR", f"Critical validation failure for {source_type}. Aborting source processing.")
            if invalid_df is not None:
                write_errors_to_s3(invalid_df, source_type, log_data)
            return False

        # Write error records if any
        if invalid_df.count() > 0:
            write_errors_to_s3(invalid_df, source_type, log_data)
            log_message(log_data, "WARN", f"{invalid_df.count()} invalid records found for {source_type}. See error logs.")

        if valid_df.count() == 0:
            log_message(log_data, "WARN", f"No valid records to process for {source_type}. Skipping transformation, silver write, and Redshift push.")
            # Still attempt to archive if raw_df was read, as files were 'processed' (even if all invalid)
            archive_processed_files(source_type, log_data)
            return True # Consider successful if valid_df is empty but processing completed.

        # Transform valid data (includes schema alignment for Redshift)
        transformed_df = transform_data(valid_df, source_type, log_data)
        if transformed_df is None:
            log_message(log_data, "ERROR", f"Transformation failed for {source_type}. Aborting source processing.")
            return False

        # Deduplicate data
        deduplicated_df = deduplicate_data(transformed_df, source_type, log_data)
        if deduplicated_df is None:
            log_message(log_data, "ERROR", f"De-duplication failed for {source_type}. Aborting source processing.")
            return False

        # Write to Silver layer
        write_to_silver(deduplicated_df, source_type, log_data)

        # New: Write to Redshift shopware_processed.inventory_daily table
        write_inventory_to_redshift(deduplicated_df, PROCESSING_DATE, log_data)

        # Archive processed files
        archive_processed_files(source_type, log_data)

        log_message(log_data, "INFO", f"Successfully completed processing for {source_type} for date {PROCESSING_DATE}")
        return True

    except Exception as e:
        log_message(log_data, "ERROR", f"Processing failed for {source_type} due to unexpected error", str(e))
        return False

def main():
    """
    Main execution function of the Glue job.
    Handles single-day or multi-day processing based on job parameters.
    """
    # Setup logging dictionary (initial setup)
    log_data = setup_logging()

    global PROCESSING_DATE # Declare global so we can modify it

    try:
        log_message(log_data, "INFO", "Starting Inventory Validation & Transformation job")
        log_message(log_data, "INFO", f"Job Run ID: {RUN_TIMESTAMP}")

        # Determine processing dates
        dates_to_process = []
        start_date_param = args.get('start_processing_date')
        end_date_param = args.get('end_processing_date')

        if start_date_param and end_date_param:
            # Process a date range if both start and end dates are provided
            try:
                start_date = datetime.strptime(start_date_param, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date_param, '%Y-%m-%d').date()

                if start_date > end_date:
                    log_message(log_data, "ERROR", "Start date cannot be after end date.")
                    raise ValueError("Invalid date range.")

                current_date_iter = start_date
                while current_date_iter <= end_date:
                    dates_to_process.append(current_date_iter.strftime('%Y-%m-%d'))
                    current_date_iter += timedelta(days=1)
                log_message(log_data, "INFO", f"Processing a date range: {start_date_param} to {end_date_param}")

            except ValueError as e:
                log_message(log_data, "ERROR", f"Invalid date format for --start_processing_date or --end_processing_date. Expected YYYY-MM-DD. Aborting.", str(e))
                raise Exception("Date parsing error.")
        else:
            # Default to current UTC date if no range specified
            default_date = current_utc_time.strftime('%Y-%m-%d') # Default to today
            dates_to_process.append(default_date)
            log_message(log_data, "INFO", f"Processing single date (default): {default_date}")

        all_succeeded = True
        for current_processing_date in dates_to_process:
            # Set the global PROCESSING_DATE for the current iteration
            PROCESSING_DATE = current_processing_date

            log_message(log_data, "INFO", f"--- Starting processing for date: {PROCESSING_DATE} ---")

            source_type = 'inventory' # Always 'inventory' for this job
            success = process_source(source_type, log_data)

            if not success:
                all_succeeded = False
                log_message(log_data, "ERROR", f"Inventory data processing FAILED for date: {PROCESSING_DATE}. This run will continue to process other dates if applicable, but the overall job status will be marked as failed.")

        # Overall job status summary
        if all_succeeded:
            log_message(log_data, "INFO", "All specified inventory data processing completed successfully.")
        else:
            log_message(log_data, "ERROR", "One or more daily inventory data processing failed within the job run.")
            # Raise an exception to mark the Glue job as failed in the console
            raise Exception("Job finished with partial failures in date range processing.")

    except Exception as e:
        log_message(log_data, "ERROR", "Job failed with an unhandled exception in main execution", str(e))
        # Re-raise to ensure Glue job status reflects failure
        raise

    finally:
        # Always attempt to save logs to S3, regardless of job success or failure
        save_logs_to_s3(log_data)

        # Commit the Glue job. This is crucial for Glue to mark the job as succeeded.
        # If an exception is re-raised before this, the job will be marked as failed.
        job.commit()

# Execute the main function when the script runs
if __name__ == "__main__":
    main()