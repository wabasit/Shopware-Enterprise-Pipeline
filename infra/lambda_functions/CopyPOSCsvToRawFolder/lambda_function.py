import json
import boto3
import urllib.parse
import datetime

s3 = boto3.client('s3')
DESTINATION_PREFIX = "raw/batch/pos-raw/"

def lambda_handler(event, context):
    try:
        # Extract values from input
        source_bucket = event['bucket']
        source_key = urllib.parse.unquote_plus(event['key'])
        destination_prefix = event.get('target_prefix', DESTINATION_PREFIX)
        processing_date = event.get('processing_date', str(datetime.date.today()))
        execution_name = event.get('execution_name', 'unknown_execution')

        # Skip if not a CSV
        if not source_key.lower().endswith('.csv'):
            print(f"Skipped non-CSV file: {source_key}")
            return {
                "status": "skipped",
                "output_location": None,
                "records_copied": 0,
                "processing_date": processing_date,
                "run_timestamp": datetime.datetime.utcnow().isoformat()
            }

        # Skip if already in destination
        if source_key.startswith(destination_prefix):
            print(f"File already in destination: {source_key}")
            return {
                "status": "already_in_destination",
                "output_location": source_key,
                "records_copied": 0,
                "processing_date": processing_date,
                "run_timestamp": datetime.datetime.utcnow().isoformat()
            }

        filename = source_key.split('/')[-1]
        destination_key = f"{destination_prefix}{filename}"

        print(f"Copying {source_key} to {destination_key}...")

        s3.copy_object(
            Bucket=source_bucket,
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Key=destination_key
        )

        print(f"Successfully copied to {destination_key}")
        return {
            "status": "success",
            "output_location": destination_key,
            "records_copied": 1,
            "processing_date": processing_date,
            "run_timestamp": datetime.datetime.utcnow().isoformat()
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

