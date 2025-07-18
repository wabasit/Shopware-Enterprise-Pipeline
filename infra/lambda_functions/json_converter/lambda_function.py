import json
import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            logger.info(f"New file uploaded: s3://{bucket}/{key}")

            # Skip files that are already in the raw output path
            if 'raw/batch/inventory-raw/' in key:
                logger.info("File is already in target folder. Skipping.")
                continue

            # Read original file content
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')

            # Parse JSON content
            try:
                data = json.loads(content)
            except Exception as e:
                logger.error(f"Failed to parse JSON from {key}: {e}")
                continue

            if not isinstance(data, list):
                logger.warning(f"File {key} is not a JSON array. Skipping.")
                continue

            # Prepare output path
            filename = os.path.basename(key)
            output_key = f"raw/batch/inventory-raw/{filename}"