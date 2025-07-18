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