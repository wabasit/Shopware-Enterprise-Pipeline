# Import necessary libraries
import json
import boto3
import urllib.parse


# Initialize the S3 client
s3 = boto3.client('s3')

# Define the destination prefix where CSV files should be moved
DESTINATION_PREFIX = "raw/batch/pos-raw/"


def lambda_handler(event, context):
    # Loop through each S3 event record
    for record in event['Records']:
  # Extract the source S3 bucket name
        source_bucket = record['s3']['bucket']['name']