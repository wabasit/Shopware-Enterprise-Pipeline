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

         # Extract the object key (file path) and decode it in case it contains special characters
        source_key = urllib.parse.unquote_plus(record['s3']['object']['key'])

