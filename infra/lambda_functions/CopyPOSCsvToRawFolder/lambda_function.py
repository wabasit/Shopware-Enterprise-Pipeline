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

# Skip processing if the file is not a CSV
        if not source_key.lower().endswith('.csv'):
            print(f"Skipped non-CSV file: {source_key}")
            continue

          # Prevent processing files already in the destination path to avoid infinite loops
        if source_key.startswith(DESTINATION_PREFIX):
            print(f"File already in destination folder, skipping: {source_key}")
            continue

         # Extract the filename from the full key/path
        filename = source_key.split('/')[-1]

         # Define the new destination key (path) where the file will be copied
        destination_key = f"{DESTINATION_PREFIX}{filename}"

        try:
            # Log the copy action
            print(f"Copying {source_key} to {destination_key}...")
            
            # Copy the file from the original location to the destination path within the same bucket
            s3.copy_object(
                Bucket=source_bucket,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Key=destination_key
            )

             # Log success message
            print(f"Successfully copied to {destination_key}")
        except Exception as e:
            # Log and raise any errors that occur during the copy operation
            print(f"Error copying file: {e}")
            raise e