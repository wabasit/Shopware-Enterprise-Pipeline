import json
import boto3
import urllib.parse

s3 = boto3.client('s3')

DESTINATION_PREFIX = "raw/batch/pos-raw/"