import requests
import json
import boto3
import datetime
import time

# AWS clients
firehose = boto3.client('firehose')

# Firehose stream names
FIREHOSE_MAP = {
    'customer-interaction': 'shopware-firehose',
    'web-logs': 'weblogs-firehose'
}
