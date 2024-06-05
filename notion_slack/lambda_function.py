import json
from scan_service import ScanService
from schema import NotionPage
import boto3

def lambda_handler(event, context):
    scan_sevice = ScanService()
    scan_sevice.scan_and_schedule_page()

    return {
        'statusCode': 200
    }
