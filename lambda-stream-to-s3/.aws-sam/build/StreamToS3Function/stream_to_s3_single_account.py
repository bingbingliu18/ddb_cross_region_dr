import os
import json
import boto3
import time
from datetime import datetime

def lambda_handler(event, context):
    """Single account Lambda function to capture DynamoDB stream changes and save to S3 with flat structure"""
    
    # Environment Variables
    target_s3_bucket = os.environ['TARGET_S3_BUCKET']
    target_region = os.environ['TARGET_REGION']
    s3_prefix = os.environ['S3_PREFIX']

    def retry_s3_operation(operation, max_retries=3):
        """S3 operation retry mechanism"""
        for attempt in range(max_retries):
            try:
                return operation()
            except Exception as e:
                error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', str(type(e).__name__))
                
                # Retriable errors
                retriable_errors = [
                    'RequestTimeout', 'ServiceUnavailable', 'SlowDown', 
                    'InternalError', 'RequestLimitExceeded'
                ]
                
                if error_code in retriable_errors and attempt < max_retries - 1:
                    delay = (2 ** attempt) * 0.5  # Exponential backoff
                    print(f"⚠️ S3 operation failed ({error_code}), retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    print(f"❌ S3 operation finally failed: {error_code}")
                    raise

    try:
        # Create S3 client for target region
        s3 = boto3.client('s3', region_name=target_region)

        # Process DynamoDB stream records
        records = event.get('Records', [])
        if not records:
            return {'statusCode': 200, 'message': 'No records to process'}

        print(f"📊 Processing {len(records)} DynamoDB Stream records")

        # Generate S3 key with flat structure - all files in ddb-changes/
        now = datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S_%f')
        s3_key = f"ddb-changes/ddb_changes_{timestamp}.json"
        
        # Save records to S3 with retry
        def upload_to_s3():
            return s3.put_object(
                Bucket=target_s3_bucket,
                Key=s3_key,
                Body=json.dumps(records, default=str),
                ContentType='application/json'
            )
        
        retry_s3_operation(upload_to_s3)
        
        print(f"✅ Successfully saved {len(records)} records to s3://{target_s3_bucket}/{s3_key}")
        
        return {
            'statusCode': 200,
            'processed_records': len(records),
            's3_location': f"s3://{target_s3_bucket}/{s3_key}"
        }
        
    except Exception as e:
        error_msg = f"Error processing records: {str(e)}"
        print(f"❌ {error_msg}")
        # Lambda will automatically retry failed invocations
        raise Exception(error_msg)
