import os
import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """Lambda function to capture DynamoDB stream changes and save to cross-account S3"""
    
    # Environment Variables
    target_aws_account_num = os.environ['TARGET_AWS_ACCOUNT_NUMBER']
    target_role_name = os.environ['TARGET_ROLE_NAME']
    target_s3_bucket = os.environ['TARGET_S3_BUCKET']
    target_region = os.environ['TARGET_REGION']
    s3_prefix = os.environ['S3_PREFIX']

    role_arn = f"arn:aws:iam::{target_aws_account_num}:role/{target_role_name}"

    try:
        # Get cross-account credentials
        sts_response = get_credentials(role_arn)
        
        # Create S3 client with cross-account credentials
        s3 = boto3.client(
            's3', 
            region_name=target_region,
            aws_access_key_id=sts_response['AccessKeyId'],
            aws_secret_access_key=sts_response['SecretAccessKey'],
            aws_session_token=sts_response['SessionToken']
        )

        # Process DynamoDB stream records
        records = event.get('Records', [])
        if not records:
            return {'statusCode': 200, 'message': 'No records to process'}

        # Generate S3 key with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        s3_key = f"{s3_prefix}/ddb_changes_{timestamp}.json"
        
        # Save records to S3
        s3.put_object(
            Bucket=target_s3_bucket,
            Key=s3_key,
            Body=json.dumps(records, default=str),
            ContentType='application/json'
        )
        
        print(f"Successfully saved {len(records)} records to s3://{target_s3_bucket}/{s3_key}")
        
        return {
            'statusCode': 200,
            'processed_records': len(records),
            's3_location': f"s3://{target_s3_bucket}/{s3_key}"
        }
        
    except Exception as e:
        print(f"Error processing records: {str(e)}")
        raise e

def get_credentials(role_arn):
    """Assume cross-account role and return temporary credentials"""
    sts_client = boto3.client('sts')

    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName="ddb_stream_to_s3_lambda"
    )

    return assumed_role_object['Credentials']
