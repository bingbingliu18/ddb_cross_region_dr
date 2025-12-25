#!/usr/bin/env python3
"""
Enhanced Batch Applier with Retry Mechanism
"""

import boto3
import json
import argparse
import logging
import time
from datetime import datetime
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer
from retry_decorator import retry_dynamodb_operation, retry_s3_operation

class EnhancedBatchApplier:
    def __init__(self, target_table_name: str, region: str = 'us-west-2', log_suffix: str = None, shared_log_file: str = None):
        self.target_table_name = target_table_name
        self.region = region
        self.deserializer = TypeDeserializer()
        
        # Unified log file naming - support custom suffix or shared log file
        if shared_log_file:
            self.error_log_file = shared_log_file
        elif log_suffix:
            self.error_log_file = f"apply_changes_{target_table_name}_{log_suffix}.log"
        else:
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.error_log_file = f"apply_changes_{target_table_name}_{timestamp_str}.log"
        
        # Configure logging - use append mode, multiple calls share same file
        logger_name = f"batch_applier_{target_table_name}_{log_suffix or 'standalone'}"
        self.logger = logging.getLogger(logger_name)
        
        # Only configure handlers if not already configured
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            
            # File handler - always append mode for shared logging
            file_handler = logging.FileHandler(self.error_log_file, mode='a')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
            # Console handler - only add if not in disaster recovery mode
            if not log_suffix or not log_suffix.startswith('disaster_recovery'):
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(formatter)
                self.logger.addHandler(console_handler)
        
        # Prevent propagation to avoid duplicate logs
        self.logger.propagate = False
        
        # Initialize AWS clients
        self.s3 = boto3.client('s3', region_name=region)
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(target_table_name)
        
        # Verify table exists
        self._verify_table()
    
    @retry_dynamodb_operation
    def _verify_table(self):
        """Verify target table exists"""
        try:
            self.table.load()
            self.logger.info(f"✅ Target table {self.target_table_name} verified successfully")
        except ClientError:
            raise ValueError(f"❌ Table {self.target_table_name} does not exist")
    
    @retry_s3_operation
    def _read_s3_file(self, bucket: str, key: str) -> list:
        """Read file from S3 with retry"""
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        records = json.loads(obj['Body'].read())
        self.logger.info(f"📄 Read {len(records)} records from S3")
        return records
    
    @retry_dynamodb_operation
    def _apply_batch_with_retry(self, batch_records: list) -> dict:
        """Apply single batch with retry mechanism"""
        stats = {'applied': 0, 'errors': 0}
        error_records = []
        
        # Use direct write, not batch_writer deduplication logic
        dynamodb_client = boto3.client('dynamodb', region_name=self.table.meta.client.meta.region_name)
        
        for i, record in enumerate(batch_records):
            try:
                event = record['eventName']
                
                if event in ['INSERT', 'MODIFY']:
                    # INSERT/MODIFY unified using idempotent put_item
                    # Prerequisite: NewImage contains complete record
                    item = {k: self.deserializer.deserialize(v) 
                           for k, v in record['dynamodb']['NewImage'].items()}
                    
                    from boto3.dynamodb.types import TypeSerializer
                    serializer = TypeSerializer()
                    ddb_item = {k: serializer.serialize(v) for k, v in item.items()}
                    
                    # Idempotent PUT operation
                    dynamodb_client.put_item(
                        TableName=self.target_table_name,
                        Item=ddb_item
                    )
                    stats['applied'] += 1
                    
                elif event == 'REMOVE':
                    # REMOVE operation - delete existing record
                    key_attrs = {k: self.deserializer.deserialize(v) 
                               for k, v in record['dynamodb']['Keys'].items()}
                    
                    from boto3.dynamodb.types import TypeSerializer
                    serializer = TypeSerializer()
                    ddb_key = {k: serializer.serialize(v) for k, v in key_attrs.items()}
                    
                    try:
                        dynamodb_client.delete_item(
                            TableName=self.target_table_name,
                            Key=ddb_key,
                            ConditionExpression='attribute_exists(#pk)',
                            ExpressionAttributeNames={'#pk': list(key_attrs.keys())[0]}
                        )
                        stats['applied'] += 1
                    except ClientError as e:
                        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                            self.logger.info(f"REMOVE skipped - record does not exist")
                            stats['applied'] += 1
                        else:
                            raise
                    
            except Exception as e:
                stats['errors'] += 1
                error_msg = f"Record {i}: {str(e)}"
                self.logger.error(error_msg)
                
                error_records.append({
                    'record_index': i,
                    'error': str(e),
                    'record_data': record
                })
        
        # Save error records
        if error_records:
            error_file = f"batch_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(error_file, 'w') as f:
                json.dump(error_records, f, indent=2, default=str)
            self.logger.warning(f"⚠️ Batch error details saved to: {error_file}")
        
        return stats
    
    def apply_changes_from_s3(self, s3_file_path: str, batch_size: int = 100) -> bool:
        """Apply changes from S3 file with complete retry mechanism"""
        try:
            # Parse S3 path
            bucket, key = s3_file_path.replace('s3://', '').split('/', 1)
            
            # Log start of file processing
            self.logger.info(f"🔄 Starting to process file: {s3_file_path}")
            
            # Read S3 file (with retry)
            records = self._read_s3_file(bucket, key)
            
            if not records:
                self.logger.info("📄 File is empty, no processing needed")
                return True
            
            # Process records in batches
            total_stats = {'applied': 0, 'errors': 0}
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            for i in range(0, len(records), batch_size):
                batch_records = records[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                self.logger.info(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch_records)} records)")
                
                # Apply batch (with retry)
                max_batch_retries = 3
                batch_success = False
                
                for retry_attempt in range(max_batch_retries):
                    try:
                        batch_stats = self._apply_batch_with_retry(batch_records)
                        
                        # Accumulate statistics
                        total_stats['applied'] += batch_stats['applied']
                        total_stats['errors'] += batch_stats['errors']
                        
                        batch_success = True
                        break
                        
                    except Exception as e:
                        if retry_attempt < max_batch_retries - 1:
                            delay = 2 ** retry_attempt  # Exponential backoff
                            self.logger.warning(f"⚠️ Batch {batch_num} failed, retrying in {delay}s: {e}")
                            time.sleep(delay)
                        else:
                            self.logger.error(f"❌ Batch {batch_num} final failure: {e}")
                            total_stats['errors'] += len(batch_records)
                
                if not batch_success:
                    self.logger.error(f"❌ Batch {batch_num} processing failed")
            
            # Output final statistics
            success_rate = (total_stats['applied'] / len(records)) * 100 if records else 100
            self.logger.info(f"📊 File {key} processing completed: {total_stats['applied']} success, {total_stats['errors']} failed ({success_rate:.1f}%)")
            
            if total_stats['errors'] > 0:
                self.logger.warning(f"⚠️ Error log: {self.error_log_file}")
            
            return total_stats['errors'] == 0
            
        except Exception as e:
            self.logger.error(f"❌ Apply changes failed: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Enhanced DynamoDB Change Applier (with retry)')
    parser.add_argument('--s3-file-path', required=True, help='S3 file path')
    parser.add_argument('--target-table', required=True, help='Target table name')
    parser.add_argument('--region', default='us-west-2', help='AWS region')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch processing size')
    
    args = parser.parse_args()
    
    applier = EnhancedBatchApplier(args.target_table, args.region)
    success = applier.apply_changes_from_s3(args.s3_file_path, args.batch_size)
    
    if success:
        print("✅ Changes applied successfully")
        return 0
    else:
        print("❌ Changes application failed")
        return 1

if __name__ == "__main__":
    exit(main())
