#!/usr/bin/env python3
"""
Disaster Recovery Manager
Implements complete recovery workflow with full + incremental backup
"""

import boto3
import json
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import time

class DisasterRecoveryManager:
    def __init__(self, source_region: str, target_region: str, backup_bucket: str):
        self.source_region = source_region
        self.target_region = target_region
        self.backup_bucket = backup_bucket
        self.ddb_source = boto3.client('dynamodb', region_name=source_region)
        self.ddb_target = boto3.client('dynamodb', region_name=target_region)
        self.s3_client = boto3.client('s3')
        self.logger = None  # Will be initialized in full_disaster_recovery
        
        # Validate backup bucket exists
        try:
            self.s3_client.head_bucket(Bucket=backup_bucket)
        except self.s3_client.exceptions.NoSuchBucket:
            raise ValueError(f"❌ Backup bucket '{backup_bucket}' does not exist")
        except Exception as e:
            raise ValueError(f"❌ Cannot access backup bucket '{backup_bucket}': {e}")
    
    def _setup_logger(self, log_filename: str):
        """Setup unified logger for disaster recovery"""
        self.logger = logging.getLogger(f"disaster_recovery_{id(self)}")
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            
            # File handler
            file_handler = logging.FileHandler(log_filename, mode='a')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
    
    def find_latest_full_backup(self, table_name: str, before_time: Optional[str] = None, 
                               backup_dir: Optional[str] = None) -> Optional[Dict]:
        """Find the latest full backup or use specified backup directory"""
        try:
            # If backup directory is specified, use it directly
            if backup_dir:
                return self._load_backup_from_dir(table_name, backup_dir)
            
            prefix = f"backup-metadata/{table_name}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.backup_bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                print(f"❌ No backup metadata found for table {table_name}")
                return None
            
            backups = []
            for obj in response['Contents']:
                if obj['Key'].endswith('.json'):
                    try:
                        metadata_obj = self.s3_client.get_object(
                            Bucket=self.backup_bucket,
                            Key=obj['Key']
                        )
                        metadata = json.loads(metadata_obj['Body'].read())
                        
                        # Check if before specified time
                        if before_time and metadata['export_time'] > before_time:
                            continue
                            
                        backups.append(metadata)
                    except Exception as e:
                        print(f"⚠️ Skip invalid backup metadata: {obj['Key']}")
            
            if not backups:
                self.logger.info("❌ No backup metadata found") if self.logger else print("❌ No backup metadata found")
                return None
                
            # Sort by time, return latest
            latest = sorted(backups, key=lambda x: x['export_time'], reverse=True)[0]
            msg = f"📦 Found latest full backup: {latest['export_time']}"
            self.logger.info(msg) if self.logger else print(msg)
            return latest
            
        except Exception as e:
            msg = f"❌ Failed to find full backup: {e}"
            self.logger.error(msg) if self.logger else print(msg)
            return None
    
    def _load_backup_from_dir(self, table_name: str, backup_dir: str) -> Optional[Dict]:
        """Load backup metadata from specified directory"""
        try:
            # Construct metadata file path
            metadata_key = f"backup-metadata/{table_name}/{backup_dir}.json"
            
            msg = f"📂 Using specified full backup directory: {backup_dir}"
            self.logger.info(msg) if self.logger else print(msg)
            
            try:
                metadata_obj = self.s3_client.get_object(
                    Bucket=self.backup_bucket,
                    Key=metadata_key
                )
                metadata = json.loads(metadata_obj['Body'].read())
                msg = f"✅ Loaded backup metadata: {metadata['export_time']}"
                self.logger.info(msg) if self.logger else print(msg)
                return metadata
                
            except self.s3_client.exceptions.NoSuchKey:
                msg = f"❌ Backup metadata not found for specified directory: {metadata_key}"
                self.logger.error(msg) if self.logger else print(msg)
                return None
                
        except Exception as e:
            msg = f"❌ Failed to load specified backup: {e}"
            self.logger.error(msg) if self.logger else print(msg)
            return None
    
    def restore_from_full_backup(self, backup_metadata: Dict, target_table: str) -> bool:
        """Restore from full backup with retry mechanism"""
        from retry_decorator import retry_import_export
        
        @retry_import_export
        def _start_import():
            # Get source table schema
            source_table = backup_metadata['table_name']
            source_table_info = self.ddb_source.describe_table(TableName=source_table)
            
            # Use DynamoDB Import functionality
            s3_prefix = backup_metadata['s3_path'].replace(f"s3://{self.backup_bucket}/", "")
            # Add AWSDynamoDB and export ID path
            export_id = backup_metadata['export_arn'].split('/')[-1]
            s3_prefix = f"{s3_prefix}AWSDynamoDB/{export_id}/data/"
            
            response = self.ddb_target.import_table(
                S3BucketSource={
                    'S3Bucket': self.backup_bucket,
                    'S3KeyPrefix': s3_prefix
                },
                InputFormat='DYNAMODB_JSON',
                InputCompressionType='GZIP',
                TableCreationParameters={
                    'TableName': target_table,
                    'AttributeDefinitions': source_table_info['Table']['AttributeDefinitions'],
                    'KeySchema': source_table_info['Table']['KeySchema'],
                    'BillingMode': 'PAY_PER_REQUEST'
                }
            )
            return response['ImportTableDescription']['ImportArn']
        
        try:
            msg = f"🔄 Starting restore from full backup to table: {target_table}"
            self.logger.info(msg) if self.logger else print(msg)
            
            # Start Import (with retry)
            import_arn = _start_import()
            msg = f"✅ Full restore started: {import_arn}"
            self.logger.info(msg) if self.logger else print(msg)
            
            # Wait for import completion (with retry check)
            max_wait_time = 3600  # Maximum wait 1 hour
            check_interval = 30   # Check every 30 seconds
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                try:
                    status_response = self.ddb_target.describe_import(ImportArn=import_arn)
                    status = status_response['ImportTableDescription']['ImportStatus']
                    
                    if status == 'COMPLETED':
                        msg = f"✅ Full restore completed"
                        self.logger.info(msg) if self.logger else print(msg)
                        return True
                    elif status == 'FAILED':
                        failure_code = status_response['ImportTableDescription'].get('FailureCode', 'Unknown')
                        failure_msg = status_response['ImportTableDescription'].get('FailureMessage', 'Unknown')
                        msg = f"❌ Full restore failed: {failure_code} - {failure_msg}"
                        self.logger.error(msg) if self.logger else print(msg)
                        return False
                    else:
                        msg = f"🔄 Full restore in progress: {status}"
                        self.logger.info(msg) if self.logger else print(msg)
                        time.sleep(check_interval)
                        elapsed_time += check_interval
                        
                except Exception as e:
                    msg = f"⚠️ Failed to check Import status, retrying: {e}"
                    self.logger.warning(msg) if self.logger else print(msg)
                    time.sleep(check_interval)
                    elapsed_time += check_interval
            
            msg = f"❌ Full restore timeout ({max_wait_time} seconds)"
            self.logger.error(msg) if self.logger else print(msg)
            return False
            
        except Exception as e:
            msg = f"❌ Full restore failed: {e}"
            self.logger.error(msg) if self.logger else print(msg)
            return False
    
    def find_incremental_changes(self, export_arn: str) -> List[str]:
        """Find incremental changes starting 60 seconds before export time"""
        try:
            # Get export details to find export time
            export_response = self.ddb_source.describe_export(ExportArn=export_arn)
            export_time_str = export_response['ExportDescription']['ExportTime']
            
            # Parse export time and subtract 60 seconds
            from datetime import datetime, timezone, timedelta
            
            # Handle both string and datetime object
            if isinstance(export_time_str, str):
                export_time = datetime.fromisoformat(export_time_str.replace('Z', '+00:00'))
            else:
                # Already a datetime object
                export_time = export_time_str
                if export_time.tzinfo is None:
                    export_time = export_time.replace(tzinfo=timezone.utc)
            
            start_time = export_time - timedelta(seconds=60)
            
            msg = f"📅 Export time: {export_time}"
            self.logger.info(msg) if self.logger else print(msg)
            msg = f"📅 Looking for changes from: {start_time}"
            self.logger.info(msg) if self.logger else print(msg)
            
            # List all change files in ddb-changes/ directory
            prefix = "ddb-changes/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.backup_bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                msg = f"ℹ️ No change files found in {prefix}"
                self.logger.info(msg) if self.logger else print(msg)
                return []
            
            # Filter and sort files by timestamp
            change_files = []
            for obj in response['Contents']:
                file_key = obj['Key']
                if file_key.endswith('.json'):
                    # Use S3 LastModified time instead of parsing filename
                    # This is more reliable than parsing complex filename timestamps
                    file_time = obj['LastModified']
                    
                    # Ensure timezone awareness
                    if file_time.tzinfo is None:
                        file_time = file_time.replace(tzinfo=timezone.utc)
                    
                    # Include files from start_time onwards
                    if file_time >= start_time:
                        change_files.append((file_time, file_key))
            
            # Sort by timestamp and return file keys
            change_files.sort(key=lambda x: x[0])
            sorted_files = [file_key for _, file_key in change_files]
            
            msg = f"📄 Found {len(sorted_files)} change files to apply"
            self.logger.info(msg) if self.logger else print(msg)
            for file_key in sorted_files[:5]:  # Show first 5
                msg = f"  - {file_key}"
                self.logger.info(msg) if self.logger else print(msg)
            if len(sorted_files) > 5:
                msg = f"  ... and {len(sorted_files) - 5} more files"
                self.logger.info(msg) if self.logger else print(msg)
            
            return sorted_files
            
        except Exception as e:
            msg = f"❌ Failed to find incremental changes: {e}"
            self.logger.error(msg) if self.logger else print(msg)
            return []
    
    def apply_incremental_changes(self, change_files: List[str], target_table: str, log_suffix: str) -> bool:
        """Apply incremental changes using flat structure (all files in ddb-changes/)"""
        try:
            msg = "📄 Using enhanced batch applier for flat structure"
            self.logger.info(msg) if self.logger else print(msg)
            from enhanced_batch_applier import EnhancedBatchApplier
            
            # Create single applier instance for all files to share the same log
            log_filename = f"apply_changes_{target_table}_{log_suffix}.log"
            applier = EnhancedBatchApplier(target_table, self.target_region, log_suffix=log_suffix, shared_log_file=log_filename)
            
            for file_key in change_files:
                msg = f"🔄 Applying change file: {file_key}"
                self.logger.info(msg) if self.logger else print(msg)
                s3_path = f"s3://{self.backup_bucket}/{file_key}"
                
                # Use enhanced applier (with retry)
                max_file_retries = 3
                file_success = False
                
                for retry_attempt in range(max_file_retries):
                    try:
                        success = applier.apply_changes_from_s3(s3_path)
                        if success:
                            msg = f"✅ Change file applied successfully: {file_key}"
                            self.logger.info(msg) if self.logger else print(msg)
                            file_success = True
                            break
                        else:
                            msg = f"⚠️ Change file application failed: {file_key}"
                            self.logger.warning(msg) if self.logger else print(msg)
                            
                    except Exception as e:
                        if retry_attempt < max_file_retries - 1:
                            delay = 2 ** retry_attempt
                            msg = f"⚠️ File {file_key} processing failed, retrying in {delay}s: {e}"
                            self.logger.warning(msg) if self.logger else print(msg)
                            time.sleep(delay)
                        else:
                            msg = f"❌ File {file_key} final failure: {e}"
                            self.logger.error(msg) if self.logger else print(msg)
                
                if not file_success:
                    msg = f"❌ Failed to apply change file: {file_key}"
                    self.logger.error(msg) if self.logger else print(msg)
                    return False
                    
            msg = f"✅ All incremental changes applied"
            self.logger.info(msg) if self.logger else print(msg)
            return True
            
        except Exception as e:
            msg = f"❌ Failed to apply incremental changes: {e}"
            self.logger.error(msg) if self.logger else print(msg)
            return False
    
    def full_disaster_recovery(self, source_table: str, target_table: str, 
                             disaster_time: Optional[str] = None,
                             backup_dir: Optional[str] = None) -> bool:
        """Complete disaster recovery workflow"""
        # Create unified log filename for entire disaster recovery process
        recovery_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_suffix = f"disaster_recovery_{recovery_timestamp}"
        log_filename = f"apply_changes_{target_table}_{log_suffix}.log"
        
        # Setup unified logger
        self._setup_logger(log_filename)
        
        self.logger.info(f"🚨 Starting disaster recovery: {source_table} → {target_table}")
        self.logger.info(f"📝 Disaster recovery log: {log_filename}")
        
        # 1. Find latest full backup or use specified directory
        backup_metadata = self.find_latest_full_backup(source_table, disaster_time, backup_dir)
        if not backup_metadata:
            self.logger.error(f"❌ No available full backup found")
            return False
        
        # 2. Restore from full backup
        if not self.restore_from_full_backup(backup_metadata, target_table):
            return False
        
        # 3. Find and apply incremental changes
        change_files = self.find_incremental_changes(backup_metadata['export_arn'])
        if change_files:
            if not self.apply_incremental_changes(change_files, target_table, log_suffix):
                return False
        else:
            self.logger.info(f"ℹ️ No incremental changes to apply")
        
        self.logger.info(f"🎉 Disaster recovery completed!")
        self.logger.info(f"📝 Detailed logs available at: {log_filename}")
        return True

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='DynamoDB Disaster Recovery')
    parser.add_argument('--source-table', required=True, help='Source table name')
    parser.add_argument('--target-table', required=True, help='Target table name')
    parser.add_argument('--source-region', default='us-east-1', help='Source region')
    parser.add_argument('--target-region', default='us-west-2', help='Target region')
    parser.add_argument('--backup-bucket', required=True, help='Backup S3 bucket name')
    parser.add_argument('--disaster-time', help='Disaster time point (YYYYMMDD_HHMMSS)')
    parser.add_argument('--backup-dir', help='Specify full backup directory (e.g.: full_backup_20251220_084513)')
    
    args = parser.parse_args()
    
    dr_manager = DisasterRecoveryManager(
        args.source_region, 
        args.target_region, 
        args.backup_bucket
    )
    
    success = dr_manager.full_disaster_recovery(
        args.source_table,
        args.target_table,
        args.disaster_time,
        args.backup_dir
    )
    
    if success:
        print(f"✅ Disaster recovery completed successfully")
    else:
        print(f"❌ Disaster recovery failed")
        exit(1)
