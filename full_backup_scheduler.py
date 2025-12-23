#!/usr/bin/env python3
"""
DynamoDB全量备份调度器
使用DynamoDB Export功能定期导出全量数据到S3
"""

import boto3
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any

class DynamoDBFullBackup:
    def __init__(self, source_region: str, backup_bucket: str, backup_region: str):
        self.source_region = source_region
        self.backup_bucket = backup_bucket
        self.backup_region = backup_region
        self.ddb_client = boto3.client('dynamodb', region_name=source_region)
        self.s3_client = boto3.client('s3', region_name=backup_region)
    
    def export_table_to_s3(self, table_name: str) -> Dict[str, Any]:
        """导出DynamoDB表到S3"""
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        export_prefix = f"full-backups/{table_name}/{timestamp}/"
        
        try:
            response = self.ddb_client.export_table_to_point_in_time(
                TableArn=f"arn:aws:dynamodb:{self.source_region}:{boto3.client('sts').get_caller_identity()['Account']}:table/{table_name}",
                S3Bucket=self.backup_bucket,
                S3Prefix=export_prefix,
                ExportFormat='DYNAMODB_JSON'
            )
            
            export_arn = response['ExportDescription']['ExportArn']
            print(f"✅ 全量备份已启动: {export_arn}")
            print(f"📁 备份路径: s3://{self.backup_bucket}/{export_prefix}")
            
            # 保存备份元数据
            metadata = {
                'export_arn': export_arn,
                'table_name': table_name,
                'export_time': timestamp,
                's3_path': f"s3://{self.backup_bucket}/{export_prefix}",
                'status': 'IN_PROGRESS'
            }
            
            metadata_key = f"backup-metadata/{table_name}/full_backup_{timestamp}.json"
            self.s3_client.put_object(
                Bucket=self.backup_bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2)
            )
            
            return metadata
            
        except Exception as e:
            print(f"❌ 全量备份失败: {e}")
            raise

    def check_export_status(self, export_arn: str) -> str:
        """检查导出状态"""
        try:
            response = self.ddb_client.describe_export(ExportArn=export_arn)
            return response['ExportDescription']['ExportStatus']
        except Exception as e:
            print(f"❌ 检查导出状态失败: {e}")
            return 'FAILED'

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='DynamoDB全量备份')
    parser.add_argument('--table-name', required=True, help='DynamoDB表名')
    parser.add_argument('--source-region', default='us-east-1', help='源区域')
    parser.add_argument('--backup-bucket', required=True, help='备份S3桶名')
    parser.add_argument('--backup-region', default='us-west-2', help='备份区域')
    
    args = parser.parse_args()
    
    backup = DynamoDBFullBackup(args.source_region, args.backup_bucket, args.backup_region)
    metadata = backup.export_table_to_s3(args.table_name)
    
    print(f"🔄 监控导出进度: aws dynamodb describe-export --export-arn {metadata['export_arn']} --region {args.source_region}")
