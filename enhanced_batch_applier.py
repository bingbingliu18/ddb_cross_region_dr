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
    def __init__(self, target_table_name: str, region: str = 'us-west-2', log_suffix: str = None):
        self.target_table_name = target_table_name
        self.region = region
        self.deserializer = TypeDeserializer()
        
        # 使用统一的日志文件
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.error_log_file = f"apply_changes_{target_table_name}_{timestamp}.log"
        
        # 配置日志 - 使用append模式避免覆盖
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(self.error_log_file, mode='a'),
                    logging.StreamHandler()
                ]
            )
        self.logger = logging.getLogger(__name__)
        
        # 初始化AWS客户端
        self.s3 = boto3.client('s3', region_name=region)
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(target_table_name)
        
        # 验证表存在
        self._verify_table()
    
    @retry_dynamodb_operation
    def _verify_table(self):
        """验证目标表存在"""
        try:
            self.table.load()
            self.logger.info(f"✅ 目标表 {self.target_table_name} 验证成功")
        except ClientError:
            raise ValueError(f"❌ 表 {self.target_table_name} 不存在")
    
    @retry_s3_operation
    def _read_s3_file(self, bucket: str, key: str) -> list:
        """从S3读取文件，带重试"""
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        records = json.loads(obj['Body'].read())
        self.logger.info(f"📄 从S3读取 {len(records)} 条记录")
        return records
    
    @retry_dynamodb_operation
    def _apply_batch_with_retry(self, batch_records: list) -> dict:
        """应用单个批次，带重试机制"""
        stats = {'applied': 0, 'errors': 0}
        error_records = []
        
        # 使用直接写入，不使用batch_writer的去重逻辑
        dynamodb_client = boto3.client('dynamodb', region_name=self.table.meta.client.meta.region_name)
        
        for i, record in enumerate(batch_records):
            try:
                event = record['eventName']
                
                if event in ['INSERT', 'MODIFY']:
                    # INSERT/MODIFY统一使用幂等put_item
                    # 前提：NewImage包含完整记录
                    item = {k: self.deserializer.deserialize(v) 
                           for k, v in record['dynamodb']['NewImage'].items()}
                    
                    from boto3.dynamodb.types import TypeSerializer
                    serializer = TypeSerializer()
                    ddb_item = {k: serializer.serialize(v) for k, v in item.items()}
                    
                    # 幂等PUT操作
                    dynamodb_client.put_item(
                        TableName=self.target_table_name,
                        Item=ddb_item
                    )
                    stats['applied'] += 1
                    
                elif event == 'REMOVE':
                    # REMOVE操作 - 删除现有记录
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
                            self.logger.info(f"REMOVE跳过 - 记录不存在")
                            stats['applied'] += 1
                        else:
                            raise
                    
            except Exception as e:
                stats['errors'] += 1
                error_msg = f"记录 {i}: {str(e)}"
                self.logger.error(error_msg)
                
                error_records.append({
                    'record_index': i,
                    'error': str(e),
                    'record_data': record
                })
        
        # 保存错误记录
        if error_records:
            error_file = f"batch_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(error_file, 'w') as f:
                json.dump(error_records, f, indent=2, default=str)
            self.logger.warning(f"⚠️ 批次错误详情保存到: {error_file}")
        
        return stats
    
    def apply_changes_from_s3(self, s3_file_path: str, batch_size: int = 100) -> bool:
        """从S3文件应用变更，带完整重试机制"""
        try:
            # 解析S3路径
            bucket, key = s3_file_path.replace('s3://', '').split('/', 1)
            
            # 读取S3文件 (带重试)
            records = self._read_s3_file(bucket, key)
            
            if not records:
                self.logger.info("📄 文件为空，无需处理")
                return True
            
            # 分批处理记录
            total_stats = {'applied': 0, 'errors': 0}
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            for i in range(0, len(records), batch_size):
                batch_records = records[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                self.logger.info(f"🔄 处理批次 {batch_num}/{total_batches} ({len(batch_records)} 条记录)")
                
                # 应用批次 (带重试)
                max_batch_retries = 3
                batch_success = False
                
                for retry_attempt in range(max_batch_retries):
                    try:
                        batch_stats = self._apply_batch_with_retry(batch_records)
                        
                        # 累计统计
                        total_stats['applied'] += batch_stats['applied']
                        total_stats['errors'] += batch_stats['errors']
                        
                        batch_success = True
                        break
                        
                    except Exception as e:
                        if retry_attempt < max_batch_retries - 1:
                            delay = 2 ** retry_attempt  # 指数退避
                            self.logger.warning(f"⚠️ 批次 {batch_num} 失败，{delay}秒后重试: {e}")
                            time.sleep(delay)
                        else:
                            self.logger.error(f"❌ 批次 {batch_num} 最终失败: {e}")
                            total_stats['errors'] += len(batch_records)
                
                if not batch_success:
                    self.logger.error(f"❌ 批次 {batch_num} 处理失败")
            
            # 输出最终统计
            success_rate = (total_stats['applied'] / len(records)) * 100 if records else 100
            self.logger.info(f"📊 处理完成: {total_stats['applied']} 成功, {total_stats['errors']} 失败 ({success_rate:.1f}%)")
            
            if total_stats['errors'] > 0:
                self.logger.warning(f"⚠️ 错误日志: {self.error_log_file}")
            
            return total_stats['errors'] == 0
            
        except Exception as e:
            self.logger.error(f"❌ 应用变更失败: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='增强版DynamoDB变更应用 (带重试)')
    parser.add_argument('--s3-file-path', required=True, help='S3文件路径')
    parser.add_argument('--target-table', required=True, help='目标表名')
    parser.add_argument('--region', default='us-west-2', help='AWS区域')
    parser.add_argument('--batch-size', type=int, default=100, help='批处理大小')
    
    args = parser.parse_args()
    
    applier = EnhancedBatchApplier(args.target_table, args.region)
    success = applier.apply_changes_from_s3(args.s3_file_path, args.batch_size)
    
    if success:
        print("✅ 变更应用成功")
        return 0
    else:
        print("❌ 变更应用失败")
        return 1

if __name__ == "__main__":
    exit(main())
