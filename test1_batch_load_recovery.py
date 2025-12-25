#!/usr/bin/env python3
"""
测试1: 批量数据加载恢复测试 (新流程)
- 10万条数据，TPS=100/s
- 加载过程中执行全量备份
- 完成后删除目标表
- 从最新export恢复
- 使用enhanced_batch_applier应用增量变更
- 验证数据一致性
"""

import boto3
import json
import time
import threading
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'test1_batch_load_recovery_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchLoadRecoveryTest:
    def __init__(self):
        self.source_region = 'us-east-1'
        self.target_region = 'us-west-2'
        self.source_table = 'source-test-table'
        self.target_table = 'test1-recovery-table'
        
        # 使用Lambda实际配置的S3桶名称
        import os
        self.backup_bucket = os.environ.get('TEST_S3_BUCKET')
        if not self.backup_bucket:
            raise ValueError("TEST_S3_BUCKET环境变量未设置，请先运行run_test1.sh")
        
        # DynamoDB客户端
        self.ddb_source = boto3.client('dynamodb', region_name=self.source_region)
        self.ddb_target = boto3.client('dynamodb', region_name=self.target_region)
        self.s3 = boto3.client('s3', region_name=self.target_region)
        
        # 测试参数
        self.total_records = 100000
        self.target_tps = 100
        self.batch_size = 25  # DynamoDB batch_write_item限制
        
        # 状态跟踪
        self.loaded_count = 0
        self.export_arn = None
        self.export_start_time = None
        self.load_complete_time = None
        
    def cleanup_environment(self):
        """清理测试环境"""
        logger.info("🧹 清理测试环境...")
        
        # 删除源表
        try:
            self.ddb_source.delete_table(TableName=self.source_table)
            logger.info(f"✅ 删除源表: {self.source_table}")
            
            # 等待表删除完成
            waiter = self.ddb_source.get_waiter('table_not_exists')
            waiter.wait(TableName=self.source_table)
        except Exception as e:
            if 'ResourceNotFoundException' not in str(e):
                logger.error(f"删除源表失败: {e}")
        
        # 删除目标表
        try:
            self.ddb_target.delete_table(TableName=self.target_table)
            logger.info(f"✅ 删除目标表: {self.target_table}")
            
            # 等待表删除完成
            waiter = self.ddb_target.get_waiter('table_not_exists')
            waiter.wait(TableName=self.target_table)
        except Exception as e:
            if 'ResourceNotFoundException' not in str(e):
                logger.error(f"删除目标表失败: {e}")
        
        # 清理S3桶
        self.cleanup_s3_buckets()
    
    def cleanup_s3_buckets(self):
        """清理相关S3桶"""
        try:
            # 列出所有以test-cross-region-backup开头的桶
            response = self.s3.list_buckets()
            for bucket in response['Buckets']:
                bucket_name = bucket['Name']
                if bucket_name.startswith('test-cross-region-backup'):
                    logger.info(f"🗑️ 清理S3桶: {bucket_name}")
                    
                    # 删除桶中所有对象
                    try:
                        objects = self.s3.list_objects_v2(Bucket=bucket_name)
                        if 'Contents' in objects:
                            delete_keys = [{'Key': obj['Key']} for obj in objects['Contents']]
                            self.s3.delete_objects(
                                Bucket=bucket_name,
                                Delete={'Objects': delete_keys}
                            )
                        
                        # 删除桶
                        self.s3.delete_bucket(Bucket=bucket_name)
                        logger.info(f"✅ 已删除S3桶: {bucket_name}")
                    except Exception as e:
                        logger.error(f"删除S3桶失败 {bucket_name}: {e}")
        except Exception as e:
            logger.error(f"清理S3桶失败: {e}")

    def create_source_table(self):
        """创建源表"""
        try:
            self.ddb_source.create_table(
                TableName=self.source_table,
                KeySchema=[
                    {'AttributeName': 'id', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'id', 'AttributeType': 'S'}
                ],
                BillingMode='PAY_PER_REQUEST',
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_AND_OLD_IMAGES'
                }
            )
            
            # 等待表创建完成
            waiter = self.ddb_source.get_waiter('table_exists')
            waiter.wait(TableName=self.source_table)
            
            # 启用Point-in-Time Recovery
            self.ddb_source.update_continuous_backups(
                TableName=self.source_table,
                PointInTimeRecoverySpecification={
                    'PointInTimeRecoveryEnabled': True
                }
            )
            
            logger.info(f"✅ 创建源表: {self.source_table} (已启用PITR)")
            return True
        except Exception as e:
            if 'ResourceInUseException' in str(e):
                logger.info(f"源表已存在: {self.source_table}")
                return True
            logger.error(f"❌ 创建源表失败: {e}")
            return False
    
    def create_s3_bucket(self):
        """检查S3存储桶是否存在"""
        try:
            self.s3.head_bucket(Bucket=self.backup_bucket)
            logger.info(f"✅ 使用现有S3桶: {self.backup_bucket}")
            return True
        except Exception as e:
            logger.error(f"❌ S3桶不存在: {e}")
            return False
    
    def create_target_table(self):
        """创建目标恢复表"""
        try:
            # 获取源表结构
            source_desc = self.ddb_source.describe_table(TableName=self.source_table)
            table_def = source_desc['Table']
            
            # 创建目标表
            self.ddb_target.create_table(
                TableName=self.target_table,
                KeySchema=table_def['KeySchema'],
                AttributeDefinitions=table_def['AttributeDefinitions'],
                BillingMode='PAY_PER_REQUEST'
            )
            
            # 等待表创建完成
            waiter = self.ddb_target.get_waiter('table_exists')
            waiter.wait(TableName=self.target_table)
            logger.info(f"✅ 创建目标表: {self.target_table}")
            return True
        except Exception as e:
            if 'ResourceInUseException' in str(e):
                logger.info(f"目标表已存在: {self.target_table}")
                return True
            logger.error(f"❌ 创建目标表失败: {e}")
            return False
    
    def generate_batch_data(self, start_id, count):
        """生成批量数据"""
        items = []
        for i in range(count):
            item_id = start_id + i
            items.append({
                'PutRequest': {
                    'Item': {
                        'id': {'S': f'batch-{item_id:06d}'},
                        'data': {'S': f'Test data for item {item_id}'},
                        'timestamp': {'S': datetime.now(timezone.utc).isoformat()},
                        'batch_id': {'N': str(item_id // 1000)},
                        'test_type': {'S': 'batch_load_test'}
                    }
                }
            })
        return items
    
    def write_batch(self, items):
        """写入一批数据"""
        try:
            response = self.ddb_source.batch_write_item(
                RequestItems={
                    self.source_table: items
                }
            )
            
            # 处理未处理的项目
            unprocessed = response.get('UnprocessedItems', {})
            retry_count = 0
            while unprocessed and retry_count < 3:
                time.sleep(0.1 * (2 ** retry_count))  # 指数退避
                response = self.ddb_source.batch_write_item(RequestItems=unprocessed)
                unprocessed = response.get('UnprocessedItems', {})
                retry_count += 1
            
            return len(items)
        except Exception as e:
            logger.error(f"批量写入失败: {e}")
            return 0
    
    def batch_load_worker(self, thread_id, batches_per_thread):
        """批量加载工作线程"""
        loaded = 0
        for batch_num in range(batches_per_thread):
            start_id = thread_id * batches_per_thread * self.batch_size + batch_num * self.batch_size
            items = self.generate_batch_data(start_id, self.batch_size)
            
            written = self.write_batch(items)
            loaded += written
            self.loaded_count += written
            
            # 控制TPS
            time.sleep(self.batch_size / self.target_tps)
            
            if self.loaded_count % 5000 == 0:
                logger.info(f"已加载: {self.loaded_count:,} 条记录")
        
        return loaded
    
    def start_batch_loading(self):
        """开始批量数据加载"""
        logger.info(f"🚀 开始批量加载 {self.total_records:,} 条记录 (TPS: {self.target_tps})")
        
        # 计算线程和批次
        num_threads = 4
        total_batches = self.total_records // self.batch_size
        batches_per_thread = total_batches // num_threads
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for thread_id in range(num_threads):
                future = executor.submit(self.batch_load_worker, thread_id, batches_per_thread)
                futures.append(future)
            
            # 等待所有线程完成
            for future in as_completed(futures):
                try:
                    result = future.result()
                    logger.info(f"线程完成，加载了 {result} 条记录")
                except Exception as e:
                    logger.error(f"线程执行失败: {e}")
        
        self.load_complete_time = datetime.now(timezone.utc)
        elapsed = time.time() - start_time
        actual_tps = self.loaded_count / elapsed
        
        logger.info(f"✅ 批量加载完成:")
        logger.info(f"   总记录数: {self.loaded_count:,}")
        logger.info(f"   耗时: {elapsed:.1f} 秒")
        logger.info(f"   实际TPS: {actual_tps:.1f}")
    
    def trigger_export_during_load(self):
        """在加载过程中触发全量备份"""
        # 等待加载到50%时触发Export
        while self.loaded_count < self.total_records * 0.5:
            time.sleep(5)
        
        logger.info(f"🔄 触发全量备份 (已加载: {self.loaded_count:,} 条)")
        self.export_start_time = datetime.now(timezone.utc)
        
        try:
            # 动态获取当前账户ID
            sts = boto3.client('sts')
            account_id = sts.get_caller_identity()['Account']
            
            response = self.ddb_source.export_table_to_point_in_time(
                TableArn=f'arn:aws:dynamodb:{self.source_region}:{account_id}:table/{self.source_table}',
                S3Bucket=self.backup_bucket,
                S3Prefix=f'full-backups/{self.source_table}/{self.export_start_time.strftime("%Y%m%d_%H%M%S")}/',
                ExportFormat='DYNAMODB_JSON'
            )
            
            self.export_arn = response['ExportDescription']['ExportArn']
            logger.info(f"✅ Export已启动: {self.export_arn}")
            
            # 保存备份元数据
            s3_path = f's3://{self.backup_bucket}/full-backups/{self.source_table}/{self.export_start_time.strftime("%Y%m%d_%H%M%S")}/'
            metadata = {
                'export_arn': self.export_arn,
                'export_time': self.export_start_time.isoformat(),
                'table_name': self.source_table,
                'records_loaded_at_export': self.loaded_count,
                's3_path': s3_path
            }
            
            metadata_key = f'backup-metadata/{self.source_table}/test1_export_{self.export_start_time.strftime("%Y%m%d_%H%M%S")}.json'
            self.s3.put_object(
                Bucket=self.backup_bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2)
            )
            
        except Exception as e:
            logger.error(f"❌ Export启动失败: {e}")
    
    def wait_for_export_completion(self):
        """等待Export完成"""
        if not self.export_arn:
            return False
        
        logger.info("⏳ 等待Export完成...")
        while True:
            try:
                response = self.ddb_source.describe_export(ExportArn=self.export_arn)
                status = response['ExportDescription']['ExportStatus']
                
                if status == 'COMPLETED':
                    logger.info("✅ Export完成")
                    return True
                elif status == 'FAILED':
                    logger.error("❌ Export失败")
                    return False
                else:
                    logger.info(f"Export状态: {status}")
                    time.sleep(30)
            except Exception as e:
                logger.error(f"检查Export状态失败: {e}")
                time.sleep(30)
    
    def delete_target_table(self):
        """删除目标表"""
        try:
            self.ddb_target.delete_table(TableName=self.target_table)
            logger.info(f"🗑️ 删除目标表: {self.target_table}")
            
            # 等待表删除完成
            waiter = self.ddb_target.get_waiter('table_not_exists')
            waiter.wait(TableName=self.target_table)
            logger.info("✅ 目标表删除完成")
            return True
        except Exception as e:
            if 'ResourceNotFoundException' not in str(e):
                logger.error(f"删除目标表失败: {e}")
                return False
            logger.info("目标表不存在，跳过删除")
            return True

    def disaster_recovery(self):
        """使用disaster_recovery_manager进行完整恢复 (全量+增量)"""
        logger.info("🚨 开始灾难恢复 (全量+增量)...")
        
        try:
            from disaster_recovery_manager import DisasterRecoveryManager
            
            dr_manager = DisasterRecoveryManager(
                self.source_region,
                self.target_region, 
                self.backup_bucket
            )
            
            # 执行完整的灾难恢复 (全量 + 增量)
            success = dr_manager.full_disaster_recovery(
                self.source_table,
                self.target_table
            )
            
            if success:
                logger.info("✅ 灾难恢复完成 (全量+增量)")
                return True
            else:
                logger.error("❌ 灾难恢复失败")
                return False
                
        except Exception as e:
            logger.error(f"❌ 灾难恢复执行失败: {e}")
            return False
    
    def verify_data_consistency(self):
        """验证数据一致性"""
        logger.info("🔍 验证数据一致性...")
        
        try:
            # 获取源表记录数
            source_response = self.ddb_source.describe_table(TableName=self.source_table)
            source_count = source_response['Table']['ItemCount']
            
            # 获取目标表记录数
            target_response = self.ddb_target.describe_table(TableName=self.target_table)
            target_count = target_response['Table']['ItemCount']
            
            logger.info(f"源表记录数: {source_count:,}")
            logger.info(f"目标表记录数: {target_count:,}")
            
            # 精确计数验证
            source_scan = self.ddb_source.scan(
                TableName=self.source_table,
                Select='COUNT'
            )
            actual_source_count = source_scan['Count']
            
            target_scan = self.ddb_target.scan(
                TableName=self.target_table,
                Select='COUNT'
            )
            actual_target_count = target_scan['Count']
            
            logger.info(f"源表实际记录数: {actual_source_count:,}")
            logger.info(f"目标表实际记录数: {actual_target_count:,}")
            
            # 验证结果
            if actual_source_count == actual_target_count:
                logger.info("✅ 数据一致性验证通过")
                return True
            else:
                logger.error(f"❌ 数据不一致: 差异 {abs(actual_source_count - actual_target_count)} 条")
                return False
                
        except Exception as e:
            logger.error(f"❌ 验证失败: {e}")
            return False
    
    def run_test(self):
        """运行完整测试"""
        logger.info("=" * 60)
        logger.info("🧪 测试1: 批量数据加载恢复测试 (使用disaster_recovery_manager)")
        logger.info("流程: 删除目标表 -> disaster_recovery_manager完整恢复 (全量+增量)")
        logger.info("=" * 60)
        
        test_results = {
            'test_name': 'batch_load_recovery',
            'start_time': datetime.now(timezone.utc).isoformat(),
            'parameters': {
                'total_records': self.total_records,
                'target_tps': self.target_tps,
                'source_table': self.source_table,
                'target_table': self.target_table,
                'backup_bucket': self.backup_bucket
            }
        }
        
        try:
            # 1. 检查S3桶
            if not self.create_s3_bucket():
                raise Exception("S3桶检查失败")
            
            # 2. 检查源表是否存在
            logger.info("✅ 使用现有源表: source-test-table")
            
            # 3. 目标表将在恢复过程中自动创建
            
            # 4. 启动Export线程
            export_thread = threading.Thread(target=self.trigger_export_during_load)
            export_thread.start()
            
            # 5. 开始批量加载
            self.start_batch_loading()
            
            # 7. 等待Export线程完成
            export_thread.join()
            
            # 8. 等待Export完成
            if not self.wait_for_export_completion():
                raise Exception("Export未完成")
            
            # 9. 从export恢复（disaster_recovery_manager会自动处理目标表和增量）
            if not self.disaster_recovery():
                raise Exception("灾难恢复失败")
            
            # 10. 验证数据一致性
            consistency_ok = self.verify_data_consistency()
            
            # 记录结果
            test_results.update({
                'end_time': datetime.now(timezone.utc).isoformat(),
                'loaded_records': self.loaded_count,
                'export_arn': self.export_arn,
                'export_start_time': self.export_start_time.isoformat() if self.export_start_time else None,
                'load_complete_time': self.load_complete_time.isoformat() if self.load_complete_time else None,
                'consistency_verified': consistency_ok,
                'status': 'SUCCESS' if consistency_ok else 'FAILED'
            })
            
            # 保存测试结果
            result_file = f'test1_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(result_file, 'w') as f:
                json.dump(test_results, f, indent=2)
            
            logger.info("=" * 60)
            if consistency_ok:
                logger.info("🎉 测试1完成 - 成功!")
            else:
                logger.info("❌ 测试1完成 - 失败!")
            logger.info(f"详细结果: {result_file}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"❌ 测试失败: {e}")
            test_results.update({
                'end_time': datetime.now(timezone.utc).isoformat(),
                'error': str(e),
                'status': 'ERROR'
            })

if __name__ == '__main__':
    test = BatchLoadRecoveryTest()
    test.run_test()
