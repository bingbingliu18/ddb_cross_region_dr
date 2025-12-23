#!/usr/bin/env python3
"""
通用重试装饰器
为所有应用提供统一的重试机制
"""

import time
import functools
from typing import Callable, Type, Tuple
from botocore.exceptions import ClientError

def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    retriable_errors: Tuple[str, ...] = (
        'ProvisionedThroughputExceededException',
        'ThrottlingException',
        'RequestLimitExceeded',
        'ServiceUnavailable',
        'InternalServerError'
    )
):
    """
    重试装饰器，支持指数退避
    
    Args:
        max_retries: 最大重试次数
        initial_delay: 初始延迟(秒)
        backoff_factor: 退避因子
        exceptions: 需要重试的异常类型
        retriable_errors: 可重试的AWS错误代码
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                    
                except ClientError as e:
                    error_code = e.response['Error']['Code']
                    
                    # 检查是否为可重试错误
                    if error_code not in retriable_errors:
                        print(f"❌ 不可重试错误: {error_code}")
                        raise
                    
                    last_exception = e
                    
                    if attempt < max_retries:
                        print(f"⚠️ {error_code}, 第{attempt + 1}次重试，{delay:.1f}秒后...")
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        print(f"❌ 达到最大重试次数({max_retries})")
                        raise
                        
                except exceptions as e:
                    last_exception = e
                    
                    if attempt < max_retries:
                        print(f"⚠️ 错误: {type(e).__name__}, 第{attempt + 1}次重试，{delay:.1f}秒后...")
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        print(f"❌ 达到最大重试次数({max_retries})")
                        raise
            
            # 不应该到达这里
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator

# 预定义的重试策略
def retry_dynamodb_operation(func: Callable) -> Callable:
    """DynamoDB操作重试装饰器"""
    return retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        backoff_factor=2.0,
        retriable_errors=(
            'ProvisionedThroughputExceededException',
            'ThrottlingException',
            'RequestLimitExceeded',
            'InternalServerError'
        )
    )(func)

def retry_s3_operation(func: Callable) -> Callable:
    """S3操作重试装饰器"""
    return retry_with_backoff(
        max_retries=3,
        initial_delay=0.5,
        backoff_factor=2.0,
        retriable_errors=(
            'RequestTimeout',
            'ServiceUnavailable',
            'SlowDown',
            'InternalError'
        )
    )(func)

def retry_import_export(func: Callable) -> Callable:
    """Import/Export操作重试装饰器"""
    return retry_with_backoff(
        max_retries=5,
        initial_delay=2.0,
        backoff_factor=2.0,
        retriable_errors=(
            'LimitExceededException',
            'ThrottlingException',
            'InternalServerError'
        )
    )(func)

# 使用示例
if __name__ == "__main__":
    import boto3
    
    # 示例1: DynamoDB操作
    @retry_dynamodb_operation
    def put_item_with_retry(table_name: str, item: dict):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        return table.put_item(Item=item)
    
    # 示例2: S3操作
    @retry_s3_operation
    def get_object_with_retry(bucket: str, key: str):
        s3 = boto3.client('s3')
        return s3.get_object(Bucket=bucket, Key=key)
    
    # 示例3: 自定义重试
    @retry_with_backoff(max_retries=5, initial_delay=2.0)
    def custom_operation():
        # 你的操作
        pass
    
    print("✅ 重试装饰器模块加载成功")
