# DynamoDB Cross-Region Disaster Recovery

AWS DynamoDB跨区域灾难恢复解决方案，支持实时增量同步和全量备份恢复。

## 功能特性

- **实时增量同步**: 基于DynamoDB Streams捕获变更数据
- **全量备份**: 使用DynamoDB Export功能定期备份
- **灾难恢复**: 自动化的全量+增量恢复流程
- **幂等处理**: 支持重复执行，避免数据重复
- **统一日志**: 优化的日志管理，便于监控和调试

## 架构组件

### 核心应用
- `enhanced_batch_applier.py` - 增强批量应用器，支持统一日志输出
- `disaster_recovery_manager.py` - 灾难恢复管理器，协调全量+增量恢复
- `retry_decorator.py` - 重试装饰器，提供可靠的错误重试机制

### Lambda组件
- `lambda-stream-to-s3/` - DynamoDB Streams到S3的实时数据捕获

### 备份工具
- `full_backup_scheduler.py` - 全量备份调度器
- `schedule_full_backup.sh` - 定期备份脚本

### 测试工具
- `test1_batch_load_recovery.py` - 完整的灾难恢复测试
- `run_test1.sh` - 自动化测试脚本

## 快速开始

### 1. 环境准备
```bash
# 安装依赖
pip install -r requirements.txt

# 配置AWS凭证
aws configure
```

### 2. 部署Lambda环境
```bash
cd lambda-stream-to-s3
./deploy-single-account.sh your-backup-bucket-name
```

### 3. 运行测试
```bash
# 完整的灾难恢复测试
./run_test1.sh
```

## 使用说明

### 全量备份
```bash
# 手动触发全量备份
python3 full_backup_scheduler.py --table-name your-table --bucket your-bucket

# 定期备份（建议cron配置）
./schedule_full_backup.sh
```

### 灾难恢复
```bash
# 从最新备份恢复
python3 disaster_recovery_manager.py \
  --source-bucket your-backup-bucket \
  --target-table your-recovery-table \
  --target-region us-west-2
```

### 增量应用
```bash
# 应用单个增量文件
python3 enhanced_batch_applier.py \
  --s3-file-path s3://bucket/path/to/changes.json \
  --target-table your-table \
  --region us-west-2
```

## 配置参数

### 主要配置
- `SOURCE_REGION`: 源DynamoDB表区域 (默认: us-east-1)
- `TARGET_REGION`: 目标DynamoDB表区域 (默认: us-west-2)
- `BACKUP_BUCKET`: S3备份桶名称
- `BATCH_SIZE`: 批处理大小 (默认: 100)

### 日志配置
- 统一日志文件: `apply_changes_{table_name}_{timestamp}.log`
- 支持INFO和WARNING级别
- 自动追加模式，避免日志覆盖

## 测试场景

### Test1: 批量加载恢复测试
1. 加载10万条测试数据 (TPS: 100/s)
2. 在加载过程中触发全量备份
3. 删除目标表
4. 从备份恢复全量数据
5. 应用增量变更
6. 验证数据一致性

```bash
./run_test1.sh
```

## 监控和日志

### 日志文件
- `apply_changes_*.log` - 增量应用日志
- `run_test1_*.log` - 测试执行日志
- `test1_batch_load_recovery_*.log` - 批量加载日志

### 关键指标
- 数据加载TPS
- 增量应用成功率
- 恢复时间目标(RTO)
- 恢复点目标(RPO)

## 故障排除

### 常见问题
1. **Lambda部署失败**: 检查SAM CLI安装和AWS权限
2. **增量应用错误**: 查看apply_changes日志文件
3. **备份恢复失败**: 验证S3权限和DynamoDB导入权限

### 重试机制
- 自动重试DynamoDB操作 (最多3次)
- 指数退避策略
- 详细错误日志记录

## 最佳实践

1. **定期备份**: 建议每周执行全量备份
2. **监控延迟**: 关注增量同步延迟
3. **测试恢复**: 定期执行灾难恢复演练
4. **权限管理**: 使用最小权限原则配置IAM角色

## 许可证

MIT License
