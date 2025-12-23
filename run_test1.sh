#!/bin/bash
# 运行测试1 - 批量数据加载恢复测试
# 执行前：清除测试环境重建，删除之前使用的S3测试环境，删除源表和目标表
# 重新创建源表（支持PITR/stream），同时调用deploy-single-account.sh删除已有lambda环境重新部署lambda
# lambda环境部署：重新开一个s3目录，更新Lambda环境变量指向正确的S3桶
# lambda表的捕获是batch:1000 时间间隔是60秒

echo "🧪 启动测试1: 批量数据加载恢复测试"
echo "测试流程:"
echo "  1. 清理测试环境"
echo "  2. 重新创建源表（支持PITR/stream）"
echo "  3. 重新部署Lambda环境"
echo "  4. 批量数据加载10万条数据，TPS约100/s"
echo "  5. 在数据加载过程中执行全量备份"
echo "  6. 等数据加载完成后，做全量恢复+增量恢复"
echo "  7. 验证DDB目标表和源表数据量是否一致"
echo ""

# 检查依赖
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 未安装"
    exit 1
fi

# 检查AWS凭证
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS凭证未配置"
    exit 1
fi

echo "🧹 步骤1: 清理现有环境..."

# 删除现有Lambda stack
echo "删除现有Lambda stack..."
aws cloudformation delete-stack --region us-east-1 --stack-name ddb-stream-single-account-test 2>/dev/null || true
echo "等待Lambda stack删除完成..."
aws cloudformation wait stack-delete-complete --region us-east-1 --stack-name ddb-stream-single-account-test 2>/dev/null || true

# 删除现有源表
echo "删除现有源表..."
aws dynamodb delete-table --region us-east-1 --table-name source-test-table 2>/dev/null || true
aws dynamodb wait table-not-exists --region us-east-1 --table-name source-test-table 2>/dev/null || true

# 删除现有目标表
echo "删除现有目标表..."
aws dynamodb delete-table --region us-west-2 --table-name test1-recovery-table 2>/dev/null || true
aws dynamodb wait table-not-exists --region us-west-2 --table-name test1-recovery-table 2>/dev/null || true

# 清理S3桶
echo "清理S3测试环境..."
for bucket in $(aws s3 ls | grep "test-cross-region-backup-" | awk '{print $3}'); do
    echo "清理S3桶: $bucket"
    aws s3 rm s3://$bucket --recursive 2>/dev/null || true
    aws s3 rb s3://$bucket 2>/dev/null || true
done

echo "✅ 环境清理完成"

echo "🏗️ 步骤2: 重新创建源DDB表（支持PITR/stream）..."
aws dynamodb create-table \
    --region us-east-1 \
    --table-name source-test-table \
    --attribute-definitions AttributeName=id,AttributeType=S \
    --key-schema AttributeName=id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES

echo "等待源表创建完成..."
aws dynamodb wait table-exists --region us-east-1 --table-name source-test-table

echo "启用Point-in-Time Recovery..."
aws dynamodb update-continuous-backups \
    --region us-east-1 \
    --table-name source-test-table \
    --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true

echo "✅ 源表创建完成（已启用PITR和Stream）"

echo "🚀 步骤3: 重新部署Lambda环境..."
cd lambda-stream-to-s3

# 修改deploy-single-account.sh以使用新的S3目录
TIMESTAMP=$(date +%s)
NEW_BUCKET_NAME="test-cross-region-backup-$TIMESTAMP"

echo "创建新的S3桶: $NEW_BUCKET_NAME"

# 使用修改后的部署脚本（会自动创建S3桶和获取Stream ARN）
./deploy-single-account.sh $NEW_BUCKET_NAME

cd ..

echo "✅ Lambda环境部署完成"
echo "   S3桶: $NEW_BUCKET_NAME"
echo "   批次大小: 1000"
echo "   时间间隔: 60秒"

echo "🧪 步骤4: 运行批量数据加载测试..."
export TEST_S3_BUCKET="$NEW_BUCKET_NAME"

# 运行测试脚本
python3 test1_batch_load_recovery.py

echo ""
echo "📊 测试完成，查看结果文件:"
ls -la test1_results_*.json 2>/dev/null || echo "未找到结果文件"

echo ""
echo "🔍 检查S3桶中的文件:"
echo "全量备份文件:"
aws s3 ls s3://$NEW_BUCKET_NAME/full-backups/ --recursive
echo ""
echo "增量变更文件:"
aws s3 ls s3://$NEW_BUCKET_NAME/ddb-changes/ --recursive

echo ""
echo "✅ 测试1完成！"
