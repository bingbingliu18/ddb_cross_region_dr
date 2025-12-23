#!/bin/bash
# 定期全量备份调度脚本
# 建议通过cron每周执行一次

set -e

# 配置参数
TABLE_NAME="source-test-table"
SOURCE_REGION="us-east-1"
BACKUP_BUCKET="test-cross-region-backup-1766218528"
BACKUP_REGION="us-west-2"

echo "🔄 开始执行定期全量备份..."
echo "📅 时间: $(date)"
echo "📊 表名: $TABLE_NAME"
echo "🌍 源区域: $SOURCE_REGION"
echo "🪣 备份桶: $BACKUP_BUCKET"

# 执行全量备份
python3 full_backup_scheduler.py \
    --table-name "$TABLE_NAME" \
    --source-region "$SOURCE_REGION" \
    --backup-bucket "$BACKUP_BUCKET" \
    --backup-region "$BACKUP_REGION"

if [ $? -eq 0 ]; then
    echo "✅ 全量备份调度成功"
    
    # 可选: 发送通知
    # aws sns publish --topic-arn "arn:aws:sns:region:account:backup-notifications" \
    #     --message "DynamoDB全量备份已启动: $TABLE_NAME"
else
    echo "❌ 全量备份调度失败"
    exit 1
fi

echo "📝 添加到crontab示例:"
echo "# 每周日凌晨2点执行全量备份"
echo "0 2 * * 0 /home/ubuntu/DDB/ddb_cross_region_dr/schedule_full_backup.sh >> /var/log/ddb_backup.log 2>&1"
