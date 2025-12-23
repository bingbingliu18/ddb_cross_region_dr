#!/bin/bash

# Single Account Cross-Region Test Setup and Deployment
# Modified to only deploy Lambda without creating table (table created by main script)

echo "=== Deploying Lambda function for cross-region stream capture ==="

# Configuration
SOURCE_REGION="us-east-1"
TARGET_REGION="us-west-2"
TABLE_NAME="source-test-table"

# Get bucket name from parameter or generate new one
if [ -z "$1" ]; then
    BUCKET_NAME="test-cross-region-backup-$(date +%s)"
else
    BUCKET_NAME="$1"
fi

echo "Source Region: $SOURCE_REGION"
echo "Target Region: $TARGET_REGION"
echo "Table Name: $TABLE_NAME"
echo "Bucket Name: $BUCKET_NAME"

echo ""
echo "1. Creating target S3 bucket in $TARGET_REGION..."
aws s3 mb s3://$BUCKET_NAME --region $TARGET_REGION

echo ""
echo "2. Getting stream ARN from existing table..."
STREAM_ARN=$(aws dynamodb describe-table \
    --region $SOURCE_REGION \
    --table-name $TABLE_NAME \
    --query 'Table.LatestStreamArn' \
    --output text)

if [ "$STREAM_ARN" == "None" ] || [ -z "$STREAM_ARN" ]; then
    echo "❌ 表 $TABLE_NAME 不存在或未启用Stream"
    exit 1
fi

echo "Stream ARN: $STREAM_ARN"

echo ""
echo "3. Building SAM application..."
sam build --template template-single-account.yml

echo ""
echo "4. Deploying Lambda function..."
sam deploy \
    --template template-single-account.yml \
    --stack-name ddb-stream-single-account-test \
    --region $SOURCE_REGION \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides \
        TargetS3Bucket=$BUCKET_NAME \
        TargetRegion=$TARGET_REGION \
        SourceTableStreamARN=$STREAM_ARN \
        S3Prefix=ddb-changes \
        MaximumRecordAgeInSeconds=3600 \
    --resolve-s3

echo ""
echo "=== Lambda Deployment Complete! ==="
echo ""
echo "Resources created:"
echo "- S3 Bucket: $BUCKET_NAME (region: $TARGET_REGION)"
echo "- Lambda Function: ddb-stream-single-account-test-StreamToS3Function-XXXXX"
echo "- Lambda配置: BatchSize=1000, MaximumBatchingWindowInSeconds=60"
echo ""
echo "Test the setup:"
echo "1. Insert test data:"
echo "   aws dynamodb put-item --region $SOURCE_REGION --table-name $TABLE_NAME --item '{\"id\":{\"S\":\"test1\"},\"name\":{\"S\":\"Test Item\"}}'"
echo ""
echo "2. Check S3 bucket for generated files:"
echo "   aws s3 ls s3://$BUCKET_NAME/ddb-changes/ --region $TARGET_REGION"
