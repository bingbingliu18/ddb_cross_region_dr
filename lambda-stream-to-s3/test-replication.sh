#!/bin/bash

# Test script for single account cross-region DynamoDB replication

echo "=== Testing DynamoDB Stream to S3 Replication ==="

# Get the bucket name from the stack outputs
BUCKET_NAME=$(aws cloudformation describe-stacks \
    --stack-name ddb-stream-single-account-test \
    --region us-east-1 \
    --query 'Stacks[0].Parameters[?ParameterKey==`TargetS3Bucket`].ParameterValue' \
    --output text)

TABLE_NAME="source-test-table"
SOURCE_REGION="us-east-1"
TARGET_REGION="us-west-2"

echo "Bucket: $BUCKET_NAME"
echo "Table: $TABLE_NAME"

echo ""
echo "1. Inserting test data into DynamoDB..."

# Insert test records
aws dynamodb put-item \
    --region $SOURCE_REGION \
    --table-name $TABLE_NAME \
    --item '{"id":{"S":"test1"},"name":{"S":"Test Item 1"},"timestamp":{"S":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}}'

aws dynamodb put-item \
    --region $SOURCE_REGION \
    --table-name $TABLE_NAME \
    --item '{"id":{"S":"test2"},"name":{"S":"Test Item 2"},"value":{"N":"42"}}'

# Update an item
aws dynamodb update-item \
    --region $SOURCE_REGION \
    --table-name $TABLE_NAME \
    --key '{"id":{"S":"test1"}}' \
    --update-expression "SET #n = :val" \
    --expression-attribute-names '{"#n":"name"}' \
    --expression-attribute-values '{":val":{"S":"Updated Test Item 1"}}'

# Delete an item
aws dynamodb delete-item \
    --region $SOURCE_REGION \
    --table-name $TABLE_NAME \
    --key '{"id":{"S":"test2"}}'

echo ""
echo "2. Waiting for Lambda processing (30 seconds)..."
sleep 30

echo ""
echo "3. Checking S3 bucket for generated files..."
aws s3 ls s3://$BUCKET_NAME/ddb-changes/ --region $TARGET_REGION

echo ""
echo "4. Downloading latest file for inspection..."
LATEST_FILE=$(aws s3 ls s3://$BUCKET_NAME/ddb-changes/ --region $TARGET_REGION | sort | tail -n 1 | awk '{print $4}')

if [ ! -z "$LATEST_FILE" ]; then
    echo "Latest file: $LATEST_FILE"
    aws s3 cp s3://$BUCKET_NAME/ddb-changes/$LATEST_FILE ./latest_changes.json --region $TARGET_REGION
    
    echo ""
    echo "5. File contents:"
    cat latest_changes.json | jq '.[0] | {eventName, dynamodb: {Keys, NewImage, OldImage}}'
    
    echo ""
    echo "Total records in file:"
    cat latest_changes.json | jq '. | length'
else
    echo "No files found. Check Lambda logs:"
    echo "aws logs describe-log-groups --log-group-name-prefix /aws/lambda/ddb-stream-single-account-test"
fi

echo ""
echo "=== Test Complete ==="
