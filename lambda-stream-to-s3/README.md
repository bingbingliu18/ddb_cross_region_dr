# DynamoDB Stream to S3 Lambda Function

This SAM application deploys a Lambda function that automatically captures DynamoDB stream changes and saves them to a cross-account S3 bucket.

## Architecture

```
Source DynamoDB (Account A) → DynamoDB Streams → Lambda → Cross-Account S3 (Account B)
```

## Prerequisites

1. **Source Account (Account A)**:
   - DynamoDB table with streams enabled
   - SAM CLI installed
   - AWS CLI configured

2. **Target Account (Account B)**:
   - S3 bucket created
   - IAM role with S3 write permissions
   - Trust relationship allowing Account A to assume the role

## Target Account Setup

### 1. Create S3 Bucket
```bash
aws s3 mb s3://my-target-backup-bucket --region us-west-2
```

### 2. Create IAM Role
```bash
# Create trust policy file
cat > trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::SOURCE-ACCOUNT-ID:root"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create role
aws iam create-role \
    --role-name CrossAccountS3WriteRole \
    --assume-role-policy-document file://trust-policy.json

# Create and attach S3 write policy
cat > s3-write-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl"
      ],
      "Resource": "arn:aws:s3:::my-target-backup-bucket/*"
    }
  ]
}
EOF

aws iam put-role-policy \
    --role-name CrossAccountS3WriteRole \
    --policy-name S3WritePolicy \
    --policy-document file://s3-write-policy.json
```

## Deployment

### 1. Build and Deploy
```bash
./deploy.sh
```

### 2. Manual Deployment
```bash
sam build
sam deploy --guided
```

### 3. Required Parameters
- **TargetS3Bucket**: Target S3 bucket name
- **TargetAccountNumber**: Target AWS account ID
- **TargetRoleName**: IAM role name in target account
- **SourceTableStreamARN**: DynamoDB stream ARN
- **TargetRegion**: Target region (default: us-west-2)
- **S3Prefix**: S3 prefix for files (default: ddb-changes)

## Features

- **Event-driven**: Automatically triggered by DynamoDB stream events
- **Cross-account**: Saves data to different AWS account
- **Batch processing**: Processes up to 100 records per invocation
- **Error handling**: Built-in retry mechanism
- **Timestamped files**: Each batch saved with unique timestamp

## Output Files

Files are saved to S3 with the pattern:
```
s3://target-bucket/ddb-changes/ddb_changes_YYYYMMDD_HHMMSS_microseconds.json
```

## Monitoring

- Check CloudWatch Logs for Lambda execution logs
- Monitor DynamoDB stream metrics
- Check S3 bucket for generated files

## Testing

After deployment, make changes to your source DynamoDB table and verify that corresponding JSON files appear in the target S3 bucket.
