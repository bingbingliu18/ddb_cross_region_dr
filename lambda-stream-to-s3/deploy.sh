#!/bin/bash

# Deploy DynamoDB Stream to S3 Lambda function using SAM

echo "Building SAM application..."
sam build

echo "Deploying SAM application..."
sam deploy --guided

# Example parameters for guided deployment:
# Stack Name: ddb-stream-to-s3
# AWS Region: us-east-1
# Parameter TargetS3Bucket: my-target-backup-bucket
# Parameter TargetRegion: us-west-2
# Parameter TargetAccountNumber: 123456789012
# Parameter TargetRoleName: CrossAccountS3WriteRole
# Parameter SourceTableStreamARN: arn:aws:dynamodb:us-east-1:111111111111:table/source-table/stream/...
# Parameter S3Prefix: ddb-changes
# Parameter MaximumRecordAgeInSeconds: 3600

echo "Deployment completed!"
