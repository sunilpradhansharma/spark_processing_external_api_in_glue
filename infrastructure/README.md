# Spark Processing Infrastructure

This project contains the AWS CDK infrastructure code for deploying a Spark processing pipeline using AWS Glue and S3.

## AWS Credentials Setup

Before deploying the infrastructure, you need to set up your AWS credentials. There are two ways to do this:

### Option 1: AWS CLI Configuration (Recommended)

1. Install the AWS CLI:
```bash
brew install awscli
```

2. Configure your AWS credentials:
```bash
aws configure
```

When prompted, enter:
- AWS Access Key ID
- AWS Secret Access Key
- Default region name (e.g., us-east-1)
- Default output format (json)

### Option 2: Environment Variables

Create a `.env` file in the infrastructure directory with the following content:
```bash
# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_REGION=your_region_here
AWS_ACCOUNT_ID=your_account_id_here

# Environment
ENVIRONMENT=development
```

To get your AWS credentials:
1. Log into AWS Console
2. Go to IAM → Users → Your User
3. Create new access keys under "Security credentials"
4. Copy the Access Key ID and Secret Access Key

To get your AWS Account ID:
1. Click on your account name in the top right
2. The Account ID is displayed in the menu

## Architecture

The infrastructure includes:

- S3 bucket for data storage with:
  - Versioning enabled
  - Server-side encryption (optional KMS)
  - Lifecycle rules for cost optimization
  - CORS configuration
  - Metrics enabled

- AWS Glue job with:
  - Python 3.9 Spark ETL
  - Auto-scaling enabled
  - Spark UI monitoring
  - Job insights
  - Configurable worker type and count

- CloudWatch monitoring with:
  - Custom dashboard
  - Standard Glue metrics
  - Custom metrics support
  - Configurable alarms for:
    - Failed tasks
    - Memory utilization
    - Processing time

- Security features:
  - KMS encryption (optional)
  - IAM roles with least privilege
  - S3 bucket policy
  - CloudWatch logging

## Prerequisites

- Node.js 14.x or later
- AWS CDK CLI
- AWS credentials configured
- TypeScript 4.x or later

## Installation

1. Install dependencies:
```bash
npm install
```

2. Build the project:
```bash
npm run build
```

## Deployment

1. Bootstrap CDK (first time only):
```bash
cdk bootstrap
```

2. Deploy the stack:
```bash
cdk deploy
```

## Configuration

The infrastructure can be configured through the `bin/app.ts` file:

```typescript
new SparkProcessingConstruct(app, 'SparkProcessingInfra', {
  bucketName: 'my-spark-processing-bucket',
  glueJobName: 'spark-processing-job',
  glueJobScript: 's3://my-spark-processing-bucket/scripts/processor.py',
  glueWorkerType: 'G.1X',
  glueNumberOfWorkers: 10,
  glueTimeout: 2880,
  enableEncryption: true,
  enableMetrics: true,
  customMetrics: [
    'glue.driver.aggregate.shuffleLocalBytesRead',
    'glue.driver.aggregate.shuffleRemoteBytesRead'
  ],
  alarmThresholds: {
    failedTasks: 5,
    memoryUtilization: 85,
    processingTime: 3600
  }
});
```

## Monitoring

The infrastructure creates a CloudWatch dashboard named `{glueJobName}-metrics` with:

1. Job Execution Metrics:
   - Completed tasks
   - Failed tasks

2. Resource Utilization:
   - JVM heap usage
   - CPU system load

3. Data Processing Metrics:
   - S3 read bytes
   - S3 write bytes

4. Custom Metrics (if configured)

## Alarms

Configurable alarms for:

1. Failed Tasks:
   - Triggers when the number of failed tasks exceeds threshold
   - 5-minute evaluation period

2. Memory Utilization:
   - Triggers when JVM heap usage exceeds threshold
   - 10-minute evaluation period (2 data points)

3. Processing Time:
   - Triggers when job runtime exceeds threshold
   - 5-minute evaluation period

## Cost Optimization

The infrastructure includes several cost optimization features:

1. S3 Lifecycle Rules:
   - Transition to IA storage after 30 days
   - Clean up old versions after 90 days
   - Abort incomplete multipart uploads after 7 days

2. Glue Job Configuration:
   - Auto-scaling enabled
   - Configurable worker count
   - Timeout settings

## Security

Security features include:

1. KMS Encryption (optional):
   - Customer-managed key
   - Automatic key rotation
   - Least privilege access

2. IAM Roles:
   - Minimal required permissions
   - Separate roles for different components

3. S3 Security:
   - Block public access
   - Versioning enabled
   - Server-side encryption

## Cleanup

To remove the infrastructure:

```bash
cdk destroy
```

Note: The S3 bucket will be retained by default to prevent accidental data loss. To delete it, change the `removalPolicy` in the construct. 