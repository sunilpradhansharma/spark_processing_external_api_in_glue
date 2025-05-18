import * as dotenv from 'dotenv';
import { Environment } from 'aws-cdk-lib';
import { fromIni } from '@aws-sdk/credential-providers';

// Load environment variables
dotenv.config();

export interface AWSConfig {
  env: Environment;
  tags: { [key: string]: string };
}

// Get AWS credentials from AWS CLI configuration
async function getAwsCredentials() {
  try {
    const credentials = await fromIni()();
    return {
      accessKeyId: credentials.accessKeyId,
      secretAccessKey: credentials.secretAccessKey
    };
  } catch (error) {
    console.error('Error loading AWS credentials:', error);
    throw error;
  }
}

// Get the AWS region from AWS CLI config or default to us-east-1
function getAwsRegion(): string {
  return process.env.AWS_REGION || 'us-east-1';
}

// Get the AWS account ID from the current session
async function getAwsAccountId(): Promise<string> {
  const { STSClient, GetCallerIdentityCommand } = require('@aws-sdk/client-sts');
  const sts = new STSClient({ region: getAwsRegion() });
  
  try {
    const response = await sts.send(new GetCallerIdentityCommand({}));
    return response.Account;
  } catch (error) {
    console.error('Error getting AWS account ID:', error);
    throw error;
  }
}

// Validate required environment variables
const requiredEnvVars = [
  'AWS_ACCESS_KEY_ID',
  'AWS_SECRET_ACCESS_KEY',
  'AWS_REGION',
  'AWS_ACCOUNT_ID'
];

requiredEnvVars.forEach(envVar => {
  if (!process.env[envVar]) {
    throw new Error(`Missing required environment variable: ${envVar}`);
  }
});

export const awsConfig: AWSConfig = {
  env: {
    account: process.env.AWS_ACCOUNT_ID || process.env.CDK_DEFAULT_ACCOUNT,
    region: getAwsRegion()
  },
  tags: {
    Environment: process.env.ENVIRONMENT || 'development',
    Project: 'SparkProcessing',
    ManagedBy: 'CDK'
  }
}; 