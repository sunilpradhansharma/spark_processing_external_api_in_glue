#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { SparkProcessingStack } from '../lib/spark_processing_stack';

const app = new cdk.App();
new SparkProcessingStack(app, 'SparkProcessingStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION
  }
}); 