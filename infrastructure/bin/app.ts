#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { SparkProcessingStack } from '../lib/spark-processing-stack';
import { awsConfig } from '../config/aws-config';

const app = new cdk.App();

// Create the Spark processing infrastructure
new SparkProcessingStack(app, 'SparkProcessingStack', {
  env: awsConfig.env,
  bucketName: `spark-processing-${awsConfig.env.account}-${awsConfig.env.region}`,
  glueJobName: 'spark-processing-job',
  glueJobScript: 's3://spark-processing-${awsConfig.env.account}-${awsConfig.env.region}/scripts/processor.py',
  glueWorkerType: 'G.1X',
  glueNumberOfWorkers: 10,
  glueTimeout: 2880,
  enableEncryption: true,
  enableMetrics: true,
  customMetrics: [
    'glue.driver.aggregate.shuffleLocalBytesRead',
    'glue.driver.aggregate.shuffleRemoteBytesRead',
    'glue.driver.BlockManager.memory.memUsed_MB'
  ],
  alarmThresholds: {
    failedTasks: 5,
    memoryUtilization: 85,
    processingTime: 3600
  },
  tags: awsConfig.tags
});

// Add tags to all resources
cdk.Tags.of(app).add('Project', awsConfig.tags.Project);
cdk.Tags.of(app).add('Environment', awsConfig.tags.Environment);
cdk.Tags.of(app).add('ManagedBy', awsConfig.tags.ManagedBy); 