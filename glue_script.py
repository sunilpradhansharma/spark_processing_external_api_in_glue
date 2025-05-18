import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from src.processor import Config, APIClient, BatchProcessor
from src.metrics import ProcessingMetrics, CloudWatchMetricsPublisher, MetricsLogger
from src.exceptions import *
import os
import logging
import json
from datetime import datetime
import time
import asyncio

def setup_logging(job_name: str) -> None:
    """Setup logging configuration"""
    log_level = os.environ.get('LOG_LEVEL', 'INFO')
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create logger
    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)
    
    # Add CloudWatch handler
    cloudwatch_handler = logging.StreamHandler()
    cloudwatch_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    logger.addHandler(cloudwatch_handler)
    
    return logger

def get_job_metrics(glue_context) -> dict:
    """Get Glue job metrics"""
    return {
        'num_executors': glue_context._jvm.GlueMetrics.numExecutors(),
        'num_allocated_executors': glue_context._jvm.GlueMetrics.numAllocatedExecutors(),
        'executor_memory': glue_context._jvm.GlueMetrics.executorMemory(),
        'driver_memory': glue_context._jvm.GlueMetrics.driverMemory()
    }

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'source_key',
    'api_endpoint',
    'batch_size',
    'max_workers',
    'metrics_namespace'
])

job.init(args['JOB_NAME'], args)

# Setup logging
logger = setup_logging(args['JOB_NAME'])

try:
    # Initialize metrics
    metrics = ProcessingMetrics()
    metrics_publisher = CloudWatchMetricsPublisher(args['metrics_namespace'])
    metrics_logger = MetricsLogger(metrics)
    metrics_logger.start()

    # Configure the processor
    config = Config(
        s3_bucket=args['source_bucket'],
        s3_key=args['source_key'],
        api_endpoint=args['api_endpoint'],
        batch_size=int(args.get('batch_size', 1000)),
        max_workers=int(args.get('max_workers', 20))
    )

    # Log configuration
    logger.info(f"Starting job with configuration: {json.dumps(config.__dict__, indent=2)}")
    logger.info(f"Glue metrics: {json.dumps(get_job_metrics(glueContext), indent=2)}")

    # Create DynamicFrame from S3
    try:
        datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [f"s3://{config.s3_bucket}/{config.s3_key}"],
                "recurse": True
            },
            format="json"
        )
    except Exception as e:
        raise S3ReadError(f"Failed to read from S3: {str(e)}", config.s3_bucket, config.s3_key)

    # Convert to DataFrame for better partitioning
    df = datasource.toDF()
    total_records = df.count()
    metrics.total_records = total_records
    
    logger.info(f"Total records to process: {total_records:,}")

    # Repartition based on executor cores
    partition_count = sc.defaultParallelism
    df = df.repartition(partition_count)
    
    logger.info(f"Repartitioned data into {partition_count} partitions")

    # Process records function
    async def process_partition_async(partition):
        # Convert partition to list of dictionaries
        records = [row.asDict() for row in partition]
        
        # Create batch processor
        processor = BatchProcessor(config)
        
        # Process the partition
        return await processor.process_partition(records)

    # Process all partitions
    logger.info("Starting partition processing")
    
    # Convert RDD processing to use async
    def process_partition_wrapper(partition):
        # Create event loop for this partition
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Run async processing
            return loop.run_until_complete(process_partition_async(partition))
        finally:
            loop.close()
    
    results = df.rdd.mapPartitions(lambda p: [process_partition_wrapper(p)]).collect()

    # Aggregate results
    total_success = sum(r['success_count'] for r in results)
    total_errors = sum(r['error_count'] for r in results)
    records_per_second = sum(r['records_per_second'] for r in results) / len(results)

    # Final metrics
    metrics_logger.stop()
    final_stats = metrics.get_statistics()
    
    # Save detailed metrics
    metrics_logger.save_metrics(f"/tmp/metrics_{datetime.now():%Y%m%d_%H%M%S}.json")
    
    # Publish final metrics to CloudWatch
    metrics_publisher.publish_processing_metrics(metrics)

    # Log final statistics
    logger.info(f"""
Processing completed:
Total records processed successfully: {total_success:,}
Total errors: {total_errors:,}
Average processing speed: {records_per_second:.2f} records/second
API Statistics:
- Average latency: {final_stats['api_latency']['mean']:.2f}ms
- Rate limit hits: {final_stats['rate_limit_hits']:,}
- Retry count: {final_stats['retry_count']:,}
Error Distribution:
{json.dumps(final_stats['error_distribution'], indent=2)}
""")

except Exception as e:
    logger.error(f"Job failed: {str(e)}", exc_info=True)
    raise
finally:
    # Ensure metrics logger is stopped
    if 'metrics_logger' in locals():
        metrics_logger.stop()
    
    job.commit() 