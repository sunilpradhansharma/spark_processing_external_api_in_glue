# S3 API Processor

A high-performance Python application designed to process 200 million records from S3 while respecting API rate limits and optimizing resource usage.

## Features

- Memory-efficient S3 file streaming
- Rate-limited API calls (5000 TPS)
- Concurrent processing with ThreadPoolExecutor
- Progress tracking with tqdm
- Comprehensive logging
- Resource optimization
- Error handling and retries

## Requirements

- Python 3.8+
- AWS credentials configured
- Access to target API endpoint

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd s3_api_processor
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Configuration

Create a `.env` file with the following variables:

```env
# Required
S3_BUCKET=your-bucket-name
S3_KEY=path/to/your/file.csv
API_ENDPOINT=https://your-api-endpoint.com/process

# Optional
BATCH_SIZE=1000
MAX_WORKERS=20
RATE_LIMIT_CALLS=5000
RATE_LIMIT_PERIOD=1
CHUNK_SIZE=8192
```

## Usage

Run the processor:

```bash
python src/processor.py
```

## Performance Optimization

The application is optimized for:

### Memory Usage
- Streams S3 data instead of loading entire file
- Uses bounded queue for producer-consumer pattern
- Processes records in configurable batches
- Efficient garbage collection

### CPU Utilization
- ThreadPoolExecutor for I/O-bound operations
- Batch processing to reduce overhead
- Efficient data structures
- Session reuse for API calls

### Network Optimization
- Rate limiting with automatic retries
- Connection pooling
- Batch processing of records

## Monitoring

- Real-time progress bar shows processing status
- Detailed logging to both console and file
- Processing statistics including:
  - Total records processed
  - Error count
  - Duration
  - Average processing speed

## Logs

Logs are stored in the `logs` directory:
- `processor.log`: Contains detailed processing information

## Error Handling

- Automatic retry for rate limit exceeded
- Error logging with stack traces
- Graceful shutdown on interruption
- Failed record tracking