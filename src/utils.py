import os
import json
import logging
from typing import Any, Dict, Optional
from datetime import datetime

def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration"""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    if not os.path.exists('logs'):
        os.makedirs('logs')
        
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join('logs', f'processor_{datetime.now():%Y%m%d}.log')),
            logging.StreamHandler()
        ]
    )

def validate_config(config: Dict[str, Any]) -> None:
    """Validate configuration values"""
    required_fields = ['S3_BUCKET', 'S3_KEY', 'API_ENDPOINT']
    missing_fields = [field for field in required_fields if not config.get(field)]
    
    if missing_fields:
        raise ValueError(f"Missing required configuration fields: {', '.join(missing_fields)}")

def parse_record(record: str) -> Dict[str, Any]:
    """Parse a record string into a dictionary"""
    try:
        if record.startswith('{') and record.endswith('}'):
            return json.loads(record)
        return {'data': record.strip()}
    except json.JSONDecodeError as e:
        logging.warning(f"Failed to parse record as JSON: {e}")
        return {'data': record.strip()}

def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable string"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0 or hours > 0:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    
    return " ".join(parts)

def calculate_eta(processed: int, total: int, elapsed_seconds: float) -> Optional[str]:
    """Calculate estimated time remaining"""
    if processed == 0:
        return None
        
    records_per_second = processed / elapsed_seconds
    remaining_records = total - processed
    eta_seconds = remaining_records / records_per_second
    
    return format_duration(eta_seconds)

def format_number(n: int) -> str:
    """Format number with thousands separator"""
    return f"{n:,}"

def get_memory_usage() -> str:
    """Get current memory usage"""
    import psutil
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    return f"{memory_mb:.1f}MB" 