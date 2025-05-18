class ProcessingError(Exception):
    """Base class for all processing related errors"""
    pass

class S3ReadError(ProcessingError):
    """Error when reading from S3"""
    def __init__(self, message: str, bucket: str, key: str):
        self.bucket = bucket
        self.key = key
        super().__init__(f"{message} (bucket: {bucket}, key: {key})")

class APIError(ProcessingError):
    """Error when calling the external API"""
    pass

class RateLimitExceeded(APIError):
    """Error when API rate limit is exceeded"""
    pass

class CircuitBreakerError(APIError):
    """Error when circuit breaker prevents API call"""
    pass

class ConfigurationError(Exception):
    """Error in configuration settings"""
    pass

class MetricsError(Exception):
    """Error in metrics collection or publishing"""
    pass

class APIConnectionError(APIError):
    """API connection failed"""
    pass

class APITimeoutError(APIError):
    """API request timed out"""
    pass

class S3Error(ProcessingError):
    """S3 related errors"""
    def __init__(self, message: str, bucket: str = None, key: str = None):
        super().__init__(message)
        self.bucket = bucket
        self.key = key

class S3WriteError(S3Error):
    """Error writing to S3"""
    pass

class ValidationError(ProcessingError):
    """Data validation errors"""
    def __init__(self, message: str, record: dict = None):
        super().__init__(message)
        self.record = record

class RetryableError(ProcessingError):
    """Errors that can be retried"""
    def __init__(self, message: str, retry_after: int = None):
        super().__init__(message)
        self.retry_after = retry_after

class NonRetryableError(ProcessingError):
    """Errors that should not be retried"""
    pass

class ResourceExhaustedError(ProcessingError):
    """Resource limits reached"""
    def __init__(self, message: str, resource_type: str = None):
        super().__init__(message)
        self.resource_type = resource_type 