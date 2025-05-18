class ProcessorError(Exception):
    """Base exception for processor errors"""
    pass

class APIError(ProcessorError):
    """API related errors"""
    def __init__(self, message: str, status_code: int = None, response: str = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response

class RateLimitExceeded(APIError):
    """API rate limit exceeded"""
    pass

class APIConnectionError(APIError):
    """API connection failed"""
    pass

class APITimeoutError(APIError):
    """API request timed out"""
    pass

class S3Error(ProcessorError):
    """S3 related errors"""
    def __init__(self, message: str, bucket: str = None, key: str = None):
        super().__init__(message)
        self.bucket = bucket
        self.key = key

class S3ReadError(S3Error):
    """Error reading from S3"""
    pass

class S3WriteError(S3Error):
    """Error writing to S3"""
    pass

class ConfigurationError(ProcessorError):
    """Configuration related errors"""
    pass

class ValidationError(ProcessorError):
    """Data validation errors"""
    def __init__(self, message: str, record: dict = None):
        super().__init__(message)
        self.record = record

class RetryableError(ProcessorError):
    """Errors that can be retried"""
    def __init__(self, message: str, retry_after: int = None):
        super().__init__(message)
        self.retry_after = retry_after

class NonRetryableError(ProcessorError):
    """Errors that should not be retried"""
    pass

class ResourceExhaustedError(ProcessorError):
    """Resource limits reached"""
    def __init__(self, message: str, resource_type: str = None):
        super().__init__(message)
        self.resource_type = resource_type 