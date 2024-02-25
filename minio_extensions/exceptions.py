from typing import Optional

class BucketException(Exception):
    
    bucket: Optional[str] = None
    
    def __init__(self, message, bucket: Optional[str] = None) -> None:
        super().__init__(message)
        self.bucket = bucket

class InvalidBucketException(BucketException):
    
    def __init__(self, message, bucket: Optional[str] = None) -> None:
        super().__init__(message, bucket)