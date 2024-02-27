from typing import Optional


class BucketException(Exception):
    bucket: Optional[str] = None
    
    def __init__(self, message, bucket: Optional[str] = None) -> None:
        self.bucket = bucket
        self.message = message
        super().__init__(message)
        

class InvalidBucketException(BucketException):
    
    def __init__(self, message, bucket: Optional[str] = None) -> None:
        super().__init__(message, bucket)
        
        
class ClientConfigurationException(Exception):
    def __init__(self, message: object) -> None:
        self.message = message
        super().__init__(self.message)


class ClientProxyConfigurationException(ClientConfigurationException):
    def __init__(self, message: object) -> None:
        super().__init__(message)
