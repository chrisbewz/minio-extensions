
import warnings
import os
from typing import Literal
from minio_extensions.environment import (
    MINIO_S3_CHECK_CERTIFICATES,
    MINIO_S3_DEFAULT_BUCKET_NAME,
    MINIO_S3_ENDPOINT_URL,
    MINIO_S3_ENDPOINT_PORT,
    MINIO_S3_PASSWORD,
    MINIO_S3_USERNAME,
    MINIO_S3_HTTP_REQUEST_PROXY_HAS_TO_FORCE_ERROR_CODES,
    MINIO_S3_HTTP_REQUEST_MAX_RETRIES,
    MINIO_S3_HTTP_REQUEST_TIMEOUT,
    MINIO_S3_HTTP_REQUEST_PROXY_FORCE_ERROR_CODES,
    MINIO_S3_HTTP_REQUEST_PROXY_URL,
    MINIO_S3_IGNORE_SECURE_CONNECTION
)

from minio_extensions.exceptions import (
    ClientConfigurationException,
    ClientProxyConfigurationException 
)

from typing import Optional

ConfigurationOptions = Literal["env", "toml", "xml"]

class ClientBuilder:
    
    def __init__(self, creation_option: ConfigurationOptions, is_proxy_conn: Optional[bool] = False) -> None:
        self._creation_option = creation_option
        self._is_proxy_conn = is_proxy_conn
        
    def _from_env(self):
        client = None
        params = {}
        
        if not MINIO_S3_USERNAME.is_defined():
            raise ClientConfigurationException(message="Expected user defined, but was not found on enviroment variables.")
        
        params["username"] = MINIO_S3_USERNAME.get()
        
        if not MINIO_S3_PASSWORD.is_defined():
            raise ClientConfigurationException(message = "Expected password defined, but wasn' found on environment.")
        
        params["password"] = MINIO_S3_PASSWORD.get()
        
        if MINIO_S3_ENDPOINT_URL.is_defined() and not MINIO_S3_ENDPOINT_PORT.is_defined():
            endport = MINIO_S3_ENDPOINT_URL.get().split(":")[-1]
            if not endport.isdigit():
                raise ClientConfigurationException("The endpoint port must be provided if MINIO_S3_ENDPOINT_PORT is unset.")
            
            params["endpoint"] = MINIO_S3_ENDPOINT_URL.get()
            params["port"] =  endport
        
        if MINIO_S3_ENDPOINT_URL.is_defined() and MINIO_S3_ENDPOINT_PORT.is_defined():
            are_equal = MINIO_S3_ENDPOINT_PORT.get() == MINIO_S3_ENDPOINT_URL.get().split(':')[-1]
            
            if not are_equal:
                warnings.warn("Values defined on environment variables {0}, {1} aren't equal. Using {0} value for port configuration.".format(MINIO_S3_ENDPOINT_URL.name, MINIO_S3_ENDPOINT_PORT.name), UserWarning)
                params["port"] = MINIO_S3_ENDPOINT_URL.get().split(':')[-1]
            
            else:    
                params["endpoint"] = MINIO_S3_ENDPOINT_PORT.get()
                
            params["endpoint"] = MINIO_S3_ENDPOINT_URL.get().replace(f':{params["port"]}', '')
            
        params["is_secure"] = MINIO_S3_IGNORE_SECURE_CONNECTION.get() if MINIO_S3_IGNORE_SECURE_CONNECTION.is_defined() else False
        params["check_cert"] = MINIO_S3_CHECK_CERTIFICATES.get() if MINIO_S3_CHECK_CERTIFICATES.is_defined() else True
        
        from minio import Minio
        if self._is_proxy_conn:
            ...
            from urllib3 import ProxyManager
            from urllib3 import Retry
            
            if not MINIO_S3_HTTP_REQUEST_PROXY_URL.is_defined():
                raise ClientProxyConfigurationException("Proxy URL expect on environment variables.")
            
            proxy = ProxyManager(
                proxy_url = MINIO_S3_HTTP_REQUEST_PROXY_URL.get(),
                timeout = MINIO_S3_HTTP_REQUEST_TIMEOUT.get()
                if MINIO_S3_HTTP_REQUEST_TIMEOUT.is_defined() else MINIO_S3_HTTP_REQUEST_TIMEOUT.default
                if MINIO_S3_HTTP_REQUEST_TIMEOUT.is_defined() else MINIO_S3_HTTP_REQUEST_TIMEOUT.default,
                retries = Retry(
                    total = MINIO_S3_HTTP_REQUEST_MAX_RETRIES.get() 
                    if MINIO_S3_HTTP_REQUEST_MAX_RETRIES.is_defined() else MINIO_S3_HTTP_REQUEST_MAX_RETRIES.default
                ),
                cert_reqs = "CERT_REQUIRED"
            )
            
            return Minio(
                endpoint = "{0}:{1}".format(params["endpoint"], params["port"]),
                access_key = params["username"],
                secret_key = params["password"],
                cert_check = params["check_cert"],
                secure = params["is_secure"],
                http_client=proxy
            )
        
        return Minio(
            endpoint = "{0}:{1}".format(params["endpoint"], params["port"]),
            access_key = params["username"],
            secret_key = params["password"],
            cert_check = params["check_cert"],
            secure = params["is_secure"]
        )
        
    def _from_toml(self):
        raise NotImplementedError
    
    def _from_json(self):
        raise NotImplementedError
    
    def configure(self):
        if self._creation_option == "env":
            return self._from_env()
        
        if self._creation_option == "toml":
            return self._from_toml()
        
        return self._from_json()

