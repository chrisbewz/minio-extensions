
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
from minio import Minio
from urllib.parse import urlparse
from urllib3 import ProxyManager
from urllib3 import Retry
from typing import Optional

ConfigurationOptions = Literal["env", "toml", "xml"]

class ClientBuilder:
    
    def __init__(self, creation_option: ConfigurationOptions, is_proxy_conn: Optional[bool] = False) -> None:
        self._creation_option = creation_option
        self._is_proxy_conn = is_proxy_conn
        
    def _from_env(self):
        client = None
        params = {}
        url_parsed = None
        
        if not MINIO_S3_USERNAME.is_defined:
            raise ClientConfigurationException(message="Expected user defined, but was not found on enviroment variables.")
        
        params["username"] = MINIO_S3_USERNAME.get()
        
        if not MINIO_S3_PASSWORD.is_defined:
            raise ClientConfigurationException(message = "Expected password defined, but wasn' found on environment.")
        
        params["password"] = MINIO_S3_PASSWORD.get()
        
        if not MINIO_S3_ENDPOINT_URL.is_defined:
            raise ClientConfigurationException(message = "Expected url to be declared on environment variables.")
        
        value = MINIO_S3_ENDPOINT_URL.get()
        url_parsed = urlparse(value)
        
        if not MINIO_S3_ENDPOINT_PORT.is_defined and url_parsed.port is None:
            raise ClientConfigurationException("Expecting a port to be declared on MINIO_S3_ENDPOINT_URL when not using MINIO_S3_ENDPOINT_PORT environment variable.")
        
        url_parsed = urlparse(f"//{url_parsed.path}:{MINIO_S3_ENDPOINT_PORT.get()}") \
            if url_parsed.port is None else url_parsed
        
        # In case the protocol is not defined on endpoint environment variable it will be inferred from the
        # environment variable MINIO_S3_CHECK_CERTIFICATES. If true the prefix https will be appended to the
        # complete endpoint dict value, otherwise http is defined
        check_cert = MINIO_S3_CHECK_CERTIFICATES.default if not MINIO_S3_CHECK_CERTIFICATES.is_defined \
            else MINIO_S3_CHECK_CERTIFICATES.get()
        
        if url_parsed.port is None:
            raise ClientConfigurationException("Endpoint port is missing on passed url.")
            
        params["endpoint"] = url_parsed.hostname
        params["port"] = url_parsed.port
        params["is_secure"] = not MINIO_S3_IGNORE_SECURE_CONNECTION.get() \
            if MINIO_S3_IGNORE_SECURE_CONNECTION.is_defined else MINIO_S3_IGNORE_SECURE_CONNECTION.default
        params["check_cert"] = MINIO_S3_CHECK_CERTIFICATES.get() \
            if MINIO_S3_CHECK_CERTIFICATES.is_defined else MINIO_S3_CHECK_CERTIFICATES.default
        
        endpoint_url = "{0}:{1}".format(url_parsed.hostname, url_parsed.port)
        
        if not self._is_proxy_conn:
            return Minio(
                endpoint = endpoint_url,
                access_key = params["username"],
                secret_key = params["password"],
                cert_check = params["check_cert"],
                secure = params["is_secure"]
            )
        
        if self._is_proxy_conn and not MINIO_S3_HTTP_REQUEST_PROXY_URL.is_defined:
            raise ClientProxyConfigurationException("Proxy URL expect on environment variables.")
            
        proxy = ProxyManager(
            proxy_url = MINIO_S3_HTTP_REQUEST_PROXY_URL.get(),
            timeout = MINIO_S3_HTTP_REQUEST_TIMEOUT.get()
            if MINIO_S3_HTTP_REQUEST_TIMEOUT.is_defined else MINIO_S3_HTTP_REQUEST_TIMEOUT.default
            if MINIO_S3_HTTP_REQUEST_TIMEOUT.is_defined else MINIO_S3_HTTP_REQUEST_TIMEOUT.default,
            retries = Retry(
                total = MINIO_S3_HTTP_REQUEST_MAX_RETRIES.get()
                if MINIO_S3_HTTP_REQUEST_MAX_RETRIES.is_defined else MINIO_S3_HTTP_REQUEST_MAX_RETRIES.default
            ),
            cert_reqs = "CERT_REQUIRED"
        )
            
        return Minio(
            endpoint = endpoint_url,
            access_key = params["username"],
            secret_key = params["password"],
            cert_check = params["check_cert"],
            secure = params["is_secure"],
            http_client=proxy
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

