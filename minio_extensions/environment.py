
import os
import sys

class _EnvVarBase:
    """
    Represents an environment variable.
    """

    def __init__(self, name, type_, default):
        self.name = name
        self.type = type_
        self.default = default

    @property
    def is_defined(self):
        return self.name in os.environ

    def get_from_environment(self):
        return os.getenv(self.name)

    def set(self, value):
        os.environ[self.name] = str(value)

    def unset(self):
        os.environ.pop(self.name, None)

    def get(self):
        """
        Reads the value of the environment variable if it exists and converts it to the desired
        type. Otherwise, returns the default value.
        """
        if (val := self.get_from_environment()) is not None:
            try:
                return self.type(val)
            except Exception as e:
                raise ValueError(f"Failed to convert {val!r} to {self.type} for {self.name}: {e}")
        return self.default

    def __str__(self):
        return f"{self.name} (default: {self.default}, type: {self.type.__name__})"

    def __repr__(self):
        return repr(self.name)

    def __format__(self, format_spec: str) -> str:
        return self.name.__format__(format_spec)


class _BooleanEnvironmentVariable(_EnvVarBase):
    """
    Represents a boolean environment variable.
    """

    def __init__(self, name, default):
        # `default not in [True, False, None]` doesn't work because `1 in [True]`
        # (or `0 in [False]`) returns True.
        if not (default is True or default is False or default is None):
            raise ValueError(f"{name} default value must be one of [True, False, None]")
        super().__init__(name, bool, default)

    def get(self):
        if not self.is_defined:
            return self.default

        val = os.getenv(self.name)
        lowercased = val.lower()
        if lowercased not in ["true", "false", "1", "0"]:
            raise ValueError(
                f"{self.name} value must be one of ['true', 'false', '1', '0'] (case-insensitive), "
                f"but got {val}"
            )
        return lowercased in ["true", "1"]

class _EnumerableEnvironmentVariable(_EnvVarBase):
    """
    Represents a enumerable environment variable.
    """
    
    _allowed_separator = ';'
    
    def __init__(self, name, default):
        super().__init__(name, default, [])

    def get(self):
        from typing import cast
        if not self.is_defined:
            return self.default

        val = os.getenv(self.name)
        lowercased = val.lower()
        values = lowercased.split(f'{self._allowed_separator}')
        
        return [cast(str, v) for v in values]
    
#: Specifies the default client endpoint port to connect on minio client.
#: (default: ``None``)
MINIO_S3_ENDPOINT_PORT = _EnvVarBase("MINIO_S3_STORAGE_PORT", str, None)

#: Specifies the default client url adress to use when creating a connection from.
#: (default: ``None``)
MINIO_S3_ENDPOINT_URL = _EnvVarBase("MINIO_S3_ENDPOINT_URL", str, None)

#: Specifies the maximum number of retries for minio HTTP requests
#: (default: ``5``)
MINIO_S3_HTTP_REQUEST_MAX_RETRIES = _EnvVarBase("MINIO_S3_HTTP_REQUEST_MAX_RETRIES", int, 5)

#: Specify the address of the proxy to a minio server if end user has any defined on its connection
MINIO_S3_HTTP_REQUEST_PROXY_URL = _EnvVarBase("MINIO_S3_HTTP_REQUEST_PROXY_URL", str, None)

#: Specifies if http proxy has to force request on provided error codes
#: (default: ``None``)
MINIO_S3_HTTP_REQUEST_PROXY_FORCE_ERROR_CODES = _EnumerableEnvironmentVariable("MINIO_S3_HTTP_REQUEST_PROXY_FORCE_ERROR_CODES", None)

#: Specifies if http proxy has to force request on provided error codes if any defined
#: (default: ``False``)
MINIO_S3_HTTP_REQUEST_PROXY_HAS_TO_FORCE_ERROR_CODES = _BooleanEnvironmentVariable("MINIO_S3_HTTP_REQUEST_PROXY_FORCE_ERROR_CODES", False)

#: Specifies the timeout in seconds for minio HTTP requests
#: (default: ``120``)
MINIO_S3_HTTP_REQUEST_TIMEOUT = _EnvVarBase("MINIO_HTTP_REQUEST_TIMEOUT", int, 120)

#: Specifies the minio server endpoint URL to use for operations.
#: (default: ``None``)
MINIO_S3_ENDPOINT_URL = _EnvVarBase("MINIO_S3_ENDPOINT_URL", str, None)

#: Specifies whether or not to skip TLS certificate verification for client connections.
#: (default: ``False``)
MINIO_S3_IGNORE_SECURE_CONNECTION = _BooleanEnvironmentVariable("MINIO_S3_IGNORE_SECURE_CONNECTION", False)

#: Specifies the username used to authenticate with client server.
#: (default: ``None``)
MINIO_S3_USERNAME = _EnvVarBase("MINIO_S3_USERNAME", str, None)

#: Specifies the password used to authenticate with a client server.
#: (default: ``None``)
MINIO_S3_PASSWORD = _EnvVarBase("MINIO_S3_PASSWORD", str, None)

#: Specifies the default bucket name to catch requests from client.
#: (default: ``None``)
MINIO_S3_DEFAULT_BUCKET_NAME = _EnvVarBase("MINIO_S3_DEFAULT_BUCKET_NAME", str, None)

#: Specifies if minio client has to check certificates at client construction
MINIO_S3_CHECK_CERTIFICATES = _BooleanEnvironmentVariable("MINIO_S3_CHECK_CERTIFICATES", True)
