from abc import ABC, abstractmethod
from minio import Minio

class Provider(ABC):
    __endpoint: str
    __port: str
    __instance: any
    
    def __init__(self, endpoint: str, port: str):
        self.__endpoint = endpoint
        self.__port = port
        self.__instance = None
    
    @abstractmethod
    def configure_client(self) -> None:
        raise NotImplementedError
    
    @property
    def client_endpoint(self):
        return self.__endpoint
    
    @property
    def client_port(self):
        return self.__port
    
    @property
    def instance(self):
        return self.__instance
    
    @instance.setter
    def instance(self, value):
        self.__instance = value
    
    def __str__(self):
        return f"{self.client_endpoint}:{self.__port}"
