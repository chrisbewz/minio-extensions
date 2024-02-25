
import warnings
import os
from typing import Literal

ConfigurationOptions = Literal["env", "toml", "xml"]

class ClientBuilder:
    
    def __init__(self, creation_option: ConfigurationOptions) -> None:
        self._creation_option = creation_option
    
    @staticmethod
    def _from_env(self):
        ...
    
    @staticmethod
    def _from_toml(self):
        ...
    
    @staticmethod
    def _from_json(self):
        ...
    
    @staticmethod
    def configure(self):
        ...  

