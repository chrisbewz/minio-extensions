import datetime
import json
import uuid
import warnings
from enum import Enum
from minio_extensions._typing import *
from minio.commonconfig import Tags
from pydantic import BaseModel, Field
from typing_extensions import Annotated

from minio_extensions.metadata.constants import (
    USER_META_VERSION_ATT,
    OBJECT_META_ETAG_ATT,
    OBJECT_META_CONTENT_LENGTH_ATT,
    OBJECT_CONTENT_TYPE_ATT,
    OBJECT_META_DATE_ATT,
    OBJECT_META_LAST_MODIFIED_ATT,
    OBJECT_META_TAGCOUNT_ATT,
    OBJECT_META_VERSION_ATT
)

from minio_extensions._typing import PosInt

T = TypeVar('T', bound = "Tags")
TT = TypeVar('TT', bound = "TagMetadata")
TagLike = Union[T, TT]
TagLikeArray = List[TagLike]


class VersionMetadata(BaseModel):
    """
    Metadata about the version of the file up/downloaded from minio.
    """
    
    major: PosInt = Field(1, alias = "major")
    """Major version of the file"""
    
    minor: Optional[PosInt] = Field(0, alias = "minor")
    """Minor version of the file"""
    
    revision: Optional[PosInt] = Field(0, alias = "revision")
    """Revision of the file"""


class TagMetadata(BaseModel):
    """
    Tag metadata about the file to be up/downloaded from minio.
    """
    
    name: str = Field(..., alias = "name")
    """Name to assign on new tag to help identify the file on bucket"""
    
    content: Optional[str] = None
    """Content of the tag."""
    
    @classmethod
    def from_value_pair(cls, data: Tuple[str, str]) -> Type[TagLike]:
        obj = cls(
            name = data[0],
            content = data[1]
        )
        return cast(Type[TT], obj)
    
    @classmethod
    def from_dict(cls: Type[TT], data: Dict[str, Any]) -> Union[TagLike, TagLikeArray]:
        if not len(data) > 0:
            return None
        
        if len(data) == 1:
            return TagMetadata.from_value_pair((list(data)[0], list(data.values())[0]))
        
        else:
            flattened = [(key, value) for key, value in data.items()]
            
            return [
                TagMetadata.from_value_pair(t)
                for t in flattened
            ]
    
    @classmethod
    def as_tag(cls,
               metadata: Optional[Union[List[TT], TT]],
               duplicate_options: Optional['TagDuplicateOptions'] = "raise",
               duplicates_prefix: Optional[str] = "_",
               separator: Optional[str] = "-") -> Type[T]:
        """
        Convert TagMetadata objects to a Tags object.
        """
        _tag = Tags.new_object_tags()
        
        if metadata is None:
            return cast(Type[T], _tag)
        
        if isinstance(metadata, list):
            for m in metadata:
                
                if not isinstance(m, cls):
                    warnings.warn(f"Warning: Element {m} is not of type {cls.__name__}. Skipping...")
                    continue
                
                if m.name not in _tag.keys():
                    _tag[m.name] = m.content
                
                if (m.name, m.content) in _tag.keys():
                    raise ValueError(f"Element {m} is already present on tags list")
                
                if duplicate_options not in ["raise", "preserve", "append"]:
                    raise ValueError("The parameter errors must be either 'raise', 'preserve' or append")
                
                if duplicate_options == "raise" and len(set(_tag.keys())) < len(_tag.keys()):
                    raise ValueError(
                        "Duplicate tag names found on provided list. First occurrence: {index} -> {m.name}")
                
                if duplicate_options == "append":
                    if m.name in _tag:
                        _tag[m.name] = f"{_tag[m.name]}{separator}{m.content}"
                    else:
                        _tag[m.name] = m.content
                    
                    continue
                
                repetitions = 0
                new_name = m.name
                
                while m.name in _tag:
                    repetitions += 1
                    new_name = f"{m.name}_{repetitions}"
                    
                _tag[new_name] = m.content
                

        else:
            if not isinstance(metadata, cls):
                warnings.warn(f"Warning: Element {metadata} is not of type {cls.__name__}. Skipping...")
            else:
                _tag[metadata.name] = metadata.content
        
        return cast(Type[T], _tag)


class ObjectMetadata(BaseModel):
    """
    Metadata about the file to be up/downloaded from minio.
    """
    
    version: Optional[VersionMetadata] = None
    
    tags: Optional[List[TagMetadata]] = []


class ObjectMetadataInfo(BaseModel):
    content_length: Optional[int] = None
    content_type: Optional[str] = None
    last_modified: Optional[datetime.datetime] = None
    id: Optional[Union[str, uuid.UUID]] = None
    tags: Optional[List[TagMetadata]] = None
    
    @classmethod
    def from_meta(cls, metadata):
        
        data_dict = { }
        data_dict["content_length"] = cls._get_meta_content_size(metadata)
        data_dict["content_type"] = cls._get_meta_content_type(metadata)
        data_dict["last_modified"] = cls._get_meta_last_modified_date(metadata)
        data_dict["id"] = cls._get_meta_version_id(metadata)
        
        return ObjectMetadataInfo.model_construct(**data_dict)
    
    @staticmethod
    def _get_meta_last_modified_date(metadata: Dict[str, Any]):
        if OBJECT_META_LAST_MODIFIED_ATT in metadata.keys():
            return datetime.datetime.strptime(str(metadata[OBJECT_META_LAST_MODIFIED_ATT]), '%a, %d %b %Y %H:%M:%S %Z')
    
    @staticmethod
    def _get_meta_tagging_count(metadata: Dict[str, Any]):
        if OBJECT_META_TAGCOUNT_ATT in metadata.keys():
            return metadata[OBJECT_META_TAGCOUNT_ATT]
    
    @staticmethod
    def _get_meta_version_id(metadata: Dict[str, Any]):
        if OBJECT_META_VERSION_ATT in metadata.keys():
            return metadata[OBJECT_META_VERSION_ATT]
    
    @staticmethod
    def _get_meta_content_type(metadata: Dict[str, Any]):
        if OBJECT_CONTENT_TYPE_ATT in metadata.keys():
            return metadata[OBJECT_CONTENT_TYPE_ATT]
    
    @staticmethod
    def _get_meta_content_size(metadata: Dict[str, Any]):
        if OBJECT_META_CONTENT_LENGTH_ATT in metadata.keys():
            return metadata[OBJECT_META_CONTENT_LENGTH_ATT]
    
    @staticmethod
    def _get_meta_e_tagging(metadata: Dict[str, Any]):
        if OBJECT_META_ETAG_ATT in metadata.keys():
            return metadata[OBJECT_META_ETAG_ATT]
