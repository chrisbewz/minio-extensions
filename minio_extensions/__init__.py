from .metadata.metadata import (
    ObjectMetadata,
    ObjectMetadataInfo,
    VersionMetadata,
    TagMetadata
)

from .extensions import MinioExtensions

__all__ = [
    "MinioExtensions",
    "VersionMetadata",
    "ObjectMetadata",
    "ObjectMetadataInfo",
    "TagMetadata"
]

__version__ = "1.0.0"
