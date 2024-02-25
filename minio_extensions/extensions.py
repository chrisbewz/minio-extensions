import os
import tempfile
import warnings
from dotenv import load_dotenv
from minio import Minio, S3Error

from minio_extensions.providers import(
    ClientBuilder,
    ConfigurationOptions
)

from minio_extensions.metadata.metadata import (
    VersionMetadata,
    ObjectMetadataInfo,
    ObjectMetadata,
    TagMetadata
)

from minio_extensions._typing import (
    VersionLike
)

from io import BytesIO
from typing import (
    Optional,
    Dict,
    List,
    Union,
    Type,
    TypeVar
)

from minio.versioningconfig import VersioningConfig

from minio_extensions.exceptions import (
    InvalidBucketException
)
class MinioExtensions:
    
    @staticmethod
    def get_object(client: Type[Minio], bucket: Optional[str] = None,
               file_name: Optional[str] = None,
               tag_version: Optional[Union[VersionMetadata, VersionLike]] = "latest"):
        """
        Retrieve a single file from minio given a bucket and file information

        Args:
            bucket:
                The bucket to retrieve the files from.
                file_name: The name of the file to retrieve from the bucket.
                tag_version: Version to catch specified file. Can be either a version metadata to find inside object
                versions metadata, or a tag representing first or latest version of file. Defaults to latest version if not defined.
            file_name:
                The name of the file to retrieve.

        Returns: A file object or None if the file does not exist.
        """
        
        if bucket is None:
            raise ValueError("The bucket was not specified.")
            
        available_files = []
        selected_file = None
        has_to_find_by_meta = isinstance(tag_version, VersionMetadata)
        
        files_found = [f for f in MinioExtensions.list_files_from_bucket(
            bucket = bucket,
            prefix = file_name,
            recurse = False,
            include_versions = True,
            include_metadata = True
        )]
        
        if has_to_find_by_meta:
            available_files = [{
                "name": f.object_name,
                "version_id": f.version_id,
                "metadata_info": ObjectMetadataInfo.from_meta(
                    MinioExtensions.get_object_metadata(bucket = bucket,
                                             object_name = f.object_name,
                                             version_id = f.version_id)
                )
            } for f in files_found]
            
            selected_file = next(
                file for file in available_files if available_files["metadata_info"].version == tag_version)
            
            return MinioExtensions.fload_file_from_bucket(client, bucket_name = bucket,
                                                          object_name = selected_file["name"],
                                                          version_id = selected_file["version_id"])
        if len(files_found) == 0:
            raise ValueError("Could not find any file corresponding to the following prefix on bucket: {}".format(file_name))
        
        # Returning first file found from search if exists only one ignoring version tag specified
        if not has_to_find_by_meta and len(files_found) == 1:
            selected_file = files_found[0]
            
        # Returning first file found from search if more than one exists
        elif not has_to_find_by_meta and len(files_found) > 1 and tag_version == "first":
            selected_file = files_found[0]
        
        # Returning last element in case of a list of file matches greather than 1
        elif not has_to_find_by_meta and len(files_found) > 1 and tag_version == "latest":
            selected_file = files_found.pop()
        
        (file_io, path) = MinioExtensions.fload_file_from_bucket(client, bucket_name = bucket,
                                                      object_name = selected_file.object_name,
                                                      version_id = selected_file.version_id)
        return file_io, path

    @staticmethod
    def is_file(client: Type[Minio], bucket_name: Optional[str] = None, object_name: Optional[str] = None) -> bool:
        """
        Checks if the given object exists in the given bucket.
        """
        return client.get_object(bucket_name, object_name) is None
    
    @staticmethod
    def as_bytes_io(file):
        """
        Converts a file-like object to a bytes object and returns it.
        """
        from io import BytesIO
        from os.path import getsize
        try:
            if getsize(file) == 0:
                return None, "The file passed has no contents", True
            
            with open(file, "rb") as fb:
                return BytesIO(fb.read()), "", True
        
        except TypeError as tErr:
            return None, tErr, False
    
    @staticmethod
    def check_bucket_exists(client: Type[Minio], bucket):
        """Checks if the passed bucket exists on informed provider"""
        try:
            bucket_exists = client.bucket_exists(bucket)
            if bucket_exists:
                return True
        except S3Error as err:
            warnings.warn(f"Unknown connection error occured on attempt : {err}", UserWarning)
            return False
    
    @staticmethod
    def load_file_from_bucket(client: Type[Minio], bucket_name: Optional[str] = None, object_name: Optional[str] = None):
        """
        Retrieve a single file from minio given a bucket, current minio client and file information.

         Args:
             client: Minio client instance to search for objects
             bucket_name: Name of the bucket to search for
             object_name: Name of the object to search for in the bucket. NOTE.: This needs to be the fully qualified path of the
             path to desired file inside the bucket including subfolders to catch the file

         Returns:
             Bytes object of the file that was loaded from the bucket
        """
        
        import io
        client_response = None
        
        if bucket_name is None:
            raise ValueError("No bucket name provider.")
        
        if object_name is None:
            raise ValueError("Object name is required to search for objects on bucket")
        
        # is_file_provided = MinioExtensions.is_file(client, bucket_name=bucket_name, object_name=object_name)
        
        if client is None:
            raise ValueError("Minio client is not available.")
        
        # if not is_file_provided:
        #     raise ValueError("The specified object does not exists at bucket.")
        
        client_response = client.get_object(bucket_name = bucket_name, object_name = object_name)
        file_io = io.BytesIO(client_response.data)
        file_path = file_io
        return file_path
    
    @staticmethod
    def fload_file_from_bucket(client: Type[Minio], bucket_name: Optional[str] = None, object_name: Optional[str] = None,
                               version_id: Optional[str] = None):
        """
                Retrieve a single file from minio given a bucket, current minio client and file information.

                 Args:
                     client: Minio client instance to search for objects
                     bucket_name: Name of the bucket to search for
                     object_name: Name of the object to search for in the bucket. NOTE.: This needs to be the fully qualified path of the
                     path to desired file inside the bucket including subfolders to catch the file
                     version_id: Version ID to search for on bucket for given file

                 Returns:
                     Bytes object of the file that was loaded from the bucket
                """
        
        import io
        client_response = None
        
        if bucket_name is None:
            warnings.warn("No bucket name was provided, using default bucket name from environment settings.")
            
            load_dotenv()
            bucket_name = os.getenv("DEVELOPMENT_DATABASE_MINIO_BUCKET_NAME")
        
        if bucket_name is None:
            raise ValueError("No bucket name found on environment settings.")
        
        if object_name is None:
            raise ValueError("Object name is required to search for objects on bucket")
        
        # is_file_provided = MinioExtensions.is_file(client=client, bucket_name=bucket_name, object_name=object_name)
        
        if client is None:
            raise ValueError("Minio client is not available.")
        
        # if not is_file_provided:
        #     raise ValueError("The specified object does not exists at bucket.")
        
        local_file_path = os.path.join(tempfile.gettempdir(), object_name)
        local_file_path = local_file_path.replace("/", "\\")
        client_response = client.fget_object(
            bucket_name = bucket_name,
            object_name = object_name,
            file_path = local_file_path,
            version_id = version_id)
        
        if not os.path.isfile(local_file_path):
            raise FileNotFoundError(
                "The downloaded file from minio provider was not found. This issue may be related with an error when downloading the file from bucket.".format(
                    local_file_path))
        
        # with open(local_file_path, "rb") as rb_file:
        #     rb_file.read()
        #     file_io = io.BytesIO()
        #     rb_file.close()
        
        file_info = client_response
        return (file_info, local_file_path)
    
    @staticmethod
    def create_provider(creation_options: ConfigurationOptions = "env"):
        import os
        import dotenv as de
        de.load_dotenv()
        builder = ClientBuilder(creation_options = creation_options)
        
        return builder.configure()
    
    @staticmethod
    def get_object_metadata(client: Type[Minio], bucket: Optional[str] = None, object_name: str = None,
                            version_id: Optional[str] = None):
        if bucket is None:
            raise InvalidBucketException("Specified bucket doesnt exists on client.", bucket)
        
        if object_name is None:
            return { }
        
        meta = client.stat_object(bucket_name = bucket, object_name = object_name, version_id = version_id)
        tags = client.get_object_tags(bucket_name = bucket, object_name = object_name, version_id = version_id)

        dict_meta = dict(zip(meta.metadata.keys(), meta.metadata.values()))
        dict_meta["tags"] = tags
        return dict_meta
    

    @staticmethod
    def get_objects(client: Type[Minio],
                    bucket: Optional[str] = None,
                    bucket_folder_path: Optional[str] = None,
                    files: Optional[List[str]] = None,
                    suppress_file_path_update: Optional[bool] = True) -> Dict[
        str, BytesIO]:
        """
        Retrieve multiple files from minio given a bucket and files fully qualified bucket path information.

        Args:
            bucket:
                The bucket to retrieve the files from.
            files:
                List of file paths to get from provider.
            bucket_folder_path:
                Folder to get files from.
            suppress_file_path_update:
                Whether to suppress file fully qualified path updating when trying to get objects from the bucket.
                Defaults to False and only intended to be used in case the files parameter passed is result of a MinioClient.list_objects call using a prefix for search
                since this call already returns the fully qualified path to resource on bucket if it exists.

        Returns:
            A dictionary containing the files and their contents as BytesIO objects.
        """
        objects: Dict[str, BytesIO] = { }
        
        if bucket is None:
            raise InvalidBucketException("Specified bucket doesnt exists on client.")
        
        if not MinioExtensions.ensure_bucket_exists(bucket = bucket):
            raise ValueError(f"Bucket {bucket} specified does not exists on provider.")
        
        if bucket_folder_path is None and not suppress_file_path_update:
            raise ValueError("Bucket folder path must be provided when suppress_file_path_update is False.")
        
        for file in files:
            
            if not suppress_file_path_update:
                file = f"{bucket_folder_path}/{file}"
            
            file_io = MinioExtensions.load_file_from_bucket(client, bucket_name = bucket,
                                                            object_name = file)
            objects[file.split("/")[-1]] = file_io
        
        return objects
    
    @staticmethod
    def get_objects_from_bucket_folder(client: Type[Minio], bucket: Optional[str] = None, bucket_folder: Optional[str] = None,
                                       recurse: bool = False) -> \
            Dict[str, BytesIO]:
        """
        Returns all objects contained inside a given bucket folder specified.
        The method only makes the search in specified folder level, not recursing through nested folders in order to retrieve also nested files on passed folder.

        Args:
            bucket:
            bucket_folder:
            recurse:
        
        Returns:

        """
        
        if bucket is None:
            raise InvalidBucketException("Specified bucket doesn't exists on client.")
        
        if not MinioExtensions.ensure_bucket_exists(bucket = bucket):
            raise ValueError(f"Bucket {bucket} specified does not exists on provider.")
        
        if bucket_folder is None:
            raise ValueError("Bucket folder must be provided to read objects from.")
        
        is_folder_valid = MinioExtensions.is_folder(
            bucket = bucket,
            folder_name = bucket_folder
        )
        
        if not is_folder_valid:
            raise S3Error(f"Folder specified do not exists on bucket {bucket}.")
        
        files_to_fetch = MinioExtensions.list_files_from_bucket(
            bucket = bucket,
            prefix = bucket_folder,
            recurse = False
        )
        
        return MinioExtensions.get_objects(
            bucket = bucket,
            files = files_to_fetch, recurse = False
        )
    
    @staticmethod
    def upload_object(client: Type[Minio], bucket: Optional[str] = None, object_name: Optional[str] = None,
                      local_path: Optional[str] = None, content_type: Optional[str] = None,
                      metadata: Optional[ObjectMetadata] = None):
        """
        Upload a file to minio given a bucket and file information

        Args:
            bucket: The bucket to upload the specified file.
            object_name: Fully qualified name of object to upload on bucket.
            local_path: Local path pointing to file stream used to upload on provider.
            content_type: Content type of local file stream to be uploaded. For now only csv files are supported for upload
            metadata: Optional metadata to add to the file.

        """
        
        from minio.commonconfig import Tags
        _metadata = metadata
        _metadata_tags = Tags.new_object_tags()
        
        if bucket is None:
            raise InvalidBucketException("Specified bucket doesn't exists on client.")
        
        if not MinioExtensions.ensure_bucket_exists(bucket):
            raise ValueError("Specified bucket does not exists on provider.")
        
        if object_name is None:
            raise ValueError("Bucket path to upload file must be specified")
        
        if not os.path.exists(local_path):
            raise ValueError("Specified local file does not exists for upload.")
        
        # if content_type != ContentType.CSV:
        #     raise TypeError("Currently only csv files are supported to upload on provider.")
        
        if metadata is None:
            _metadata = { }
        
        if _metadata.tags is not None and len(_metadata.tags) > 0:
            _metadata = _metadata.dict()
            _metadata.pop("tags")
            _metadata_tags = TagMetadata.as_tag(metadata.tags)
        
        client.fput_object(
            bucket_name = bucket,
            object_name = object_name,
            file_path = local_path,
            content_type = content_type,
            metadata = _metadata,
            tags = _metadata_tags
        )
    
    @staticmethod
    def remove_object(client: Type[Minio], bucket: Optional[str], file: Optional[str]):
        """Try removing a specified object/file from passed bucket"""
        try:
            if bucket is None:
                raise InvalidBucketException("Bucket not specified.")
            
            if MinioExtensions.is_bucket_active(client = client, bucket = bucket, file = file):
                client.remove_object(bucket, file)
                return "File deleted with success", True
            
            else:
                return f"None files named as {str(file)} exists on bucket {str(bucket)}", True
        
        except S3Error:
            return (
                f"Unknown error when trying to remove object from bucket {bucket}. \nException Message: {S3Error}",
                False)
    
    @staticmethod
    def is_bucket_active(client: Type[Minio], bucket: Optional[str], file: Optional[str]):
        """Checks and returns if current minio bucket instance is active"""
        try:
            if bucket is None:
                raise InvalidBucketException("Bucket not specified.")
            
            file_metadata = client.stat_object(bucket, file)
            return True
        
        except S3Error:
            return False
    
    @staticmethod
    def is_folder(client: Type[Minio], bucket: Optional[str] = None, folder_name: Optional[str] = None) -> bool:
        """
        Checks if the given object exists in the given bucket.

        Args:
            bucket: Target bucket name to try finding folder existence from.
            folder_name: Path inside bucket to the folder whose existence should be checked.

        Returns:
            True if specified folder exists inside bucket on informed path, otherwise false.
        """
        if bucket is None:
            raise InvalidBucketException("Bucket not specified.")
        
        bucket_objects = MinioExtensions.list_files_from_bucket(
            bucket = bucket,
            prefix = folder_name)
        
        for obj in bucket_objects:
            if obj.object_name.enswith("/"):
                return True
        
        return False
    
    @staticmethod
    def list_files_from_bucket(client: Type[Minio], bucket: Optional[str], prefix: Optional[str] = None,
                               recurse: Optional[bool] = False,
                               include_versions: Optional[bool] = False,
                               include_metadata: Optional[bool] = False):
        """List all files in the specified bucket on current provider if exists any"""
        if not MinioExtensions.bucket_exists(bucket = bucket):
            raise InvalidBucketException(f"Bucket {bucket} does not exist on current provider")
        
        return client.list_objects(
            bucket_name = bucket,
            recursive = recurse,
            prefix = prefix,
            include_version = include_versions,
            include_user_meta = include_metadata
        )
    
    @staticmethod
    def enable_object_versioning(client: Type[Minio], bucket: Optional[str] = None):
        """
        Configures minio client instance to enable objects/files versioning in provided bucket.

        Args:
            bucket: Bucket to set version policy on.
        """
        from minio.commonconfig import ENABLED
        
        if bucket is None:
            raise InvalidBucketException("Bucket not specified.")
        
        client.set_bucket_versioning(bucket, VersioningConfig(ENABLED))
    
    @staticmethod
    def disable_object_versioning(client: Type[Minio], bucket: Optional[str] = None):
        """
        Configures minio client instance to disable objects/files versioning in provided bucket.

        Args:
            bucket: Bucket to set version policy off.
        """
        from minio.commonconfig import DISABLED
        if bucket is None:
            raise InvalidBucketException("Bucket not specified.")
        
        client.set_bucket_versioning(bucket, VersioningConfig(DISABLED))
