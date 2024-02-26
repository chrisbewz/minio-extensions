import os
import unittest


class MinioObjectMetadataTests(unittest.TestCase):
    from minio import Minio
    _client: Minio = None
    SAMPLE_FILE_NAME = "sample_test.txt"
    bucket = None
    
    def setUp(self):
        from dotenv import load_dotenv
        from minio_extensions.extensions import MinioExtensions
        load_dotenv()
        self._client = MinioExtensions.create_provider("env")
    
    def _gen_dummy_txt_file(self, location):
        file_path_generated = f"{location}{self.SAMPLE_FILE_NAME}"
        with open(file_path_generated, "w") as txtf:
            txtf.write("hello\n")
            txtf.write("world")
        return file_path_generated
    
    def test_object_metadata_tag_equality_should_not_throw(self):
        try:
            from minio_extensions import ObjectMetadata, TagMetadata, VersionMetadata, \
                ObjectMetadataInfo
            from minio_extensions.extensions import MinioExtensions
            from minio_extensions.metadata.constants import Text
            from minio_extensions.environment import MINIO_S3_DEFAULT_BUCKET_NAME
            
            path_gen = self._gen_dummy_txt_file("./")
            
            local_path = os.path.abspath(path_gen)
            file_metadata = ObjectMetadata(
                version = VersionMetadata(
                    major = 1,
                    minor = 0,
                    revision = 0
                ),
                tags = [
                    TagMetadata(
                        name = "kind",
                        content = "test"
                    ),
                    TagMetadata(
                        name = "kind",
                        content = "preview"
                    )
                ])
            
            self.bucket = MINIO_S3_DEFAULT_BUCKET_NAME.get() if not MINIO_S3_DEFAULT_BUCKET_NAME is None else 'sample_bucket'
            
            if not MinioExtensions.check_bucket_exists(client = self._client, bucket = self.bucket):
                self._client.make_bucket(self.bucket)
            
            MinioExtensions.upload_object(
                client = self._client,
                bucket = self.bucket,
                object_name = self.SAMPLE_FILE_NAME,
                local_path = local_path,
                content_type = Text,
                metadata = file_metadata
            )
            meta = MinioExtensions.get_object_metadata(client = self._client,
                                                       bucket = self.bucket,
                                                       object_name = self.SAMPLE_FILE_NAME)
            
            tags = TagMetadata.from_dict(meta["tags"])
            model = ObjectMetadataInfo.from_meta(meta)
            
            # Asserting uploaded file metadata equality
            self.assertEquals(file_metadata.version, model.version)
        except:
            raise
        
        finally:
            self._client.remove_object(bucket_name = self.bucket,
                                       object_name = self.SAMPLE_FILE_NAME)
    
    def test_model_metadata_version_reading_equality_should_not_throw(self):
        from minio_extensions import ObjectMetadata, TagMetadata, VersionMetadata, \
            ObjectMetadataInfo
        from minio_extensions.extensions import MinioExtensions
        from minio_extensions.metadata.constants import Text
        from minio_extensions.environment import MINIO_S3_DEFAULT_BUCKET_NAME
        
        try:
            
            path_gen = self._gen_dummy_txt_file("./")
            
            local_path = os.path.abspath(path_gen)
            file_metadata = ObjectMetadata(
                version = VersionMetadata(major = 1, minor = 0, revision = 13),
                tags = [
                    TagMetadata(
                        name = "kind", content = "test"
                    )])
            
            MinioExtensions.upload_object(
                client = self._client,
                bucket = self.bucket,
                object_name = self.SAMPLE_FILE_NAME,
                local_path = local_path,
                content_type = Text,
                metadata = file_metadata
            )
            meta = MinioExtensions.get_object_metadata(client = self._client,
                                                       bucket = self.bucket,
                                                       object_name = self.SAMPLE_FILE_NAME)
            model = ObjectMetadataInfo.from_meta(meta)
            
            # Asserting uploaded file metadata equality
            self.assertEquals(file_metadata.version, model.version)
        except:
            raise
        finally:
            self._client.remove_object(MINIO_S3_DEFAULT_BUCKET_NAME.get(),
                                       self.SAMPLE_FILE_NAME)


if __name__ == '__main__':
    unittest.main()
