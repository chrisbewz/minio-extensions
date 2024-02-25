import unittest
import os, sys

import pandas as pd


class MinioObjectMetadataTests(unittest.TestCase):
    from minio_extensions.providers import MinioProvider
    _provider: MinioProvider = None
    SAMPLE_FILE_NAME = "sample_test.txt"
    def setUp(self):
        from dotenv import load_dotenv
        from minio_extensions.extensions import MinioExtensions
        load_dotenv()
        self._provider = MinioExtensions.create_provider()
    
    def _gen_dummy_txt_file(self, location):
        file_path_generated = f"{location}{self.SAMPLE_FILE_NAME}"
        with open(file_path_generated, "w") as txtf:
            txtf.write("hello\n")
            txtf.write("world")
        return file_path_generated
        
    def test_object_metadata_tag_equality_should_not_throw(self):
        try:
            from minio_extensions.metadata.constants import INITIAL_VERSION, TEST_TAG, PREVIEW_TAG
            from minio_extensions.metadata import ObjectMetadata, TagMetadata, VersionMetadata, ContentType, \
                ObjectMetadataInfo
            
            path_gen = self._gen_dummy_txt_file("./")
            
            local_path = os.path.abspath(path_gen)
            file_metadata = ObjectMetadata(
                version = INITIAL_VERSION,
                tags = [
                    TEST_TAG,
                    PREVIEW_TAG
                ])
            
            self._provider.upload_object(
                bucket = os.getenv("DEVELOPMENT_DATABASE_MINIO_BUCKET_NAME"),
                object_name = self.SAMPLE_FILE_NAME,
                local_path = local_path,
                content_type = ContentType.TXT.value,
                metadata = file_metadata
            )
            meta = self._provider.get_object_metadata(bucket = os.getenv("DEVELOPMENT_DATABASE_MINIO_BUCKET_NAME"),
                                                      object_name = self.SAMPLE_FILE_NAME)
            
            tags = TagMetadata.from_dict(meta["tags"])
            model = ObjectMetadataInfo.from_meta(meta)
            
            # Asserting uploaded file metadata equality
            self.assertEquals(file_metadata.version, model.version)
        except:
            raise
        finally:
            self._provider.instance.remove_object(os.getenv("DEVELOPMENT_DATABASE_MINIO_BUCKET_NAME"),
                                                  self.SAMPLE_FILE_NAME)
    
    def test_model_metadata_version_reading_equality_should_not_throw(self):
        
        try:
            from minio_extensions.metadata import ObjectMetadata, TagMetadata, VersionMetadata, ContentType, \
                ObjectMetadataInfo
            
            path_gen = self._gen_dummy_txt_file("./")
            
            local_path = os.path.abspath(path_gen)
            file_metadata = ObjectMetadata(
                version = VersionMetadata(major = 1, minor = 0, revision = 13),
                tags = [
                    TagMetadata(
                        name = "kind", content = "test"
                    )])
            
            self._provider.upload_object(
                bucket = os.getenv("DEVELOPMENT_DATABASE_MINIO_BUCKET_NAME"),
                object_name = self.SAMPLE_FILE_NAME,
                local_path = local_path,
                content_type = ContentType.TXT.value,
                metadata = file_metadata
            )
            meta = self._provider.get_object_metadata(bucket = os.getenv("DEVELOPMENT_DATABASE_MINIO_BUCKET_NAME"), object_name = self.SAMPLE_FILE_NAME)
            model = ObjectMetadataInfo.from_meta(meta)
            
            # Asserting uploaded file metadata equality
            self.assertEquals(file_metadata.version, model.version)
        except:
            raise
        finally:
            self._provider.instance.remove_object(os.getenv("DEVELOPMENT_DATABASE_MINIO_BUCKET_NAME"), self.SAMPLE_FILE_NAME)
            

class MinioObjectDownloadTests(unittest.TestCase):
    from database.providers.minio.provider import MinioProvider
    _provider: MinioProvider = None
    
    def setUp(self):
        from dotenv import load_dotenv
        from database.providers.minio.extensions import MinioExtensions
        load_dotenv()
        self._provider = MinioExtensions.create_provider()
    
    def test_multiple_object_downloads(self):
        search_pattern = f"data_sources/schedule/v1.1"
        expected_1_1_subfolder_files = [
            'apr_schedule_hist_v1.1.csv',
            'calc_schedule_hist_v1.1.csv',
            'enr_schedule_hist_v1.1.csv',
            'epa_schedule_hist_v1.1.csv',
            'epm_schedule_hist_v1.1.csv',
            'full_projects_schedule_hist_v1.1.csv',
            'gtec_schedule_hist_v1.1.csv',
            'mec_schedule_hist_v1.1.csv',
            'pa_schedule_hist_v1.1.csv',
            'ref_schedule_hist_v1.1.csv',
            'scc_schedule_hist_v1.1.csv'
        ]
        files = []
        files_found_on_folder = self._provider.list_files_from_bucket(
            bucket = os.getenv("DEVELOPMENT_DATABASE_MINIO_BUCKET_NAME"),
            prefix = search_pattern,
            recurse = True)
        
        # Iterating the generator to get the files found
        for f in files_found_on_folder:
            print(f.object_name)
            files.extend([f.object_name])
            
        # Removing the search prefix returned from the recurse enabled call as well the backwards backslash from path string
        files = [
            f.replace(search_pattern, '').replace('/', '') for f in files
        ]
        
        self.assertSetEqual(set(files), set(expected_1_1_subfolder_files))
        
    
    def test_dataset_import_should_not_throw(self):
        
        file_info, path = self._provider.get_object("projeto1",
                                               "data_sources/schedule/v1.1/full_projects_schedule_hist_v1.1.csv")
        
        # Asserting if the file_info bytes is not None
        self.assertNotEqual(file_info, None)
        
        # Asserting if the minio object was effectibely dowloaded locally from bucket to computer at user's temp directory
        self.assertTrue(os.path.isfile(path))
        
        # Asserting if bytes io returned can be read as dataframe
        self.assertNotEqual(file_info, None)
        
        # Asserting if file_info path returned can be read as dataframe
        self.assertIsInstance(pd.read_csv(path), pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
