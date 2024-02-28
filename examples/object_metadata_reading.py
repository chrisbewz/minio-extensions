from minio_extensions.extensions import MinioExtensions
from minio_extensions.providers import ClientBuilder
from minio_extensions.metadata.metadata import TagMetadata,ObjectMetadataInfo
from dotenv import load_dotenv

load_dotenv()

builder = ClientBuilder("env", is_proxy_conn = False)
client = builder.configure()

#sample_bucket = "<my-sample-bucket>"
sample_bucket = "projeto1"
sample_object_name = "/data_sources/sap/afpo.parquet"
meta = MinioExtensions.get_object_metadata(client = client,
                                           bucket = sample_bucket,
                                           object_name = sample_object_name)

# Retrieving stored object tags
tags = TagMetadata.from_dict(meta["tags"])

# Retrieving stored object complete metadata from client
model = ObjectMetadataInfo.from_meta(meta)

print(model)