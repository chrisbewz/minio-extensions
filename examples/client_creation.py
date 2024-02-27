from minio_extensions.extensions import MinioExtensions
from minio_extensions.providers import ClientBuilder

from dotenv import load_dotenv

load_dotenv()

# Assuming the following structure present on .env file in project for this example to properly run
# export MINIO_S3_ENDPOINT_PORT='<some-port>'
# export MINIO_S3_ENDPOINT_URL='<some-endpoint>' example : 10.x.x.xxx
# export MINIO_S3_USERNAME='<my-minio-access-key>'
# export MINIO_S3_PASSWORD='<my-minio-access-password>'
# export MINIO_S3_DEFAULT_BUCKET_NAME='<my-bucket>'
# export MINIO_S3_IGNORE_SECURE_CONNECTION='<is-minio-conn-secure>'

# Instantiating builder class and passing the option to create minio client
# from existent/declared environment variables
builder = ClientBuilder("env", is_proxy_conn = False)

# Returning generated instance from builder class
client = builder.configure()

# The same client can also be generated from environment through a direct call to MinioExtensions.create_provider()
# In this case is not specified any arguments since it already considers the "env" as default option.
#client = MinioExtensions.create_provider()

print(client.bucket_exists("sample-bucket"))

