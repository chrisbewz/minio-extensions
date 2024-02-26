from minio_extensions.extensions import MinioExtensions
from minio_extensions.providers import ClientBuilder

# Instantiating builder class and passing the option to create minio client
# from existent/declared environment variables
builder = ClientBuilder("env", is_proxy_conn = False)

# Returning generated instance from builder class
client = builder.configure()

# The same client can also be generated from environment through a direct call to MinioExtensions.create_provider()
# In this case is not specified any arguments since it already considers the "env" as default option.
client = MinioExtensions.create_provider()

