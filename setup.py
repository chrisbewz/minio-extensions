import codecs
import re
import sys

from setuptools import setup

if sys.argv[-1] == "publish":
    sys.argv = sys.argv[:-1] + ["sdist", "upload"]

with codecs.open("./minio_extensions/__init__.py") as file:
    version = re.search(
        r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
        file.read(),
        re.MULTILINE,
    ).group(1)

with codecs.open("README.md", encoding="utf-8") as file:
    readme = file.read()

setup(
    name="minio-extensions",
    description="MinIO Python Extension Package for S3 Storage Operations",
    author="Christian Celso Bewzenko",
    url="https://github.com/chrisbewz/minio-extensions",
    #download_url="https://github.com/chrisbewz/minio-extensions/releases",
    author_email="crisbewz@gmail.com",
    version=version,
    long_description_content_type="text/markdown",
    package_dir={"minio_extensions": "./minio_extensions"},
    packages=[
        "minio_extensions",
        "minio_extensions.metadata"
    ],
    install_requires=[
        "minio",
        "urllib3",
        "pydantic",
        "typing",
        "typing-extensions",
        "python-dotenv"
        ],
    tests_require=[],
    license="Apache-2.0",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    long_description=readme,
    package_data={"": ["LICENSE", "README.md"]},
    include_package_data=True,
)