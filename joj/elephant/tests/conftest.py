import boto3
import pytest
from lakefs_client import Configuration
from lakefs_client.client import LakeFSClient
from loguru import logger

from joj.elephant.tests.config import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(scope="session")
def rclone_config(settings: Settings) -> str:
    return f"""
[lakefs]
type = s3
provider = AWS
env_auth = false
access_key_id = {settings.lakefs_username}
secret_access_key = {settings.lakefs_password}
endpoint = http://{settings.lakefs_s3_domain}:{settings.lakefs_port}
    """


@pytest.fixture(scope="session", autouse=True)
def s3_bucket(settings: Settings) -> str:
    bucket_name = "joj-test"
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{settings.s3_host}:{settings.s3_port}",
        aws_access_key_id=settings.s3_username,
        aws_secret_access_key=settings.s3_password,
        # config=Config(signature_version="s3v4"),
        # region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=bucket_name)
    except Exception:
        logger.exception("s3.create_bucket exception: ")
    return f"s3://{bucket_name}"


@pytest.fixture(scope="session")
def lakefs_client(settings: Settings) -> LakeFSClient:
    configuration = Configuration(
        host=f"{settings.lakefs_host}:{settings.lakefs_port}",
        username=settings.lakefs_username,
        password=settings.lakefs_password,
    )
    return LakeFSClient(configuration)
