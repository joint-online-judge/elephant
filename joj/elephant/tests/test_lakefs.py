import pytest
from lakefs_client import __version__, models
from lakefs_client.client import LakeFSClient


def test_connection(lakefs_client: LakeFSClient) -> None:
    response: models.VersionConfig = lakefs_client.config.get_lake_fs_version()
    assert response["version"]
    print(
        f"LakeFS connected: client version {__version__}, "
        f"server version {response['version']}."
    )


@pytest.mark.parametrize("repo_name", ["config", "submission"])
def test_create_repo(
    lakefs_client: LakeFSClient, s3_bucket: str, repo_name: str
) -> None:
    namespace = f"{s3_bucket}/{repo_name}"
    try:
        lakefs_client.repositories.delete_repository(repo_name)
    except Exception:
        pass
    new_repo = models.RepositoryCreation(
        storage_namespace=namespace,
        name=repo_name,
        default_branch="master",
    )
    repo = lakefs_client.repositories.create_repository(new_repo)
    assert repo
