from typing import Optional, IO
from abc import abstractmethod, ABC

from tempfile import mkdtemp
from shutil import rmtree
from pathlib import Path

from fs.base import FS
from fs_s3fs import S3FS
from fs.osfs import OSFS
from fs.tempfs import TempFS
from fs.zipfs import ZipFS
from fs.tarfs import TarFS

from joj.elephant.schemas import ArchiveType


class Storage(ABC):
    def __init__(self):
        self._fs: Optional[FS] = None

    @property
    def fs(self) -> FS:
        return self._fs


class S3Storage(Storage):
    def __init__(
        self,
        bucket_name: str,
        dir_path: str = "/",
        username: Optional[str] = None,
        password: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._fs = S3FS(
            bucket_name=bucket_name,
            dir_path=dir_path,
            aws_access_key_id=username,
            aws_secret_access_key=password,
            endpoint_url=endpoint_url,
        )


class LakeFSStorage(S3Storage):
    def __init__(
        self,
        endpoint_url: str,
        repo_name: str,
        branch_name: str = "main",
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        super().__init__(repo_name, f"/{branch_name}", username, password, endpoint_url)


class LocalStorage(Storage):
    def __init__(self, local_path: str, create=False, create_mode=511) -> None:
        super().__init__()
        self._fs = OSFS(local_path, create=create, create_mode=create_mode)


class TempStorage(Storage):
    def __init__(self):
        super().__init__()
        self._fs = TempFS()


class ArchiveStorage(Storage):
    def __init__(
        self,
        filename: Optional[str] = None,
        fd: Optional[IO] = None,
        write: bool = False,
        archive_type: ArchiveType = ArchiveType.unknown,
    ):
        super().__init__()
        if filename and archive_type == ArchiveType.unknown:
            if filename.endswith(".zip"):
                archive_type = ArchiveType.zip
            elif filename.endswith((".tar", ".gz", ".tgz")):
                archive_type = ArchiveType.tgz

        if fd is not None:
            file = fd
        elif filename:
            file = filename
        else:
            raise ValueError(f"archive not found!")

        if archive_type == ArchiveType.zip:
            self._fs = ZipFS(file, write=write)
        elif archive_type == ArchiveType.tgz:
            self._fs = TarFS(file, write=write)
        else:
            raise ValueError(f"archive type {archive_type} not supported!")
