from typing import Optional, IO, BinaryIO
from abc import ABC
from pathlib import Path

from fs.base import FS
from fs_s3fs import S3FS
from fs.osfs import OSFS
from fs.tempfs import TempFS
from fs.zipfs import ZipFS
from fs.tarfs import TarFS
from fs.errors import FSError
from fs.info import Info

from joj.elephant.schemas import ArchiveType, FileInfo
from joj.elephant.errors import FileSystemError


class Storage(ABC):
    def __init__(self):
        self._fs: Optional[FS] = None

    @property
    def fs(self) -> FS:
        return self._fs

    @staticmethod
    def parse_file_info(path: Path, info: Info) -> FileInfo:
        return FileInfo(
            path=info.make_path(str(path.parent)),
            is_dir=info.is_dir,
            mtime=info.modified,
            size_bytes=info.size,
        )

    def getinfo(self, path: Path) -> FileInfo:
        info = self.fs.getinfo(path=str(path), namespaces=["details"])
        return self.parse_file_info(path, info)

    def upload(
        self, path: Path, file: BinaryIO, chunk_size: Optional[int] = None
    ) -> FileInfo:
        try:
            self.fs.makedirs(path=str(path.parent), recreate=True)
            self.fs.upload(path=str(path), file=file, chunk_size=chunk_size)
            return self.getinfo(path)
        except FSError as e:
            raise FileSystemError(str(e))

    def download(
        self, path: Path, file: BinaryIO, chunk_size: Optional[int] = None
    ) -> None:
        try:
            self.fs.download(path=str(path), file=file, chunk_size=chunk_size)
            # return self.fs.getinfo(path=str(path))
        except FSError as e:
            raise FileSystemError(str(e))

    def delete(self, path: Path) -> FileInfo:
        try:
            file_info = self.getinfo(path)
            self.fs.remove(path=str(path))
            return file_info
        except FSError as e:
            raise FileSystemError(str(e))

    def delete_dir(self, path: Path) -> FileInfo:
        try:
            file_info = self.getinfo(path)
            self.fs.removedir(path=str(path))
            return file_info
        except FSError as e:
            raise FileSystemError(str(e))

    def delete_tree(self, path: Path) -> FileInfo:
        try:
            file_info = self.getinfo(path)
            self.fs.removetree(dir_path=str(path))
            return file_info
        except FSError as e:
            raise FileSystemError(str(e))

    def close(self) -> None:
        self.fs.close()


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

    def getinfo(self, path: Path) -> FileInfo:
        info = self.fs.getinfo(path=str(path), namespaces=["details", "s3"])
        file_info = self.parse_file_info(path, info)
        checksum: Optional[str] = info.raw["s3"].get("e_tag", None)
        if checksum:
            checksum = checksum.strip('"')
        file_info.checksum = checksum
        return file_info


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
        compression: Optional[str] = None,
    ):
        super().__init__()
        if filename and archive_type == ArchiveType.unknown:
            if filename.endswith(".zip"):
                archive_type = ArchiveType.zip
            elif filename.endswith((".tar", ".tar.gz", ".tgz")):
                archive_type = ArchiveType.tar

        if fd is not None:
            file = fd
        elif filename:
            file = filename
        else:
            raise ValueError(f"archive not found!")

        if archive_type == ArchiveType.zip:
            self._fs = ZipFS(file, write=write)
        elif archive_type == ArchiveType.tar:
            self._fs = TarFS(file, write=write, compression=compression)
        else:
            raise ValueError(f"archive type {archive_type} not supported!")
