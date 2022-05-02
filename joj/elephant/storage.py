from abc import ABC
from pathlib import Path
from typing import IO, Any, BinaryIO, Optional, SupportsInt

import patoolib
from fs.base import FS
from fs.errors import FSError
from fs.info import Info
from fs.osfs import OSFS
from fs.tempfs import TempFS
from fs_s3fs import S3FS

from joj.elephant.errors import FileSystemError
from joj.elephant.schemas import FileInfo


class Storage(ABC):
    _fs: Optional[FS]

    def __init__(self, path: str):
        self.path = path

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

    def extract_all(self) -> None:
        pass


class S3Storage(Storage):
    def __init__(
        self,
        host_in_config: str,
        bucket_name: str,
        dir_path: str = "/",
        username: Optional[str] = None,
        password: Optional[str] = None,
        endpoint_url: Optional[str] = None,
    ) -> None:
        super().__init__(path=f"{host_in_config}:{bucket_name}{dir_path}")
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

    # def download(self, remote_path: Path, local_path: Path):

    def extract_all(self) -> None:
        raise NotImplementedError()


class LakeFSStorage(S3Storage):
    def __init__(
        self,
        host_in_config: str,
        endpoint_url: str,
        repo_name: str,
        branch_name: str = "master",
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        super().__init__(
            host_in_config,
            repo_name,
            f"/{branch_name}",
            username,
            password,
            endpoint_url,
        )


class LocalStorage(Storage):
    def __init__(
        self, local_path: str, create: bool = False, create_mode: SupportsInt = 511
    ) -> None:
        self._fs = OSFS(local_path, create=create, create_mode=create_mode)
        super().__init__(self._fs.getsyspath("/"))


class TempStorage(Storage):
    def __init__(self) -> None:
        self._fs = TempFS()
        super().__init__(self._fs.getsyspath("/"))


class ArchiveStorage(TempStorage):
    file_path: str
    temp_file: Optional[IO[Any]]

    def __init__(self, file_path: str) -> None:
        super().__init__()
        self.file_path = file_path

    def extract_all(self) -> None:  # FIXME: should it be async-ed?
        patoolib.extract_archive(self.file_path, outdir=self.path)

    def compress_all(self) -> None:
        patoolib.create_archive(self.file_path, [self.path])


class CodeTextStorage(TempStorage):
    def __init__(self, filename: str, code_text: str) -> None:
        super().__init__()
        self.fs.writetext(filename, code_text)
        self.filename = filename
