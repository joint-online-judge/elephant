# from joj.elephant.schemas import Config, ArchiveType
# from joj.elephant.models import File

from abc import ABC, abstractmethod
from io import BytesIO
from tarfile import TarInfo
from typing import IO, Any
from zipfile import ZipFile


class Archive(ABC):
    def __init__(self) -> None:
        self.file_buffer = BytesIO()
        self.file: Any = None

    def close(self) -> None:
        if self.file:
            self.file.close()
            self.file = None

    @abstractmethod
    def write_file(self, arcname: str, data: bytes) -> None:
        raise NotImplementedError()

    @abstractmethod
    def extract_all(self, fp: IO[Any], dest_path: str) -> None:
        raise NotImplementedError()

    def __del__(self) -> None:
        self.close()


class ZipArchive(Archive):
    def __init__(self) -> None:
        super().__init__()
        # self.file = ZipFile(self.file_buffer, mode="w", compression=ZIP_DEFLATED)

    def extract_all(self, fp: IO[Any], dest_path: str) -> None:
        self.file = ZipFile(fp, mode="r")
        self.file.extractall(path=dest_path)

    def write_file(self, filepath: str, data: bytes) -> None:
        self.file.writestr(filepath, data)


class TgzArchive(Archive):
    def __init__(self) -> None:
        super().__init__()
        # self.file = TarFile.open(mode="w:gz", fileobj=self.file_buffer)

    def extract_all(self, fp: IO[Any], dest_path: str) -> None:
        raise NotImplementedError()  # TODO: finish this part

    def write_file(self, filepath: str, data: bytes) -> None:
        file_obj = BytesIO(data)
        tar_info = TarInfo(name=filepath)
        tar_info.size = len(data)
        self.file.addfile(tarinfo=tar_info, fileobj=file_obj)


# class RarArchive(Archive):
#     def __init__(self):
#         super().__init__()
#         self.file = RarFile(self.file_buffer, mode="w")
#
#     def write_file(self, filepath: str, data: bytes):
#         with open()
#         self.file.(filepath, data)
