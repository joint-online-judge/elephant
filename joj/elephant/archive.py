from joj.elephant.schemas import Config, ArchiveType
from joj.elephant.models import File

from abc import abstractmethod, ABC
from zipfile import ZipFile, ZIP_DEFLATED
from tarfile import TarFile, TarInfo

from io import BytesIO
import json


class Archive(ABC):
    def __init__(self):
        self.file_buffer = BytesIO()
        self.file = None

    def close(self):
        if self.file:
            self.file.close()
            self.file = None

    @abstractmethod
    def write_file(self, arcname: str, data: bytes):
        pass

    def __del__(self):
        self.close()


class ZipArchive(Archive):
    def __init__(self):
        super().__init__()
        self.file = ZipFile(self.file_buffer, mode="w", compression=ZIP_DEFLATED)

    def write_file(self, filepath: str, data: bytes):
        self.file.writestr(filepath, data)


class TgzArchive(Archive):
    def __init__(self):
        super().__init__()
        self.file = TarFile.open(mode="w:gz", fileobj=self.file_buffer)

    def write_file(self, filepath: str, data: bytes):
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


