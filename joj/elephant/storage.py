from abc import abstractmethod, ABC
from tempfile import mkdtemp
from shutil import rmtree
from pathlib import Path


class Storage(ABC):
    @property
    @abstractmethod
    def path(self) -> str:
        raise NotImplementedError()


class S3Storage(Storage):
    def __init__(self, s3_path: str) -> None:
        self.s3_path = s3_path

    @property
    def path(self) -> str:
        return self.s3_path


class LocalStorage(Storage):
    def __init__(self, local_path: str) -> None:
        self.local_path = local_path
        Path(self.local_path).mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> str:
        return self.local_path


class LocalTempStorage(LocalStorage):
    def __init__(self):
        super().__init__(mkdtemp(prefix="joj.elephant."))

    def __del__(self):
        rmtree(self.path, ignore_errors=True)
