from pydantic import BaseModel
from enum import Enum
from pathlib import Path
from typing import Dict, Any, Union, Generator, Callable


class ArchiveType(str, Enum):
    zip = "zip"
    tgz = "tgz"
    rar = "rar"
    unknown = "unknown"


class FileType(str, Enum):
    default = "default"
    compile = "compile"
    runtime = "runtime"
    testcase = "testcase"
    judge = "judge"

    def __str__(self):
        return self.value


class Config(BaseModel):
    files: Dict[str, FileType] = {}

    def update_files(
        self, files_in_config: Dict[str, str], files_in_fs: Dict[str, Any]
    ) -> None:
        for path in files_in_fs.keys():
            if path in files_in_config:
                file_type = files_in_config[path]
                if file_type not in FileType:
                    file_type = FileType.default
            else:
                file_type = FileType.default
            self.files[path] = file_type


class File(BaseModel):
    type: FileType
    path: Path
    digest: str
