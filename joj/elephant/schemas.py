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
    compile = "compile"
    runtime = "runtime"
    testcase = "testcase"
    judge = "judge"

    def __str__(self):
        return self.value


class File(BaseModel):
    type: FileType
    path: Path
    digest: str


class Config(BaseModel):
    files: Dict[str, File] = {}
