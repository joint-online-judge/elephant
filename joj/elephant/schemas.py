from pydantic import BaseModel
from enum import Enum
from pathlib import Path
from bson import ObjectId
from bson.errors import InvalidId
from typing import Dict, Any, Union, Generator, Callable


class PydanticObjectId(str):
    @classmethod
    def __get_validators__(
        cls,
    ) -> Generator[Callable[[Union[str, Any]], str], None, None]:
        yield cls.validate

    @classmethod
    def validate(cls, v: Union[str, ObjectId]) -> str:
        try:
            if isinstance(v, str):
                v = ObjectId(v)
            elif not isinstance(v, ObjectId):
                raise InvalidId() from None
        except InvalidId:
            raise TypeError("ObjectId required")
        return str(v)


class ArchiveType(str, Enum):
    zip = "zip"
    tgz = "tgz"
    rar = "rar"


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
    files: Dict[str, File]
