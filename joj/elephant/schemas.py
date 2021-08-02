from pydantic import BaseModel, Field
from enum import Enum
from pathlib import Path
from typing import Dict, Any, Union, Generator, Callable, List, Optional, Literal


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


class Command(BaseModel):
    name: Optional[str] = Field(
        None,
        description="name of the command, not used in the execution.",
    )
    execute_file: Optional[str] = Field(
        None,
        description="the program of the command.",
    )
    execute_args: Optional[Union[str, List[str]]] = Field(
        None,
        description="the arguments of the command, not including execute_file."
        "can be a string and be parsed as shell args.",
    )
    working_directory: Optional[str] = Field(
        None, description="working directory of the command, default is root (/)."
    )
    log_size: Optional[str] = Field(
        None,
        description="limit the size of stdout and stderr saved in object storage.",
    )
    time: Optional[str] = Field(
        None,
        description="limit the time used by the command.",
    )
    memory: Optional[str] = Field(
        None,
        description="limit the memory used by the command.",
    )
    score: Optional[float] = Field(
        None,
        description="score of a case, can be defined in runtime as a default score.",
    )
    extend_args: bool = Field(
        True,
        description="if true, execute_args will be extended instead of replaced.",
    )
    halt_on_error: bool = Field(
        True,
        description="if true, stop all future commands in the chain and return an error.",
    )
    input: Optional[str] = Field(
        None,
        description="if set, pipe the file as stdin.",
    )


Commands = List[Optional[Command]]
CommandOrCommands = Union[Command, Commands]


class Language(BaseModel):
    name: Optional[str] = Field(
        None,
        description="(should be unique)"
        "If set and can be found in tiger, inherits the depends and compile definition.",
    )
    display_name: Optional[str] = Field(
        None,
        description="The name displayed in frontend.",
    )
    depends: List[str] = Field(
        [],
        description="Each tiger instance has a list of supported toolchains."
        "All toolchains listed in depends must be met to execute the task."
        "Overwrite the depends definition in toolchain if provided.",
    )
    compile: Optional[CommandOrCommands] = Field(
        None,
        description="Command(s) in compile stage, can be omitted for an interpreter."
        "Overwrite the compile definition in toolchain if provided.",
    )
    runtime: Optional[Command] = Field(
        None,
        description="Command in runtime stage."
        "Overwrite the runtime definition in config.",
    )
    judge: Optional[CommandOrCommands] = Field(
        None,
        description="Command(s) in judge stage. (not implemented)"
        "Overwrite the judge definition in config.",
    )


class Case(Command):
    """A case extends all attributes from a runtime config."""

    output: Optional[str] = Field(
        None,
        description="If set, the file will be used as output file in default diff judge."
        "if not set, the stdout of the runtime command will be used."
        "It will be used as the file displayed in <Your Answer>",
    )
    answer: Optional[str] = Field(
        None,
        description="If set, the file will be used as answer file in default diff judge."
        "if not set, the default judge will accept this case directly."
        "It will be used as the file displayed in <JOJ Answer>",
    )
    judge: Optional[CommandOrCommands] = Field(
        None,
        description="Command(s) in judge stage. (not implemented)"
        "Overwrite the judge definition in language.",
    )
    languages: Optional[List[str]] = Field(
        None, description="If set, only include the case in specific languages"
    )
    category: Optional[List[str]] = Field(
        None, description="If set, only include the case when the category is used."
    )


class Config(BaseModel):
    runtime: Optional[Command] = Field(
        None,
        description="Command in runtime stage.",
    )
    judge: Optional[CommandOrCommands] = Field(
        None,
        description="Command(s) in judge stage. (not implemented, only supports diff now)"
        "If not provided, a specially designed diff program will be used.",
    )
    languages: List[Language] = Field(
        [],
        description="List of languages supported, also saved in Problem model in horse.",
    )
    cases: List[Case] = Field(
        [],
        description="List of cases."
        "If empty, the record will be accepted directly after a successful compilation.",
    )

    # TODO: design files format
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
