import re
from copy import deepcopy
from datetime import datetime
from enum import Enum
from functools import partial
from typing import Any, Dict, List, Optional, Union, cast

from loguru import logger
from pydantic import BaseConfig, BaseModel, root_validator


class StrEnumMixin(str, Enum):
    def __str__(self) -> str:
        return self.value


class ArchiveType(StrEnumMixin, Enum):
    zip = "zip"
    tar = "tar"
    rar = "rar"
    unknown = "unknown"


class FileInfo(BaseModel):
    path: str
    is_dir: bool
    checksum: Optional[str] = None
    mtime: Optional[Union[datetime, str]] = None
    size_bytes: Optional[int] = None


def snake2camel(snake: str, start_lower: bool = False) -> str:
    """
    Converts a snake_case string to camelCase.

    The `start_lower` argument determines whether the first letter in the generated camelcase should
    be lowercase (if `start_lower` is True), or capitalized (if `start_lower` is False).
    """
    camel = snake.title()
    camel = re.sub("([0-9A-Za-z])_(?=[0-9A-Z])", lambda m: m.group(1), camel)
    if start_lower:
        camel = re.sub("(^_*[A-Z])", lambda m: m.group(1).lower(), camel)
    return camel


class APIModel(BaseModel):
    class Config(BaseConfig):
        orm_mode = True
        allow_population_by_field_name = True
        alias_generator = partial(snake2camel, start_lower=True)
        validate_all = True


class Case(APIModel):
    category: str = "pretest"
    time: str = "1s"
    memory: str = "64m"
    score: int = 10
    ignore_whitespace: bool = True
    execute_files: List[str] = ["./a.out"]
    execute_args: List[str] = ["./a.out", "--test"]
    execute_input_file: str = "case.in"
    execute_output_file: str = "case.out"


class LanguageDefault(APIModel):
    compile_files: List[str] = ["a.cpp"]
    compile_args: List[str] = ["gcc", "a.cpp"]
    case_default: Optional[Case]
    cases: Optional[List[Case]]


class Language(LanguageDefault):
    name: str = "c++"


class Config(APIModel):
    languages: List[Language]
    language_default: Optional[LanguageDefault]

    @classmethod
    def generate_default_value(cls) -> "Config":
        return cls(languages=[Language(name="c++", cases=[Case()])])

    @classmethod
    def parse_defaults_dict(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        for i, language in enumerate(values["languages"]):
            if values.get("language_default"):
                language = {
                    **values["language_default"],
                    **{k: v for k, v in language.items() if v is not None},
                }
            if language["cases"] is None:
                language["cases"] = []
            cases = cast(List[Dict[str, Any]], language["cases"])
            for j, case in enumerate(cases):
                if language.get("case_default"):
                    cases[j] = {
                        **language["case_default"],
                        **{k: v for k, v in case.items() if v is not None},
                    }
            values["languages"][i] = language
        return values

    @classmethod
    def parse_defaults(cls, config: "Config") -> "Config":
        return cls(**cls.parse_defaults_dict(config.dict()))

    @root_validator
    def validate_config(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        old_values = deepcopy(values)
        # make everything a python dict
        values["languages"] = [language.dict() for language in values["languages"]]
        if values.get("language_default"):
            values["language_default"] = values["language_default"].dict()
        logger.debug(f"original config values: {values}")
        parsed_values = cls.parse_defaults_dict(values)
        logger.debug(f"parsed config values: {parsed_values}")
        for i, language in enumerate(parsed_values["languages"]):
            for j, case in enumerate(language["cases"]):
                for field in Case.__fields__.keys():
                    if case.get(field) is None:
                        raise ValueError(
                            f"languages[{i}].cases[{j}] missing field {field}"
                        )
        return old_values
