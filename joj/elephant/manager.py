from typing import IO, Optional, Tuple, Dict, Any, Callable
import rapidjson
from gitignore_parser import rule_from_pattern, handle_negation
from pathlib import Path
from os.path import dirname
from logging import Logger
from contextlib import contextmanager

from fs.mirror import mirror
from fs.errors import FSError

from joj.elephant.storage import Storage, LocalStorage, S3Storage
from joj.elephant.schemas import ArchiveType, Config
from joj.elephant.archive import Archive, ZipArchive, TgzArchive
from joj.elephant.errors import (
    FileSystemError,
    FileSystemUndefinedError,
    FileSystemSyncError,
    ConfigError,
    ArchiveError,
)


def fs_parse_gitignore_fd(
    ignore_file: IO, base_dir: Optional[str] = None
) -> Callable[[str], bool]:
    full_path = "/.gitignore"
    if base_dir is None:
        base_dir = dirname(full_path)
    rules = []
    counter = 0
    for line in ignore_file:
        counter += 1
        line = line.rstrip("\n")
        rule = rule_from_pattern(
            line, base_path=Path(base_dir).resolve(), source=(full_path, counter)
        )
        if rule:
            rules.append(rule)
    if not any(r.negation for r in rules):
        return lambda file_path: any(r.match(file_path) for r in rules)
    else:
        # We have negation rules. We can't use a simple "any" to evaluate them.
        # Later rules override earlier rules.
        return lambda file_path: handle_negation(file_path, rules)


def get_archive(
    filename: str, archive_type: ArchiveType
) -> Tuple[Archive, ArchiveType]:
    if filename and archive_type == ArchiveType.unknown:
        if filename.endswith(".zip"):
            archive_type = ArchiveType.zip
        elif filename.endswith(".rar"):
            archive_type = ArchiveType.rar
        elif filename.endswith((".tar", ".gz", ".tgz")):
            archive_type = ArchiveType.tgz

    if archive_type == ArchiveType.zip:
        archive = ZipArchive()
    elif archive_type == ArchiveType.tgz:
        archive = TgzArchive()
    else:
        raise ArchiveError(f"archive type {archive_type.value} not supported!")

    return archive, archive_type


class Manager:
    def __init__(self, logger: Logger, source: Storage, dest: Optional[Storage] = None):
        self.logger = logger
        self.source: Storage = source
        self.dest: Optional[Storage] = dest
        self.ignore: Optional[Callable[[str], bool]] = None
        self.config: Optional[Config] = None

        # self.files = {}
        # self.config = Config()

    # def extract_archive_source(
    #     self,
    #     archive_fp: IO,
    #     filename: str = "",
    #     archive_type: ArchiveType = ArchiveType.unknown,
    # ) -> None:
    #     """Extract an archive to source path"""
    #     if not isinstance(self.source, LocalStorage):
    #         raise TypeError("Archive can only be extracted to local storage!")
    #     archive, archive_type = get_archive(filename, archive_type)
    #     archive.extract_all(archive_fp, self.source.path)

    @contextmanager
    def _open_source_file(self, filename: str, mode: str = "r"):
        if self.source.fs.exists(filename):
            yield self.source.fs.open(filename, mode)
        yield None

    def _init_ignore(self):
        with self._open_source_file(".gitignore") as ignore_file:
            if ignore_file:
                self.ignore = fs_parse_gitignore_fd(ignore_file)

    def _list_files(self, source: bool = False, dest: bool = False) -> None:
        if dest and self.dest is None:
            raise FileSystemUndefinedError("destination not defined!")
        if source and not dest:
            fs = self.source.fs
        elif dest and not source and self.dest is not None:
            fs = self.dest.fs
        else:
            raise FileSystemError("you can only use source or (not and) destination!")
        for step in fs.walk.walk():
            self.logger.info("In dir {}".format(step.path))
            self.logger.info("sub-directories: {!r}".format(step.dirs))
            self.logger.info("files: {!r}".format(step.files))
            for dir in step.dirs:
                fs.makedir(f"{step.path}/{dir.name}", recreate=True)

        # response = self.rclone.lsjson(path, ["-R"])
        # if response["code"] != 0:
        #     raise ValueError(f"ls repository failed, error: {response['error']}!")
        # files = rapidjson.loads(response["out"])
        # result = {file["Path"]: file for file in files}
        # self.rclone.log.info(result)
        # self.files = result

    # def ensure_file_in_local_path(self, filename: str) -> Optional[str]:
    #     if filename not in self.files:
    #         return None
    #     source_file_path = str(Path(self.source.path) / filename)
    #     if isinstance(self.source, LocalStorage):
    #         return source_file_path
    #     elif isinstance(self.source, S3Storage):
    #         if self.dest is None:
    #             self.dest = LocalTempStorage()
    #         if not isinstance(self.dest, LocalStorage):
    #             raise TypeError("destination must be a local storage!")
    #         dest_file_path = str(Path(self.dest.path) / filename)
    #         response = self.rclone.run_cmd("copyto", [source_file_path, dest_file_path])
    #         if response["code"] != 0:
    #             raise ValueError(f"copy file failed, error: {response['error']}!")
    #         return dest_file_path
    #     return None
    #
    # def filter_files_by_ignore(self) -> None:
    #     ignore_file = self.ensure_file_in_local_path(".gitignore")
    #     if not ignore_file:
    #         return
    #     matches = parse_gitignore(ignore_file)
    #     base_dir = Path(ignore_file).parent
    #     new_files = {}
    #     for path in self.files.keys():
    #         file_path = base_dir / path
    #         if path == "config.generated.json":
    #             continue
    #         # do not remove .gitignore and config.json
    #         if path != ".gitignore" and path != "config.json":
    #             # remove files outside base directory or matched .gitignore
    #             if base_dir in file_path.parents and not matches(file_path):
    #                 new_files[path] = self.files[path]
    #     # TODO: should we delete ignored files on the server?
    #     self.files = new_files

    def _parse_and_update_config(self):
        with self._open_source_file("config.json") as config_file:
            if config_file is None:
                raise ConfigError("config file not found!")
            data = rapidjson.load(config_file)
        self.config = Config(**data)
        self.logger.info(self.config)

    def _generate_config(self):
        filename = "config.generated.json"
        with self.source.fs.open(filename, mode="w") as f:
            rapidjson.dump(f, self.config.dict(), indent=2)

    def validate_source(self) -> None:
        """Validate config.json on source path and generate self.config."""
        if isinstance(self.source, LocalStorage) or isinstance(self.source, S3Storage):
            try:
                self._list_files(source=True)
                # self.filter_files_by_ignore()
                self._parse_and_update_config()
            except FSError as e:
                raise FileSystemError(str(e))
        else:
            raise FileSystemError("validation failed, source type not supported!")

    def sync_with_validation(self) -> None:
        """Sync source to dest after validation"""
        if self.dest is None:
            raise FileSystemSyncError("sync failed, destination not defined!")
        try:
            self.validate_source()
            self._generate_config()
            self.sync_without_validation()
        except FSError as e:
            raise FileSystemError(str(e))

    def sync_without_validation(self, workers: int = 4):
        """Sync source to dest directly, can be use as clone."""
        if self.dest is None:
            raise FileSystemSyncError("sync failed, destination not defined!")
        try:
            mirror(self.source.fs, self.dest.fs, copy_if_newer=False, workers=workers)
        except FSError as e:
            raise FileSystemError(str(e))
