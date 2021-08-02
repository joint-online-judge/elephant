from typing import IO, Optional, Tuple, Dict, Any
import rapidjson
from gitignore_parser import parse_gitignore
from pathlib import Path

from fs.base import FS
from fs.mirror import mirror

from joj.elephant.rclone import RClone
from joj.elephant.storage import Storage, LocalStorage, S3Storage, LocalTempStorage
from joj.elephant.schemas import ArchiveType, Config
from joj.elephant.archive import Archive, ZipArchive, TgzArchive


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
        raise ValueError(f"archive type {archive_type.value} not supported!")

    return archive, archive_type


class Manager:
    def __init__(self, source: FS, dest: Optional[FS] = None):
        self.source: FS = source
        self.dest: Optional[FS] = dest
        # self.files = {}
        # self.config = Config()

    def sync(self, workers=4):
        if self.dest is None:
            raise ValueError("destination not specified!")
        mirror(self.source, self.dest, copy_if_newer=False, workers=workers)

    def extract_archive_source(
        self,
        archive_fp: IO,
        filename: str = "",
        archive_type: ArchiveType = ArchiveType.unknown,
    ) -> None:
        """Extract an archive to source path"""
        if not isinstance(self.source, LocalStorage):
            raise TypeError("Archive can only be extracted to local storage!")
        archive, archive_type = get_archive(filename, archive_type)
        archive.extract_all(archive_fp, self.source.path)

    def list_files(self, fs: FS) -> None:
        for step in fs.walk.walk():
            print('In dir {}'.format(step.path))
            print('sub-directories: {!r}'.format(step.dirs))
            print('files: {!r}'.format(step.files))

        # response = self.rclone.lsjson(path, ["-R"])
        # if response["code"] != 0:
        #     raise ValueError(f"ls repository failed, error: {response['error']}!")
        # files = rapidjson.loads(response["out"])
        # result = {file["Path"]: file for file in files}
        # self.rclone.log.info(result)
        # self.files = result

    def ensure_file_in_local_path(self, filename: str) -> Optional[str]:
        if filename not in self.files:
            return None
        source_file_path = str(Path(self.source.path) / filename)
        if isinstance(self.source, LocalStorage):
            return source_file_path
        elif isinstance(self.source, S3Storage):
            if self.dest is None:
                self.dest = LocalTempStorage()
            if not isinstance(self.dest, LocalStorage):
                raise TypeError("destination must be a local storage!")
            dest_file_path = str(Path(self.dest.path) / filename)
            response = self.rclone.run_cmd("copyto", [source_file_path, dest_file_path])
            if response["code"] != 0:
                raise ValueError(f"copy file failed, error: {response['error']}!")
            return dest_file_path
        return None

    def filter_files_by_ignore(self) -> None:
        ignore_file = self.ensure_file_in_local_path(".gitignore")
        if not ignore_file:
            return
        matches = parse_gitignore(ignore_file)
        base_dir = Path(ignore_file).parent
        new_files = {}
        for path in self.files.keys():
            file_path = base_dir / path
            if path == "config.generated.json":
                continue
            # do not remove .gitignore and config.json
            if path != ".gitignore" and path != "config.json":
                # remove files outside base directory or matched .gitignore
                if base_dir in file_path.parents and not matches(file_path):
                    new_files[path] = self.files[path]
        # TODO: should we delete ignored files on the server?
        self.files = new_files

    def parse_and_update_config(self):
        config_file = self.ensure_file_in_local_path("config.json")
        if not config_file:
            raise ValueError("config file not found!")
        with open(config_file) as f:
            data = rapidjson.load(f)

        files = data.get("files", {})
        self.config.update_files(files, self.files)

    def generate_config(self):
        filename = "config.generated.json"
        if isinstance(self.source, LocalStorage):
            source_file_path = str(Path(self.source.path) / filename)
        elif isinstance(self.source, S3Storage):
            temp = LocalTempStorage()
            source_file_path = str(Path(temp.path) / filename)
        else:
            raise TypeError("generate config failed, source type not supported!")
        with open(source_file_path, "w") as f:
            rapidjson.dump(f, self.config.dict(), indent=2)
        if isinstance(self.source, S3Storage):
            dest_file_path = str(Path(self.source.path) / filename)
            response = self.rclone.run_cmd("copyto", [source_file_path, dest_file_path])
            if response["code"] != 0:
                raise ValueError(f"copy file failed, error: {response['error']}!")

    def validate_source(self) -> None:
        """Validate config.json on source path and generate self.config."""
        if isinstance(self.source, LocalStorage) or isinstance(self.source, S3Storage):
            self.list_files(self.source.path)
            self.filter_files_by_ignore()
            self.parse_and_update_config()
        else:
            raise TypeError("validation failed, source type not supported!")

    def sync_with_validation(self) -> None:
        """Sync source to dest after validation"""
        if self.dest is None:
            raise TypeError("sync failed, dest not defined!")
        self.validate_source()
        self.generate_config()
        self.sync_without_validation()

    def sync_without_validation(self) -> None:
        """Sync source to dest directly, can be use as clone."""
        response = self.rclone.sync(self.source.path, self.dest.path)
        if response["code"] != 0:
            raise ValueError(f"sync failed, error: {response['error']}!")
