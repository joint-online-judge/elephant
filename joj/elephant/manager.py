from typing import IO, Optional, Tuple
import rapidjson
from gitignore_parser import parse_gitignore

from joj.elephant.rclone import RClone
from joj.elephant.storage import Storage, LocalStorage, S3Storage
from joj.elephant.schemas import ArchiveType, Config, File
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
    def __init__(self, rclone: RClone, source: Storage, dest: Optional[Storage] = None):
        self.rclone: RClone = rclone
        self.source: Storage = source
        self.dest: Optional[Storage] = dest

    def sync(self):
        if self.dest is None:
            raise ValueError("destination not specified!")
        self.rclone.sync(self.source.path, self.dest.path)

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

    def list_files(self, path: str):
        response = self.rclone.lsjson(path, ["-R"])
        if response["code"] != 0:
            raise ValueError(f"ls repository failed, error: {response['error']}!")
        result = rapidjson.loads(response["out"])
        self.rclone.log.info(result)
        return result

    def filter_files_by_ignore(self, files, path: Optional[str] = None):

        pass

    def validate_source(self) -> bool:
        """Validate config.json and generate config.generated.json on source path"""
        if isinstance(self.source, LocalStorage) or isinstance(self.source, S3Storage):
            # print(self.rclone.run_cmd("config", ["userinfo", "lakefs:"]))
            # self.rclone.log.info(self.source.path)
            files = self.list_files(self.source.path)
        else:
            raise TypeError("source type not supported!")
        return False
