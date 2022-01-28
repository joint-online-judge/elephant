import json

from joj.elephant.archive import Archive, TgzArchive, ZipArchive
from joj.elephant.schemas import ArchiveType, Config


async def export_to_archive(config: Config, archive_type: ArchiveType) -> None:
    archive: Archive
    if archive_type == ArchiveType.zip:
        archive = ZipArchive()
    elif archive_type == ArchiveType.tar:
        archive = TgzArchive()
    else:
        raise ValueError(archive_type)

    # for filepath, file in config.files.items():
    #     file_model = File(**file.dict())
    #     data = await file_model.read()
    #     yield filepath, file, data
    #     if data:
    #         archive.write_file(filepath, data)

    config_data = json.dumps(config.dict())
    archive.write_file("config.json", config_data.encode("utf-8"))
    archive.close()
