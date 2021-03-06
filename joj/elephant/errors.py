class ElephantError(Exception):
    def __init__(self, message: str = ""):
        self.message = message

    def __str__(self) -> str:
        return self.message


class FileSystemError(ElephantError):
    pass


class ArchiveError(FileSystemError):
    pass


class FileSystemSyncError(FileSystemError):
    pass


class FileSystemUndefinedError(FileSystemError):
    pass


class ConfigError(ElephantError):
    pass
