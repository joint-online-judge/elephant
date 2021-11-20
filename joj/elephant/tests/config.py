from pydantic import BaseSettings


class Settings(BaseSettings):
    s3_host: str = ""
    s3_port: int = 80
    s3_username: str = ""
    s3_password: str = ""

    lakefs_s3_domain: str = "s3.lakefs.example.com"
    lakefs_host: str = ""
    lakefs_port: int = 34766
    lakefs_username: str = "lakefs"
    lakefs_password: str = "lakefs"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
