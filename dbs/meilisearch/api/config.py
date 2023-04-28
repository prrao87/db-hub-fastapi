from pydantic import BaseSettings


class Settings(BaseSettings):
    meili_service: str
    meili_master_key: str
    meili_port: int
    meili_url: str
    tag: str

    class Config:
        env_file = ".env"
