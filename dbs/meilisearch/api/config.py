from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="allow",
    )

    meili_service: str
    meili_master_key: str
    meili_port: int
    meili_url: str
    tag: str
