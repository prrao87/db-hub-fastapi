from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="allow",
    )

    elastic_service: str
    elastic_user: str
    elastic_password: str
    elastic_url: str
    elastic_port: int
    elastic_index_alias: str
    tag: str

