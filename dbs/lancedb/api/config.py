from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="allow",
    )

    lancedb_dir: str
    api_port: str
    embedding_model_checkpoint: str
    tag: str
