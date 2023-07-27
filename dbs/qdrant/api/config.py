from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="allow",
    )

    qdrant_service: str
    qdrant_port: str
    qdrant_host: str
    qdrant_service: str
    api_port: str
    embedding_model_checkpoint: str
    onnx_model_filename: str
    tag: str
