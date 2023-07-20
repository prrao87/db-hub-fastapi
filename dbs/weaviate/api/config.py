from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="allow",
    )

    weaviate_service: str
    weaviate_port: str
    weaviate_host: str
    weaviate_service: str
    api_port: int
    embedding_model_checkpoint: str
    onnx_model_filename: str
    tag: str
