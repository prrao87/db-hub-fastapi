from pydantic import BaseSettings


class Settings(BaseSettings):
    weaviate_service: str
    weaviate_port: str
    weaviate_host: str
    weaviate_service: str
    api_port = str
    embedding_model_checkpoint: str
    onnx_model_filename: str
    tag: str

    class Config:
        env_file = ".env"
