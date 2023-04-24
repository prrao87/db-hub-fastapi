from pydantic import BaseSettings


class Settings(BaseSettings):
    qdrant_service: str
    qdrant_port: str
    qdrant_host: str
    qdrant_service: str
    api_port = str
    embedding_model_checkpoint: str
    onnx_model_filename: str
    tag: str

    class Config:
        env_file = ".env"
