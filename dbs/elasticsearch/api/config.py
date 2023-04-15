from pydantic import BaseSettings


class Settings(BaseSettings):
    elastic_service: str
    elastic_user: str
    elastic_password: str
    elastic_url: str
    elastic_port: int

    class Config:
        env_file = ".env"
