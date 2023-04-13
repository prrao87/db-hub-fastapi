from pydantic import BaseSettings


class Settings(BaseSettings):
    db_service: str
    neo4j_password: str

    class Config:
        env_file = ".env"
