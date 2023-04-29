from pydantic import BaseSettings


class Settings(BaseSettings):
    neo4j_service: str
    neo4j_url: str
    neo4j_user: str
    neo4j_password: str
    tag: str

    class Config:
        env_file = ".env"
