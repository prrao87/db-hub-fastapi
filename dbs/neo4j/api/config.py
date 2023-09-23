from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="allow",
    )

    neo4j_service: str
    neo4j_url: str
    neo4j_user: str
    neo4j_password: str
    tag: str
