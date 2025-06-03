from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    ALPHAVANTAGE_API_KEY: str = "demo"
    DATABASE_URL: str = "sqlite:///./data.db"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
