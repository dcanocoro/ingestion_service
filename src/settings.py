from pydantic_settings import BaseSettings, SettingsConfigDict
import logging

# Create a basic logger for settings (before our main logging config is loaded)
logger = logging.getLogger("settings")

class Settings(BaseSettings):
    ALPHAVANTAGE_API_KEY: str = "demo"
    DATABASE_URL: str = "sqlite:///./data.db"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Don't log the API key for security reasons
        logger.info(f"Settings loaded - Database URL: {self.DATABASE_URL}")
        logger.info("API key loaded from configuration")

settings = Settings()
