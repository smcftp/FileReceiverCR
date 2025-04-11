from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    API_KEY: str
    PROJECT_NAME: str = "JSON File Receiver"
    VERSION: str = "1.0.0"
    
    # Google Drive credentials
    GOOGLE_CREDENTIALS: str | None = None
    GOOGLE_TOKEN: str | None = None
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    return Settings() 