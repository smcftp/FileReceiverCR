import os
from typing import Optional, List, Union, Dict, Any

from pydantic import Field, AnyHttpUrl, EmailStr, validator
from pydantic_settings import BaseSettings
from functools import lru_cache
import secrets


class Settings(BaseSettings):
    """
    Настройки приложения.
    """
    
    # Общие настройки
    APP_NAME: str = "FileReceiverCR"
    DEBUG: bool = Field(default=False)
    API_V1_STR: str = "/api/v1"
    
    # Директория для временных файлов
    UPLOAD_DIR: str = Field(default="./uploads")
    
    # API безопасность
    API_KEY: str = Field(default_factory=lambda: secrets.token_urlsafe(32))
    
    # JWT токены и безопасность
    SECRET_KEY: str = secrets.token_urlsafe(32)
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8
    
    # Google Drive credentials
    GOOGLE_CREDENTIALS: Optional[str] = None
    GOOGLE_TOKEN: Optional[str] = None
    GOOGLE_CREDENTIALS_PATH: str = "credentials.json"
    GOOGLE_TOKEN_PATH: str = "token.json"
    
    # PostgreSQL settings
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: str = "5432"
    POSTGRES_DB: str = "filereceiverdb"
    
    # Direct database URL (takes priority if set)
    DATABASE_URL: Optional[str] = None
    
    # Additional settings
    SERVER_NAME: str = "FileReceiverCR"
    SERVER_HOST: AnyHttpUrl = "http://localhost:8000"
    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []
    
    PROJECT_NAME: str = "FileReceiverCR"
    
    EMAIL_TEST_USER: EmailStr = "test@example.com"
    FIRST_SUPERUSER: EmailStr = "admin@example.com"
    FIRST_SUPERUSER_PASSWORD: str = "admin"
    USERS_OPEN_REGISTRATION: bool = False
    ALGORITHM: str = "HS256"
    
    # Создаем функцию-свойство для формирования DATABASE_URL
    @property
    def get_database_url(self) -> str:
        """Get the database connection URL"""
        # Используем готовый DATABASE_URL если он задан, иначе формируем из отдельных параметров
        if self.DATABASE_URL:
            return self.DATABASE_URL
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    @validator("BACKEND_CORS_ORIGINS", pre=True)
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # Игнорировать лишние поля в .env


# Создаем экземпляр настроек
settings = Settings()

@lru_cache()
def get_settings() -> Settings:
    return Settings()
    
    #