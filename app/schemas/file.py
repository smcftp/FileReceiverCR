from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field


class FileBase(BaseModel):
    """Базовая схема файла с общими атрибутами"""
    filename: str
    original_filename: str
    file_path: str
    file_size: int
    file_type: str
    description: Optional[str] = None
    is_active: bool = True


class FileCreate(FileBase):
    """Схема для создания файла"""
    pass


class FileUpdate(BaseModel):
    """Схема для обновления файла"""
    filename: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None


class FileInDBBase(FileBase):
    """Базовая схема для отображения файла из БД"""
    id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        orm_mode = True


class File(FileInDBBase):
    """Схема файла для возврата клиенту"""
    pass 