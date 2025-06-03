from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional


class FileDetails(BaseModel):
    """Детали загруженного файла в Google Drive"""
    file_id: str
    filename: str
    web_link: str


class JsonFileInfo(BaseModel):
    """Информация о загруженном файле"""
    original_filename: str
    saved_filename: Optional[str] = None
    file_path: Optional[str] = None
    content_type: str
    size_bytes: int = 0
    google_drive: Optional[Dict[str, Any]] = None
    drive_upload_status: Optional[str] = None


class JsonMetadata(BaseModel):
    """Метаданные извлеченные из JSON для БД"""
    file_type: str
    processor: str = ""
    version: str = "1.0"
    record_count: int = 0
    processed_at: str = ""


class ProcessedRecord(BaseModel):
    """Обработанная запись из JSON файла"""
    original_data: Dict[str, Any] = Field(default_factory=dict)
    processed: bool = True
    processor_type: str = ""


class JsonExtractedData(BaseModel):
    """Данные извлеченные из JSON файла"""
    metadata: JsonMetadata = Field(default_factory=JsonMetadata)
    records: List[Dict[str, Any]] = Field(default_factory=list)


class DbReadyData(BaseModel):
    """Данные подготовленные для загрузки в БД"""
    metadata: JsonMetadata = Field(default_factory=JsonMetadata)
    records: List[Dict[str, Any]] = Field(default_factory=list)
    file_path: Optional[str] = None
    error: Optional[str] = None


class JsonUploadResponse(BaseModel):
    """Ответ API при загрузке JSON"""
    status: str = Field(description="Status of the operation: success, partial_success, or error")
    message: str = Field(description="Message describing the result of the operation")
    data_size: int = Field(default=0, description="Size of the uploaded data in bytes")
    file: Optional[JsonFileInfo] = None
    db_ready_data: Optional[JsonExtractedData] = None
