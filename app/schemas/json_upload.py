from pydantic import BaseModel
from typing import Dict, Any, List, Optional

class JsonUploadResponse(BaseModel):
    status: str = Field(description="Status of the operation: success, partial_success, or error")
    message: str = Field(description="Message describing the result of the operation")
    data_size: int = Field(default=0, description="Size of the uploaded data in bytes")
    file_details: Dict[str, str] = Field(description="Details about the uploaded file")
