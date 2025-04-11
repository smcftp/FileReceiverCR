from pydantic import BaseModel
from typing import Dict, Any

class JsonUploadResponse(BaseModel):
    status: str
    message: str
    data_size: int
    file_details: Dict[str, str] 