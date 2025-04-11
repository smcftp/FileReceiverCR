from pydantic import BaseModel
from typing import Dict, Any, List

class JsonUploadResponse(BaseModel):
    status: str
    message: str
    data_size: int
    file_details: List[Dict] 
