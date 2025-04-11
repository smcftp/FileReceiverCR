from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from typing import Dict, Any
from datetime import datetime
import json

from app.core.config import get_settings
from app.services.google_drive import drive_service
from app.schemas.json_upload import JsonUploadResponse

router = APIRouter()

api_key_header = APIKeyHeader(name="X-API-Key")

async def get_api_key(api_key_header: str = Security(api_key_header)) -> str:
    if api_key_header == get_settings().API_KEY:
        return api_key_header
    raise HTTPException(
        status_code=403,
        detail="Invalid API Key"
    )

@router.post("/upload-json/", response_model=JsonUploadResponse)
async def upload_json(
    json_data: Dict[str, Any],
    api_key: str = Depends(get_api_key)
) -> Dict[str, Any]:
    """
    Upload JSON data to Google Drive
    """
    try:
        # Generate unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"json_upload_{timestamp}.json"
        
        # Upload to Google Drive
        upload_result = drive_service.upload_json(json_data, filename)
        
        return {
            "status": "success",
            "message": "JSON data uploaded to Google Drive successfully",
            "data_size": len(json.dumps(json_data)),
            "file_details": upload_result
        }
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Error processing JSON data: {str(e)}"
        ) 