from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from typing import Dict, Any, List
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
    request: Request,
    api_key: str = Depends(get_api_key)
) -> Dict[str, Any]:
    """
    Upload JSON data to Google Drive.
    Accepts any valid JSON structure (object, array, etc.).
    """
    try:
        # Пытаемся извлечь JSON данные из запроса
        try:
            json_data = await request.json()
        except Exception as json_error:
            # Если не получается, пробуем получить тело запроса как строку
            body = await request.body()
            try:
                # Пытаемся разобрать как JSON
                json_data = json.loads(body.decode())
            except:
                # Если все не получается, просто сохраняем как строку
                json_data = {"raw_data": str(body)}
        
        # Generate unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"json_upload_{timestamp}.json"
        
        # Upload to Google Drive
        upload_result = drive_service.upload_json(json_data, filename)
        
        # Определяем размер данных безопасно
        try:
            if isinstance(json_data, (dict, list)):
                data_size = len(json.dumps(json_data))
            elif isinstance(json_data, str):
                data_size = len(json_data)
            else:
                data_size = len(str(json_data))
        except:
            data_size = 0
        
        return {
            "status": "success",
            "message": "JSON data uploaded to Google Drive successfully",
            "data_size": data_size,
            "file_details": upload_result
        }
    except Exception as e:
        # Логируем для отладки
        import traceback
        error_trace = traceback.format_exc()
        print(f"Error in upload_json endpoint: {str(e)}\n{error_trace}")
        
        # Даже при ошибке пытаемся сохранить данные
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"error_upload_{timestamp}.txt"
            
            # Пытаемся сохранить ошибку и трейс
            error_data = {
                "error": str(e),
                "traceback": error_trace,
                "timestamp": timestamp
            }
            
            upload_result = drive_service.upload_json(error_data, filename)
            
            return {
                "status": "partial_success",
                "message": f"Error occurred but saved error details: {str(e)}",
                "data_size": 0,
                "file_details": upload_result
            }
        except:
            # Если даже это не удалось, возвращаем информацию об ошибке
            dummy_result = {
                "file_id": "error",
                "filename": "error.txt",
                "web_link": "#" 
            }
            
            return {
                "status": "error",
                "message": f"Failed to process request: {str(e)}",
                "data_size": 0,
                "file_details": dummy_result
            } 
