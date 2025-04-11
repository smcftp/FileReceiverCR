from fastapi import FastAPI
from app.api.endpoints import json_upload
from app.core.config import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION
)

app.include_router(json_upload.router, prefix="/api/v1", tags=["json"]) 