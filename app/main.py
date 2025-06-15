import os
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from fastapi import APIRouter
import logging
from contextlib import asynccontextmanager

from app.core.config import settings
# Импортируем только json_upload
from app.api.endpoints import json_upload
from app.db.init_db import create_tables # <-- Импортируем функцию

# Настраиваем базовое логирование для приложения
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Создаем директорию для загрузки файлов, если она не существует
os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
os.makedirs(os.path.join(settings.UPLOAD_DIR, "json"), exist_ok=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Код, выполняемый ДО старта приложения
    logger.info("Запуск приложения...")
    logger.info("Проверка и создание таблиц базы данных...")
    # Вызываем создание таблиц
    success = await create_tables()
    if success:
        logger.info("Проверка/создание таблиц завершено успешно.")
    else:
        logger.error("Не удалось проверить/создать таблицы БД. Приложение может работать некорректно.")
    yield
    # Код, выполняемый ПОСЛЕ остановки приложения
    logger.info("Остановка приложения...")

app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan # <-- Используем lifespan
)

# Настройка CORS
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.get("/")
def read_root():
    return {"message": "Welcome to FileReceiverCR API"}


# Удаляем эндпоинт health_check, так как он использует синхронный get_db
# Если он нужен, его нужно будет переписать на асинхронный
# @app.get("/api/health-check")
# def health_check(db: Session = Depends(get_db)):
#     """Проверка работоспособности API и подключения к базе данных"""
#     try:
#         # Попытка выполнить простой запрос
#         db.execute("SELECT 1")
#         return {"status": "ok", "database": "connected"}
#     except Exception as e:
#         return {"status": "error", "database": "error", "message": str(e)}


# Включаем только роутер для JSON upload
api_router = APIRouter()
api_router.include_router(json_upload.router, prefix="/json", tags=["json"])

app.include_router(api_router, prefix="/api")