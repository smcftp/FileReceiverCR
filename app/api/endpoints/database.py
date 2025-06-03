"""
API эндпоинты для работы с базой данных
"""
import logging
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta

from app.db.database import get_async_db
from app.db.init_db import create_tables, initialize_dim_date

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/initialize", response_model=Dict[str, Any])
async def initialize_database(db: AsyncSession = Depends(get_async_db)):
    """
    Инициализирует базу данных:
    1. Создает все необходимые таблицы
    2. Заполняет таблицу измерений DimDate данными
    """
    try:
        # Создаем таблицы
        tables_created = await create_tables()
        if not tables_created:
            raise HTTPException(status_code=500, detail="Не удалось создать таблицы в БД")
        
        # Инициализируем таблицу DimDate
        current_year = datetime.now().year
        start_date = datetime(current_year, 1, 1).date()
        end_date = datetime(current_year + 1, 12, 31).date()
        
        await initialize_dim_date(db, start_date, end_date)
        
        return {
            "status": "success",
            "message": "БД успешно инициализирована",
            "details": {
                "tables_created": True,
                "dim_date_initialized": True,
                "date_range": {
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat()
                }
            }
        }
    except Exception as e:
        logger.error(f"Ошибка при инициализации БД: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при инициализации БД: {str(e)}") 