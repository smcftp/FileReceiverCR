from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base

from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

# Получаем URL подключения к базе данных
DATABASE_URL = settings.DATABASE_URL if settings.DATABASE_URL else settings.get_database_url

# Заменяем postgresql:// на postgresql+asyncpg:// для асинхронных операций
ASYNC_DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")

logger.info(f"Using database connection: {DATABASE_URL}")
logger.info(f"Using async database connection: {ASYNC_DATABASE_URL}")

# Создаем синхронный движок SQLAlchemy
engine = create_engine(DATABASE_URL)

# Создаем асинхронный движок SQLAlchemy
async_engine = create_async_engine(
    ASYNC_DATABASE_URL,
    echo=settings.DEBUG,
    future=True,
)

# Создаем фабрики сессий
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
AsyncSessionLocal = sessionmaker(
    class_=AsyncSession, 
    expire_on_commit=False,
    autocommit=False, 
    autoflush=False, 
    bind=async_engine
)

# Для совместимости с моделями
Base = declarative_base()

# Функция-зависимость для получения соединения с БД
def get_db():
    """
    Возвращает синхронную сессию БД и гарантирует ее закрытие после использования.
    Используется как зависимость в FastAPI.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close() 

# Функция-зависимость для получения асинхронного соединения с БД
async def get_async_db():
    """
    Возвращает асинхронную сессию БД и гарантирует ее закрытие после использования.
    Используется как зависимость в FastAPI.
    """
    async_session = AsyncSessionLocal()
    try:
        yield async_session
    finally:
        await async_session.close() 