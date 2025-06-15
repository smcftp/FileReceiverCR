"""
Модуль для инициализации базы данных
"""
import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import insert, inspect
from datetime import datetime, timedelta
import calendar

from app.db.base_class import Base
from app.db.models_maket import DimDate, Account, DailyAccountBalance
from app.db.database import async_engine

logger = logging.getLogger(__name__)

async def check_tables_exist():
    """
    Проверяет существование необходимых таблиц в базе данных
    Возвращает: True если все таблицы существуют, иначе False
    """
    try:
        async with async_engine.begin() as conn:
            # Получить инспектор для проверки схемы БД
            inspector = await conn.run_sync(inspect)
            
            # Список необходимых таблиц
            required_tables = ['accounts', 'dim_date', 'daily_account_balances']
            existing_tables = await conn.run_sync(lambda sync_conn: inspector.get_table_names())
            
            logger.info(f"Существующие таблицы: {existing_tables}")
            
            # Проверяем, все ли необходимые таблицы существуют
            all_exist = True
            for table in required_tables:
                if table.lower() not in [t.lower() for t in existing_tables]:
                    all_exist = False
                    logger.warning(f"Таблица {table} отсутствует в БД")
            
            if all_exist:
                logger.info("Все необходимые таблицы существуют")
            else:
                logger.warning("Некоторые таблицы отсутствуют и будут созданы")
                
            return all_exist
    except Exception as e:
        logger.error(f"Ошибка при проверке таблиц: {str(e)}")
        return False

async def check_table_structure(table_name, expected_columns):
    """
    Проверяет структуру таблицы на соответствие ожидаемым колонкам
    
    Args:
        table_name: Имя таблицы для проверки
        expected_columns: Список ожидаемых имен колонок
        
    Returns:
        (bool, list): Кортеж (структура_корректна, отсутствующие_колонки)
    """
    try:
        async with async_engine.begin() as conn:
            inspector = await conn.run_sync(inspect)
            
            # Получаем список колонок в таблице
            table_columns = [col['name'] for col in await conn.run_sync(lambda sync_conn: inspector.get_columns(table_name))]
            logger.info(f"Колонки в таблице {table_name}: {table_columns}")
            
            # Проверяем, все ли ожидаемые колонки присутствуют
            missing_columns = [col for col in expected_columns if col.lower() not in [c.lower() for c in table_columns]]
            
            if missing_columns:
                logger.warning(f"В таблице {table_name} отсутствуют колонки: {missing_columns}")
                return False, missing_columns
            
            logger.info(f"Структура таблицы {table_name} соответствует ожидаемой")
            return True, []
            
    except Exception as e:
        logger.error(f"Ошибка при проверке структуры таблицы {table_name}: {str(e)}")
        return False, []

async def force_recreate_tables():
    """
    Принудительно пересоздает все таблицы (удаляет существующие и создает заново)
    ВНИМАНИЕ: Эта операция приведет к потере всех данных в таблицах!
    """
    try:
        async with async_engine.begin() as conn:
            logger.warning("ВНИМАНИЕ: Удаление и пересоздание всех таблиц!")
            # Удаляем существующие таблицы
            await conn.run_sync(Base.metadata.drop_all)
            # Создаем таблицы заново
            await conn.run_sync(Base.metadata.create_all)
            logger.info("Таблицы успешно пересозданы")
        return True
    except Exception as e:
        logger.error(f"Ошибка при пересоздании таблиц: {str(e)}")
        return False

async def create_tables():
    """
    Асинхронное создание недостающих таблиц в базе данных
    """
    try:
        # Проверяем наличие таблиц
        tables_exist = await check_tables_exist()
        
        if not tables_exist:
            # Создаем только недостающие таблицы
            async with async_engine.begin() as conn:
                logger.info("Создание недостающих таблиц в БД...")
                # Создаем таблицы (SQLAlchemy не трогает существующие)
                await conn.run_sync(Base.metadata.create_all)
                logger.info("Таблицы успешно созданы")
        else:
            # Проверяем структуру таблиц
            dim_date_structure_ok, _ = await check_table_structure('dim_date', ['date_id', 'date', 'day', 'month', 'year', 'day_name', 'month_name', 'is_weekend', 'is_holiday', 'quarter'])
            accounts_structure_ok, _ = await check_table_structure('accounts', ['account_id', 'account_name', 'currency'])
            
            if not dim_date_structure_ok or not accounts_structure_ok:
                logger.warning("Обнаружено несоответствие структуры таблиц. Требуется миграция.")
                # Здесь можно добавить код для миграции или предложить ручное вмешательство
                # В данном случае просто логируем предупреждение
            else:
                logger.info("Все таблицы уже существуют и имеют правильную структуру")
            
        return True
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц: {str(e)}")
        return False

async def initialize_dim_date(db: AsyncSession, start_date=None, end_date=None):
    """
    Инициализация таблицы измерений для дат
    
    Args:
        db: Асинхронная сессия БД
        start_date: Начальная дата (по умолчанию текущий год)
        end_date: Конечная дата (по умолчанию текущий год + 1 год)
    """
    try:
        # Определяем диапазон дат
        if not start_date:
            current_year = datetime.now().year
            start_date = datetime(current_year, 1, 1).date()
        
        if not end_date:
            end_date = datetime(start_date.year + 1, 12, 31).date()
        
        # Проверяем, есть ли уже записи в таблице
        result = await db.execute(select(DimDate).limit(1))
        if result.scalar_one_or_none():
            logger.info("Таблица DimDate уже содержит данные")
            return
        
        # Создаем данные для таблицы измерений
        dates = []
        current_date = start_date
        while current_date <= end_date:
            # Форматируем date_id как YYYYMMDD
            date_id = int(current_date.strftime("%Y%m%d"))
            
            # Получаем информацию о дне
            weekday = current_date.weekday()
            day_name = calendar.day_name[weekday]
            month_name = calendar.month_name[current_date.month]
            
            # Определяем, является ли день выходным (0 - понедельник, 6 - воскресенье)
            is_weekend = weekday >= 5  # Суббота и воскресенье
            
            dates.append({
                "date_id": date_id,
                "date": current_date,
                "day": current_date.day,
                "month": current_date.month,
                "year": current_date.year,
                "day_name": day_name,
                "month_name": month_name,
                "is_weekend": is_weekend,
                "is_holiday": False,  # По умолчанию не праздник
                "quarter": (current_date.month - 1) // 3 + 1
            })
            
            # Переходим к следующему дню
            current_date += timedelta(days=1)
        
        # Вставляем данные в таблицу
        if dates:
            await db.execute(insert(DimDate).values(dates))
            await db.commit()
            logger.info(f"Таблица DimDate инициализирована, добавлено {len(dates)} записей")
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Ошибка при инициализации таблицы DimDate: {str(e)}")
        raise 