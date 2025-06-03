from typing import Any
from sqlalchemy.ext.declarative import as_declarative, declared_attr, declarative_base
from sqlalchemy.orm import DeclarativeBase


# class Base(DeclarativeBase):
#     """
#     Базовый класс для моделей SQLAlchemy.
#     Обеспечивает автоматическое имя таблицы и общие методы.
#     """
#     id: Any
#     __name__: str
    
#     # Генерирует имя таблицы автоматически из имени класса модели
#     @declared_attr
#     def __tablename__(cls) -> str:
#         return cls.__name__.lower()

# Здесь должно быть определение Base
Base = declarative_base() 