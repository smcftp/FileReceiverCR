"""
Фабрика обработчиков файлов с динамической регистрацией и поддержкой асинхронных обработчиков
"""
from typing import Dict, Any, List, Tuple, Callable, Optional, Union
from functools import wraps
import inspect


class FileProcessorFactory:
    """
    Фабрика для получения обработчиков различных типов файлов с возможностью
    динамической регистрации обработчиков и поддержкой асинхронных функций
    """
    
    # Словарь для хранения зарегистрированных обработчиков
    _processors = {}
    
    @classmethod
    def register_processor(cls, file_type: str, processor_func: Callable = None, *, description: str = None, is_async: bool = None):
        """
        Регистрирует обработчик для определенного типа файла.
        Может использоваться как декоратор или напрямую.
        Автоматически определяет, является ли функция асинхронной.
        
        Args:
            file_type: Тип файла (например, "PaymentsPartners")
            processor_func: Функция-обработчик (опционально при использовании как декоратор)
            description: Описание обработчика (опционально)
            is_async: Флаг, указывающий, является ли обработчик асинхронным (определяется автоматически, если None)
            
        Returns:
            Callable: Декоратор или зарегистрированная функция
        """
        def decorator(func):
            # Определяем, является ли функция асинхронной
            func_is_async = is_async
            if func_is_async is None:
                func_is_async = inspect.iscoroutinefunction(func)
            
            metadata = {
                "function": func,
                "description": description or func.__doc__ or f"Processor for {file_type}",
                "is_async": func_is_async
            }
            cls._processors[file_type] = metadata
            
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            
            # Добавляем метку для возможности обнаружения
            wrapper._is_file_processor = True
            wrapper._file_type = file_type
            wrapper._is_async = func_is_async
            return wrapper
        
        # Позволяет использовать как @register_processor("Type") так и register_processor("Type", func)
        if processor_func is not None:
            return decorator(processor_func)
        
        return decorator
    
    @classmethod
    def get_processor(cls, file_type: str) -> Tuple[Optional[Callable], bool]:
        """
        Возвращает обработчик для указанного типа файла и флаг асинхронности
        
        Args:
            file_type: Тип файла
            
        Returns:
            Tuple[Optional[Callable], bool]: 
                - Функция-обработчик для данного типа файла или None если тип не поддерживается
                - Флаг, указывающий является ли обработчик асинхронным
        """
        processor_data = cls._processors.get(file_type)
        if not processor_data:
            return None, False
        return processor_data["function"], processor_data["is_async"]
    
    @classmethod
    def is_supported_type(cls, file_type: str) -> bool:
        """
        Проверяет, поддерживается ли указанный тип файла
        
        Args:
            file_type: Тип файла для проверки
            
        Returns:
            bool: True если тип поддерживается, иначе False
        """
        return file_type in cls._processors
    
    @classmethod
    def get_supported_types(cls) -> List[str]:
        """
        Возвращает список поддерживаемых типов файлов
        
        Returns:
            List[str]: Список поддерживаемых типов файлов
        """
        return list(cls._processors.keys())
    
    @classmethod
    def get_processor_info(cls, file_type: str = None) -> Dict[str, Any]:
        """
        Возвращает информацию о зарегистрированных обработчиках.
        Если указан file_type, возвращает информацию только о нем.
        
        Args:
            file_type: Тип файла (опционально)
            
        Returns:
            Dict[str, Any]: Информация об обработчике(ах)
        """
        if file_type:
            if file_type in cls._processors:
                processor_data = cls._processors[file_type]
                return {
                    "type": file_type,
                    "description": processor_data["description"],
                    "is_async": processor_data["is_async"]
                }
            return None
        
        return {
            file_type: {
                "description": data["description"],
                "is_async": data["is_async"]
            }
            for file_type, data in cls._processors.items()
        }


# Создаем функцию-декоратор для удобной регистрации синхронных обработчиков
def register_file_processor(file_type: str, description: str = None):
    """
    Декоратор для регистрации функции-обработчика для определенного типа файла.
    
    Args:
        file_type: Тип файла для обработки
        description: Описание обработчика (опционально)
    
    Returns:
        Callable: Декорированная функция-обработчик
        
    Пример:
        @register_file_processor("PaymentsPartners", "Обработчик платежей партнерам")
        def process_payments_partners(data):
            # логика обработки
            return metadata, processed_records
    """
    return FileProcessorFactory.register_processor(file_type, description=description, is_async=False)


# Создаем функцию-декоратор для удобной регистрации асинхронных обработчиков
def register_async_file_processor(file_type: str, description: str = None):
    """
    Декоратор для регистрации асинхронной функции-обработчика для определенного типа файла.
    
    Args:
        file_type: Тип файла для обработки
        description: Описание обработчика (опционально)
    
    Returns:
        Callable: Декорированная асинхронная функция-обработчик
        
    Пример:
        @register_async_file_processor("PaymentsPartners", "Обработчик платежей партнерам")
        async def process_payments_partners(data):
            # асинхронная логика обработки
            return metadata, processed_records
    """
    return FileProcessorFactory.register_processor(file_type, description=description, is_async=True)


# Создаем экземпляр фабрики для обратной совместимости
file_processor_factory = FileProcessorFactory() 