"""
Модуль для обработки JSON-файлов
"""
import json
import os
import uuid
import logging
import aiofiles
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from app.services.file_processor_factory import file_processor_factory
# Импортируем все обработчики для их авто-регистрации
import app.services.processors

# Настройка базового логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class JsonProcessorException(Exception):
    """Исключение для обработки ошибок в модуле JsonProcessor"""
    pass


class JsonProcessor:
    """Класс для обработки JSON файлов и подготовки данных для сохранения в БД"""
    
    def __init__(self, save_raw_files: bool = True, save_directory: str = "./uploads/json"):
        """
        Инициализация обработчика JSON
        
        Args:
            save_raw_files: Нужно ли сохранять исходные JSON файлы
            save_directory: Директория для сохранения файлов
        """
        self.save_raw_files = save_raw_files
        self.save_directory = save_directory
        os.makedirs(self.save_directory, exist_ok=True)
        # Используем фабрику для получения списка поддерживаемых типов
        self.supported_file_types = file_processor_factory.get_supported_types()
        logger.info(f"JsonProcessor инициализирован. Директория: {self.save_directory}, Сохранение файлов: {self.save_raw_files}")
        logger.info(f"Поддерживаемые типы файлов: {self.supported_file_types}")
    
    async def save_json_file(self, json_data: Dict[str, Any], filename: Optional[str] = None) -> str:
        """
        Сохраняет JSON данные в файл
        
        Args:
            json_data: Данные JSON для сохранения
            filename: Опциональное имя файла. Если не указано, будет сгенерировано
            
        Returns:
            str: Путь к сохраненному файлу
        
        Raises:
            JsonProcessorException: Если произошла ошибка при сохранении
        """
        try:
            if not os.path.exists(self.save_directory):
                os.makedirs(self.save_directory, exist_ok=True)
                
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"json_{timestamp}_{uuid.uuid4().hex[:8]}.json"
                
            file_path = os.path.join(self.save_directory, filename)
            
            # Асинхронная запись в файл с использованием aiofiles
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as file:
                json_string = json.dumps(json_data, ensure_ascii=False, indent=2)
                await file.write(json_string)
                
            logger.info(f"JSON данные успешно сохранены в файл: {file_path}")
            return file_path
            
        except Exception as e:
            error_msg = f"Ошибка при сохранении JSON в файл: {str(e)}"
            logger.error(error_msg)
            raise JsonProcessorException(error_msg)
    
    async def load_json_from_file(self, file_path: str) -> Dict[str, Any]:
        """
        Асинхронно загружает JSON из файла
        
        Args:
            file_path: Путь к файлу JSON
            
        Returns:
            Dict[str, Any]: Загруженные данные
            
        Raises:
            JsonProcessorException: Если файл не существует или произошла ошибка при чтении
        """
        logger.info(f"Асинхронная загрузка JSON из файла: {file_path}")
        if not os.path.exists(file_path):
            logger.error(f"Файл не найден: {file_path}")
            raise JsonProcessorException(f"Файл не найден: {file_path}")
        
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                data = json.loads(content)
            logger.info(f"JSON успешно загружен из {file_path} (асинхронно)")
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON в файле {file_path}: {e}")
            raise JsonProcessorException(f"Ошибка при декодировании JSON: {str(e)}")
        except Exception as e:
            logger.error(f"Ошибка чтения файла {file_path}: {e}")
            raise JsonProcessorException(f"Ошибка при чтении файла: {str(e)}")
    
    def validate_json_structure(self, json_data: Dict[str, Any]) -> Tuple[bool, str, List]:
        """
        Проверяет структуру JSON на соответствие требованиям
        
        Args:
            json_data: Данные для проверки
            
        Returns:
            Tuple[bool, str, List]: 
                - True если структура верна, иначе False
                - Тип файла (ключ верхнего уровня)
                - Список записей из файла
        """
        logger.info("Начало валидации структуры JSON")
        # Проверяем, что JSON - это словарь
        if not isinstance(json_data, dict):
            logger.warning("Ошибка валидации: JSON не является словарем")
            return False, "", []
        
        # Проверяем, что в JSON есть только один ключ верхнего уровня (тип файла)
        if len(json_data) != 1:
            logger.warning(f"Ошибка валидации: Ожидается один ключ верхнего уровня, получено: {len(json_data)}")
            return False, "", []
        
        # Получаем тип файла (единственный ключ верхнего уровня)
        file_type = list(json_data.keys())[0]
        logger.info(f"Обнаружен тип файла: {file_type}")
        
        # Проверяем, поддерживается ли такой тип файла
        if not file_processor_factory.is_supported_type(file_type):
            logger.warning(f"Ошибка валидации: Неподдерживаемый тип файла '{file_type}'")
            return False, file_type, []
        
        # Получаем список записей
        records = json_data[file_type]
        
        # Проверяем, что записи представлены в виде списка словарей
        if not isinstance(records, list):
            logger.warning(f"Ошибка валидации: Записи для типа '{file_type}' не являются списком")
            return False, file_type, []
        
        # Проверяем, что каждая запись - это словарь
        for i, record in enumerate(records):
            if not isinstance(record, dict):
                logger.warning(f"Ошибка валидации: Запись {i} для типа '{file_type}' не является словарем")
                return False, file_type, []
        
        logger.info(f"Валидация структуры JSON для типа '{file_type}' прошла успешно. Найдено записей: {len(records)}")
        return True, file_type, records
    
    async def process_json_data(self, json_data: Dict[str, Any], 
                          save_file: bool = True,
                          db_session = None) -> Tuple[str, Dict[str, Any], List[Dict[str, Any]]]:
        """
        Асинхронная комплексная обработка JSON данных: сохранение, валидация, извлечение
        
        Args:
            json_data: JSON данные для обработки
            save_file: Сохранять ли файл на диск
            db_session: Опциональная сессия БД для ETL операций (передается обработчику)
            
        Returns:
            Tuple[str, Dict[str, Any], List[Dict[str, Any]]]:
                - Путь к сохраненному файлу (или пустая строка, если файл не сохранялся)
                - Метаданные для главной таблицы
                - Список записей для деталей
                
        Raises:
            JsonProcessorException: Если произошла ошибка при обработке
        """
        file_path = ""
        logger.info("Начало асинхронной обработки JSON данных")
        
        # Сохраняем JSON на диск, если нужно
        if save_file and self.save_raw_files:
            try:
                file_path = await self.save_json_file(json_data)
            except JsonProcessorException as e:
                logger.error(f"Не удалось сохранить исходный JSON (асинхронно): {e}")
                # Можно решить, прерывать ли выполнение
                # raise e 
        
        # Валидируем структуру JSON (синхронно)
        is_valid, file_type, records = self.validate_json_structure(json_data)
        if not is_valid:
            error_msg = f"Неверная структура JSON. Ожидается {{{{'ТИП_ФАЙЛА': [...]}}}}. Поддерживаемые типы: {self.supported_file_types}"
            logger.error(error_msg)
            raise JsonProcessorException(error_msg)
        
        # Создаем базовые метаданные (синхронно)
        metadata = {
            "file_type": file_type,
            "record_count": len(records),
            "processed_at": datetime.now().isoformat()
        }
        logger.info(f"Базовые метаданные созданы: {metadata}")
        
        # Получаем нужный обработчик из фабрики
        processor, is_async = file_processor_factory.get_processor(file_type)
        if not processor:
            error_msg = f"Нет обработчика для типа файла: {file_type}"
            logger.error(error_msg)
            raise JsonProcessorException(error_msg)
            
        logger.info(f"Выбран обработчик для типа файла: {file_type} (асинхронный: {is_async})")
        
        # Обрабатываем данные соответствующим процессором
        try:
            if is_async:
                # Если процессор асинхронный, используем await
                if db_session and file_type == "BankAccounts":
                    # Передаем сессию БД для процессора BankAccounts
                    logger.info("Передача сессии БД в процессор BankAccounts для ETL операций")
                    processor_metadata, processed_records = await processor(records, db_session=db_session)
                elif db_session and file_type == "Zaimy":
                    # Передаем сессию БД для процессора Zaimy
                    logger.info("Передача сессии БД в процессор Zaimy для ETL операций")
                    processor_metadata, processed_records = await processor(records, db_session=db_session)
                else:
                    processor_metadata, processed_records = await processor(records)
                logger.info(f"Данные успешно обработаны асинхронным процессором для типа '{file_type}'. Получено записей: {len(processed_records)}")
            else:
                # Если процессор синхронный, вызываем напрямую
                processor_metadata, processed_records = processor(records)
                logger.info(f"Данные успешно обработаны синхронным процессором для типа '{file_type}'. Получено записей: {len(processed_records)}")
        except Exception as e:
            logger.error(f"Ошибка при обработке данных процессором для типа '{file_type}': {e}", exc_info=True)
            raise JsonProcessorException(f"Ошибка в обработчике для типа '{file_type}': {e}")
        
        # Объединяем метаданные (синхронно)
        metadata.update(processor_metadata)
        logger.info(f"Итоговые метаданные: {metadata}")
        
        logger.info("Асинхронная обработка JSON данных успешно завершена.")
        return file_path, metadata, processed_records


# Создаем экземпляр для импорта в другие модули
json_processor = JsonProcessor() 