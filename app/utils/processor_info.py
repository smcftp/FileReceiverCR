"""
Утилита для вывода информации о зарегистрированных обработчиках
"""

import json
from app.services.file_processor_factory import FileProcessorFactory
# Импорт обработчиков для их регистрации (при запуске скрипта напрямую)
from app.services.processors import process_payments_partners, process_bank_accounts


def print_processor_info():
    """Выводит информацию о зарегистрированных обработчиках"""
    
    # Получаем список всех поддерживаемых типов
    supported_types = FileProcessorFactory.get_supported_types()
    
    print(f"Зарегистрированные обработчики файлов ({len(supported_types)}):")
    print("-" * 50)
    
    # Получаем информацию обо всех обработчиках
    processors_info = FileProcessorFactory.get_processor_info()
    
    # Форматируем и выводим информацию
    print(json.dumps(processors_info, indent=2, ensure_ascii=False))
    
    print("\nПример вызова обработчика:")
    print("-" * 50)
    
    # Пример тестового вызова одного из обработчиков (для демонстрации)
    if supported_types:
        test_type = supported_types[0]
        print(f"Тип: {test_type}")
        
        processor = FileProcessorFactory.get_processor(test_type)
        if processor:
            print(f"Функция: {processor.__name__}")
            
            # Генерируем тестовые данные
            test_data = [
                {"id": "1", "name": "Test", "value": 100}
            ]
            
            # Вызываем обработчик
            try:
                metadata, records = processor(test_data)
                print(f"Результат обработки: {len(records)} записей")
                print(f"Метаданные: {json.dumps(metadata, indent=2, ensure_ascii=False)}")
                print(f"Первая запись: {json.dumps(records[0], indent=2, ensure_ascii=False)}")
            except Exception as e:
                print(f"Ошибка при вызове обработчика: {e}")


if __name__ == "__main__":
    # Если скрипт запускается напрямую, выводим информацию
    print_processor_info() 