"""
Обработчик файлов типа PaymentsPartners
"""
from typing import Dict, Any, List, Tuple
from app.services.file_processor_factory import register_file_processor


@register_file_processor("PaymentsPartners", "Обработчик платежей партнерам")
def process_payments_partners(records: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Обрабатывает данные из файла типа PaymentsPartners
    
    Args:
        records: Список записей из файла
        
    Returns:
        Tuple[Dict[str, Any], List[Dict[str, Any]]]:
            - Метаданные обработки
            - Обработанные записи для сохранения в БД
    """
    metadata = {
        "processor": "payments_partners_processor",
        "version": "1.0",
        "specific_info": "Обработка платежей партнерам"
    }
    
    processed_records = []
    for record in records:
        # Здесь должна быть логика обработки и трансформации записей
        # Например, проверка обязательных полей, форматирование дат и т.д.
        
        # Пример трансформации
        processed_record = {
            "original_data": record,
            "processed": True,
            "processor_type": "PaymentsPartners",
            # Дополнительные поля после обработки
            "payment_id": record.get("id", ""),
            "partner_name": record.get("partner", ""),
            "amount": record.get("amount", 0),
            "processed_at": record.get("date", "")
        }
        
        processed_records.append(processed_record)
    
    return metadata, processed_records 