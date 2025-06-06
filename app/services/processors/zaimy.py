"""
Обработчик файлов типа Zaimy
"""

from app.services.file_processor_factory import register_async_file_processor

@register_async_file_processor("Zaimy", "Обработчик займов")
async def process_zaimy(data, db_session=None):
    """
    Обработка займов из JSON файла.
    data: dict или list — данные из JSON
    db_session: опциональная сессия БД для ETL
    """
    # Пример обработки
    records = data.get("Zaimy", []) if isinstance(data, dict) else data
    processed_records = []
    for record in records:
        # Здесь ваша логика обработки одной записи
        processed_records.append(record)  # или трансформируйте как нужно

    # Верните метаданные и обработанные записи
    metadata = {
        "file_type": "Zaimy",
        "record_count": len(processed_records),
        "success": True
    }
    return metadata, processed_records
