from fastapi import APIRouter, Depends, HTTPException, status, Security, BackgroundTasks, Body # Добавляем Body
from fastapi.security.api_key import APIKeyHeader
from fastapi.responses import JSONResponse
from typing import Dict, Any, Optional, List
import json
import os
import logging
import traceback
from datetime import datetime
from pydantic import BaseModel, Field # Импортируем Pydantic

from app.core.config import get_settings
# Убедимся, что verify_api_key работает с заголовком или используем get_api_key
from app.core.security import verify_api_key # Если verify_api_key читает заголовок
# Или:
# from app.core.security import get_api_key # Если get_api_key это зависимость для заголовка

from app.services.google_drive import drive_service
from app.services.json_processor import json_processor, JsonProcessorException
from app.services.file_processor_factory import file_processor_factory
# Импортируем схемы ответа (они могут остаться прежними или потребовать адаптации)
from app.schemas.json_upload import JsonUploadResponse, JsonExtractedData, JsonMetadata, JsonFileInfo
from app.db.database import get_async_db
from sqlalchemy.ext.asyncio import AsyncSession

# Настройка логирования (без изменений)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

router = APIRouter()

# --- ИЗМЕНЕНО: Используем APIKeyHeader для зависимости ---
api_key_header_scheme = APIKeyHeader(name="X-API-Key", auto_error=False) # auto_error=False для своей обработки

async def get_api_key_dependency(api_key: Optional[str] = Security(api_key_header_scheme)):
    """Зависимость для проверки API ключа из заголовка X-API-Key."""
    if api_key and verify_api_key(api_key): # Вызываем вашу функцию проверки
        logger.debug("API ключ валиден.")
        return api_key
    logger.warning(f"Попытка доступа с невалидным или отсутствующим API ключом: {api_key[:5] if api_key else 'None'}...")
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Invalid or missing API Key"
    )
# --- Конец изменений зависимости ---


# --- ИЗМЕНЕНО: Pydantic модель для тела запроса ---
class JsonUploadPayload(BaseModel):
    description: Optional[str] = None
    BankAccounts: Optional[List[Dict[str, Any]]] = Field(None, alias="BankAccounts")
    Zaimy: Optional[List[Dict[str, Any]]] = Field(None, alias="Zaimy")
    # Можно добавить другие ожидаемые ключи верхнего уровня из JSON
    # ...

    # Если структура JSON может сильно варьироваться, можно использовать:
    # data: Dict[str, Any] = Field(..., alias="data") # Если все данные под ключом "data"
    # Или разрешить любые поля:
    class Config:
        extra = 'allow' # Позволяет принимать поля, не описанные в модели
# --- Конец Pydantic модели ---


# Функция фоновой задачи (без изменений, но проверим передачу данных)
def _upload_to_drive_background(json_data_payload: Dict[str, Any], filename: str, description: Optional[str]):
    """Функция для выполнения загрузки на Google Drive в фоне."""
    # Получаем логгер внутри задачи для корректного отображения имени модуля
    task_logger = logging.getLogger(f"{__name__}._upload_to_drive_background")
    task_logger.info(f"Начинается фоновая задача по загрузке файла '{filename}' (Описание: '{description if description else 'N/A'}') на Google Drive.")
    try:
        task_logger.debug(f"Попытка загрузки JSON на Google Drive: {json_data_payload.keys()} для файла {filename}")
        result = drive_service.upload_json(json_data_payload, filename=filename, description=description)

        if result.get('file_id') and result.get('file_id') not in ('error', 'local_only'):
            task_logger.info(f"✅ Успешно завершена загрузка файла '{filename}' на Google Drive. "
                             f"ID файла: {result.get('file_id')}, Ссылка: {result.get('web_link', 'N/A')}")
        else:
            error_details = result.get('error') or result.get('web_link')
            task_logger.error(f"❌ Ошибка загрузки файла '{filename}' на Google Drive. "
                              f"Результат: {result}. Детали ошибки: {error_details}")
    except Exception as e:
        task_logger.exception(f"Произошла непредвиденная ошибка при выполнении фоновой задачи для файла '{filename}': {e}")
        # Использование task_logger.exception() автоматически включает информацию о трассировке стека
    finally:
        task_logger.info(f"Фоновая задача для файла '{filename}' завершена.")


@router.post("/upload", response_model=JsonUploadResponse)
async def upload_json_body(
    *, # Запрещает передачу параметров как query string, только тело запроса
    background_tasks: BackgroundTasks,
    # --- ИЗМЕНЕНО: Получаем тело запроса через Pydantic модель ---
    payload: JsonUploadPayload = Body(...),
    # --- ИЗМЕНЕНО: Используем зависимость для проверки ключа из заголовка ---
    api_key: str = Depends(get_api_key_dependency),
    db: AsyncSession = Depends(get_async_db)
):
    """
    Загрузка JSON данных из тела запроса, их обработка и подготовка к загрузке в БД.
    Для типа файла BankAccounts выполняется процесс ETL с сохранением данных в базу.
    """
    logger.info(f"Запрос на загрузку JSON из тела запроса.")
    logger.info(f"Описание: {payload.description}, загрузка на Google Drive: всегда True")

    # Всегда формируем json_data из всех пользовательских полей, кроме служебных
    json_data = payload.dict(exclude={'description'}, exclude_unset=True)
    if not json_data:
        logger.warning("В теле запроса не найдено ни одного пользовательского поля для загрузки.")

    file_type_for_naming = list(json_data.keys())[0] if json_data else "data"
    original_filename_mock = f"{file_type_for_naming}_upload.json"

    try:
        encoded_payload = json.dumps(payload.dict(), ensure_ascii=False).encode('utf-8')
        data_size_bytes = len(encoded_payload)
        logger.info(f"Размер полученных JSON данных: {data_size_bytes} байт")
    except Exception:
        data_size_bytes = 0
        logger.warning("Не удалось определить размер JSON данных.")

    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{file_type_for_naming}_{timestamp}.json"
        file_type = file_type_for_naming

        # Обрабатываем данные через JsonProcessor
        try:
            db_session_param = db # Всегда передаем сессию БД
            file_path, metadata, processed_records = await json_processor.process_json_data(
                json_data,
                save_file=True,
                db_session=db_session_param
            )
            if file_type == "BankAccounts":
                etl_result = metadata.get("etl_result", {})
                if etl_result and "error" not in etl_result:
                    logger.info(f"ETL операции завершены успешно: ...")
                elif "error" in etl_result:
                    logger.warning(f"ETL операции выполнены с ошибками: {etl_result['error']}")
            logger.info(f"JSON данные успешно обработаны, получено {len(processed_records)} записей")
        except JsonProcessorException as e:
            if "Неподдерживаемый тип файла" in str(e) or "Нет обработчика для типа файла" in str(e) or "Неверная структура JSON" in str(e):
                supported_types = file_processor_factory.get_supported_types()
                error_message = f"Ошибка обработки JSON: {str(e)}"
                logger.warning(error_message)
                return JSONResponse(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    content={
                        "status": "error", "message": error_message,
                        "supported_file_types": supported_types,
                        "expected_structure": "{'ТИП_ФАЙЛА': [...]}",
                        "data_size": data_size_bytes,
                        "file": None,
                        "db_ready_data": None
                    }
                )
            else:
                logger.error(f"Ошибка обработки JSON: {e}")
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Ошибка обработки JSON: {str(e)}")

        # --- Google Drive: всегда True ---
        drive_status = "skipped"
        response_message = "JSON данные успешно обработаны и подготовлены к загрузке в БД."
        response_status = "success"

        # Всегда True
        if drive_service.service:
            logger.info(f"Планирование фоновой загрузки '{filename}' на Google Drive...")
            background_tasks.add_task(
                    _upload_to_drive_background,
                json_data_payload=json_data.copy(),
                    filename=filename,
                description=payload.description
            )
            drive_status = "pending"
            response_message += " Загрузка на Google Drive запущена в фоновом режиме."
        else:
            logger.warning("Сервис Google Drive не инициализирован...")
            drive_status = "skipped_init_error"
            response_status = "success_with_warnings"
            response_message += " Загрузка на Google Drive не была запущена..."

        file_info = JsonFileInfo(
            original_filename=original_filename_mock,
            saved_filename=os.path.basename(file_path) if file_path else filename,
            file_path=file_path,
            content_type="application/json",
            size_bytes=data_size_bytes,
            google_drive=None,
            drive_upload_status=drive_status
        )
        json_metadata = JsonMetadata(
            file_type=metadata.get("file_type", file_type),
            record_count=metadata.get("record_count", 0),
            processed_at=metadata.get("processed_at", ""),
            **{k: v for k, v in metadata.items() if k not in ["file_type", "record_count", "processed_at"]}
        )
        response_data = JsonUploadResponse(
            status=response_status,
            message=response_message,
            data_size=data_size_bytes,
            file=file_info,
            db_ready_data=JsonExtractedData(
                metadata=json_metadata,
                records=processed_records
            )
        )
        logger.info(f"Ответ отправлен клиенту со статусом: {response_status}")
        return response_data
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обработке запроса: {str(e)}")
        logger.error(traceback.format_exc())
        error_file_info = {
            "original_filename": original_filename_mock if 'original_filename_mock' in locals() else None,
             "content_type":"application/json"
        }
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status": "error",
                "message": f"Внутренняя ошибка сервера: {str(e)}",
                "data_size": data_size_bytes if 'data_size_bytes' in locals() else 0,
                "file": error_file_info,
                "db_ready_data": None
            }
        )
