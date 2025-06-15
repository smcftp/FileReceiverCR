import json
import os
from typing import Dict, Any, Optional
from io import BytesIO
import tempfile
from datetime import datetime
import logging

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
from google.oauth2.credentials import Credentials
# No longer need google_auth_oauthlib.flow for interactive auth
# from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from json.decoder import JSONDecodeError # Import JSONDecodeError

from app.core.config import settings # Assuming your settings are correctly loaded

logger = logging.getLogger(__name__)

class GoogleDriveService:
    """Сервис для работы с Google Drive, инициализируемый из переменных окружения."""

    # Keep scopes as they are needed by the Credentials object
    SCOPES = ['https://www.googleapis.com/auth/drive.file']

    def __init__(self, config: settings):
        """Инициализация сервиса с использованием настроек"""
        self.creds = None
        self.service = None
        self.config = config
        self.initialize_service()

    def initialize_service(self) -> None:
        """
        Initialize Google Drive service using credentials/token from config variables.
        This version does NOT use local files (credentials.json, token.json)
        and does NOT perform interactive browser authorization.
        It relies solely on GOOGLE_TOKEN variable containing authorized user info JSON.
        """
        token_str = self.config.GOOGLE_TOKEN
        # creds_str = self.config.GOOGLE_CREDENTIALS # Not strictly needed for initialization from token
        logger.info("Инициализация Google Drive из переменных окружения.")

        if not token_str:
            logger.warning("Переменная GOOGLE_TOKEN не найдена или пуста в настройках. Сервис Google Drive не инициализирован.")
            self.creds = None
            self.service = None
            return # Cannot proceed without a token string

        try:
            # Attempt to load credentials directly from the GOOGLE_TOKEN string
            logger.debug("Попытка декодировать GOOGLE_TOKEN как JSON.")
            token_info = json.loads(token_str)
            logger.debug("JSON из GOOGLE_TOKEN успешно декодирован.")

            # This creates Credentials from the dictionary (should contain token, refresh_token, client_id, client_secret, etc.)
            self.creds = Credentials.from_authorized_user_info(token_info, self.SCOPES)
            logger.info("Креды Google успешно загружены из переменной GOOGLE_TOKEN.")

            # Check if creds are valid or refresh if necessary (requires a valid refresh_token in GOOGLE_TOKEN)
            if self.creds.expired and self.creds.refresh_token:
                 logger.info("Токен истек, пытаемся обновить с помощью refresh_token...")
                 try:
                     self.creds.refresh(Request())
                     logger.info("Токен успешно обновлен.")
                     # IMPORTANT: If refreshing works, the token_info changes (new access token).
                     # In a production system using only ENV vars, you might want to
                     # log this new token JSON so the user can update their .env file,
                     # or find a way to dynamically update the running config if possible.
                     # For this example, we'll just log it as a suggestion.
                     logger.info(f"Новый токен получен после обновления. Возможно, потребуется обновить переменную GOOGLE_TOKEN в .env: {self.creds.to_json()}")
                 except Exception as refresh_error:
                     logger.warning(f"Не удалось обновить токен с помощью refresh_token: {refresh_error}. Возможно, refresh_token отсутствует, невалиден или истек. Сервис Google Drive не может быть полностью инициализирован.")
                     self.creds = None # Mark credentials as invalid if refresh fails
            elif not self.creds.valid:
                 logger.warning("Токен из GOOGLE_TOKEN невалиден и не имеет refresh_token, или refresh_token не сработал. Сервис Google Drive не может быть полностью инициализирован.")
                 self.creds = None


        except JSONDecodeError as json_error:
            logger.error(f"Ошибка декодирования JSON для переменной GOOGLE_TOKEN: {json_error}. Убедитесь, что GOOGLE_TOKEN содержит корректный JSON-строку авторизованного пользователя (включая 'token', 'refresh_token', 'client_id', 'client_secret', 'token_uri').", exc_info=True)
            self.creds = None
        except Exception as e:
            logger.error(f"Неожиданная ошибка при инициализации Google Drive из GOOGLE_TOKEN: {e}", exc_info=True)
            self.creds = None

        # Final check and build service
        if self.creds and self.creds.valid:
             try:
                 self.service = build('drive', 'v3', credentials=self.creds)
                 logger.info("Клиент Google Drive API успешно создан.")
             except Exception as build_error:
                 logger.error(f"Не удалось создать клиент Google Drive API: {build_error}", exc_info=True)
                 self.service = None
                 logger.warning("Сервис Google Drive не инициализирован из-за ошибки создания клиента API.")
        else:
             logger.warning("Сервис Google Drive не инициализирован из-за отсутствия валидных учетных данных после попытки загрузки из GOOGLE_TOKEN.")
             self.service = None

    def upload_json(self, json_data: Any, filename: str, description: Optional[str]=None) -> Dict[str, str]:
        """
        Upload JSON data to Google Drive

        Args:
            json_data: Any JSON-serializable data (dict, list, etc.)
            filename: Name for the file on Google Drive
            description: Optional description for the file

        Returns:
            dict: File metadata from Google Drive {file_id, filename, web_link}
                  or {'file_id': 'local_only', ...} if service not initialized
                  or {'file_id': 'error', ...} if upload failed
        """
        logger.info(f"Попытка загрузки файла '{filename}' на Google Drive.")
        if not self.service:
            logger.warning("Сервис Google Drive не инициализирован. Загрузка невозможна.")
            return {
                "file_id": "service_not_initialized", # Changed from local_only to be more accurate
                "filename": filename,
                "web_link": "Service not initialized"
            }

        try:
            try:
                # Added sort_keys=True for consistent output, ensure_ascii=False is good
                json_bytes = json.dumps(json_data, indent=2, sort_keys=True, ensure_ascii=False).encode('utf-8')
            except (TypeError, ValueError) as serialization_error:
                logger.warning(f"Ошибка сериализации данных в JSON для файла '{filename}': {serialization_error}. Пытаемся сохранить как строку.")
                # Fallback to string representation - this might not be ideal depending on data
                try:
                    json_bytes = json.dumps({"raw_data_str": str(json_data), "serialization_error": str(serialization_error)}, indent=2, ensure_ascii=False).encode('utf-8')
                except Exception as fallback_error:
                     logger.error(f"Fallback serialization failed for file '{filename}': {fallback_error}")
                     # Cannot serialize even the fallback - return an error indicating this
                     return {
                        'file_id': 'error',
                        'filename': filename,
                        'web_link': '#',
                        'error': f"Serialization failed: {serialization_error} and fallback failed: {fallback_error}"
                     }


            file_metadata = {
                'name': filename,
                'mimeType': 'application/json'
            }
            if description:
                file_metadata['description'] = description
            logger.debug(f"Метаданные файла: {file_metadata}")

            file_content = BytesIO(json_bytes)

            media = MediaIoBaseUpload(
                file_content,
                mimetype='application/json',
                resumable=True
            )

            logger.debug(f"Выполнение запроса к Google Drive API для создания файла '{filename}'...")
            # Use file.execute() directly
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink'
            ).execute()
            logger.info(f"Файл '{filename}' успешно загружен на Google Drive. ID: {file.get('id')}")

            result = {
                'file_id': file.get('id', 'unknown'),
                'filename': file.get('name', filename),
                'web_link': file.get('webViewLink', '#')
            }
            logger.debug(f"Результат загрузки: {result}")
            return result

        except Exception as e:
            logger.error(f"Ошибка при загрузке файла '{filename}' на Google Drive: {e}", exc_info=True)
            return {
                'file_id': 'error',
                'filename': filename,
                'web_link': '#',
                'error': str(e)
            }


# Assuming settings is correctly initialized elsewhere
drive_service = GoogleDriveService(config=settings)