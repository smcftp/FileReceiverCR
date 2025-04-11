from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import os
import json
from io import BytesIO
from typing import Dict, Any
from app.core.config import get_settings

class GoogleDriveService:
    SCOPES = ['https://www.googleapis.com/auth/drive.file']

    def __init__(self):
        self.creds = None
        self.service = None
        self.settings = get_settings()
        self.initialize_service()

    def initialize_service(self) -> None:
        """Initialize Google Drive service using credentials from settings"""
        try:
            # Пытаемся использовать креды из настроек
            if self.settings.GOOGLE_CREDENTIALS and self.settings.GOOGLE_TOKEN:
                # Загружаем токен из настроек
                token_data = json.loads(self.settings.GOOGLE_TOKEN)
                self.creds = Credentials.from_authorized_user_info(token_data, self.SCOPES)
                
                # Обновляем токен если нужно
                if self.creds.expired and self.creds.refresh_token:
                    self.creds.refresh(Request())
            else:
                # Если нет в настройках, пытаемся использовать локальные файлы
                if os.path.exists('token.json'):
                    self.creds = Credentials.from_authorized_user_file('token.json', self.SCOPES)

                if not self.creds or not self.creds.valid:
                    if self.creds and self.creds.expired and self.creds.refresh_token:
                        self.creds.refresh(Request())
                    else:
                        # Если нет валидных кредов, запускаем процесс авторизации
                        if not os.path.exists('credentials.json'):
                            raise FileNotFoundError(
                                "credentials.json не найден. Добавьте креды в .env или создайте файл credentials.json"
                            )
                        
                        flow = InstalledAppFlow.from_client_secrets_file(
                            'credentials.json', self.SCOPES)
                        self.creds = flow.run_local_server(
                            port=8080,
                            success_message='Авторизация успешна! Можете закрыть это окно.',
                            open_browser=True
                        )
                    
                    # Сохраняем новый токен
                    with open('token.json', 'w') as token:
                        token.write(self.creds.to_json())

            # Создаем сервис
            self.service = build('drive', 'v3', credentials=self.creds)

        except Exception as e:
            raise Exception(f"Ошибка инициализации Google Drive сервиса: {str(e)}")

    def upload_json(self, json_data: Dict[str, Any], filename: str) -> Dict[str, str]:
        """
        Upload JSON data to Google Drive
        
        Args:
            json_data: Dictionary containing JSON data
            filename: Name for the file on Google Drive
            
        Returns:
            dict: File metadata from Google Drive
        """
        try:
            json_bytes = json.dumps(json_data, indent=2).encode('utf-8')
            file_metadata = {'name': filename}
            
            file_content = BytesIO(json_bytes)
            
            media = MediaIoBaseUpload(
                file_content,
                mimetype='application/json',
                resumable=True
            )
            
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink'
            ).execute()
            
            return {
                'file_id': file.get('id'),
                'filename': file.get('name'),
                'web_link': file.get('webViewLink')
            }
            
        except Exception as e:
            raise Exception(f"Error uploading to Google Drive: {str(e)}")

# Создаем синглтон для сервиса
drive_service = GoogleDriveService() 