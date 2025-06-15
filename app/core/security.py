from typing import Optional, Dict, Any

from app.core.config import get_settings

def verify_api_key(api_key: str) -> bool:
    """
    Проверка API ключа
    
    Args:
        api_key: API ключ для проверки
        
    Returns:
        bool: True если ключ верный, иначе False
    """
    return api_key == get_settings().API_KEY 