"""
Модуль с обработчиками для различных типов файлов.
Каждый обработчик отвечает за трансформацию данных определенного типа файла.

Импорты в этом файле обеспечивают авто-регистрацию обработчиков при запуске приложения.
"""

# Импортируем все обработчики файлов для их авто-регистрации
from app.services.processors.payments_partners import process_payments_partners  # noqa
from app.services.processors.bank_accounts import process_bank_accounts  # noqa

# Здесь можно добавлять импорты новых обработчиков в будущем
# Пример: from app.services.processors.invoices import process_invoices  # noqa 