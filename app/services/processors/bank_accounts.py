"""
Модуль для обработки банковских счетов
"""
import logging
import json
import traceback
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, inspect, func, update
from decimal import Decimal
import os
from dotenv import load_dotenv

from app.db.models_maket import Account, DailyAccountBalance, DimDate, DailyAccountSummary
from app.services.file_processor_factory import register_async_file_processor

# Загружаем переменные окружения в начале файла
load_dotenv()

# Словарь маппингов полей для поддержки мультиязычности
FIELD_MAPPINGS = {
    "ID": ["ID", "id", "ид", "идентификатор"],
    "account_id": ["account_id", "accountId", "ID", "id", "ид"],
    "account_name": ["account_name", "accountName", "название_счета", "названиеСчета", "Название счета"],
    "balance": ["balance", "баланс", "Остаток"],
    "balance_byn": ["balance_byn", "balanceBYN", "баланс_бел", "Остаток BLR"],
    "currency": ["currency", "валюта", "Валюта"],
    "processing_date": ["processing_date", "processingDate", "дата_обработки", "датаОбработки"],
}

# Настраиваем логгер
logger = logging.getLogger(__name__)

# Определяем курсы валют, подтягивая их из переменных окружения
_EXCHANGE_RATES = {}
try:
    _EXCHANGE_RATES = {
        "USD": Decimal(os.getenv("EXCHANGE_RATE_USD_TO_BYN", "3.07")),
        "EUR": Decimal(os.getenv("EXCHANGE_RATE_EUR_TO_BYN", "3.5")),
        "RUR": Decimal(os.getenv("EXCHANGE_RATE_RUR_TO_BYN", "0.037")),
        "KZT": Decimal(os.getenv("EXCHANGE_RATE_KZT_TO_BYN", "0.0059")),
        "UZS": Decimal(os.getenv("EXCHANGE_RATE_UZS_TO_BYN", "0.0002")),
    }
    logger.info("✅ (BankAccounts) Курсы валют успешно загружены из переменных окружения.")
except Exception as e:
    logger.error(f"❌ (BankAccounts) Ошибка загрузки курсов валют из переменных окружения. Используются жестко заданные значения по умолчанию. Ошибка: {e}")
    _EXCHANGE_RATES = {
        "USD": Decimal("3.07"),
        "EUR": Decimal("3.5"),
        "RUR": Decimal("0.037"),
        "KZT": Decimal("0.0059"),
        "UZS": Decimal("0.0002"),
    }

# Изменена функция convert_currency для работы с Decimal
def convert_currency(balance_currency: str, amount: Decimal) -> Decimal:
    """
    Конвертирует сумму из заданной валюты в BYN (BLR).
    
    Args:
        balance_currency: Название валюты для конвертации
        amount: Сумма для конвертации (Decimal)
        
    Returns:
        Decimal: Конвертированная сумма в BYN
    """
    rate = _EXCHANGE_RATES.get(balance_currency)
    if rate is not None:
        return amount * rate
    else:
        logger.warning(f"Курс для валюты {balance_currency} не найден, возвращаем исходную сумму.")
        return amount

def get_field_value(record: Dict[str, Any], field_name: str, default=None) -> Any:
    """
    Получает значение из записи, проверяя все возможные варианты имени поля
    
    Args:
        record: Словарь с данными записи
        field_name: Стандартное имя поля
        default: Значение по умолчанию, если поле не найдено
        
    Returns:
        Значение поля или default, если не найдено
    """
    for possible_name in FIELD_MAPPINGS.get(field_name, [field_name]):
        if possible_name in record:
            return record.get(possible_name)
    
    return default

async def calculate_and_store_daily_summary(
    db_session: AsyncSession, 
    date_id: int, 
    processing_date: str
) -> Dict[str, Any]:
    """
    Рассчитывает и сохраняет ежедневную сводку балансов всех счетов
    
    Args:
        db_session: Сессия базы данных
        date_id: ID даты для которой создается сводка
        processing_date: Строка с датой обработки в формате YYYY-MM-DD
        
    Returns:
        Dict[str, Any]: Словарь с информацией о сводке
    """
    logger.info(f"Расчет ежедневной сводки для date_id={date_id}, processing_date={processing_date}")
    
    try:
        # Запрос для подсчета общего баланса и количества счетов для указанной даты
        query = select(
            func.sum(DailyAccountBalance.balance_byn).label("total_balance_byn"),
            func.count(DailyAccountBalance.account_id.distinct()).label("account_count")
        ).where(DailyAccountBalance.date_id == date_id)
        
        logger.info(f"Выполнение запроса для расчета сводки: {query}")
        result = await db_session.execute(query)
        summary_data = result.fetchone()
        
        if not summary_data or summary_data[0] is None:
            logger.warning(f"Не найдены данные о балансах для date_id={date_id}")
            return {
                "created": False,
                "error": "No balance data found for the specified date",
                "total_balance_byn": 0.0,
                "account_count": 0
            }
        
        total_balance_byn = summary_data[0] or 0.0
        account_count = summary_data[1] or 0
        
        logger.info(f"Результаты расчета сводки: total_balance_byn={total_balance_byn}, account_count={account_count}")
        
        # Проверяем, существует ли уже сводка для этой даты
        existing_summary_query = select(DailyAccountSummary).where(DailyAccountSummary.date_id == date_id)
        existing_summary_result = await db_session.execute(existing_summary_query)
        existing_summary = existing_summary_result.scalar_one_or_none()
        
        if existing_summary:
            logger.info(f"Обновление существующей сводки для date_id={date_id}")
            existing_summary.total_balance_byn = total_balance_byn
            existing_summary.account_count = account_count
            existing_summary.processing_date = processing_date
            is_created = False
        else:
            logger.info(f"Создание новой сводки для date_id={date_id}")
            new_summary = DailyAccountSummary(
                date_id=date_id,
                total_balance_byn=total_balance_byn,
                account_count=account_count,
                processing_date=processing_date
            )
            db_session.add(new_summary)
            is_created = True
            
        # Выполняем промежуточный flush для сохранения сводки
        await db_session.flush()
        logger.info(f"✅ Сводка для date_id={date_id} успешно {'создана' if is_created else 'обновлена'}")
        
        return {
            "created": is_created,
            "total_balance_byn": float(total_balance_byn),
            "account_count": account_count
        }
    
    except Exception as e:
        logger.error(f"❌ Ошибка при расчете и сохранении сводки: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "created": False,
            "error": str(e),
            "total_balance_byn": 0.0,
            "account_count": 0
        }

@register_async_file_processor("BankAccounts", "Обработчик банковских счетов")
async def process_bank_accounts(
    data: Union[Dict, List],
    db_session: Optional[AsyncSession] = None
) -> Tuple[Dict, List]:
    """
    Обработка банковских счетов из JSON файла
    
    Args:
        data: Данные из JSON файла (словарь или список)
        db_session: Опциональная сессия БД для ETL операций
        
    Returns:
        Tuple[Dict, List]: Кортеж из метаданных и обработанных записей
    """
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("НАЧАЛО ОБРАБОТКИ БАНКОВСКИХ СЧЕТОВ")
    logger.info(f"Время начала: {start_time.isoformat()}")
    logger.info(f"Тип данных: {type(data)}")
    
    # Проверка наличия сессии БД сразу в начале функции
    if not db_session:
        logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Отсутствует сессия БД для ETL операций!")
        error_metadata = {
            "file_type": "BankAccounts",
            "record_count": 0,
            "processing_time_ms": int((datetime.now() - start_time).total_seconds() * 1000),
            "processed_timestamp": datetime.now().isoformat(),
            "processing_date": datetime.now().date().isoformat(),
            "async_processing": True,
            "success": False,
            "error": "Database session is required for ETL operations",
            "etl_stats": {
                "accounts_processed": 0,
                "balances_created": 0,
                "etl_performed": False
            }
        }
        logger.info("="*80)
        return error_metadata, []
    
    # Подробная информация о сессии БД
    logger.info(f"DB_SESSION ПРЕДОСТАВЛЕНА: {db_session}")
    try:
        if hasattr(db_session, 'bind') and db_session.bind:
            logger.info(f"DB URL: {db_session.bind.url}")
        logger.info(f"DB SESSION STATE: {db_session.is_active}")
    except Exception as db_info_err:
        logger.warning(f"Не удалось получить информацию о сессии БД: {db_info_err}")
    
    # Проверяем тип входных данных и получаем записи
    if isinstance(data, dict):
        logger.info(f"Обнаружен тип данных DICT, ключи: {list(data.keys())}")
        records = get_field_value(data, "BankAccounts", [])
        logger.info(f"Извлечение записей по ключу 'BankAccounts', найдено записей: {len(records)}")
    else:
        logger.info(f"Обнаружен тип данных LIST")
        records = data  # Если данные уже в виде списка
        logger.info(f"Использование данных как списка, количество элементов: {len(records)}")
    
    # Проверка пустых данных
    if not records:
        logger.warning("❌ КРИТИЧЕСКАЯ ОШИБКА: Нет записей для обработки!")
        logger.warning(f"Исходные данные: {data}")
        logger.info("="*80)
        return {
            "file_type": "BankAccounts",
            "record_count": 0,
            "processing_time_ms": 0,
            "processed_timestamp": datetime.now().isoformat(),
            "processing_date": datetime.now().date().isoformat(),
            "async_processing": True,
            "success": False,
            "error": "No records found",
            "etl_stats": {
                "accounts_processed": 0,
                "balances_created": 0,
                "etl_performed": False
            }
        }, []
    
    # Готовим переменные для обработки
    processed_records = []
    accounts_processed = 0
    accounts_updated = 0
    balances_created = 0
    balances_updated = 0
    errors_count = 0
    
    # Генерируем дату обработки
    processing_date_str = datetime.now().strftime("%Y-%m-%d")
    processing_date_dt = datetime.strptime(processing_date_str, "%Y-%m-%d").date()
    date_id_for_batch = int(processing_date_str.replace("-", ""))
    
    # Set a specific date (December 31, 2025)
    # specific_date = datetime(2025, 3, 21)

    # Generate the same formats as in the original code
    # processing_date_str = specific_date.strftime("%Y-%m-%d")  # "2025-12-31"
    # processing_date_dt = specific_date.date()  # datetime.date(2025, 12, 31)
    # date_id_for_batch = int(processing_date_str.replace("-", ""))  # 20251231
    
    logger.info(f"Дата обработки: {processing_date_str} (date_id={date_id_for_batch})")
    logger.info(f"Количество записей для обработки: {len(records)}")
    
    # Печатаем первые 3 записи для отладки
    if records and len(records) > 0:
        logger.info(f"Пример первых записей (до 3):")
        for i, record in enumerate(records[:3]):
            logger.info(f"Запись #{i+1}: {json.dumps(record, ensure_ascii=False)}")
    
    # --- Логика ETL ---
    logger.info("НАЧАЛО ВЫПОЛНЕНИЯ ETL ОПЕРАЦИЙ")
    logger.info("ЭТАП 1: Подготовка данных для ETL")
    
    try:
        # Проверяем состояние сессии БД перед началом операций
        logger.info(f"Проверка состояния сессии БД: is_active={db_session.is_active}")
        
        # Добавляем больше логирования для отладки
        try:
            logger.info(f"Начало ETL операций с базой данных. База: {db_session.bind.url if hasattr(db_session, 'bind') and hasattr(db_session.bind, 'url') else 'Unknown'}")
        except Exception as db_url_error:
            logger.warning(f"Не удалось получить URL базы данных: {db_url_error}")
        
        logger.info(f"Дата обработки: {processing_date_str}, date_id: {date_id_for_batch}")

        # Шаг 1: Получаем или создаем DimDate ОДИН РАЗ перед циклом
        logger.info("ЭТАП 2: Получение/создание записи DimDate")
        date_query = select(DimDate).where(DimDate.date_id == date_id_for_batch)
        logger.info(f"SQL запрос: {date_query}")
        
        try:
            logger.info(f"Выполнение запроса DimDate...")
            date_result = await db_session.execute(date_query)
            date_dim = date_result.scalar_one_or_none()
            
            if date_dim:
                logger.info(f"✅ Найдена существующая запись DimDate: date_id={date_dim.date_id}, date={date_dim.date}")
            else:
                logger.info(f"Запись DimDate не найдена, создаем новую для date_id={date_id_for_batch}, дата={processing_date_dt}")
                date_dim = DimDate(
                    date_id=date_id_for_batch,
                    date=processing_date_dt,
                    day=processing_date_dt.day,
                    month=processing_date_dt.month,
                    year=processing_date_dt.year,
                    day_name=processing_date_dt.strftime("%A"),
                    month_name=processing_date_dt.strftime("%B"),
                    is_weekend=processing_date_dt.weekday() >= 5,
                    quarter=(processing_date_dt.month - 1) // 3 + 1,
                    is_holiday=False  # Временно установим как False
                )
                logger.info(f"Создан объект DimDate: {date_dim}")
                db_session.add(date_dim)
                
                # Делаем промежуточный flush для получения корректного date_id
                logger.info("Промежуточный flush для dim_date...")
                try:
                    await db_session.flush()
                    logger.info(f"✅ Промежуточный flush выполнен успешно")
                    logger.info(f"DimDate создан с date_id={date_dim.date_id}")
                except Exception as flush_err:
                    logger.error(f"❌ Ошибка при выполнении промежуточного flush: {flush_err}", exc_info=True)
                    raise flush_err
        except Exception as date_err:
            logger.error(f"❌ Ошибка при получении/создании DimDate: {date_err}", exc_info=True)
            raise date_err

        # Начинаем обработку записей
        logger.info("ЭТАП 3: Обработка записей банковских счетов")
        logger.info(f"Начинаем обработку {len(records)} записей...")

        # Шаг 2: Обрабатываем каждую запись в цикле
        for idx, record in enumerate(records):
            logger.info(f"Обработка записи #{idx+1}/{len(records)}")
            
            balance = 0.0
            balance_byn = 0.0
            
            # Получаем ID счета
            account_id_str = get_field_value(record, "ID")
            if not account_id_str:
                logger.warning(f"❌ Пропуск записи без ID: {record}")
                errors_count += 1
                continue
                
            try:
                account_id_int = int(account_id_str)
                logger.info(f"ID счета: {account_id_int}")
            except (ValueError, TypeError) as id_err:
                logger.error(f"❌ Ошибка преобразования ID '{account_id_str}' в число: {id_err}")
                errors_count += 1
                continue

            # Трансформируем данные для ответа API
            try:
                # Улучшенный парсинг баланса с более детальной обработкой ошибок
                balance_raw = get_field_value(record, "balance")
                balance_byn_raw = get_field_value(record, "balance_byn")
                balance_currency = get_field_value(record, "currency")
                
                # Логика проверки и конвертации
                if balance_raw == balance_byn_raw and balance_currency != "BLR":
                    logger.info(f"Конвертация валюты для счета {account_id_int}: {balance_currency}, сумма: {balance_raw}")
                    balance_byn_raw = convert_currency(balance_currency, Decimal(balance_raw))
                    logger.info(f"Конвертированная сумма в BYN: {balance_byn_raw}")
                else:
                    balance_byn_raw = balance_byn_raw if balance_byn_raw is not None else Decimal(balance_raw)  # Используем balance_byn_raw или balance_raw

                
                # Логируем значения для отладки
                logger.info(f"Обработка счета {account_id_int} - balance_raw: {balance_raw}, balance_byn_raw: {balance_byn_raw}")
                
                if balance_raw is not None:
                    # Если строка - убираем пробелы, заменяем запятые на точки
                    if isinstance(balance_raw, str):
                        balance_raw = balance_raw.strip().replace(',', '.')
                        logger.info(f"Преобразование строкового значения баланса: '{get_field_value(record, 'balance')}' -> '{balance_raw}'")
                    balance = float(balance_raw)
                else:
                    balance = 0.0
                    logger.warning(f"Отсутствует balance для счета {account_id_int}, используем 0.0")
                    
                if balance_byn_raw is not None:
                    # Если строка - убираем пробелы, заменяем запятые на точки
                    if isinstance(balance_byn_raw, str):
                        balance_byn_raw = balance_byn_raw.strip().replace(',', '.')
                        logger.info(f"Преобразование строкового значения баланса в BYN: '{get_field_value(record, 'balance_byn')}' -> '{balance_byn_raw}'")
                    balance_byn = float(balance_byn_raw)
                else:
                    balance_byn = Decimal(balance)  # Если нет balance_byn, используем balance
                    logger.warning(f"Отсутствует balance_byn для счета {account_id_int}, используем balance: {balance}")
            except (ValueError, TypeError) as e:
                logger.error(f"❌ Некорректное значение баланса для счета {account_id_int}: {e}, используем 0")
                balance = 0.0
                balance_byn = 0.0

            # Создание выходной записи
            transformed_record = {
                "account_id": account_id_int,
                "account_name": get_field_value(record, "account_name", f"Account {account_id_int}"),
                "currency": get_field_value(record, "currency", "BYN"),
                "balance": balance,
                "balance_byn": balance_byn,
                "processing_date": processing_date_str
            }
            logger.info(f"Трансформированная запись: {json.dumps(transformed_record, ensure_ascii=False)}")
            processed_records.append(transformed_record)

            # --- Операции с БД для текущей записи ---
            try:
                # Проверяем счет
                account_query = select(Account).where(Account.account_id == account_id_int)
                logger.info(f"Выполнение запроса счета: {account_query}")
                account_result = await db_session.execute(account_query)
                account = account_result.scalar_one_or_none()
                
                if not account:
                    # Создаем новый счет
                    logger.info(f"Создание нового счета: id={account_id_int}, name={transformed_record['account_name']}, currency={transformed_record['currency']}")
                    account = Account(
                        account_id=account_id_int,
                        account_name=transformed_record["account_name"],
                        currency=transformed_record["currency"]
                    )
                    db_session.add(account)
                    
                    # Важно: добавляем немедленный flush для каждого созданного счета
                    try:
                        await db_session.flush()
                        accounts_processed += 1
                        logger.info(f"✅ Счет {account_id_int} создан успешно и сохранен в БД")
                    except Exception as acc_flush_err:
                        logger.error(f"❌ Ошибка при сохранении счета {account_id_int}: {acc_flush_err}")
                        raise acc_flush_err
                else:
                    logger.info(f"Счет {account_id_int} уже существует, проверка необходимости обновления")
                    # Проверяем, нужно ли обновить существующий счет
                    if (account.account_name != transformed_record["account_name"] or 
                        account.currency != transformed_record["currency"]):
                        logger.info(f"Обновление данных счета {account_id_int}")
                        account.account_name = transformed_record["account_name"]
                        account.currency = transformed_record["currency"]
                        accounts_updated += 1
                    else:
                        logger.info(f"Данные счета {account_id_int} не изменились, пропуск обновления")

                # Проверяем, существует ли уже баланс для этого счета и даты
                balance_check_query = select(DailyAccountBalance).where(
                    (DailyAccountBalance.account_id == account_id_int) & 
                    (DailyAccountBalance.date_id == date_id_for_batch)
                )
                logger.info(f"Проверка существующего баланса: {balance_check_query}")
                existing_balance_result = await db_session.execute(balance_check_query)
                existing_balance = existing_balance_result.scalar_one_or_none()

                if existing_balance:
                    logger.info(f"Существующий баланс найден для account_id={account_id_int}, date_id={date_id_for_batch}")
                    # Проверяем, изменились ли значения баланса
                    if (existing_balance.balance != balance or 
                        existing_balance.balance_byn != balance_byn):
                        logger.info(f"Обновление существующего баланса: старый (balance={existing_balance.balance}, balance_byn={existing_balance.balance_byn}) -> новый (balance={balance}, balance_byn={balance_byn})")
                        existing_balance.balance = balance
                        existing_balance.balance_byn = balance_byn
                        existing_balance.processing_date = processing_date_str
                        balances_updated += 1
                    else:
                        logger.info(f"Значения баланса не изменились, пропуск обновления")
                else:
                    # Создаем запись баланса, используя УЖЕ полученный/созданный date_id
                    logger.info(f"Создание нового баланса для account_id={account_id_int}, date_id={date_id_for_batch}, balance={balance}, balance_byn={balance_byn}")
                    balance_record = DailyAccountBalance(
                        account_id=account_id_int,
                        date_id=date_id_for_batch,
                        balance=balance,
                        balance_byn=balance_byn,
                        processing_date=processing_date_str
                    )
                    db_session.add(balance_record)
                    balances_created += 1
                    
                # Делаем промежуточный flush каждые 50 записей
                if idx > 0 and idx % 50 == 0:
                    logger.info(f"Промежуточный flush после {idx} записей...")
                    try:
                        await db_session.flush()
                        logger.info(f"✅ Промежуточный flush #{idx//50} выполнен успешно")
                    except Exception as batch_flush_err:
                        logger.error(f"❌ Ошибка при выполнении промежуточного flush #{idx//50}: {batch_flush_err}")
                        raise batch_flush_err

            except Exception as inner_e:
                # Ошибка при обработке ОДНОЙ записи в БД
                errors_count += 1
                logger.error(f"❌ Ошибка обработки счета {account_id_int} в БД: {inner_e}")
                logger.error(f"Стек вызовов:\n{traceback.format_exc()}")
                logger.warning(f"Пропуск записи для account_id={account_id_int} из-за ошибки БД.")
                continue # Переходим к следующей записи

        # Шаг 3: Выполняем flush перед commit для выявления возможных ошибок
        logger.info("ЭТАП 4: Финальный flush и commit транзакции")
        logger.info("Выполнение финального flush перед коммитом...")
        try:
            await db_session.flush()
            logger.info("✅ Финальный flush выполнен успешно")
        except Exception as final_flush_err:
            logger.error(f"❌ Ошибка при выполнении финального flush: {final_flush_err}", exc_info=True)
            raise final_flush_err
        
        # Шаг 3.5: Расчет и сохранение ежедневной сводки
        logger.info("ЭТАП 4.5: Расчет и сохранение ежедневной сводки балансов")
        summary_result = await calculate_and_store_daily_summary(
            db_session=db_session,
            date_id=date_id_for_batch,
            processing_date=processing_date_str
        )
        
        if "error" in summary_result:
            logger.warning(f"⚠️ Сводка была рассчитана с ошибками: {summary_result['error']}")
        else:
            logger.info(f"✅ Сводка успешно рассчитана. Общий баланс BYN: {summary_result['total_balance_byn']}, количество счетов: {summary_result['account_count']}")
        
        # Шаг 4: Коммит транзакции
        logger.info("Выполнение commit транзакции...")
        try:
            await db_session.commit()
            logger.info("✅ COMMIT ТРАНЗАКЦИИ ВЫПОЛНЕН УСПЕШНО")
        except Exception as commit_err:
            logger.error(f"❌ ОШИБКА ПРИ КОММИТЕ: {commit_err}", exc_info=True)
            raise commit_err
        
        logger.info(f"✅ ETL операции завершены успешно:")
        logger.info(f"   - Счета: создано {accounts_processed}, обновлено {accounts_updated}")
        logger.info(f"   - Балансы: создано {balances_created}, обновлено {balances_updated}")
        logger.info(f"   - Ежедневная сводка: {'создана' if summary_result.get('created', False) else 'обновлена'}")
        logger.info(f"   - Общий баланс BYN: {summary_result.get('total_balance_byn', 0)}")
        logger.info(f"   - Ошибки: {errors_count}")
    
    except Exception as e:
        # Ошибка на уровне всей ETL операции
        logger.error(f"❌❌❌ КРИТИЧЕСКАЯ ОШИБКА при обработке банковских счетов: {str(e)}")
        logger.error(f"Стек вызовов:\n{traceback.format_exc()}")
        if db_session:
            logger.info("Откат транзакции из-за ошибки ETL...")
            try:
                await db_session.rollback()
                logger.info("✅ Откат транзакции выполнен успешно")
            except Exception as rollback_err:
                logger.error(f"❌ Ошибка при откате транзакции: {rollback_err}")
        
        # Возвращаем метаданные с ошибкой
        error_metadata = {
            "file_type": "BankAccounts",
            "record_count": len(records),
            "processing_time_ms": int((datetime.now() - start_time).total_seconds() * 1000),
            "processed_timestamp": datetime.now().isoformat(),
            "processing_date": processing_date_str,
            "success": False,
            "error": f"ETL Error: {str(e)}",
            "etl_stats": {
                "accounts_processed": accounts_processed,
                "accounts_updated": accounts_updated,
                "balances_created": balances_created,
                "balances_updated": balances_updated,
                "errors": errors_count,
                "daily_summary": summary_result if 'summary_result' in locals() else None,
                "etl_performed": True
            }
        }
        
        processing_time = datetime.now() - start_time
        logger.error(f"❌ Обработка завершена С ОШИБКАМИ. Время: {processing_time.total_seconds():.2f} сек.")
        logger.info("="*80)
        
        return error_metadata, processed_records # Возвращаем то, что успели трансформировать до ошибки

    # Формируем успешный ответ, если все прошло хорошо
    processing_time_ms = int((datetime.now() - start_time).total_seconds() * 1000)
    processing_time = datetime.now() - start_time
    
    metadata = {
        "file_type": "BankAccounts",
        "record_count": len(processed_records),
        "processing_time_ms": processing_time_ms,
        "processed_timestamp": datetime.now().isoformat(),
        "processing_date": processing_date_str,
        "success": True,
        "etl_stats": {
            "accounts_processed": accounts_processed,
            "accounts_updated": accounts_updated,
            "balances_created": balances_created,
            "balances_updated": balances_updated,
            "errors": errors_count,
            "daily_summary": summary_result if 'summary_result' in locals() else None,
            "etl_performed": True
        }
    }

    logger.info(f"✅ Успешная обработка {len(processed_records)} записей банковских счетов за {processing_time.total_seconds():.2f} сек.")
    logger.info(f"Итоговая статистика:")
    logger.info(f"   - Счета: создано {accounts_processed}, обновлено {accounts_updated}")
    logger.info(f"   - Балансы: создано {balances_created}, обновлено {balances_updated}")
    logger.info(f"   - Ежедневная сводка: {'создана' if summary_result.get('created', False) else 'обновлена'}")
    logger.info(f"   - Общий баланс BYN: {summary_result.get('total_balance_byn', 0)}")
    logger.info(f"   - Ошибки: {errors_count}")
    logger.info("="*80)
    
    return metadata, processed_records