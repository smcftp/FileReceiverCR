"""
Модуль для обработки займов
"""
import logging
import json
import traceback
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Union, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, update
from decimal import Decimal
import sys
import os # <-- Добавляем импорт os
from dotenv import load_dotenv # <-- Добавляем импорт load_dotenv

from app.db.models_maket import Loan, DimDate, DailyLoanBalance
from app.services.file_processor_factory import register_async_file_processor

# Загружаем переменные окружения в начале файла
load_dotenv()

# Словарь маппингов полей для поддержки мультиязычности
FIELD_MAPPINGS = {
    "ID": ["ID", "id", "ид", "идентификатор"],
    "loan_id": ["loan_id", "loanId", "ID", "id", "ид"],
    "contract_number": ["contract_number", "contractNumber", "Дата договора", "Номер договора"],
    "contract_date": ["contract_date", "contractDate", "Дата договора", "дата_договора"],
    "initial_amount": ["initial_amount", "initialAmount", "начальная_сумма", "Начальная сумма", "Сумма займа"],
    "duty": ["Долг"],
    "duty_byn": ["amount_byn", "amountBYN", "сумма_бел", "Сумма BLR", "Сумма возврата"],
    "currency": ["currency", "валюта", "Валюта", "Валюта займа"],
    "interest_rate": ["interest_rate", "interestRate", "процентная_ставка", "Процентная ставка"],
    "start_date": ["start_date", "startDate", "дата_начала", "Дата начала"],
    "end_date": ["end_date", "endDate", "дата_окончания", "Дата окончания"],
    "status": ["status", "статус", "Статус"],
    "processing_date": ["processing_date", "processingDate", "дата_обработки", "датаОбработки", "Дата договора"],
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
    logger.info("✅ Курсы валют успешно загружены из переменных окружения.")
except Exception as e:
    logger.error(f"❌ Ошибка загрузки курсов валют из переменных окружения. Используются жестко заданные значения по умолчанию. Ошибка: {e}")
    # Если загрузка из переменных окружения не удалась, используем жестко заданные значения
    _EXCHANGE_RATES = {
        "USD": Decimal("3.07"),
        "EUR": Decimal("3.5"),
        "RUR": Decimal("0.037"),
        "KZT": Decimal("0.0059"),
        "UZS": Decimal("0.0002"),
    }

def get_field_value(record: Dict[str, Any], field_name: str, default=None) -> Any:
    """Получает значение из записи, проверяя все возможные варианты имени поля"""
    for possible_name in FIELD_MAPPINGS.get(field_name, [field_name]):
        if possible_name in record:
            return record.get(possible_name)
    return default

def convert_currency(balance_currency: str, amount: Decimal) -> Decimal:
    """Конвертирует сумму из заданной валюты в BYN (BLR)"""
    rate = _EXCHANGE_RATES.get(balance_currency)
    if rate is not None:
        return amount * rate
    else:
        logger.warning(f"Курс для валюты {balance_currency} не найден, возвращаем исходную сумму.")
        return amount # Возвращаем Decimal, если курс не найден

async def calculate_and_store_daily_summary(
    db_session: AsyncSession, 
    date_id: int, 
    processing_date: str
) -> Dict[str, Any]:
    """Рассчитывает и сохраняет ежедневную сводку займов"""
    logger.info(f"Расчет ежедневной сводки для date_id={date_id}, processing_date={processing_date}")
    
    try:
        # Запрос для подсчета общего баланса и количества займов
        query = select(
            func.sum(DailyLoanBalance.amount_byn).label("total_amount_byn"),
            func.count(DailyLoanBalance.loan_id.distinct()).label("loan_count")
        ).where(DailyLoanBalance.date_id == date_id)
        
        result = await db_session.execute(query)
        summary_data = result.fetchone()
        
        if not summary_data or summary_data[0] is None:
            return {
                "created": False,
                "error": "No loan data found for the specified date",
                "total_amount_byn": 0.0,
                "loan_count": 0
            }
        
        total_amount_byn = summary_data[0] or 0.0
        loan_count = summary_data[1] or 0
        
        # Проверяем существующую сводку
        existing_summary_query = select(DailyLoanSummary).where(DailyLoanSummary.date_id == date_id)
        existing_summary_result = await db_session.execute(existing_summary_query)
        existing_summary = existing_summary_result.scalar_one_or_none()
        
        if existing_summary:
            existing_summary.total_amount_byn = total_amount_byn
            existing_summary.loan_count = loan_count
            existing_summary.processing_date = processing_date
            is_created = False
        else:
            new_summary = DailyLoanSummary(
                date_id=date_id,
                total_amount_byn=total_amount_byn,
                loan_count=loan_count,
                processing_date=processing_date
            )
            db_session.add(new_summary)
            is_created = True
            
        await db_session.flush()
        
        return {
            "created": is_created,
            "total_amount_byn": float(total_amount_byn),
            "loan_count": loan_count
        }
    
    except Exception as e:
        logger.error(f"Ошибка при расчете и сохранении сводки: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            "created": False,
            "error": str(e),
            "total_amount_byn": 0.0,
            "loan_count": 0
        }

@register_async_file_processor("Zaimy", "Обработчик займов")
async def process_zaimy(
    data: Union[Dict, List],
    db_session: Optional[AsyncSession] = None
) -> Tuple[Dict, List]:
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("НАЧАЛО ОБРАБОТКИ ЗАЙМОВ")
    
    # --- Добавлено для отладки кодировки ---
    logger.info(f"DEBUG_ENCODING: sys.getdefaultencoding() = {sys.getdefaultencoding()}")
    if sys.stdout and hasattr(sys.stdout, 'encoding'):
        logger.info(f"DEBUG_ENCODING: sys.stdout.encoding = {sys.stdout.encoding}")
    else:
        logger.info("DEBUG_ENCODING: sys.stdout is not available or has no encoding property.")
    # --- Конец добавлено для отладки кодировки ---
    
    if not db_session:
        logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Отсутствует сессия БД!")
        return {
            "error": "Отсутствует сессия БД",
            "processing_time_ms": 0,
            "records_processed": 0,
            "records_updated": 0,
            "records_created": 0,
            "errors_count": 0
        }, []

    # Извлекаем записи из JSON
    if isinstance(data, dict):
        records = get_field_value(data, "Zaimy", [])
    else:
        records = data

    if not records:
        logger.warning("❌ Нет записей для обработки!")
        return {
            "error": "Нет записей для обработки",
            "processing_time_ms": 0,
            "records_processed": 0,
            "records_updated": 0,
            "records_created": 0,
            "errors_count": 0
        }, []

    processed_records = []
    loans_processed = 0
    loans_updated = 0
    loans_created = 0
    errors_count = 0
    
    # NEW: Set для отслеживания уникальных date_id, обработанных в DailyLoanBalance
    processed_date_ids = set() 

    try:
        # Получаем все существующие займы одним запросом
        existing_loans_query = select(Loan).where(
            Loan.source_loan_id.in_([str(get_field_value(record, "ID")) for record in records])
        )
        existing_loans_result = await db_session.execute(existing_loans_query)
        existing_loans = {loan.source_loan_id: loan for loan in existing_loans_result.scalars()}

        for idx, record in enumerate(records):
            try:
                # Получаем ID займа
                loan_id = str(get_field_value(record, "ID"))
                
                logger.info(f"Обработка записи {idx} для loan_id {loan_id}")
                
                # Дополнительное логирование для проверки всех ключей в record
                logger.info("DEBUG_KEYS: Точные представления ключей в 'record':")
                for k, v in record.items():
                    # Попытка получить байтовое представление ключа
                    try:
                        key_bytes = k.encode('utf-8')
                        logger.info(f"  Ключ (UTF-8 bytes repr): {key_bytes!r}, Значение: {v!r}") 
                    except UnicodeEncodeError:
                        logger.info(f"  Ключ (UnicodeEncodeError, str repr): {k!r}, Значение: {v!r} (не удалось кодировать в UTF-8)")
                    
                    # Всегда выводим строковое представление ключа, как его видит Python
                    logger.info(f"  Ключ (str repr): {k!r}, Значение: {v!r}")

                # Получаем исходные значения из записи
                initial_amount_raw = get_field_value(record, "initial_amount", 0)
                loan_currency_value = get_field_value(record, "currency", "BYN")
                duty_raw = get_field_value(record, "duty", 0)
                
                # Преобразуем к Decimal для точных расчетов
                initial_amount_decimal = Decimal(initial_amount_raw)
                duty_decimal = Decimal(duty_raw)

                # Инициализируем duty_byn_money значением duty (по умолчанию или если конвертация не нужна)
                calculated_duty_byn_money = duty_decimal

                # Условие для конвертации duty_byn_money
                if loan_currency_value not in ["BLR", "BYN"]:
                    try:
                        # Передаем Decimal в convert_currency, и она вернет Decimal
                        calculated_duty_byn_money = convert_currency(loan_currency_value, duty_decimal)
                        logger.info(f"✅ Конвертация duty_money: {duty_decimal} {loan_currency_value} -> {calculated_duty_byn_money} BYN")
                    except Exception as e:
                        logger.error(f"❌ Ошибка конвертации 'duty_money' из {loan_currency_value} в BYN: {e}")
                        logger.error(traceback.format_exc())
                        # В случае ошибки конвертации, оставляем значение duty_decimal
                        calculated_duty_byn_money = duty_decimal
                
                # Подготавливаем данные для обновления/создания Loan
                loan_data = {
                    "source_loan_id": loan_id,
                    "contract_number": get_field_value(record, "contract_number", f"LOAN-{loan_id}"),
                    "contract_date": date(2000, 1, 1),
                    "initial_amount": initial_amount_decimal,
                    "loan_currency": loan_currency_value,
                    "duty_money": str(duty_decimal),
                    "duty_byn_money": str(calculated_duty_byn_money),
                    "interest_rate": Decimal(get_field_value(record, "interest_rate", 0)),
                    "start_date": date(2000, 1, 1),
                    "end_date": date(2000, 12, 31),
                    "status": get_field_value(record, "status", "active"),
                    "updated_at": datetime.now()
                }

                # Проверяем существование займа
                existing_loan = existing_loans.get(loan_id)

                if existing_loan:
                    # Проверяем, есть ли реальные изменения
                    has_changes = any(
                        getattr(existing_loan, key) != value 
                        for key, value in loan_data.items() 
                        if key != 'updated_at'
                    )

                    if has_changes:
                        # Обновляем существующий займ
                        for key, value in loan_data.items():
                            setattr(existing_loan, key, value)
                        loans_updated += 1
                        logger.info(f"✅ Обновлен займ {loan_id}")
                    else:
                        logger.info(f"ℹ️ Займ {loan_id} не изменился")
                else:
                    # Создаем новый займ
                    new_loan = Loan(**loan_data)
                    db_session.add(new_loan)
                    loans_created += 1
                    logger.info(f"✅ Создан новый займ {loan_id}")

                processed_records.append(loan_data)
                loans_processed += 1

                # Промежуточный flush каждые 50 записей
                if idx > 0 and idx % 50 == 0:
                    await db_session.flush()
                    logger.info(f"✅ Промежуточный flush после {idx} записей (Loan)")

            except Exception as e:
                errors_count += 1
                logger.error(f"❌ Ошибка при обработке записи {idx} (Loan): {str(e)}")
                logger.error(traceback.format_exc())
                continue

        # Финальный flush и коммит для всех индивидуальных записей Loan
        await db_session.flush()
        await db_session.commit()
        logger.info("✅ Все индивидуальные Loan записи зафиксированы.")

        # --- Логика для DailyLoanBalance ---
        # Теперь проходим по записям еще раз, чтобы обработать DailyLoanBalance
        # Это делается после коммита Loan, чтобы гарантировать наличие loan.id
        for idx, record in enumerate(records):
            try:
                loan_id_from_source = str(get_field_value(record, "ID"))
                
                # Находим только что созданный или обновленный Loan, чтобы получить его внутренний ID
                current_loan_query = select(Loan).where(Loan.source_loan_id == loan_id_from_source)
                current_loan_result = await db_session.execute(current_loan_query)
                current_loan = current_loan_result.scalar_one_or_none()

                if not current_loan:
                    logger.warning(f"ℹ️ Не найден Loan для source_loan_id {loan_id_from_source} после обработки. Пропускаем DailyLoanBalance.")
                    continue

                # NEW: Используем текущую дату для DailyLoanBalance
                daily_balance_date_obj = datetime.now().date() # <-- Установка текущей даты
                
                date_dimension_query = select(DimDate.date_id).where(DimDate.date == daily_balance_date_obj)
                date_dimension_result = await db_session.execute(date_dimension_query)
                date_id = date_dimension_result.scalar_one_or_none()

                if not date_id:
                    logger.warning(f"❌ Не найдена запись в DimDate для текущей даты {daily_balance_date_obj}. Пропускаем DailyLoanBalance для {loan_id_from_source}. Убедитесь, что DimDate заполнена для этой даты.")
                    # В реальной системе здесь можно добавить логику для создания записи в DimDate
                    continue
                
                # Добавляем date_id в наш set для последующей агрегации
                processed_date_ids.add(date_id)

                # 2. Получаем current_debt_byn
                duty_raw = get_field_value(record, "duty", 0)
                loan_currency_value = get_field_value(record, "currency", "BYN")
                duty_decimal = Decimal(duty_raw)
                calculated_duty_byn_money = duty_decimal # Инициализация

                if loan_currency_value not in ["BLR", "BYN"]:
                    try:
                        calculated_duty_byn_money = convert_currency(loan_currency_value, duty_decimal)
                    except Exception as e:
                        logger.error(f"❌ Ошибка конвертации 'duty_money' для DailyLoanBalance из {loan_currency_value} в BYN: {e}")
                        calculated_duty_byn_money = duty_decimal # В случае ошибки, используем исходное значение

                # 3. Проверяем существование DailyLoanBalance и обновляем/создаем
                existing_daily_balance_query = select(DailyLoanBalance).where(
                    DailyLoanBalance.loan_id == current_loan.id,
                    DailyLoanBalance.date_id == date_id
                )
                existing_daily_balance_result = await db_session.execute(existing_daily_balance_query)
                existing_daily_balance = existing_daily_balance_result.scalar_one_or_none()

                if existing_daily_balance:
                    # Обновляем, если значение долга изменилось
                    if existing_daily_balance.current_debt_byn != calculated_duty_byn_money:
                        existing_daily_balance.current_debt_byn = calculated_duty_byn_money
                        # total_repaid_byn: будет обновлен позже в пакетной операции
                        logger.info(f"✅ Обновлен current_debt_byn в DailyLoanBalance для займа {current_loan.source_loan_id} на дату {daily_balance_date_obj}")
                else:
                    # Создаем новую запись. total_repaid_byn пока ставим 0.00, будет обновлен позже.
                    new_daily_balance = DailyLoanBalance(
                        loan_id=current_loan.id,
                        date_id=date_id,
                        current_debt_byn=calculated_duty_byn_money,
                        total_repaid_byn=Decimal('0.00') # <-- Временное значение, будет обновлено пакетно
                    )
                    db_session.add(new_daily_balance)
                    logger.info(f"✅ Создан DailyLoanBalance для займа {current_loan.source_loan_id} на дату {daily_balance_date_obj}")

            except Exception as e:
                errors_count += 1
                logger.error(f"❌ Ошибка при обработке DailyLoanBalance для записи {loan_id_from_source}: {str(e)}")
                logger.error(traceback.format_exc())
                continue

        # Финальный flush и коммит для всех индивидуальных DailyLoanBalance записей
        await db_session.flush()
        await db_session.commit()
        logger.info("✅ Все индивидуальные DailyLoanBalance записи зафиксированы.")

        # --- NEW LOGIC: Рассчитываем и обновляем total_repaid_byn (как суммарный долг за день) ---
        if processed_date_ids:
            logger.info(f"Начинается расчет и обновление суммарного долга для {len(processed_date_ids)} дат.")
            for date_id_to_update in processed_date_ids:
                try:
                    # Рассчитываем сумму current_debt_byn для этой date_id
                    sum_query = select(func.sum(DailyLoanBalance.current_debt_byn)).where(
                        DailyLoanBalance.date_id == date_id_to_update
                    )
                    sum_result = await db_session.execute(sum_query)
                    total_debt_for_day = sum_result.scalar_one_or_none()

                    if total_debt_for_day is None:
                        total_debt_for_day = Decimal('0.00')
                    
                    # Обновляем ВСЕ записи DailyLoanBalance для этой date_id
                    # Устанавливаем total_repaid_byn в рассчитанную сумму
                    update_statement = update(DailyLoanBalance).where(
                        DailyLoanBalance.date_id == date_id_to_update
                    ).values(total_repaid_byn=total_debt_for_day)
                    
                    await db_session.execute(update_statement)
                    logger.info(f"✅ Обновлено total_repaid_byn для date_id {date_id_to_update} на сумму: {total_debt_for_day} (суммарный долг).")

                except Exception as e:
                    logger.error(f"❌ Ошибка при расчете/обновлении total_repaid_byn для date_id {date_id_to_update}: {str(e)}")
                    logger.error(traceback.format_exc())
                    # Продолжаем к следующему date_id, даже если возникла ошибка
                    continue

            # Финальный коммит пакетных обновлений
            await db_session.commit()
            logger.info("✅ Все агрегированные данные total_repaid_byn зафиксированы.")
        else:
            logger.info("ℹ️ Нет обработанных date_id для обновления суммарного долга.")
        # --- END NEW LOGIC ---

        processing_time = datetime.now() - start_time
        processing_time_ms = int(processing_time.total_seconds() * 1000)

        logger.info("="*80)
        logger.info(f"ОБРАБОТКА ЗАЙМОВ ЗАВЕРШЕНА")
        logger.info(f"Время обработки: {processing_time_ms} мс")
        logger.info(f"Обработано записей: {loans_processed}")
        logger.info(f"Создано новых: {loans_created}")
        logger.info(f"Обновлено: {loans_updated}")
        logger.info(f"Ошибок: {errors_count}")
        logger.info("="*80)

        return {
            "processing_time_ms": processing_time_ms,
            "records_processed": loans_processed,
            "records_updated": loans_updated,
            "records_created": loans_created,
            "errors_count": errors_count
        }, processed_records

    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}")
        logger.error(traceback.format_exc())
        
        if db_session:
            await db_session.rollback()
        
        return {
            "error": str(e),
            "processing_time_ms": 0,
            "records_processed": 0,
            "records_updated": 0,
            "records_created": 0,
            "errors_count": errors_count
        }, []
