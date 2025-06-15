"""
Модуль для ETL операций с банковскими счетами
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.db.models_maket import Account, DailyAccountBalance, DimDate
from app.db.init_db import create_tables

logger = logging.getLogger(__name__)

class BankAccountETL:
    """Класс для выполнения ETL операций с банковскими счетами"""
    
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
    
    async def process_records(self, records: List[Dict]) -> Dict:
        """
        Обработка записей банковских счетов
        
        Args:
            records: Список записей для обработки
            
        Returns:
            Dict с результатами обработки
        """
        start_time = datetime.now()
        logger.info(f"Начало ETL обработки {len(records)} записей")
        
        try:
            # Создаем таблицы
            # await create_tables()
            
            # Получаем текущую дату
            processing_date = datetime.now().date()
            
            # Получаем или создаем запись в DimDate
            date_dim = await self._get_or_create_date_dim(processing_date)
            
            accounts_processed = 0
        balances_created = 0
            
            for record in records:
                try:
                    # Получаем или создаем счет
                    account = await self._get_or_create_account(record)
                    accounts_processed += 1
                    
                    # Создаем запись о балансе
                    balance = DailyAccountBalance(
                        account_id=account.account_id,
                        date_id=date_dim.date_id,
                        balance_byn=record.get("balance_byn", 0.0),
                        processing_date=processing_date
                    )
                    self.db_session.add(balance)
                balances_created += 1
                
            except Exception as e:
                    logger.error(f"Ошибка при обработке записи: {str(e)}")
                    continue
            
            # Сохраняем изменения
            await self.db_session.commit()
            
            return {
                "accounts_processed": accounts_processed,
                "balances_created": balances_created,
                "etl_performed": True
            }
            
        except Exception as e:
            await self.db_session.rollback()
            logger.error(f"Ошибка при выполнении ETL: {str(e)}")
            return {
                "accounts_processed": 0,
                "balances_created": 0,
                "etl_performed": False,
                "error": str(e)
            }
    
    async def _get_or_create_date_dim(self, date: datetime.date) -> DimDate:
        """Получает или создает запись в таблице DimDate"""
        stmt = select(DimDate).where(DimDate.date == date)
        result = await self.db_session.execute(stmt)
        date_dim = result.scalar_one_or_none()
        
        if not date_dim:
            date_dim = DimDate(
                date=date,
                day=date.day,
                month=date.month,
                year=date.year,
                day_of_week=date.weekday() + 1,
                is_weekend=date.weekday() >= 5
            )
            self.db_session.add(date_dim)
            await self.db_session.commit()
        
        return date_dim
    
    async def _get_or_create_account(self, record: Dict) -> Account:
        """Получает или создает запись в таблице Account"""
        account_id = record.get("account_id")
        if not account_id:
            raise ValueError("Account ID is required")
        
        stmt = select(Account).where(Account.account_id == account_id)
        result = await self.db_session.execute(stmt)
        account = result.scalar_one_or_none()
        
        if not account:
            account = Account(
                account_id=account_id,
                account_name=record.get("account_name", f"Account {account_id}"),
                owner_id="unknown",
                currency=record.get("currency", "BYN"),
                account_type="current",
                account_status="active",
                creation_date=datetime.now().date()
            )
            self.db_session.add(account)
            await self.db_session.commit()
        
        return account 