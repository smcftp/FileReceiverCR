import datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    create_engine, String, Numeric, Date, TIMESTAMP, Boolean, ForeignKey,
    UniqueConstraint, BigInteger, Integer, Text, Column, DateTime
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import (
    declarative_base, Mapped, mapped_column, relationship
)
from sqlalchemy.sql import func

from app.db.base_class import Base

# --- Модели Таблиц ---

class DimDate(Base):
    """
    Dimension table for dates to ensure data integrity in reports.
    This table stores calendar dates and related attributes.
    """
    __tablename__ = "dim_date"
    
    date_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[datetime.date] = mapped_column(Date, unique=True, index=True, nullable=False)
    day: Mapped[int] = mapped_column(Integer, nullable=False)  # Day of month (1-31)
    month: Mapped[int] = mapped_column(Integer, nullable=False)  # Month (1-12)
    year: Mapped[int] = mapped_column(Integer, nullable=False)  # Year (e.g., 2023)
    day_name: Mapped[str] = mapped_column(String(10), nullable=False)  # Monday, Tuesday, etc.
    month_name: Mapped[str] = mapped_column(String(10), nullable=False)  # January, February, etc.
    is_weekend: Mapped[bool] = mapped_column(Boolean, nullable=False)  # True if Saturday or Sunday
    is_holiday: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)  # True if a holiday
    quarter: Mapped[int] = mapped_column(Integer, nullable=False)  # 1, 2, 3, or 4
    
    # Relationships
    daily_account_balances: Mapped[List["DailyAccountBalance"]] = relationship(back_populates="date_dimension")
    daily_loan_balances: Mapped[List["DailyLoanBalance"]] = relationship(back_populates="date_dimension")
    
    def __repr__(self):
        return f"<DimDate(date_id={self.date_id}, date='{self.date}')>"


class Account(Base):
    """
    Account model representing basic account information.
    """
    __tablename__ = "accounts"
    
    account_id = Column(Integer, primary_key=True)
    account_name = Column(String(255), nullable=False)
    currency = Column(String(3), nullable=False)
    
    # Relationships
    daily_balances: Mapped[List["DailyAccountBalance"]] = relationship(back_populates="account")
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="account")
    
    def __repr__(self):
        return f"<Account(account_id={self.account_id}, account_name='{self.account_name}', currency='{self.currency}')>"


class DailyAccountBalance(Base):
    """
    Daily balance snapshot model for bank accounts.
    This table tracks the daily balance of each account.
    """
    __tablename__ = "daily_account_balances"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey("accounts.account_id"), nullable=False, index=True)
    date_id = Column(Integer, ForeignKey("dim_date.date_id"), nullable=False, index=True)
    balance = Column(Numeric(precision=18, scale=2), nullable=False)  # "Остаток"
    balance_byn = Column(Numeric(precision=18, scale=2), nullable=False)  # "Остаток BLR"
    processing_date = Column(String(10), nullable=False)  # YYYY-MM-DD format
    
    # Relationships
    account: Mapped["Account"] = relationship(back_populates="daily_balances")
    date_dimension: Mapped["DimDate"] = relationship(back_populates="daily_account_balances")
    
    def __repr__(self):
        return f"<DailyAccountBalance(account_id={self.account_id}, date_id={self.date_id}, balance={self.balance}, balance_byn={self.balance_byn})>"


class ExpenseType(Base):
    """Типы расходов"""
    __tablename__ = 'expense_types'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, comment="Суррогатный PK")
    type_name: Mapped[str] = mapped_column(String(150), unique=True, nullable=False, comment="Название типа расхода")
    description: Mapped[Optional[str]] = mapped_column(Text, comment="Описание")

    # Связи
    transactions: Mapped[List["Transaction"]] = relationship(back_populates="expense_type")


class Transaction(Base):
    """Транзакции"""
    __tablename__ = 'transactions'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, comment="Суррогатный PK")
    source_transaction_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True, comment="ID транзакции из источника")
    account_id: Mapped[int] = mapped_column(ForeignKey('accounts.account_id'), nullable=False, index=True, comment="ID счета (FK)")
    expense_type_id: Mapped[Optional[int]] = mapped_column(ForeignKey('expense_types.id'), index=True, comment="ID типа расхода (FK, Optional)")
    transaction_date: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, index=True, comment="Дата и время транзакции")
    amount: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False, comment="Сумма (отрицательная для расхода)")
    amount_byn: Mapped[Optional[Decimal]] = mapped_column(Numeric(19, 4), comment="Сумма в BYN (рассчитанная)")
    description: Mapped[str] = mapped_column(Text, nullable=False, comment="Назначение / Описание")
    contract_reference: Mapped[Optional[str]] = mapped_column(String(255), comment="Ссылка на договор (текст)")
    invoice_code: Mapped[Optional[str]] = mapped_column(String(100), comment="Код акта/счета")
    payment_method: Mapped[Optional[str]] = mapped_column(String(100), comment="Способ оплаты")
    dw_created_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), comment="Время загрузки в DWH")

    # Связи
    account: Mapped["Account"] = relationship(back_populates="transactions")
    expense_type: Mapped[Optional["ExpenseType"]] = relationship(back_populates="transactions")


class Loan(Base):
    """Займы (Основная информация)"""
    __tablename__ = 'loans'

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, comment="Суррогатный PK")
    source_loan_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True, comment="ID займа из источника")
    contract_number: Mapped[str] = mapped_column(String(100), nullable=False, comment="Номер договора")
    contract_date: Mapped[datetime.date] = mapped_column(Date, nullable=False, comment="Дата договора")
    initial_amount: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False, comment="Начальная сумма займа")
    start_date: Mapped[Optional[datetime.date]] = mapped_column(Date, comment="Дата начала")
    end_date: Mapped[Optional[datetime.date]] = mapped_column(Date, comment="Плановая дата погашения")
    interest_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2), comment="Процентная ставка")
    created_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

    # Связи
    daily_balances: Mapped[List["DailyLoanBalance"]] = relationship(back_populates="loan")


class DailyLoanBalance(Base):
    """Ежедневные остатки по займам"""
    __tablename__ = 'daily_loan_balances'
    __table_args__ = (
        UniqueConstraint('loan_id', 'date_id', name='uq_daily_loan_balance_loan_date'),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, comment="Суррогатный PK")
    loan_id: Mapped[int] = mapped_column(ForeignKey('loans.id'), nullable=False, index=True, comment="ID займа (FK)")
    date_id: Mapped[int] = mapped_column(ForeignKey('dim_date.date_id'), nullable=False, index=True, comment="ID Даты (FK)")
    current_debt_byn: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False, comment="Остаток долга в BYN")
    total_repaid_byn: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False, comment="Всего погашено в BYN")
    source_created_at: Mapped[Optional[datetime.datetime]] = mapped_column(TIMESTAMP(timezone=True), comment="Время создания исходной записи (если есть)")
    dw_created_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), comment="Время загрузки в DWH")

    # Связи
    loan: Mapped["Loan"] = relationship(back_populates="daily_balances")
    date_dimension: Mapped["DimDate"] = relationship(back_populates="daily_loan_balances")


class DailyAccountSummary(Base):
    """
    Daily summary of all account balances for easier analytics
    """
    __tablename__ = "daily_account_summary"
    
    date_id = Column(Integer, ForeignKey("dim_date.date_id"), primary_key=True, index=True)
    total_balance_byn = Column(Numeric(precision=20, scale=2), nullable=False)
    account_count = Column(Integer, nullable=False)
    processing_date = Column(String(10), nullable=False)  # YYYY-MM-DD format
    
    # Relationship
    date_dimension: Mapped["DimDate"] = relationship()
    
    def __repr__(self):
        return f"<DailyAccountSummary(date_id={self.date_id}, total_balance_byn={self.total_balance_byn})>"

# Таблица для хранения исходных JSON данных
# class RawJsonData(Base):
#     """Хранилище исходных JSON данных"""
#     __tablename__ = 'raw_json_data'
    
#     id: Mapped[int] = mapped_column(BigInteger, primary_key=True, comment="Суррогатный PK")
#     filename: Mapped[str] = mapped_column(String(255), nullable=False, comment="Имя файла источника")
#     data: Mapped[dict] = mapped_column(JSONB, nullable=False, comment="JSON данные")
#     processed: Mapped[bool] = mapped_column(Boolean, default=False, comment="Обработаны через ETL?")
#     created_at: Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now())
#     processed_at: Mapped[Optional[datetime.datetime]] = mapped_column(TIMESTAMP(timezone=True), comment="Время обработки ETL")
#     error_message: Mapped[Optional[str]] = mapped_column(Text, comment="Сообщение об ошибке, если ETL не удался") 