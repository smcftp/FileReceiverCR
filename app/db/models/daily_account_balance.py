from sqlalchemy import Column, Integer, Date, String, Boolean, Float, ForeignKey, DateTime, func
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.ext.declarative import declarative_base
from typing import List

Base = declarative_base()

class DimDate(Base):
    """
    Date dimension table for ensuring data integrity in reports.
    This table stores date information to be referenced by fact tables.
    """
    __tablename__ = "dim_date"

    date_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    full_date: Mapped[Date] = mapped_column(Date, unique=True, nullable=False, index=True, comment="Calendar date")
    day: Mapped[int] = mapped_column(Integer, nullable=False, comment="Day of month")
    month: Mapped[int] = mapped_column(Integer, nullable=False, comment="Month number")
    year: Mapped[int] = mapped_column(Integer, nullable=False, comment="Year")
    day_name: Mapped[str] = mapped_column(String(10), nullable=False, comment="Name of day")
    month_name: Mapped[str] = mapped_column(String(10), nullable=False, comment="Name of month")
    is_weekend: Mapped[bool] = mapped_column(Boolean, nullable=False, comment="True if weekend")
    is_holiday: Mapped[bool] = mapped_column(Boolean, nullable=False, comment="True if holiday")
    quarter: Mapped[int] = mapped_column(Integer, nullable=False, comment="Quarter of year")

    # Relationships
    daily_balances = relationship("DailyAccountBalance", back_populates="dim_date")
    daily_loan_balances: Mapped[List["DailyLoanBalance"]] = relationship(back_populates="date_dimension")

    def __repr__(self):
        return f"<DimDate(date_id={self.date_id}, full_date='{self.full_date}')>"

class Account(Base):
    """
    Table storing current state of bank accounts.
    """
    __tablename__ = "accounts"

    account_id = Column(String(50), primary_key=True, comment="Account ID")
    currency = Column(String(3), nullable=False, comment="Currency code")
    owner_id = Column(String(50), nullable=False, index=True, comment="Owner ID")
    account_type = Column(String(20), nullable=False, comment="Type of account")
    account_status = Column(String(20), nullable=False, comment="Status of account (active/inactive)")
    creation_date = Column(Date, nullable=False, comment="Date when account was created")
    last_updated = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now(), 
                        comment="Last time account was updated")
    
    # Relationships
    daily_balances = relationship("DailyAccountBalance", back_populates="account")
    
    def __repr__(self):
        return f"<Account(account_id='{self.account_id}', owner_id='{self.owner_id}', status='{self.account_status}')>"

class DailyAccountBalance(Base):
    """
    Table storing daily balances for each account.
    """
    __tablename__ = "daily_account_balances"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(50), ForeignKey("accounts.account_id"), nullable=False, index=True,
                       comment="Reference to account")
    date_id = Column(Integer, ForeignKey("dim_date.date_id"), nullable=False, index=True,
                    comment="Reference to date dimension")
    balance = Column(Float, nullable=False, comment="Account balance in original currency")
    balance_usd = Column(Float, nullable=False, comment="Account balance converted to USD")
    exchange_rate = Column(Float, nullable=False, comment="Exchange rate used for USD conversion")
    
    # Relationships
    account = relationship("Account", back_populates="daily_balances")
    dim_date = relationship("DimDate", back_populates="daily_balances")
    
    def __repr__(self):
        return f"<DailyAccountBalance(account_id='{self.account_id}', date_id={self.date_id}, balance={self.balance})>" 