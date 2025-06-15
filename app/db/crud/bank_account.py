from typing import List, Optional, Dict, Any
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, select

from app.db.models_maket import Account, DailyAccountBalance, DimDate


def get_account_by_number(db: Session, account_number: str):
    """Get an account by its account number."""
    return db.query(Account).filter(Account.account_number == account_number).first()


def create_account(db: Session, account_data: Dict[str, Any]) -> Account:
    """Create a new account."""
    db_account = Account(
        account_number=account_data["account_number"],
        account_name=account_data["account_name"],
        account_currency=account_data["account_currency"],
        is_active=account_data.get("is_active", True)
    )
    db.add(db_account)
    db.commit()
    db.refresh(db_account)
    return db_account


def update_account(db: Session, account_number: str, account_data: Dict[str, Any]):
    """Update an existing account."""
    db_account = get_account_by_number(db, account_number)
    if not db_account:
        return None
        
    for key, value in account_data.items():
        if hasattr(db_account, key):
            setattr(db_account, key, value)
    
    db.commit()
    db.refresh(db_account)
    return db_account


def upsert_daily_balance(db: Session, account_id: int, balance_data: Dict[str, Any]) -> DailyAccountBalance:
    """Create or update a daily balance for an account."""
    # Parse the date from the balance_data
    balance_date = balance_data.get("date")
    if isinstance(balance_date, str):
        balance_date = datetime.strptime(balance_date, "%Y-%m-%d").date()
        
    # Check if a balance already exists for this day
    db_balance = db.query(DailyAccountBalance).filter(
        and_(
            DailyAccountBalance.account_id == account_id,
            DailyAccountBalance.date == balance_date
        )
    ).first()
    
    if db_balance:
        # Update existing balance
        for key, value in balance_data.items():
            if hasattr(db_balance, key) and key != "date" and key != "account_id":
                setattr(db_balance, key, value)
    else:
        # Create new balance
        db_balance = DailyAccountBalance(
            account_id=account_id,
            date=balance_date,
            balance=balance_data["balance"],
            available_balance=balance_data["available_balance"],
            balance_usd=balance_data.get("balance_usd")
        )
        db.add(db_balance)
    
    db.commit()
    db.refresh(db_balance)
    return db_balance


def deactivate_accounts_except(db: Session, active_account_numbers: List[str]) -> int:
    """Mark accounts as inactive if they are not in the active_account_numbers list."""
    query = db.query(Account).filter(
        Account.is_active == True,
        Account.account_number.notin_(active_account_numbers)
    )
    
    # Get the count before updating
    count = query.count()
    
    # Update accounts to inactive
    query.update({Account.is_active: False}, synchronize_session=False)
    
    db.commit()
    return count 