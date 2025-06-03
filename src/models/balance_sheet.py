from sqlalchemy import Column, Integer, String, Numeric, Date, UniqueConstraint
from datetime import datetime, date
from ..database import Base

class BalanceSheet(Base):
    __tablename__ = "balance_sheets"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True, nullable=False)
    fiscal_date_ending = Column(Date, nullable=False)
    reported_currency = Column(String, nullable=False)

    total_assets = Column(Numeric, nullable=True)
    total_liabilities = Column(Numeric, nullable=True)
    total_shareholder_equity = Column(Numeric, nullable=True)

    created_at = Column(Date, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "fiscal_date_ending", name="uq_symbol_date"),
    )

