from datetime import date, datetime
from sqlalchemy import Column, Integer, String, Numeric, Date, UniqueConstraint
from ..database import Base


class IncomeStatement(Base):
    __tablename__ = "income_statements"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True, nullable=False)
    fiscal_date_ending = Column(Date, nullable=False)
    reported_currency = Column(String, nullable=False)

    total_revenue = Column(Numeric, nullable=True)
    gross_profit = Column(Numeric, nullable=True)
    operating_income = Column(Numeric, nullable=True)
    ebit = Column(Numeric, nullable=True)
    ebitda = Column(Numeric, nullable=True)
    net_income = Column(Numeric, nullable=True)

    created_at = Column(Date, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "fiscal_date_ending", name="uq_income_symbol_date"),
    )
