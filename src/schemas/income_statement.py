from typing import Optional
from datetime import date
from pydantic import BaseModel


class IncomeStatementIn(BaseModel):
    symbol: str
    fiscal_date_ending: date
    reported_currency: str

    total_revenue: Optional[float]
    gross_profit: Optional[float]
    operating_income: Optional[float]
    ebit: Optional[float]
    ebitda: Optional[float]
    net_income: Optional[float]


class IncomeStatementOut(IncomeStatementIn):
    id: int

    class Config:
        orm_mode = True
