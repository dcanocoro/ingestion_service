from pydantic import BaseModel
from datetime import date
from typing import Optional

class BalanceSheetIn(BaseModel):
    symbol: str
    fiscal_date_ending: date
    reported_currency: str
    total_assets: Optional[float]
    total_liabilities: Optional[float]
    total_shareholder_equity: Optional[float]

class BalanceSheetOut(BalanceSheetIn):
    id: int

    class Config:
        orm_mode = True
