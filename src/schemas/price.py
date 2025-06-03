from pydantic import BaseModel, Field
from datetime   import date

class DailyPriceIn(BaseModel):
    symbol: str
    trade_date: date = Field(alias="date")
    open_price:  float
    high_price:  float
    low_price:   float
    close_price: float
    volume:      int

class DailyPriceOut(DailyPriceIn):
    id: int
    class Config:
        orm_mode = True
