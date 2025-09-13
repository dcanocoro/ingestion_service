from sqlalchemy import Column, Integer, String, Numeric, Date, UniqueConstraint
from datetime import datetime, date
from ..database import Base

class DailyPrice(Base):
    __tablename__ = "daily_prices"

    id            = Column(Integer, primary_key=True)
    symbol        = Column(String, index=True, nullable=False)
    trade_date    = Column(Date,   nullable=False)
    open_price    = Column(Numeric)
    high_price    = Column(Numeric)
    low_price     = Column(Numeric)
    close_price   = Column(Numeric)
    volume        = Column(Integer)
    created_at = Column(Date, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("symbol", "trade_date", name="uq_symbol_date_price"),
    )
