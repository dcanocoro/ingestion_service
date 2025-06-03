from sqlalchemy.orm import Session
from typing import List

from ..models.balance_sheet import BalanceSheet
from ..schemas.balance_sheet import BalanceSheetIn

from ..models.daily_price import DailyPrice
from ..schemas.price import DailyPriceIn


class BalanceSheetRepository:
    def __init__(self, db: Session):
        self.db = db

    def upsert_many(self, sheets: List[BalanceSheetIn]) -> None:
        for sheet in sheets:
            existing = (
                self.db.query(BalanceSheet)
                .filter(
                    BalanceSheet.symbol == sheet.symbol,
                    BalanceSheet.fiscal_date_ending == sheet.fiscal_date_ending,
                )
                .one_or_none()
            )
            if existing:
                # Update existing fields
                existing.total_assets = sheet.total_assets
                existing.total_liabilities = sheet.total_liabilities
                existing.total_shareholder_equity = sheet.total_shareholder_equity
            else:
                self.db.add(
                    BalanceSheet(
                        symbol=sheet.symbol,
                        fiscal_date_ending=sheet.fiscal_date_ending,
                        reported_currency=sheet.reported_currency,
                        total_assets=sheet.total_assets,
                        total_liabilities=sheet.total_liabilities,
                        total_shareholder_equity=sheet.total_shareholder_equity,
                    )
                )
        self.db.commit()


class DailyPriceRepository:
    def __init__(self, db: Session):
        self.db = db

    def upsert_many(self, prices: List[DailyPriceIn]) -> None:
        for p in prices:
            row = (
                self.db.query(DailyPrice)
                .filter(DailyPrice.symbol == p.symbol,
                        DailyPrice.trade_date == p.trade_date)
                .one_or_none()
            )
            if row:
                row.open_price  = p.open_price
                row.high_price  = p.high_price
                row.low_price   = p.low_price
                row.close_price = p.close_price
                row.volume      = p.volume
            else:
                self.db.add(DailyPrice(**p.dict()))
        self.db.commit()
