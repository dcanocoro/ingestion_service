from sqlalchemy.orm import Session
from typing import List

from ..models.balance_sheet import BalanceSheet
from ..schemas.balance_sheet import BalanceSheetIn

from ..models.daily_price import DailyPrice
from ..schemas.price import DailyPriceIn

from ..logging_config import get_logger

logger = get_logger("repositories.data_repository")


class BalanceSheetRepository:
    def __init__(self, db: Session):
        self.db = db
        logger.debug("BalanceSheetRepository initialized")

    def upsert_many(self, sheets: List[BalanceSheetIn]) -> None:
        logger.info(f"Starting upsert operation for {len(sheets)} balance sheet records")
        
        updated_count = 0
        inserted_count = 0
        
        try:
            for i, sheet in enumerate(sheets):
                logger.debug(f"Processing balance sheet record {i+1}/{len(sheets)} for {sheet.symbol}")
                
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
                    updated_count += 1
                    logger.debug(f"Updated existing balance sheet record for {sheet.symbol} - {sheet.fiscal_date_ending}")
                else:
                    # Insert new record
                    new_record = BalanceSheet(
                        symbol=sheet.symbol,
                        fiscal_date_ending=sheet.fiscal_date_ending,
                        reported_currency=sheet.reported_currency,
                        total_assets=sheet.total_assets,
                        total_liabilities=sheet.total_liabilities,
                        total_shareholder_equity=sheet.total_shareholder_equity,
                    )
                    self.db.add(new_record)
                    inserted_count += 1
                    logger.debug(f"Inserted new balance sheet record for {sheet.symbol} - {sheet.fiscal_date_ending}")
            
            self.db.commit()
            logger.info(f"Successfully completed balance sheet upsert: {inserted_count} inserted, {updated_count} updated")
            
        except Exception as e:
            logger.error(f"Error during balance sheet upsert operation: {str(e)}", exc_info=True)
            self.db.rollback()
            raise


class DailyPriceRepository:
    def __init__(self, db: Session):
        self.db = db
        logger.debug("DailyPriceRepository initialized")

    def upsert_many(self, prices: List[DailyPriceIn]) -> None:
        logger.info(f"Starting upsert operation for {len(prices)} daily price records")
        
        updated_count = 0
        inserted_count = 0
        
        try:
            for i, p in enumerate(prices):
                logger.debug(f"Processing daily price record {i+1}/{len(prices)} for {p.symbol}")
                
                row = (
                    self.db.query(DailyPrice)
                    .filter(DailyPrice.symbol == p.symbol,
                            DailyPrice.trade_date == p.trade_date)
                    .one_or_none()
                )
                
                if row:
                    # Update existing record
                    row.open_price  = p.open_price
                    row.high_price  = p.high_price
                    row.low_price   = p.low_price
                    row.close_price = p.close_price
                    row.volume      = p.volume
                    updated_count += 1
                    logger.debug(f"Updated existing daily price record for {p.symbol} - {p.trade_date}")
                else:
                    # Insert new record
                    new_record = DailyPrice(**p.dict())
                    self.db.add(new_record)
                    inserted_count += 1
                    logger.debug(f"Inserted new daily price record for {p.symbol} - {p.trade_date}")
            
            self.db.commit()
            logger.info(f"Successfully completed daily price upsert: {inserted_count} inserted, {updated_count} updated")
            
        except Exception as e:
            logger.error(f"Error during daily price upsert operation: {str(e)}", exc_info=True)
            self.db.rollback()
            raise
