from pydantic import ValidationError
from ..schemas.balance_sheet import BalanceSheetIn
from ..schemas.price import DailyPriceIn
from ..repositories.data_repository import BalanceSheetRepository, DailyPriceRepository
from ..database import SessionLocal
from ..connectors.alphavantage import (AlphavantageBalanceSheetConnector,
                                       AlphavantageDailyPriceConnector)
from ..logging_config import get_logger

logger = get_logger("services.ingestion")

class IngestionService:
    def __init__(self):
        logger.info("Initializing IngestionService")
        self.balance_connector = AlphavantageBalanceSheetConnector()
        self.price_connector = AlphavantageDailyPriceConnector()
        logger.info("IngestionService connectors initialized successfully")

    async def ingest_balance_sheet(self, symbol: str) -> int:
        logger.info(f"Starting balance sheet ingestion for symbol: {symbol}")
        
        try:
            # Fetch raw data
            logger.debug(f"Fetching raw balance sheet data for {symbol}")
            raw = await self.balance_connector.fetch(symbol)
            logger.info(f"Successfully fetched raw data for {symbol}")
            
            # Parse data
            logger.debug(f"Parsing balance sheet data for {symbol}")
            parsed_dicts = self.balance_connector.parse(raw)
            logger.info(f"Parsed {len(parsed_dicts)} balance sheet records for {symbol}")

            # Validate and convert to models
            parsed_models = []
            validation_errors = 0
            for i, item in enumerate(parsed_dicts):
                try:
                    model = BalanceSheetIn(**item)
                    parsed_models.append(model)
                    logger.debug(f"Successfully validated balance sheet record {i+1} for {symbol}")
                except ValidationError as e:
                    validation_errors += 1
                    logger.warning(f"Skipping invalid balance sheet record {i+1} for {symbol}: {e}")

            if validation_errors > 0:
                logger.warning(f"Skipped {validation_errors} invalid records out of {len(parsed_dicts)} for {symbol}")

            # Save to database
            logger.debug(f"Saving {len(parsed_models)} balance sheet records to database for {symbol}")
            with SessionLocal() as db:
                repo = BalanceSheetRepository(db)
                repo.upsert_many(parsed_models)
            
            logger.info(f"Successfully ingested {len(parsed_models)} balance sheet records for {symbol}")
            return len(parsed_models)
            
        except Exception as e:
            logger.error(f"Error ingesting balance sheet for {symbol}: {str(e)}", exc_info=True)
            raise

    async def ingest_daily_prices(self, symbol: str) -> int:
        logger.info(f"Starting daily prices ingestion for symbol: {symbol}")
        
        try:
            # Fetch raw data
            logger.debug(f"Fetching raw daily price data for {symbol}")
            raw = await self.price_connector.fetch(symbol)
            logger.info(f"Successfully fetched raw price data for {symbol}")
            
            # Parse data
            logger.debug(f"Parsing daily price data for {symbol}")
            parsed_dicts = self.price_connector.parse(raw)
            logger.info(f"Parsed {len(parsed_dicts)} daily price records for {symbol}")

            # Validate and convert to models
            parsed_models = []
            validation_errors = 0
            for i, item in enumerate(parsed_dicts):
                try:
                    model = DailyPriceIn(**item)
                    parsed_models.append(model)
                    logger.debug(f"Successfully validated daily price record {i+1} for {symbol}")
                except ValidationError as e:
                    validation_errors += 1
                    logger.warning(f"Skipping invalid daily price record {i+1} for {symbol}: {e}")

            if validation_errors > 0:
                logger.warning(f"Skipped {validation_errors} invalid records out of {len(parsed_dicts)} for {symbol}")

            # Save to database
            logger.debug(f"Saving {len(parsed_models)} daily price records to database for {symbol}")
            with SessionLocal() as db:
                repo = DailyPriceRepository(db)
                repo.upsert_many(parsed_models)
            
            logger.info(f"Successfully ingested {len(parsed_models)} daily price records for {symbol}")
            return len(parsed_models)
            
        except Exception as e:
            logger.error(f"Error ingesting daily prices for {symbol}: {str(e)}", exc_info=True)
            raise
