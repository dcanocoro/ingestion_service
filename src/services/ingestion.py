from pydantic import ValidationError
from ..schemas.balance_sheet import BalanceSheetIn
from ..schemas.price import DailyPriceIn
from ..repositories.data_repository import BalanceSheetRepository, DailyPriceRepository
from ..database import SessionLocal
from ..connectors.alphavantage import (AlphavantageBalanceSheetConnector,
                                       AlphavantageDailyPriceConnector)

class IngestionService:
    def __init__(self):
        self.balance_connector = AlphavantageBalanceSheetConnector()
        self.price_connector = AlphavantageDailyPriceConnector()

    async def ingest_balance_sheet(self, symbol: str) -> int:
        raw = await self.balance_connector.fetch(symbol)
        parsed_dicts = self.balance_connector.parse(raw)

        parsed_models = []
        for item in parsed_dicts:
            try:
                parsed_models.append(BalanceSheetIn(**item))
            except ValidationError as e:
                print("Skipping invalid record:", e)

        with SessionLocal() as db:
            repo = BalanceSheetRepository(db)
            repo.upsert_many(parsed_models)

        return len(parsed_models)

    async def ingest_daily_prices(self, symbol: str) -> int:
        raw = await self.price_connector.fetch(symbol)
        parsed_dicts = self.price_connector.parse(raw)

        parsed_models = []
        for item in parsed_dicts:
            try:
                parsed_models.append(DailyPriceIn(**item))
            except ValidationError as e:
                print("Skipping invalid record:", e)

        with SessionLocal() as db:
            repo = DailyPriceRepository(db)
            repo.upsert_many(parsed_models)

        return len(parsed_models)
