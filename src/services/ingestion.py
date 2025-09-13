from typing import List, Type, Tuple
from pydantic import BaseModel, ValidationError

from ..schemas.balance_sheet import BalanceSheetIn
from ..schemas.price import DailyPriceIn
from ..schemas.income_statement import IncomeStatementIn

from ..repositories.data_repository import BalanceSheetRepository, DailyPriceRepository
from ..repositories.income_repository import IncomeStatementRepository

from ..database import SessionLocal
from ..connectors.alphavantage import (
    AlphavantageBalanceSheetConnector,
    AlphavantageDailyPriceConnector,
)
from ..connectors.alphavantage_income import AlphavantageIncomeStatementConnector

from ..logging_config import get_logger

logger = get_logger("services.ingestion")


class IngestionService:
    def __init__(self) -> None:
        logger.info("Initializing IngestionService")
        self.balance_connector = AlphavantageBalanceSheetConnector()
        self.price_connector = AlphavantageDailyPriceConnector()
        self.is_connector = AlphavantageIncomeStatementConnector()
        logger.info("IngestionService connectors initialized successfully")

    # ─────────────────────────────── UTILITIES ────────────────────────────
    @staticmethod
    def _validate_records(
        parsed: List[dict],
        model_cls: Type[BaseModel],
        symbol: str,
        record_label: str,
    ) -> Tuple[List[BaseModel], int]:
        """Turn raw dictionaries into Pydantic models with per-record logging."""
        models, errors = [], 0
        for i, item in enumerate(parsed):
            try:
                models.append(model_cls(**item))
                logger.debug(
                    "Validated %s record %d for %s", record_label, i + 1, symbol
                )
            except ValidationError as exc:
                errors += 1
                logger.warning(
                    "Skipping invalid %s record %d for %s: %s",
                    record_label,
                    i + 1,
                    symbol,
                    exc,
                )
        if errors:
            logger.warning(
                "Skipped %d invalid %s records out of %d for %s",
                errors,
                record_label,
                len(parsed),
                symbol,
            )
        return models, errors

    # ─────────────────────────────── BALANCE ──────────────────────────────
    async def ingest_balance_sheet(self, symbol: str) -> int:
        logger.info("Starting balance-sheet ingestion for %s", symbol)
        try:
            raw = await self.balance_connector.fetch(symbol)
            parsed = self.balance_connector.parse(raw)

            models, _ = self._validate_records(
                parsed, BalanceSheetIn, symbol, "balance-sheet"
            )

            with SessionLocal() as db:
                BalanceSheetRepository(db).upsert_many(models)

            logger.info("Ingested %d balance-sheet rows for %s", len(models), symbol)
            return len(models)

        except Exception as exc:
            logger.error("Balance-sheet ingestion failed for %s", symbol, exc_info=exc)
            raise

    # ─────────────────────────────── PRICES ───────────────────────────────
    async def ingest_daily_prices(self, symbol: str) -> int:
        logger.info("Starting daily-price ingestion for %s", symbol)
        try:
            raw = await self.price_connector.fetch(symbol)
            parsed = self.price_connector.parse(raw)

            models, _ = self._validate_records(
                parsed, DailyPriceIn, symbol, "daily-price"
            )

            with SessionLocal() as db:
                DailyPriceRepository(db).upsert_many(models)

            logger.info("Ingested %d price rows for %s", len(models), symbol)
            return len(models)

        except Exception as exc:
            logger.error("Daily-price ingestion failed for %s", symbol, exc_info=exc)
            raise

    # ──────────────────────────── INCOME STMT ─────────────────────────────
    async def ingest_income_statement(self, symbol: str) -> int:
        """
        Fetch, validate, and upsert annual income-statement rows.
        """
        logger.info("Starting income-statement ingestion for %s", symbol)
        try:
            # 1. fetch
            raw = await self.is_connector.fetch(symbol)
            # 2. parse
            parsed = self.is_connector.parse(raw)
            logger.info("Parsed %d income-statement rows for %s", len(parsed), symbol)

            # 3. validate ➜ pydantic
            models, _ = self._validate_records(
                parsed, IncomeStatementIn, symbol, "income-statement"
            )

            # 4. upsert
            with SessionLocal() as db:
                IncomeStatementRepository(db).upsert_many(models)

            logger.info("Ingested %d income-statement rows for %s", len(models), symbol)
            return len(models)

        except Exception as exc:
            logger.error("Income-statement ingestion failed for %s", symbol, exc_info=exc)
            raise
