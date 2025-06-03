from fastapi import APIRouter, HTTPException
from ..services.ingestion import IngestionService
from ..logging_config import get_logger

logger = get_logger("routes.ingest")
router = APIRouter()
service = IngestionService()

@router.post("/ingest/{symbol}", response_model=dict)
async def ingest_symbol(symbol: str):
    logger.info(f"Received balance sheet ingestion request for symbol: {symbol}")
    try:
        count = await service.ingest_balance_sheet(symbol)
        logger.info(f"Successfully completed balance sheet ingestion for {symbol} - inserted {count} records")
        return {"inserted": count}
    except Exception as exc:
        logger.error(f"Failed to ingest balance sheet for {symbol}: {str(exc)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))
    
@router.post("/ingest/daily/{symbol}", response_model=dict)
async def ingest_daily(symbol: str):
    logger.info(f"Received daily prices ingestion request for symbol: {symbol}")
    try:
        count = await service.ingest_daily_prices(symbol)
        logger.info(f"Successfully completed daily prices ingestion for {symbol} - inserted {count} records")
        return {"inserted": count}
    except Exception as exc:
        logger.error(f"Failed to ingest daily prices for {symbol}: {str(exc)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))

