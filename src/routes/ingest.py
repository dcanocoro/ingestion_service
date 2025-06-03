from fastapi import APIRouter, HTTPException
from ..services.ingestion import IngestionService

router = APIRouter()
service = IngestionService()

@router.post("/ingest/{symbol}", response_model=dict)
async def ingest_symbol(symbol: str):
    try:
        count = await service.ingest_balance_sheet(symbol)
        return {"inserted": count}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    
@router.post("/ingest/daily/{symbol}", response_model=dict)
async def ingest_daily(symbol: str):
    try:
        count = await service.ingest_daily_prices(symbol)
        return {"inserted": count}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

