import httpx
from datetime import date
from typing import List, Dict, Any
from .base import BaseAPIConnector
from ..settings import settings

class AlphavantageBalanceSheetConnector(BaseAPIConnector):
    BASE_URL = "https://www.alphavantage.co/query"

    async def fetch(self, symbol: str) -> Dict[str, Any]:
        params = {
            "function": "BALANCE_SHEET",
            "symbol": symbol,
            "apikey": settings.ALPHAVANTAGE_API_KEY,
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

    def parse(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        symbol = raw.get("symbol")
        reports = raw.get("annualReports", [])
        result: List[Dict[str, Any]] = []
        for rep in reports:
            # Skip rows without mandatory fields
            if "fiscalDateEnding" not in rep:
                continue
            result.append({
                "symbol": symbol,
                "fiscal_date_ending": date.fromisoformat(rep["fiscalDateEnding"]),
                "reported_currency": rep.get("reportedCurrency", "USD"),
                "total_assets": float(rep["totalAssets"]) if rep.get("totalAssets") and rep["totalAssets"] != "None" else None,
                "total_liabilities": float(rep["totalLiabilities"]) if rep.get("totalLiabilities") and rep["totalLiabilities"] != "None" else None,
                "total_shareholder_equity": float(rep["totalShareholderEquity"]) if rep.get("totalShareholderEquity") and rep["totalShareholderEquity"] != "None" else None,
            })
        return result


class AlphavantageDailyPriceConnector(BaseAPIConnector):
    BASE_URL = "https://www.alphavantage.co/query"

    async def fetch(self, symbol: str, output_size: str = "compact") -> Dict[str, Any]:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol":   symbol,
            "outputsize": output_size,
            "apikey":   settings.ALPHAVANTAGE_API_KEY,
        }
        async with httpx.AsyncClient() as client:
            r = await client.get(self.BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            return r.json()

    def parse(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        symbol = raw["Meta Data"]["2. Symbol"]
        series = raw["Time Series (Daily)"]
        parsed = []
        for day, values in series.items():
            parsed.append(
                {
                    "symbol":      symbol,
                    "date":        day,                       # alias in schema
                    "open_price":  float(values["1. open"]),
                    "high_price":  float(values["2. high"]),
                    "low_price":   float(values["3. low"]),
                    "close_price": float(values["4. close"]),
                    "volume":      int(values["5. volume"]),
                }
            )
        return parsed
