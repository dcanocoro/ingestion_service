import httpx
from datetime import date
from typing import List, Dict, Any
from ..settings import settings, logger  # Assuming logger is in settings
import json  # Import json for logging and JSONDecodeError
from .base import BaseAPIConnector


class AlphavantageIncomeStatementConnector(BaseAPIConnector):
    BASE_URL = "https://www.alphavantage.co/query"

    async def fetch(self, symbol: str) -> Dict[str, Any]:
        params = {
            "function": "INCOME_STATEMENT",
            "symbol": symbol,
            "apikey": settings.ALPHAVANTAGE_API_KEY,
        }
        async with httpx.AsyncClient() as client:
            response = await client.get(self.BASE_URL, params=params, timeout=30)
            response.raise_for_status()  # Raises an exception for 4XX/5XX errors

            try:
                data = response.json()
            except json.JSONDecodeError:
                logger.error(
                    f"Failed to decode JSON response for {symbol}. "
                    f"Status: {response.status_code}. Raw response text: {response.text}"
                )
                # Return a minimal structure that parse can handle gracefully, leading to empty results
                return {"symbol": symbol, "annualReports": []}  # Ensure parse gets what it expects or can handle

            # Check if the expected key "annualReports" is in the response
            # This handles cases where the response is valid JSON but not the expected data structure
            # (e.g., a JSON response detailing a rate limit or other API message)
            if "annualReports" not in data:
                logger.error(
                    f"Key 'annualReports' not found in Alphavantage response for {symbol}. "
                    f"Full response: {json.dumps(data, indent=2)}"
                )
                # Ensure 'annualReports' key exists if parse method strictly expects it,
                # even if it's empty, to prevent KeyErrors in parse.
                if "symbol" not in data:  # If symbol is also missing from an error JSON
                    data["symbol"] = symbol
                data.setdefault("annualReports", [])

            return data

    def parse(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        symbol = raw.get("symbol")
        rows = raw.get("annualReports", [])
        parsed: List[Dict[str, Any]] = []

        for row in rows:
            parsed.append(
                {
                    "symbol": symbol,
                    "fiscal_date_ending": date.fromisoformat(row["fiscalDateEnding"]),
                    "reported_currency": row.get("reportedCurrency", "USD"),
                    "total_revenue": _to_float(row.get("totalRevenue")),
                    "gross_profit": _to_float(row.get("grossProfit")),
                    "operating_income": _to_float(row.get("operatingIncome")),
                    "ebit": _to_float(row.get("ebit")),
                    "ebitda": _to_float(row.get("ebitda")),
                    "net_income": _to_float(row.get("netIncome")),
                }
            )
        return parsed


def _to_float(value: str | None) -> float | None:
    if value and value != "None":
        return float(value)
    return None
