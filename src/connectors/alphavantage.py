import httpx
from datetime import date
from typing import List, Dict, Any
from .base import BaseAPIConnector
from ..settings import settings
from ..logging_config import get_logger

logger = get_logger("connectors.alphavantage")

class AlphavantageBalanceSheetConnector(BaseAPIConnector):
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self):
        logger.info("Initializing AlphavantageBalanceSheetConnector")

    async def fetch(self, symbol: str) -> Dict[str, Any]:
        logger.info(f"Fetching balance sheet data from Alphavantage for symbol: {symbol}")
        
        params = {
            "function": "BALANCE_SHEET",
            "symbol": symbol,
            "apikey": settings.ALPHAVANTAGE_API_KEY,
        }
        
        logger.debug(f"Making API request to {self.BASE_URL} with params: {params}")
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                # Check for API error responses
                if "Error Message" in data:
                    error_msg = data["Error Message"]
                    logger.error(f"Alphavantage API error for {symbol}: {error_msg}")
                    raise ValueError(f"API Error: {error_msg}")
                
                if "Note" in data:
                    note = data["Note"]
                    logger.warning(f"Alphavantage API note for {symbol}: {note}")
                
                logger.info(f"Successfully fetched balance sheet data from Alphavantage for {symbol}")
                return data
                
        except httpx.RequestError as e:
            logger.error(f"Request error while fetching balance sheet for {symbol}: {str(e)}")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP status error while fetching balance sheet for {symbol}: {e.response.status_code}")
            raise

    def parse(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        symbol = raw.get("symbol")
        logger.debug(f"Parsing balance sheet data for symbol: {symbol}")
        
        reports = raw.get("annualReports", [])
        logger.debug(f"Found {len(reports)} annual reports for {symbol}")
        
        result: List[Dict[str, Any]] = []
        
        for i, rep in enumerate(reports):
            # Skip rows without mandatory fields
            if "fiscalDateEnding" not in rep:
                logger.warning(f"Skipping report {i+1} for {symbol}: missing fiscalDateEnding field")
                continue
                
            try:
                parsed_record = {
                    "symbol": symbol,
                    "fiscal_date_ending": date.fromisoformat(rep["fiscalDateEnding"]),
                    "reported_currency": rep.get("reportedCurrency", "USD"),
                    "total_assets": float(rep["totalAssets"]) if rep.get("totalAssets") and rep["totalAssets"] != "None" else None,
                    "total_liabilities": float(rep["totalLiabilities"]) if rep.get("totalLiabilities") and rep["totalLiabilities"] != "None" else None,
                    "total_shareholder_equity": float(rep["totalShareholderEquity"]) if rep.get("totalShareholderEquity") and rep["totalShareholderEquity"] != "None" else None,
                }
                result.append(parsed_record)
                logger.debug(f"Successfully parsed balance sheet record {i+1} for {symbol} - fiscal date: {rep['fiscalDateEnding']}")
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Error parsing balance sheet record {i+1} for {symbol}: {str(e)}")
                continue
        
        logger.info(f"Successfully parsed {len(result)} balance sheet records for {symbol}")
        return result


class AlphavantageDailyPriceConnector(BaseAPIConnector):
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self):
        logger.info("Initializing AlphavantageDailyPriceConnector")

    async def fetch(self, symbol: str, output_size: str = "compact") -> Dict[str, Any]:
        logger.info(f"Fetching daily price data from Alphavantage for symbol: {symbol} (output_size: {output_size})")
        
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol":   symbol,
            "outputsize": output_size,
            "apikey":   settings.ALPHAVANTAGE_API_KEY,
        }
        
        logger.debug(f"Making API request to {self.BASE_URL} with params: {params}")
        
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(self.BASE_URL, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
                
                # Check for API error responses
                if "Error Message" in data:
                    error_msg = data["Error Message"]
                    logger.error(f"Alphavantage API error for {symbol}: {error_msg}")
                    raise ValueError(f"API Error: {error_msg}")
                
                if "Note" in data:
                    note = data["Note"]
                    logger.warning(f"Alphavantage API note for {symbol}: {note}")
                
                logger.info(f"Successfully fetched daily price data from Alphavantage for {symbol}")
                return data
                
        except httpx.RequestError as e:
            logger.error(f"Request error while fetching daily prices for {symbol}: {str(e)}")
            raise
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP status error while fetching daily prices for {symbol}: {e.response.status_code}")
            raise

    def parse(self, raw: Dict[str, Any]) -> List[Dict[str, Any]]:
        try:
            symbol = raw["Meta Data"]["2. Symbol"]
            logger.debug(f"Parsing daily price data for symbol: {symbol}")
            
            series = raw["Time Series (Daily)"]
            logger.debug(f"Found {len(series)} daily price records for {symbol}")
            
            parsed = []
            for day, values in series.items():
                try:
                    parsed_record = {
                        "symbol":      symbol,
                        "date":        day,                       # alias in schema
                        "open_price":  float(values["1. open"]),
                        "high_price":  float(values["2. high"]),
                        "low_price":   float(values["3. low"]),
                        "close_price": float(values["4. close"]),
                        "volume":      int(values["5. volume"]),
                    }
                    parsed.append(parsed_record)
                    logger.debug(f"Successfully parsed daily price record for {symbol} on {day}")
                    
                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(f"Error parsing daily price record for {symbol} on {day}: {str(e)}")
                    continue
            
            logger.info(f"Successfully parsed {len(parsed)} daily price records for {symbol}")
            return parsed
            
        except KeyError as e:
            logger.error(f"Missing expected field in daily price response: {str(e)}")
            raise ValueError(f"Invalid response format: missing {str(e)}")
