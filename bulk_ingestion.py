# bulk_ingest_from_csv.py
import asyncio
import csv
import httpx
import time
from pathlib import Path

API_BASE = "http://localhost:8000/api/ingest"
CSV_PATH = "symbols.csv"
RATE_LIMIT_PER_MIN = 20
SECONDS_BETWEEN_CALLS = 60 / RATE_LIMIT_PER_MIN

def load_symbols(csv_file: str) -> list[str]:
    with open(csv_file, newline="") as f:
        reader = csv.DictReader(f)
        return [row["symbol"].strip().upper() for row in reader]

async def call_endpoint(session: httpx.AsyncClient, url: str) -> dict:
    try:
        response = await session.post(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"⚠️  Error calling {url}: {e}")
        return {}

async def ingest_symbol(session: httpx.AsyncClient, symbol: str) -> None:
    endpoints = [
        f"{API_BASE}/{symbol}",           # balance sheet
        f"{API_BASE}/daily/{symbol}",     # daily prices
        f"{API_BASE}/income/{symbol}",    # income statement
    ]
    for endpoint in endpoints:
        print(f"→ Calling {endpoint}")
        result = await call_endpoint(session, endpoint)
        print(f"✔ {symbol} → {endpoint.split('/')[-2]} inserted: {result.get('inserted', '?')}")
        await asyncio.sleep(SECONDS_BETWEEN_CALLS)

async def main():
    symbols = load_symbols(CSV_PATH)
    async with httpx.AsyncClient(timeout=60) as session:
        for symbol in symbols:
            await ingest_symbol(session, symbol)

if __name__ == "__main__":
    asyncio.run(main())
