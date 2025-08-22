import os
import time
import json
from typing import Dict, Iterable, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
AV_FUNCTION = os.getenv("AV_FUNCTION", "TIME_SERIES_DAILY").strip()
AV_INTERVAL = os.getenv("AV_INTERVAL", "5min").strip()
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECS = int(os.getenv("RETRY_DELAY_SECS", "10"))

PG_DSN = (
    f"host=postgres port=5432 dbname={os.getenv('POSTGRES_DB')} "
    f"user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')}"
)

BASE_URL = "https://www.alphavantage.co/query"

class RecoverableAPIError(Exception):
    pass

class IrrecoverableAPIError(Exception):
    pass


def _fetch(symbol: str) -> Dict:
    if not API_KEY:
        raise IrrecoverableAPIError("ALPHAVANTAGE_API_KEY is missing")

    params = {
        "function": AV_FUNCTION,
        "symbol": symbol,
        "apikey": API_KEY,
    }
    if AV_FUNCTION.upper() == "TIME_SERIES_INTRADAY":
        params["interval"] = AV_INTERVAL

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            # Alpha Vantage returns keys like "Error Message" or "Note" for throttling
            if "Error Message" in data:
                # irrecoverable for wrong symbol / function
                raise IrrecoverableAPIError(data["Error Message"])
            if "Note" in data:
                # rate-limited; treat as recoverable
                raise RecoverableAPIError(data["Note"])
            return data
        except RecoverableAPIError:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY_SECS)
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt == MAX_RETRIES:
                raise RecoverableAPIError(str(e))
            time.sleep(RETRY_DELAY_SECS)
        except requests.HTTPError as e:
            # 4xx likely irrecoverable, 5xx recoverable
            status = e.response.status_code if e.response else 0
            if 500 <= status < 600:
                if attempt == MAX_RETRIES:
                    raise RecoverableAPIError(str(e))
                time.sleep(RETRY_DELAY_SECS)
            else:
                raise IrrecoverableAPIError(str(e))


def _extract_rows(symbol: str, payload: Dict) -> Iterable[Tuple]:
    # Determine the correct time series key based on function
    series_key_map = {
        "TIME_SERIES_DAILY": "Time Series (Daily)",
        "TIME_SERIES_INTRADAY": f"Time Series ({AV_INTERVAL})",
        "TIME_SERIES_WEEKLY": "Weekly Time Series",
        "TIME_SERIES_MONTHLY": "Monthly Time Series",
    }
    key = series_key_map.get(AV_FUNCTION.upper())
    if not key or key not in payload:
        raise IrrecoverableAPIError(f"Expected key '{key}' not in response. Got keys: {list(payload.keys())}")

    series = payload[key]
    rows = []
    for ts, vals in series.items():
        try:
            open_ = float(vals.get("1. open")) if vals.get("1. open") is not None else None
            high_ = float(vals.get("2. high")) if vals.get("2. high") is not None else None
            low_ = float(vals.get("3. low")) if vals.get("3. low") is not None else None
            close_ = float(vals.get("4. close")) if vals.get("4. close") is not None else None
            vol = int(float(vals.get("5. volume"))) if vals.get("5. volume") is not None else None
            rows.append((symbol, ts, open_, high_, low_, close_, vol))
        except Exception:
            # Skip malformed datapoint but continue
            continue
    return rows


def upsert_rows(rows: Iterable[Tuple]):
    if not rows:
        return 0
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO stock_prices (symbol, ts, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (symbol, ts) DO UPDATE SET
                  open = EXCLUDED.open,
                  high = EXCLUDED.high,
                  low  = EXCLUDED.low,
                  close = EXCLUDED.close,
                  volume = EXCLUDED.volume,
                  fetched_at = NOW()
                """,
                rows,
                page_size=500,
            )
    return len(rows)


def fetch_and_store(symbol: str) -> int:
    payload = _fetch(symbol)
    rows = _extract_rows(symbol, payload)
    count = upsert_rows(rows)
    return count

if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    n = fetch_and_store(sym)
    print(f"Upserted {n} rows for {sym}")