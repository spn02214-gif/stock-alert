# -*- coding: utf-8 -*-
"""
KOSPI and KOSDAQ market data collection with FinanceDataReader.
"""
import logging
from datetime import datetime, timedelta

import FinanceDataReader as fdr
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]
FDR_COLUMN_RENAME = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
}
KR_MARKETS = ("KOSPI", "KOSDAQ")
KR_PRICE_SOURCES = ("NAVER", "KRX")


def get_market_date(offset: int = 0) -> str:
    """Return YYYY-MM-DD with a signed day offset."""
    date = datetime.today() + timedelta(days=offset)
    return date.strftime("%Y-%m-%d")


def get_all_tickers() -> list[dict]:
    """Return ticker metadata for all KOSPI and KOSDAQ symbols."""
    frames = []

    for market in KR_MARKETS:
        try:
            listing = fdr.StockListing(market).copy()
        except Exception as exc:
            logger.warning("%s listing fetch failed - skipped (%s)", market, exc)
            continue

        if listing.empty:
            logger.warning("%s listing fetch returned empty - skipped", market)
            continue

        if "Code" not in listing.columns:
            logger.warning("%s listing missing Code column - skipped", market)
            continue

        listing["Code"] = listing["Code"].astype(str).str.zfill(6)
        listing["Name"] = listing.get("Name", listing["Code"]).astype(str)
        listing["Market"] = market
        frames.append(listing[["Code", "Name", "Market"]])

    if not frames:
        logger.error("No KR market listings available from FinanceDataReader")
        return []

    combined = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["Code"])
    logger.info("KR ticker listing loaded: %d symbols", len(combined))
    return combined.to_dict("records")


def _fetch_ohlcv_from_source(ticker: str, source: str, start: str, end: str) -> pd.DataFrame:
    symbol = f"{source}:{ticker}"
    df = fdr.DataReader(symbol, start, end)
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns=FDR_COLUMN_RENAME)
    df.columns = [str(col).lower() for col in df.columns]

    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        logger.warning("%s: source %s missing columns %s", ticker, source, sorted(missing))
        return pd.DataFrame()

    df = df[REQUIRED_COLUMNS].copy()
    df = df.apply(pd.to_numeric, errors="coerce").dropna().sort_index()
    return df


def get_ohlcv(ticker: str, name: str = "", days: int = 30) -> pd.DataFrame:
    """
    Return an OHLCV DataFrame for a single ticker.
    Columns: open, high, low, close, volume
    """
    label = f"{name}({ticker})" if name else ticker
    end = get_market_date(0)
    start = get_market_date(-(days * 2))

    for source in KR_PRICE_SOURCES:
        try:
            df = _fetch_ohlcv_from_source(ticker, source, start, end)

            if df.empty:
                logger.info("%s: no data from %s", label, source)
                continue

            if len(df) < 21:
                logger.warning(
                    "%s: insufficient OHLCV rows from %s (%d) - skipped",
                    label,
                    source,
                    len(df),
                )
                return pd.DataFrame()

            logger.info("%s: loaded OHLCV from %s (%d rows)", label, source, len(df))
            return df.tail(days)

        except Exception as exc:
            logger.warning("%s: %s fetch error - skipped (%s)", label, source, exc)

    logger.warning("%s: no OHLCV data returned from any KR source - skipped", label)
    return pd.DataFrame()


def get_ticker_name(ticker_info: dict) -> str:
    """Return the ticker name from listing metadata."""
    return str(ticker_info.get("Name", ticker_info.get("Code", "")))


def get_all_stocks_data(days: int = 25) -> dict:
    """
    Collect OHLCV data for all KOSPI and KOSDAQ stocks.
    Returns: {ticker: {"name": str, "ohlcv": DataFrame}}
    """
    tickers = get_all_tickers()
    result = {}

    for ticker_info in tickers:
        try:
            ticker = str(ticker_info["Code"]).zfill(6)
            name = get_ticker_name(ticker_info)
            df = get_ohlcv(ticker, name=name, days=days)

            if df is None or df.empty:
                continue

            result[ticker] = {
                "name": name,
                "ohlcv": df,
            }
        except Exception as exc:
            ticker = ticker_info.get("Code", "UNKNOWN")
            logger.warning("%s: stock data build error - skipped (%s)", ticker, exc)

    logger.info("KR market data collection done: %d / %d stocks", len(result), len(tickers))
    return result
