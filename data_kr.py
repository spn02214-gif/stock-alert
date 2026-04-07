# -*- coding: utf-8 -*-
"""
KOSPI and KOSDAQ market data collection with pykrx.
"""
import logging
from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]
KRX_COLUMN_RENAME = {
    "\uc2dc\uac00": "open",
    "\uace0\uac00": "high",
    "\uc800\uac00": "low",
    "\uc885\uac00": "close",
    "\uac70\ub798\ub7c9": "volume",
}


def get_market_date(offset: int = 0) -> str:
    """Return YYYYMMDD with a signed day offset."""
    date = datetime.today() + timedelta(days=offset)
    return date.strftime("%Y%m%d")


def get_latest_market_date(lookback_days: int = 10) -> str:
    """Return the most recent date that pykrx can resolve for market data."""
    for offset in range(0, lookback_days + 1):
        date = get_market_date(-offset)
        logger.info("Trying KR market date lookup: %s", date)
        try:
            kospi = stock.get_market_ticker_list(date=date, market="KOSPI")
            kosdaq = stock.get_market_ticker_list(date=date, market="KOSDAQ")
            if kospi or kosdaq:
                logger.info(
                    "Resolved KR market date: %s (KOSPI=%d, KOSDAQ=%d)",
                    date,
                    len(kospi),
                    len(kosdaq),
                )
                return date
        except Exception as exc:
            logger.warning("Market date lookup failed for %s (%s)", date, exc)

    raise RuntimeError("Could not resolve a valid KR market date from pykrx")


def get_all_tickers() -> list[str]:
    """Return all KOSPI and KOSDAQ tickers."""
    market_date = get_latest_market_date()
    kospi = stock.get_market_ticker_list(date=market_date, market="KOSPI")
    kosdaq = stock.get_market_ticker_list(date=market_date, market="KOSDAQ")
    logger.info("Using KR market date: %s", market_date)
    return sorted(set(kospi + kosdaq))


def get_ohlcv(ticker: str, name: str = "", days: int = 30) -> pd.DataFrame:
    """
    Return an OHLCV DataFrame for a single ticker.
    Columns: open, high, low, close, volume
    """
    label = f"{name}({ticker})" if name else ticker
    end = get_latest_market_date()
    start = (datetime.strptime(end, "%Y%m%d") - timedelta(days=days * 2)).strftime("%Y%m%d")

    try:
        df = stock.get_market_ohlcv_by_date(start, end, ticker)

        if df is None or df.empty:
            logger.warning("%s: no OHLCV data returned - skipped", label)
            return pd.DataFrame()

        df = df.rename(columns=KRX_COLUMN_RENAME)
        df.columns = [str(col).lower() for col in df.columns]

        missing = set(REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            logger.warning("%s: missing columns %s - skipped", label, sorted(missing))
            return pd.DataFrame()

        df = df[REQUIRED_COLUMNS].copy()
        df = df.apply(pd.to_numeric, errors="coerce").dropna().sort_index()

        if df.empty:
            logger.warning("%s: OHLCV became empty after cleanup - skipped", label)
            return pd.DataFrame()

        if len(df) < 21:
            logger.warning("%s: insufficient OHLCV rows (%d) - skipped", label, len(df))
            return pd.DataFrame()

        return df.tail(days)

    except Exception as exc:
        logger.warning("%s: pykrx fetch error - skipped (%s)", label, exc)
        return pd.DataFrame()


def get_ticker_name(ticker: str) -> str:
    """Return the ticker name from pykrx."""
    try:
        return stock.get_market_ticker_name(ticker)
    except Exception as exc:
        logger.warning("%s: ticker name lookup failed (%s)", ticker, exc)
        return ticker


def get_all_stocks_data(days: int = 25) -> dict:
    """
    Collect OHLCV data for all KOSPI and KOSDAQ stocks.
    Returns: {ticker: {"name": str, "ohlcv": DataFrame}}
    """
    tickers = get_all_tickers()
    result = {}

    for ticker in tickers:
        try:
            name = get_ticker_name(ticker)
            df = get_ohlcv(ticker, name=name, days=days)

            if df is None or df.empty:
                continue

            result[ticker] = {
                "name": name,
                "ohlcv": df,
            }
        except Exception as exc:
            logger.warning("%s: stock data build error - skipped (%s)", ticker, exc)

    logger.info("KR market data collection done: %d / %d stocks", len(result), len(tickers))
    return result
