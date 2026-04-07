# -*- coding: utf-8 -*-
"""
KOSPI + KOSDAQ full market data collection (pykrx)
"""
import logging
from pykrx import stock
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"open", "high", "low", "close", "volume"}


def get_market_date(offset=0):
    """Return date string with optional day offset (0=today, -1=yesterday, ...)"""
    date = datetime.today() - timedelta(days=abs(offset))
    return date.strftime("%Y%m%d")


def get_all_tickers():
    """Return all KOSPI + KOSDAQ tickers"""
    kospi = stock.get_market_ticker_list(market="KOSPI")
    kosdaq = stock.get_market_ticker_list(market="KOSDAQ")
    return list(set(kospi + kosdaq))


def get_ohlcv(ticker: str, name: str = "", days: int = 30) -> pd.DataFrame:
    """
    Return OHLCV DataFrame for a single ticker (most recent `days` trading days).
    Columns: open, high, low, close, volume
    """
    label = f"{name}({ticker})" if name else ticker
    end = datetime.today().strftime("%Y%m%d")
    start = (datetime.today() - timedelta(days=days * 2)).strftime("%Y%m%d")  # extra buffer for trading days
    try:
        df = stock.get_market_ohlcv_by_date(start, end, ticker)

        # Check for None or empty DataFrame
        if df is None or df.empty:
            logger.warning("%s: no data returned — skipped", label)
            return pd.DataFrame()

        # Normalize column names
        df.columns = [c.lower() for c in df.columns]
        rename = {"시가": "open", "고가": "high", "저가": "low",
                  "종가": "close", "거래량": "volume"}
        df = df.rename(columns=rename)

        # Check required columns
        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            logger.warning("%s: missing columns %s — skipped", label, missing)
            return pd.DataFrame()

        df = df[["open", "high", "low", "close", "volume"]].dropna()

        # Check minimum row count (21 rows needed for MA20)
        if len(df) < 21:
            logger.warning("%s: insufficient data (%d rows) — skipped", label, len(df))
            return pd.DataFrame()

        return df.tail(days)

    except Exception as e:
        logger.warning("%s: pykrx error — skipped (%s)", label, e)
        return pd.DataFrame()


def get_ticker_name(ticker: str) -> str:
    try:
        return stock.get_market_ticker_name(ticker)
    except Exception:
        return ticker


def get_all_stocks_data(days: int = 25) -> dict:
    """
    Collect OHLCV data for all KOSPI + KOSDAQ stocks.
    Returns: {ticker: {"name": str, "ohlcv": DataFrame}}
    """
    tickers = get_all_tickers()
    result = {}
    for ticker in tickers:
        name = get_ticker_name(ticker)
        df = get_ohlcv(ticker, name=name, days=days)
        if not df.empty:
            result[ticker] = {
                "name": name,
                "ohlcv": df,
            }
    logger.info("KR market data collection done: %d / %d stocks", len(result), len(tickers))
    return result
