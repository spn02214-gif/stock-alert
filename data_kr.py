# -*- coding: utf-8 -*-
"""
KOSPI and KOSDAQ market data collection with pykrx.
Covers KOSPI200 + KOSDAQ150 index constituents.
"""
import logging
from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]
KRX_COLUMN_RENAME = {
    "시가": "open",
    "고가": "high",
    "저가": "low",
    "종가": "close",
    "거래량": "volume",
}

# pykrx index codes
KOSPI200_CODE = "1028"
KOSDAQ150_CODE = "2203"


def _today() -> str:
    return datetime.today().strftime("%Y%m%d")


def _start_date(days: int) -> str:
    return (datetime.today() - timedelta(days=days)).strftime("%Y%m%d")


def get_index_tickers() -> list[tuple[str, str]]:
    """
    Return (ticker, name) pairs for KOSPI200 + KOSDAQ150 constituents.
    Falls back to full KOSPI + KOSDAQ listing if index fetch fails.
    """
    raw_tickers: list[str] = []

    for code, label in ((KOSPI200_CODE, "KOSPI200"), (KOSDAQ150_CODE, "KOSDAQ150")):
        try:
            constituents = stock.get_index_portfolio_deposit_file(code)
            if constituents:
                logger.info("%s: %d tickers loaded", label, len(constituents))
                raw_tickers.extend(constituents)
            else:
                logger.warning("%s: empty result - skipped", label)
        except Exception as exc:
            logger.warning("%s: fetch failed - skipped (%s)", label, exc)

    if not raw_tickers:
        logger.warning("Index fetch failed, falling back to get_market_ticker_list")
        try:
            kospi = stock.get_market_ticker_list(market="KOSPI")
            kosdaq = stock.get_market_ticker_list(market="KOSDAQ")
            raw_tickers = list(kospi) + list(kosdaq)
            logger.info("Fallback: %d tickers loaded", len(raw_tickers))
        except Exception as exc:
            logger.error("Fallback ticker list fetch failed: %s", exc)
            return []

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for t in raw_tickers:
        if t not in seen:
            seen.add(t)
            unique.append(t)

    result: list[tuple[str, str]] = []
    for ticker in unique:
        try:
            name = stock.get_market_ticker_name(ticker)
        except Exception:
            name = ticker
        result.append((ticker, name))

    logger.info("KR ticker list ready: %d stocks (KOSPI200 + KOSDAQ150)", len(result))
    return result


def get_ohlcv(ticker: str, name: str = "", days: int = 30) -> pd.DataFrame:
    """
    Return an OHLCV DataFrame for a single ticker.
    Columns: open, high, low, close, volume
    """
    label = f"{name}({ticker})" if name else ticker
    start = _start_date(days * 2)  # extra buffer for trading days
    end = _today()

    try:
        df = stock.get_market_ohlcv_by_date(start, end, ticker)

        if df is None or df.empty:
            logger.warning("%s: no data returned - skipped", label)
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
            logger.warning("%s: OHLCV empty after cleanup - skipped", label)
            return pd.DataFrame()

        if len(df) < 21:
            logger.warning("%s: insufficient rows (%d) - skipped", label, len(df))
            return pd.DataFrame()

        return df.tail(days)

    except Exception as exc:
        logger.warning("%s: pykrx error - skipped (%s)", label, exc)
        return pd.DataFrame()


def get_all_stocks_data(days: int = 25) -> dict:
    """
    Collect OHLCV data for KOSPI200 + KOSDAQ150 stocks.
    Returns: {ticker: {"name": str, "ohlcv": DataFrame}}
    """
    tickers = get_index_tickers()
    result = {}

    for ticker, name in tickers:
        try:
            df = get_ohlcv(ticker, name=name, days=days)
            if df is None or df.empty:
                continue
            result[ticker] = {"name": name, "ohlcv": df}
        except Exception as exc:
            logger.warning("%s: stock data build error - skipped (%s)", ticker, exc)

    logger.info("KR market data collection done: %d / %d stocks", len(result), len(tickers))
    return result
