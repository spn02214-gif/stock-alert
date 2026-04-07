# -*- coding: utf-8 -*-
"""
KOSPI and KOSDAQ market data collection with pykrx.
Limited to top 300 stocks by market cap.
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
TOP_N = 300


def _today() -> str:
    return datetime.today().strftime("%Y%m%d")


def _start_date(days: int) -> str:
    return (datetime.today() - timedelta(days=days)).strftime("%Y%m%d")


def _get_recent_trading_date() -> str | None:
    """Return the most recent date (up to 5 days back) with available KOSPI market cap data."""
    for i in range(5):
        date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
        try:
            df = stock.get_market_cap_by_ticker(date, market="KOSPI")
            if df is not None and not df.empty:
                return date
        except Exception:
            continue
    return None


def get_top_tickers_by_market_cap(n: int = TOP_N) -> list[tuple[str, str]]:
    """
    Return (ticker, name) pairs for the top n stocks by market cap
    across KOSPI and KOSDAQ, using the most recent available trading date.
    """
    trading_date = _get_recent_trading_date()
    if trading_date is None:
        logger.error("Market cap: no trading date found within last 5 days")
        return []

    logger.info("Market cap: using date %s", trading_date)
    frames = []

    for market in ("KOSPI", "KOSDAQ"):
        try:
            df = stock.get_market_cap_by_ticker(trading_date, market=market)
            if df is None or df.empty:
                logger.warning("%s market cap data empty - skipped", market)
                continue
            frames.append(df[["시가총액"]])
        except Exception as exc:
            logger.warning("%s market cap fetch failed - skipped (%s)", market, exc)

    if not frames:
        logger.error("No market cap data available from pykrx")
        return []

    combined = (
        pd.concat(frames)
        .sort_values("시가총액", ascending=False)
        .head(n)
    )

    result = []
    for ticker in combined.index:
        try:
            name = stock.get_market_ticker_name(ticker)
        except Exception:
            name = ticker
        result.append((ticker, name))

    logger.info("Top %d tickers by market cap loaded (%d returned)", n, len(result))
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
    Collect OHLCV data for top 300 KOSPI + KOSDAQ stocks by market cap.
    Returns: {ticker: {"name": str, "ohlcv": DataFrame}}
    """
    tickers = get_top_tickers_by_market_cap()
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
