# -*- coding: utf-8 -*-
"""
NASDAQ-100 data collection with yfinance.
"""
import logging

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

NASDAQ100_WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"
REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]


def fetch_nasdaq100_tickers() -> list[str]:
    """Parse NASDAQ-100 constituent tickers from Wikipedia."""
    try:
        tables = pd.read_html(NASDAQ100_WIKI_URL)
        for table in tables:
            for col in table.columns:
                if str(col).strip().lower() in ("ticker", "symbol"):
                    tickers = table[col].dropna().astype(str).str.strip().tolist()
                    tickers = [ticker for ticker in tickers if ticker.isalpha()]
                    if len(tickers) >= 90:
                        logger.info("NASDAQ-100: parsed %d tickers from Wikipedia", len(tickers))
                        return tickers
        raise ValueError("Ticker column not found")
    except Exception as exc:
        logger.error("Wikipedia parse failed: %s - using fallback list", exc)
        return _FALLBACK_TICKERS


_FALLBACK_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
    "NFLX", "ADBE", "AMD", "QCOM", "INTU", "CSCO", "TMUS", "AMGN", "PEP", "TXN",
    "AMAT", "HON", "BKNG", "ISRG", "VRTX", "REGN", "PANW", "ADP", "GILD", "SBUX",
    "MU", "LRCX", "ADI", "MDLZ", "MELI", "KLAC", "SNPS", "CDNS", "ASML", "MAR",
    "PYPL", "CRWD", "CEG", "CTAS", "FTNT", "ABNB", "NXPI", "MRVL", "ORLY", "PCAR",
    "CPRT", "AZN", "ROST", "MNST", "PAYX", "DXCM", "ODFL", "KDP", "FAST", "VRSK",
    "CTSH", "BIIB", "GEHC", "EA", "IDXX", "BKR", "EXC", "CCEP", "TTWO", "ON",
    "XEL", "ZS", "TEAM", "DDOG", "FANG", "GFS", "ILMN", "MDB", "ALGN", "ENPH",
    "MRNA", "PDD", "CDW", "EBAY", "ANSS", "LULU", "WDAY", "CHTR", "MSTR", "AXON",
    "CSGP", "SMCI", "TTD", "COIN", "ARM", "DASH", "PLTR", "ROP", "FICO", "IDXX",
]


def get_ohlcv(ticker: str, days: int = 30) -> pd.DataFrame:
    """
    Return an OHLCV DataFrame for a single ticker.
    Columns: open, high, low, close, volume
    """
    try:
        period = f"{days * 2}d"
        df = yf.download(ticker, period=period, progress=False, auto_adjust=True)

        if df is None or df.empty:
            logger.warning("%s: no data returned - skipped", ticker)
            return pd.DataFrame()

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.rename(columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })

        missing = set(REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            logger.warning("%s: missing columns %s - skipped", ticker, sorted(missing))
            return pd.DataFrame()

        df = df[REQUIRED_COLUMNS].copy()
        df = df.apply(pd.to_numeric, errors="coerce").dropna().sort_index()

        if len(df) < 21:
            logger.warning("%s: insufficient OHLCV rows (%d) - skipped", ticker, len(df))
            return pd.DataFrame()

        return df.tail(days)
    except Exception as exc:
        logger.warning("%s: yfinance error - skipped (%s)", ticker, exc)
        return pd.DataFrame()


def get_all_stocks_data(days: int = 25) -> dict:
    """
    Collect OHLCV data for all NASDAQ-100 stocks.
    Returns: {ticker: {"name": str, "ohlcv": DataFrame}}
    """
    tickers = fetch_nasdaq100_tickers()
    result = {}

    for ticker in tickers:
        try:
            df = get_ohlcv(ticker, days=days)
            if df is None or df.empty:
                continue

            result[ticker] = {
                "name": ticker,
                "ohlcv": df,
            }
        except Exception as exc:
            logger.warning("%s: stock data build error - skipped (%s)", ticker, exc)

    logger.info("US market data collection done: %d / %d stocks", len(result), len(tickers))
    return result
