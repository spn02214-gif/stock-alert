# -*- coding: utf-8 -*-
"""
NASDAQ-100 data collection (yfinance)
"""
import logging
import yfinance as yf
import pandas as pd

logger = logging.getLogger(__name__)

NASDAQ100_WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"


def fetch_nasdaq100_tickers() -> list[str]:
    """Parse NASDAQ-100 constituent tickers from Wikipedia."""
    try:
        tables = pd.read_html(NASDAQ100_WIKI_URL)
        # Find the table that has a 'Ticker' or 'Symbol' column
        for table in tables:
            for col in table.columns:
                if str(col).strip().lower() in ("ticker", "symbol"):
                    tickers = table[col].dropna().str.strip().tolist()
                    tickers = [t for t in tickers if isinstance(t, str) and t.isalpha()]
                    if len(tickers) >= 90:
                        logger.info("NASDAQ-100: parsed %d tickers from Wikipedia", len(tickers))
                        return tickers
        raise ValueError("ticker column not found")
    except Exception as e:
        logger.error("Wikipedia parse failed: %s — using fallback list", e)
        return _FALLBACK_TICKERS


# Fallback list (as of 2025) used when Wikipedia parsing fails
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
    Return OHLCV DataFrame for a single ticker (most recent `days` trading days).
    Columns: open, high, low, close, volume
    """
    try:
        period = f"{days * 2}d"  # extra buffer for trading days
        df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
        if df.empty:
            logger.warning("%s: no data returned — skipped", ticker)
            return pd.DataFrame()
        # Flatten MultiIndex columns (yfinance 0.2+)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.rename(columns={
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume"
        })
        df = df[["open", "high", "low", "close", "volume"]].dropna()
        return df.tail(days)
    except Exception as e:
        logger.warning("%s: yfinance error — skipped (%s)", ticker, e)
        return pd.DataFrame()


def get_all_stocks_data(days: int = 25) -> dict:
    """
    Collect OHLCV data for all NASDAQ-100 stocks.
    Returns: {ticker: {"name": str, "ohlcv": DataFrame}}
    """
    tickers = fetch_nasdaq100_tickers()
    result = {}
    for ticker in tickers:
        df = get_ohlcv(ticker, days=days)
        if len(df) >= 21:
            result[ticker] = {
                "name": ticker,
                "ohlcv": df,
            }
    logger.info("US market data collection done: %d / %d stocks", len(result), len(tickers))
    return result
