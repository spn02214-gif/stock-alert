# -*- coding: utf-8 -*-
"""
KOSPI and KOSDAQ market data collection with FinanceDataReader.
Covers top 300 stocks by market cap.
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
TOP_N = 300


def _today() -> str:
    return datetime.today().strftime("%Y-%m-%d")


def _start_date(days: int) -> str:
    return (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")


def get_all_tickers() -> list[tuple[str, str]]:
    """
    Return (ticker, name) pairs from KOSPI + KOSDAQ listings.
    Selects top 300 by market cap if the column is available,
    otherwise returns all tickers.
    """
    frames = []

    for market in ("KOSPI", "KOSDAQ"):
        try:
            listing = fdr.StockListing(market)
            if listing is None or listing.empty:
                logger.warning("%s: listing empty - skipped", market)
                continue

            # FDR version differences: column may be "Code" or "Symbol"
            code_col = next((c for c in listing.columns if c in ("Code", "Symbol")), None)
            if code_col is None:
                logger.warning("%s: no Code/Symbol column (got %s) - skipped",
                               market, list(listing.columns))
                continue

            listing = listing.rename(columns={code_col: "Code"})
            listing["Code"] = listing["Code"].astype(str).str.zfill(6)

            name_col = next((c for c in listing.columns if c == "Name"), None)
            listing["Name"] = listing[name_col].astype(str) if name_col else listing["Code"]

            listing["Market"] = market
            frames.append(listing[["Code", "Name", "Market"] +
                                   (["Marcap"] if "Marcap" in listing.columns else [])])
            logger.info("%s: %d tickers loaded", market, len(listing))

        except Exception as exc:
            logger.warning("%s: listing fetch failed - skipped (%s)", market, exc)

    if not frames:
        logger.error("No KR market listings available")
        return []

    combined = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["Code"])

    # Select top N by market cap if column exists, otherwise use all
    if "Marcap" in combined.columns:
        combined = (combined
                    .sort_values("Marcap", ascending=False)
                    .head(TOP_N))
        logger.info("Selected top %d tickers by market cap", len(combined))
    else:
        logger.info("Marcap column not found, using all %d tickers", len(combined))

    return list(zip(combined["Code"], combined["Name"]))


def get_ohlcv(ticker: str, name: str = "", days: int = 60) -> pd.DataFrame:
    """
    Return an OHLCV DataFrame for a single ticker.
    Columns: open, high, low, close, volume
    """
    label = f"{name}({ticker})" if name else ticker
    start = _start_date(days)
    end = _today()

    try:
        df = fdr.DataReader(ticker, start, end)

        if df is None or df.empty:
            logger.warning("%s: no data returned - skipped", label)
            return pd.DataFrame()

        # Flatten MultiIndex columns if present (some FDR versions)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.rename(columns=FDR_COLUMN_RENAME)
        df.columns = [str(c).lower() for c in df.columns]

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

        return df

    except Exception as exc:
        logger.warning("%s: FDR fetch error - skipped (%s)", label, exc)
        return pd.DataFrame()


def get_all_stocks_data(days: int = 60) -> dict:
    """
    Collect OHLCV data for top 300 KOSPI + KOSDAQ stocks.
    Returns: {ticker: {"name": str, "ohlcv": DataFrame}}
    """
    tickers = get_all_tickers()
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
