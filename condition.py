# -*- coding: utf-8 -*-
"""
Buy condition logic.

Conditions:
1. Price dropped >= 5% from previous close
2. Bullish candle on the day (close > open)
3. Volume >= 2x the 5-day average volume
4. Current price > 20-day moving average
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]


def _empty_result() -> dict:
    """Return a consistent result shape for skipped symbols."""
    return {
        "pass": False,
        "cond1_drop": None,
        "cond2_bullish": False,
        "cond3_vol_ratio": None,
        "cond4_above_ma20": False,
        "current_price": None,
        "ma20": None,
    }


def check_conditions(ohlcv: pd.DataFrame) -> dict:
    """
    Evaluate all four buy conditions against the given OHLCV data.

    Parameters
    ----------
    ohlcv : DataFrame
        Columns [open, high, low, close, volume], minimum 21 rows required

    Returns
    -------
    dict
        {
            "pass": bool,
            "cond1_drop": float | None,
            "cond2_bullish": bool,
            "cond3_vol_ratio": float | None,
            "cond4_above_ma20": bool,
            "current_price": float | None,
            "ma20": float | None,
        }
    """
    if ohlcv is None or not isinstance(ohlcv, pd.DataFrame) or ohlcv.empty:
        return _empty_result()

    missing = set(REQUIRED_COLUMNS) - set(ohlcv.columns)
    if missing:
        logger.warning("Condition check skipped: missing columns %s", sorted(missing))
        return _empty_result()

    df = ohlcv[REQUIRED_COLUMNS].copy()
    df = df.apply(pd.to_numeric, errors="coerce").dropna().sort_index()

    if len(df) < 21:
        return _empty_result()

    today = df.iloc[-1]
    yesterday = df.iloc[-2]

    prev_close = float(yesterday["close"])
    cur_close = float(today["close"])

    if prev_close <= 0:
        logger.warning("Condition check skipped: invalid previous close (%s)", prev_close)
        return _empty_result()

    drop_pct = (cur_close - prev_close) / prev_close * 100.0
    cond1 = drop_pct <= -5.0

    cond2 = float(today["close"]) > float(today["open"])

    vol_5d_avg = float(df.iloc[-6:-1]["volume"].mean())
    cur_vol = float(today["volume"])
    vol_ratio = cur_vol / vol_5d_avg if vol_5d_avg > 0 else 0.0
    cond3 = vol_ratio >= 2.0

    ma20 = float(df["close"].iloc[-21:-1].mean())
    cond4 = cur_close > ma20 if ma20 > 0 else False

    return {
        "pass": cond1 and cond2 and cond3 and cond4,
        "cond1_drop": round(drop_pct, 2),
        "cond2_bullish": cond2,
        "cond3_vol_ratio": round(vol_ratio, 2),
        "cond4_above_ma20": cond4,
        "current_price": cur_close,
        "ma20": round(ma20, 2),
    }


def filter_stocks(stocks_data: dict) -> list:
    """
    Filter stocks that satisfy all buy conditions.

    Parameters
    ----------
    stocks_data : dict
        {ticker: {"name": str, "ohlcv": DataFrame}}

    Returns
    -------
    list of dict
        [{"ticker": str, "name": str, **condition_result}, ...]
    """
    results = []

    for ticker, data in stocks_data.items():
        try:
            ohlcv = data.get("ohlcv")
            if ohlcv is None or not isinstance(ohlcv, pd.DataFrame) or ohlcv.empty:
                logger.warning("%s: empty OHLCV - skipped", ticker)
                continue

            result = check_conditions(ohlcv)
            if result.get("pass"):
                results.append({
                    "ticker": ticker,
                    "name": data.get("name", ticker),
                    **result,
                })
        except Exception as exc:
            logger.warning("%s: condition check error - skipped (%s)", ticker, exc)

    return results
