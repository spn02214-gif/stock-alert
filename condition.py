# -*- coding: utf-8 -*-
"""
Buy condition logic

Conditions:
1. Price dropped >= 5% from previous close
2. Bullish candle on the day (close > open)
3. Volume >= 2x the 5-day average volume
4. Current price > 20-day moving average
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


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
            "pass": bool,               # True if all four conditions met
            "cond1_drop": float,        # % change from previous close
            "cond2_bullish": bool,      # bullish candle
            "cond3_vol_ratio": float,   # volume / 5-day average
            "cond4_above_ma20": bool,   # price above MA20
            "current_price": float,
            "ma20": float,
        }
    """
    if ohlcv is None or ohlcv.empty or len(ohlcv) < 21:
        return {"pass": False}

    today = ohlcv.iloc[-1]
    yesterday = ohlcv.iloc[-2]

    # Condition 1: price dropped >= 5% from previous close
    prev_close = float(yesterday["close"])
    cur_close = float(today["close"])
    drop_pct = (cur_close - prev_close) / prev_close * 100
    cond1 = drop_pct <= -5.0

    # Condition 2: bullish candle (close > open)
    cond2 = float(today["close"]) > float(today["open"])

    # Condition 3: volume >= 2x the 5-day average (excluding today)
    vol_5d_avg = float(ohlcv.iloc[-6:-1]["volume"].mean())
    cur_vol = float(today["volume"])
    vol_ratio = cur_vol / vol_5d_avg if vol_5d_avg > 0 else 0.0
    cond3 = vol_ratio >= 2.0

    # Condition 4: current price > 20-day moving average (excluding today)
    ma20 = float(ohlcv["close"].iloc[-21:-1].mean())
    cond4 = cur_close > ma20

    all_pass = cond1 and cond2 and cond3 and cond4

    return {
        "pass": all_pass,
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
            result = check_conditions(data["ohlcv"])
            if result.get("pass"):
                results.append({
                    "ticker": ticker,
                    "name": data.get("name", ticker),
                    **result,
                })
        except Exception as e:
            logger.warning("%s: condition check error — skipped (%s)", ticker, e)
    return results
