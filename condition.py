"""
매수 조건 판단 로직

조건:
1. 전일 대비 -5% 이상 하락
2. 당일 양봉 (종가 > 시가)
3. 거래량 5일 평균 × 2배 이상
4. 현재가 > 20일 이동평균선
"""
import pandas as pd


def check_conditions(ohlcv: pd.DataFrame) -> dict:
    """
    조건 검사 후 결과 반환.

    Parameters
    ----------
    ohlcv : DataFrame
        컬럼 [open, high, low, close, volume], 최소 21행 필요

    Returns
    -------
    dict
        {
            "pass": bool,           # 4개 조건 모두 충족 여부
            "cond1_drop": float,    # 전일 대비 등락률(%)
            "cond2_bullish": bool,  # 양봉 여부
            "cond3_vol_ratio": float, # 거래량 / 5일 평균 배수
            "cond4_above_ma20": bool, # 현재가 > MA20
            "current_price": float,
            "ma20": float,
        }
    """
    if len(ohlcv) < 21:
        return {"pass": False}

    today = ohlcv.iloc[-1]
    yesterday = ohlcv.iloc[-2]

    # 1. 전일 대비 -5% 이상 하락
    prev_close = float(yesterday["close"])
    cur_close = float(today["close"])
    drop_pct = (cur_close - prev_close) / prev_close * 100
    cond1 = drop_pct <= -5.0

    # 2. 당일 양봉 (종가 > 시가)
    cond2 = float(today["close"]) > float(today["open"])

    # 3. 거래량 5일 평균 × 2배 이상
    #    오늘 포함 이전 5거래일 평균 (오늘 제외 5일)
    vol_5d_avg = float(ohlcv.iloc[-6:-1]["volume"].mean())
    cur_vol = float(today["volume"])
    vol_ratio = cur_vol / vol_5d_avg if vol_5d_avg > 0 else 0.0
    cond3 = vol_ratio >= 2.0

    # 4. 현재가 > 20일 이동평균선
    ma20 = float(ohlcv["close"].iloc[-21:-1].mean())  # 오늘 제외 20일 평균
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
    전체 종목 데이터에서 조건 충족 종목만 필터링.

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
        result = check_conditions(data["ohlcv"])
        if result.get("pass"):
            results.append({
                "ticker": ticker,
                "name": data.get("name", ticker),
                **result,
            })
    return results
