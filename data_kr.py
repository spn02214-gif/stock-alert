"""
KOSPI + KOSDAQ 전종목 데이터 수집 (pykrx)
"""
from pykrx import stock
from datetime import datetime, timedelta
import pandas as pd


def get_market_date(offset=0):
    """영업일 기준 날짜 반환 (offset: 0=오늘, -1=어제, ...)"""
    date = datetime.today() - timedelta(days=abs(offset))
    return date.strftime("%Y%m%d")


def get_all_tickers():
    """KOSPI + KOSDAQ 전종목 티커 반환"""
    kospi = stock.get_market_ticker_list(market="KOSPI")
    kosdaq = stock.get_market_ticker_list(market="KOSDAQ")
    return list(set(kospi + kosdaq))


def get_ohlcv(ticker: str, days: int = 30) -> pd.DataFrame:
    """
    단일 종목 OHLCV 반환 (최근 days일)
    컬럼: open, high, low, close, volume
    """
    end = datetime.today().strftime("%Y%m%d")
    start = (datetime.today() - timedelta(days=days * 2)).strftime("%Y%m%d")  # 영업일 여유분
    try:
        df = stock.get_market_ohlcv_by_date(start, end, ticker)
        df.columns = [c.lower() for c in df.columns]
        # 컬럼명 정규화
        rename = {"시가": "open", "고가": "high", "저가": "low",
                  "종가": "close", "거래량": "volume"}
        df = df.rename(columns=rename)
        df = df[["open", "high", "low", "close", "volume"]].dropna()
        return df.tail(days)
    except Exception:
        return pd.DataFrame()


def get_ticker_name(ticker: str) -> str:
    try:
        return stock.get_market_ticker_name(ticker)
    except Exception:
        return ticker


def get_all_stocks_data(days: int = 25) -> dict:
    """
    KOSPI+KOSDAQ 전종목 데이터 수집
    반환: {ticker: {"name": str, "ohlcv": DataFrame}}
    """
    tickers = get_all_tickers()
    result = {}
    for ticker in tickers:
        df = get_ohlcv(ticker, days=days)
        if len(df) >= 21:  # MA20 계산에 최소 21일 필요
            result[ticker] = {
                "name": get_ticker_name(ticker),
                "ohlcv": df,
            }
    return result
