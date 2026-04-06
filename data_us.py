"""
나스닥100 데이터 수집 (yfinance)
"""
import yfinance as yf
import pandas as pd

# 나스닥100 구성 종목 (2024년 기준 주요 100종목)
NASDAQ100_TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
    "NFLX", "AMD", "ADBE", "QCOM", "PEP", "INTC", "INTU", "CSCO", "CMCSA", "TMUS",
    "AMGN", "TXN", "AMAT", "HON", "BKNG", "ISRG", "VRTX", "REGN", "PANW", "ADP",
    "GILD", "SBUX", "MU", "LRCX", "ADI", "MDLZ", "MELI", "KLAC", "SNPS", "CDNS",
    "ASML", "MAR", "PYPL", "CRWD", "CEG", "CTAS", "FTNT", "ABNB", "NXPI", "MRVL",
    "ORLY", "PCAR", "CPRT", "AZN", "ROST", "MNST", "PAYX", "DXCM", "ODFL", "KDP",
    "FAST", "VRSK", "CTSH", "BIIB", "GEHC", "EA", "IDXX", "BKR", "EXC", "CCEP",
    "TTWO", "ON", "XEL", "ZS", "TEAM", "DDOG", "FANG", "GFS", "WBD", "ILMN",
    "MDB", "ALGN", "LCID", "SIRI", "DLTR", "WBA", "MTCH", "RIVN", "ENPH", "MRNA",
    "PDD", "OKTA", "SPLK", "CDW", "EBAY", "ANSS", "LULU", "WDAY", "SGEN", "CHTR",
]


def get_ohlcv(ticker: str, days: int = 30) -> pd.DataFrame:
    """
    단일 종목 OHLCV 반환
    컬럼: open, high, low, close, volume
    """
    try:
        period = f"{days * 2}d"  # 영업일 여유분
        df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
        if df.empty:
            return pd.DataFrame()
        df = df.rename(columns={
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume"
        })
        df = df[["open", "high", "low", "close", "volume"]].dropna()
        # MultiIndex 평탄화 (yfinance 0.2+)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df.tail(days)
    except Exception:
        return pd.DataFrame()


def get_all_stocks_data(days: int = 25) -> dict:
    """
    나스닥100 전종목 데이터 수집
    반환: {ticker: {"name": str, "ohlcv": DataFrame}}
    """
    result = {}
    for ticker in NASDAQ100_TICKERS:
        df = get_ohlcv(ticker, days=days)
        if len(df) >= 21:
            result[ticker] = {
                "name": ticker,
                "ohlcv": df,
            }
    return result
