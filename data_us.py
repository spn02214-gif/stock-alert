"""
나스닥100 데이터 수집 (yfinance)
"""
import logging
import yfinance as yf
import pandas as pd

logger = logging.getLogger(__name__)

NASDAQ100_WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"


def fetch_nasdaq100_tickers() -> list[str]:
    """Wikipedia에서 나스닥100 구성 종목 티커 파싱."""
    try:
        tables = pd.read_html(NASDAQ100_WIKI_URL)
        # 'Ticker' 또는 'Symbol' 컬럼을 가진 테이블 탐색
        for table in tables:
            for col in table.columns:
                if str(col).strip().lower() in ("ticker", "symbol"):
                    tickers = table[col].dropna().str.strip().tolist()
                    tickers = [t for t in tickers if isinstance(t, str) and t.isalpha()]
                    if len(tickers) >= 90:
                        logger.info("나스닥100 종목 %d개 파싱 완료 (Wikipedia)", len(tickers))
                        return tickers
        raise ValueError("티커 컬럼을 찾을 수 없음")
    except Exception as e:
        logger.error("Wikipedia 파싱 실패: %s — 폴백 리스트 사용", e)
        return _FALLBACK_TICKERS


# Wikipedia 파싱 실패 시 사용할 폴백 (2025년 기준)
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
    단일 종목 OHLCV 반환
    컬럼: open, high, low, close, volume
    """
    try:
        period = f"{days * 2}d"  # 영업일 여유분
        df = yf.download(ticker, period=period, progress=False, auto_adjust=True)
        if df.empty:
            logger.warning("%s: 데이터 없음 — 스킵", ticker)
            return pd.DataFrame()
        # MultiIndex 평탄화 (yfinance 0.2+)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.rename(columns={
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume"
        })
        df = df[["open", "high", "low", "close", "volume"]].dropna()
        return df.tail(days)
    except Exception as e:
        logger.warning("%s: yfinance 오류 — 스킵 (%s)", ticker, e)
        return pd.DataFrame()


def get_all_stocks_data(days: int = 25) -> dict:
    """
    나스닥100 전종목 데이터 수집
    반환: {ticker: {"name": str, "ohlcv": DataFrame}}
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
    logger.info("나스닥100 데이터 수집 완료: %d / %d 종목", len(result), len(tickers))
    return result
