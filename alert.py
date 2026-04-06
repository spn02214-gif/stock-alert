"""
텔레그램 알림 전송
"""
import os
import requests
import logging

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "8789518786")

TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

logger = logging.getLogger(__name__)


def send_message(text: str) -> bool:
    """텔레그램 메시지 전송. 성공 시 True 반환."""
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }
    try:
        resp = requests.post(TELEGRAM_URL, data=payload, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error("텔레그램 전송 실패: %s", e)
        return False


def format_kr_alert(stock: dict) -> str:
    return (
        f"🇰🇷 <b>[한국 매수 신호]</b>\n"
        f"종목: {stock['name']} ({stock['ticker']})\n"
        f"현재가: {stock['current_price']:,.0f}원\n"
        f"전일 대비: {stock['cond1_drop']:+.2f}%\n"
        f"양봉: {'✅' if stock['cond2_bullish'] else '❌'}\n"
        f"거래량 배수: {stock['cond3_vol_ratio']:.1f}x\n"
        f"MA20 대비: {'✅ 위' if stock['cond4_above_ma20'] else '❌ 아래'} (MA20={stock['ma20']:,.0f})"
    )


def format_us_alert(stock: dict) -> str:
    return (
        f"🇺🇸 <b>[미국 매수 신호]</b>\n"
        f"종목: {stock['name']} ({stock['ticker']})\n"
        f"현재가: ${stock['current_price']:.2f}\n"
        f"전일 대비: {stock['cond1_drop']:+.2f}%\n"
        f"양봉: {'✅' if stock['cond2_bullish'] else '❌'}\n"
        f"거래량 배수: {stock['cond3_vol_ratio']:.1f}x\n"
        f"MA20 대비: {'✅ 위' if stock['cond4_above_ma20'] else '❌ 아래'} (MA20=${stock['ma20']:.2f})"
    )


def send_kr_alerts(stocks: list) -> None:
    if not stocks:
        logger.info("한국 조건 충족 종목 없음")
        return
    for s in stocks:
        msg = format_kr_alert(s)
        send_message(msg)
        logger.info("한국 알림 전송: %s (%s)", s['name'], s['ticker'])


def send_us_alerts(stocks: list) -> None:
    if not stocks:
        logger.info("미국 조건 충족 종목 없음")
        return
    for s in stocks:
        msg = format_us_alert(s)
        send_message(msg)
        logger.info("미국 알림 전송: %s (%s)", s['name'], s['ticker'])


def send_scan_start(market: str) -> None:
    send_message(f"🔍 {market} 스캔 시작 ({__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')})")


def send_no_signal(market: str) -> None:
    send_message(f"📭 {market} 조건 충족 종목 없음 ({__import__('datetime').datetime.now().strftime('%H:%M')})")
