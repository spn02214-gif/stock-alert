# -*- coding: utf-8 -*-
"""
Telegram alert sender.
"""
import logging
import os
from datetime import datetime

import requests

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "8789518786")
TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

logger = logging.getLogger(__name__)


def send_message(text: str) -> bool:
    """Send a Telegram message. Returns True on success."""
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }
    try:
        resp = requests.post(TELEGRAM_URL, data=payload, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        logger.error("Telegram send failed: %s", exc)
        return False


def format_kr_alert(stock: dict) -> str:
    """Format a Korean market buy signal alert."""
    return (
        "<b>[KR Buy Signal]</b>\n"
        f"Stock: {stock['name']} ({stock['ticker']})\n"
        f"Price: {stock['current_price']:,.0f} KRW\n"
        f"Change: {stock['cond1_drop']:+.2f}%\n"
        f"Bullish: {'Yes' if stock['cond2_bullish'] else 'No'}\n"
        f"Vol ratio: {stock['cond3_vol_ratio']:.1f}x\n"
        f"vs MA20: {'above' if stock['cond4_above_ma20'] else 'below'} "
        f"(MA20={stock['ma20']:,.0f})"
    )


def format_us_alert(stock: dict) -> str:
    """Format a US market buy signal alert."""
    return (
        "<b>[US Buy Signal]</b>\n"
        f"Stock: {stock['name']} ({stock['ticker']})\n"
        f"Price: ${stock['current_price']:.2f}\n"
        f"Change: {stock['cond1_drop']:+.2f}%\n"
        f"Bullish: {'Yes' if stock['cond2_bullish'] else 'No'}\n"
        f"Vol ratio: {stock['cond3_vol_ratio']:.1f}x\n"
        f"vs MA20: {'above' if stock['cond4_above_ma20'] else 'below'} "
        f"(MA20=${stock['ma20']:.2f})"
    )


def send_kr_alerts(stocks: list) -> None:
    """Send Korean market alerts."""
    if not stocks:
        logger.info("KR market: no stocks met conditions")
        return

    for stock in stocks:
        msg = format_kr_alert(stock)
        send_message(msg)
        logger.info("KR alert sent: %s (%s)", stock["name"], stock["ticker"])


def send_us_alerts(stocks: list) -> None:
    """Send US market alerts."""
    if not stocks:
        logger.info("US market: no stocks met conditions")
        return

    for stock in stocks:
        msg = format_us_alert(stock)
        send_message(msg)
        logger.info("US alert sent: %s (%s)", stock["name"], stock["ticker"])


def send_scan_start(market: str) -> None:
    """Send a scan start notification."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    send_message(f"{market} scan started ({timestamp})")


def send_no_signal(market: str) -> None:
    """Send a no-signal notification."""
    timestamp = datetime.now().strftime("%H:%M")
    send_message(f"{market} scan finished with no signals ({timestamp})")
