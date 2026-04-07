# -*- coding: utf-8 -*-
"""
Scheduler — separates KR market and US market scan times

KR market : 09:00 ~ 15:30 KST, every hour
US market : 23:30 ~ 06:00 KST, every hour
"""
import logging
import schedule
import time
from datetime import datetime
import pytz

import data_kr
import data_us
import condition
import alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("stock_alert.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

KST = pytz.timezone("Asia/Seoul")


def now_kst() -> datetime:
    return datetime.now(KST)


# ────────────────────────────────────────────
# Scan functions
# ────────────────────────────────────────────

def scan_kr():
    """Scan all KOSPI + KOSDAQ stocks."""
    now = now_kst()
    hour = now.hour
    minute = now.minute

    # KR market session: 09:00 ~ 15:30 KST
    in_session = (hour == 9 and minute >= 0) or (10 <= hour <= 14) or (hour == 15 and minute <= 30)
    if not in_session:
        logger.info("Outside KR market hours (%s) — skipped", now.strftime("%H:%M"))
        return

    logger.info("=== KR market scan start ===")
    alert.send_scan_start("KR market (KOSPI+KOSDAQ)")
    try:
        stocks_data = data_kr.get_all_stocks_data()
        signals = condition.filter_stocks(stocks_data)
        logger.info("KR signals found: %d", len(signals))
        if signals:
            alert.send_kr_alerts(signals)
        else:
            alert.send_no_signal("KR market")
    except Exception as e:
        logger.exception("KR market scan error: %s", e)
        alert.send_message(f"⚠️ KR market scan error: {e}")


def scan_us():
    """Scan NASDAQ-100 stocks."""
    now = now_kst()
    hour = now.hour
    minute = now.minute

    # US market session (KST): 23:30 ~ 06:00
    after_open = (hour == 23 and minute >= 30) or hour == 0 or (1 <= hour <= 5) or (hour == 6 and minute == 0)
    if not after_open:
        logger.info("Outside US market hours (%s) — skipped", now.strftime("%H:%M"))
        return

    logger.info("=== US market scan start ===")
    alert.send_scan_start("US market (NASDAQ-100)")
    try:
        stocks_data = data_us.get_all_stocks_data()
        signals = condition.filter_stocks(stocks_data)
        logger.info("US signals found: %d", len(signals))
        if signals:
            alert.send_us_alerts(signals)
        else:
            alert.send_no_signal("US market")
    except Exception as e:
        logger.exception("US market scan error: %s", e)
        alert.send_message(f"⚠️ US market scan error: {e}")


# ────────────────────────────────────────────
# Schedule registration (every hour, on the hour)
# ────────────────────────────────────────────

def setup_schedule():
    # KR market: 09:00 ~ 15:00 KST, on the hour (last scan before 15:30 close)
    for h in range(9, 16):
        schedule.every().day.at(f"{h:02d}:00").do(scan_kr)

    # US market: 23:30, then 00:00 ~ 06:00 KST, on the hour
    schedule.every().day.at("23:30").do(scan_us)
    for h in range(0, 7):
        schedule.every().day.at(f"{h:02d}:00").do(scan_us)

    logger.info("Schedule registered")
    logger.info("KR market scan: 09:00 ~ 15:00 KST (hourly)")
    logger.info("US market scan: 23:30, 00:00 ~ 06:00 KST (hourly)")


# ────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Stock alert system started")
    alert.send_message("🚀 Stock alert system started")

    setup_schedule()

    while True:
        schedule.run_pending()
        time.sleep(30)
