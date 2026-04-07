# -*- coding: utf-8 -*-
"""
Scheduler that separates KR market and US market scan times.

KR market: 09:00 to 15:30 KST, every hour
US market: 23:30 to 06:00 KST, every hour
"""
import logging
import time
import schedule

import alert
import condition
import data_kr
import data_us

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("stock_alert.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

def scan_kr():
    """Scan all KOSPI and KOSDAQ stocks."""
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

    except Exception as exc:
        logger.exception("KR market scan error: %s", exc)
        alert.send_message(f"KR market scan error: {exc}")


def scan_us():
    """Scan NASDAQ-100 stocks."""
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

    except Exception as exc:
        logger.exception("US market scan error: %s", exc)
        alert.send_message(f"US market scan error: {exc}")


def setup_schedule():
    """Register scan schedules."""
    for hour in range(9, 16):
        schedule.every().day.at(f"{hour:02d}:00").do(scan_kr)

    schedule.every().day.at("23:30").do(scan_us)
    for hour in range(0, 7):
        schedule.every().day.at(f"{hour:02d}:00").do(scan_us)

    logger.info("Schedule registered")
    logger.info("KR market scan: 09:00 to 15:00 KST (hourly)")
    logger.info("US market scan: 23:30, 00:00 to 06:00 KST (hourly)")


if __name__ == "__main__":
    logger.info("Stock alert system started")
    alert.send_message("Stock alert system started")

    setup_schedule()

    while True:
        schedule.run_pending()
        time.sleep(30)
