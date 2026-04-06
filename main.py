"""
스케줄러 - 한국장/미국장 시간 분리

한국장: 09:00 ~ 15:30 (KST), 매 1시간
미국장: 23:30 ~ 06:00 (KST 기준), 매 1시간
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
# 스캔 함수
# ────────────────────────────────────────────

def scan_kr():
    """KOSPI+KOSDAQ 전종목 스캔"""
    now = now_kst()
    hour = now.hour
    minute = now.minute

    # 한국장 운영 시간: 09:00 ~ 15:30
    in_session = (hour == 9 and minute >= 0) or (10 <= hour <= 14) or (hour == 15 and minute <= 30)
    if not in_session:
        logger.info("한국장 운영 시간 외 (%s) — 스킵", now.strftime("%H:%M"))
        return

    logger.info("=== 한국장 스캔 시작 ===")
    alert.send_scan_start("한국장(KOSPI+KOSDAQ)")
    try:
        stocks_data = data_kr.get_all_stocks_data()
        signals = condition.filter_stocks(stocks_data)
        logger.info("한국 조건 충족: %d종목", len(signals))
        if signals:
            alert.send_kr_alerts(signals)
        else:
            alert.send_no_signal("한국장")
    except Exception as e:
        logger.exception("한국장 스캔 오류: %s", e)
        alert.send_message(f"⚠️ 한국장 스캔 오류: {e}")


def scan_us():
    """나스닥100 스캔"""
    now = now_kst()
    hour = now.hour
    minute = now.minute

    # 미국장 운영 시간(KST): 23:30 ~ 06:00
    # 23:30 이후 또는 06:00 이전
    after_open = (hour == 23 and minute >= 30) or hour == 0 or (1 <= hour <= 5) or (hour == 6 and minute == 0)
    if not after_open:
        logger.info("미국장 운영 시간 외 (%s) — 스킵", now.strftime("%H:%M"))
        return

    logger.info("=== 미국장 스캔 시작 ===")
    alert.send_scan_start("미국장(NASDAQ100)")
    try:
        stocks_data = data_us.get_all_stocks_data()
        signals = condition.filter_stocks(stocks_data)
        logger.info("미국 조건 충족: %d종목", len(signals))
        if signals:
            alert.send_us_alerts(signals)
        else:
            alert.send_no_signal("미국장")
    except Exception as e:
        logger.exception("미국장 스캔 오류: %s", e)
        alert.send_message(f"⚠️ 미국장 스캔 오류: {e}")


# ────────────────────────────────────────────
# 스케줄 등록 (매 1시간, 정각)
# ────────────────────────────────────────────

def setup_schedule():
    # 한국장: 09:00 ~ 15:00 매 정각 (15:30 마감 전 마지막 스캔은 15:00)
    for h in range(9, 16):
        schedule.every().day.at(f"{h:02d}:00").do(scan_kr)

    # 미국장: 23:30, 00:00 ~ 06:00 매 정각
    schedule.every().day.at("23:30").do(scan_us)
    for h in range(0, 7):
        schedule.every().day.at(f"{h:02d}:00").do(scan_us)

    logger.info("스케줄 등록 완료")
    logger.info("한국장 스캔: 09:00 ~ 15:00 (매 정각)")
    logger.info("미국장 스캔: 23:30, 00:00 ~ 06:00 (매 정각)")


# ────────────────────────────────────────────
# 진입점
# ────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("주식 알림 시스템 시작")
    alert.send_message("🚀 주식 알림 시스템 시작")

    setup_schedule()

    while True:
        schedule.run_pending()
        time.sleep(30)
