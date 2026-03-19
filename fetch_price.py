"""
KOSPI ticker 목록에서 2025-10-01 종가를 가져와 Excel에 추가하는 스크립트
실행 전: pip install pandas openpyxl yfinance
"""

import pandas as pd
import yfinance as yf
from datetime import datetime
import time

INPUT_FILE = "KOSPI_1차 수정용.xlsx"
OUTPUT_FILE = "KOSPI_1차_수정용_종가추가.xlsx"
TARGET_DATE = "2025-10-01"

df = pd.read_excel(INPUT_FILE)
print(f"총 {len(df)}개 종목 로드 완료")

# 2025-10-01이 공휴일(개천절)이므로, 가장 가까운 직전 거래일 자동 탐색
# yfinance는 해당일 데이터가 없으면 빈 DataFrame 반환 → 전후 날짜로 탐색
def get_close_price(ticker_code: int) -> float | None:
    ticker_str = f"{ticker_code:06d}.KS"
    try:
        # 10/01 전후 5거래일 범위로 다운로드
        data = yf.download(
            ticker_str,
            start="2025-09-25",
            end="2025-10-07",
            auto_adjust=True,
            progress=False,
            timeout=10
        )
        if data.empty:
            return None
        # Close 컬럼 추출 (MultiIndex 대응)
        if isinstance(data.columns, pd.MultiIndex):
            close = data["Close"][ticker_str]
        else:
            close = data["Close"]
        # 2025-10-01 이전 마지막 거래일 종가 반환
        target = pd.Timestamp("2025-10-01")
        valid = close[close.index <= target]
        if valid.empty:
            return None
        return round(float(valid.iloc[-1]), 0)
    except Exception as e:
        print(f"  오류 [{ticker_code}]: {e}")
        return None

prices = []
errors = []

for i, row in df.iterrows():
    ticker = int(row["ticker"])
    price = get_close_price(ticker)
    prices.append(price)
    status = f"{price:,.0f}원" if price else "❌ 실패"
    print(f"[{i+1:>4}/{len(df)}] {ticker:06d} {str(row['corp_name']):<15} → {status}")
    # API 과부하 방지
    if (i + 1) % 20 == 0:
        time.sleep(2)

df["price_20251001"] = prices
success = sum(1 for p in prices if p is not None)
print(f"\n완료: {success}/{len(df)}개 성공")

df.to_excel(OUTPUT_FILE, index=False)
print(f"저장 완료 → {OUTPUT_FILE}")
