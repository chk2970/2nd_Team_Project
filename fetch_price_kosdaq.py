"""
KOSDAQ ticker 목록에서 2025-10-01 종가를 가져와 Excel에 추가하는 스크립트
수정사항:
  - ticker를 문자열로 처리 (int 변환 제거 → '0015G0' 같은 특수 코드 대응)
  - KOSDAQ suffix: .KQ 사용
실행 전: pip install pandas openpyxl yfinance
"""

import pandas as pd
import yfinance as yf
import time

INPUT_FILE = "KOSDAQ_3분기_1차_수정용.xlsx"
OUTPUT_FILE = "KOSDAQ_3분기_1차_수정용_종가추가.xlsx"

df = pd.read_excel(INPUT_FILE, dtype={"ticker": str})  # ticker를 문자열로 읽기
print(f"총 {len(df)}개 종목 로드 완료")


def get_close_price(ticker_code: str) -> float | None:
    # 6자리 zero-padding 후 .KQ suffix
    ticker_str = f"{ticker_code.strip().zfill(6)}.KQ"
    try:
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
        if isinstance(data.columns, pd.MultiIndex):
            close = data["Close"][ticker_str]
        else:
            close = data["Close"]
        target = pd.Timestamp("2025-10-01")
        valid = close[close.index <= target]
        if valid.empty:
            return None
        return round(float(valid.iloc[-1]), 0)
    except Exception as e:
        print(f"  오류 [{ticker_code}]: {e}")
        return None


prices = []

for i, row in df.iterrows():
    ticker = str(row["ticker"]).strip()
    price = get_close_price(ticker)
    prices.append(price)
    status = f"{price:,.0f}원" if price else "❌ 실패"
    corp = str(row.get("corp_name", ""))
    print(f"[{i+1:>4}/{len(df)}] {ticker.zfill(6)} {corp:<15} → {status}")
    if (i + 1) % 20 == 0:
        time.sleep(2)

df["price_20251001"] = prices
success = sum(1 for p in prices if p is not None)
print(f"\n완료: {success}/{len(df)}개 성공 / {len(df) - success}개 실패")

df.to_excel(OUTPUT_FILE, index=False)
print(f"저장 완료 → {OUTPUT_FILE}")
