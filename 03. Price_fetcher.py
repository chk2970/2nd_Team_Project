"""
분기말 종가(price) 통합 수집 스크립트
======================================
- 대상: KOSPI / KOSDAQ
- 기간: 2022Q1 ~ 2025Q3 (설정 가능)
- 소스: FinanceDataReader (Naver 소스, KOSPI/KOSDAQ 모두 지원)
- 로직: 분기말 날짜 기준 직전 영업일 종가 자동 탐색
- 출력: price_quarter.csv          ← (ticker, quarter, price) lookup 테이블
        price_null_report.csv      ← 수집 실패 종목

입력 파일 요구사항:
    - ticker 컬럼 (6자리 종목코드)
    - quarter 컬럼 (예: "22Q1", "23Q4", "25Q3")
    - 또는 ticker만 있으면 전 분기 자동 생성

사용법:
    pip install finance-datareader pandas openpyxl
    python unified_price_fetcher.py
"""

import pandas as pd
import FinanceDataReader as fdr
import time
import os
import json
from datetime import datetime


# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIG                                                      ║
# ╚══════════════════════════════════════════════════════════════╝

# 입력 파일 목록: (label, filepath)
# unified_dart_fetcher.py의 merged CSV 또는 기존 엑셀 모두 사용 가능
INPUT_FILES = [
    ("KOSPI",  "dart_output/KOSPI/KOSPI_2022_merged.csv"),
    ("KOSPI",  "dart_output/KOSPI/KOSPI_2023_merged.csv"),
    ("KOSPI",  "dart_output/KOSPI/KOSPI_2024_merged.csv"),
    ("KOSPI",  "dart_output/KOSPI/KOSPI_2025_merged.csv"),
    ("KOSDAQ", "dart_output/KOSDAQ/KOSDAQ_2022_merged.csv"),
    ("KOSDAQ", "dart_output/KOSDAQ/KOSDAQ_2023_merged.csv"),
    ("KOSDAQ", "dart_output/KOSDAQ/KOSDAQ_2024_merged.csv"),
    ("KOSDAQ", "dart_output/KOSDAQ/KOSDAQ_2025_merged.csv"),
]

OUTPUT_FILE = "price_quarter.csv"
FAIL_FILE   = "price_null_report.csv"
CACHE_FILE  = "price_quarter_cache.csv"

# API 호출 간격 (초) — FDR은 Naver 기반이라 빠르게 가능
REQUEST_DELAY = 0.15

# 직전 영업일 탐색 범위 (달력일 기준)
LOOKBACK_DAYS = 10

# 체크포인트 저장 주기
CHECKPOINT_EVERY = 100


# ╔══════════════════════════════════════════════════════════════╗
# ║  분기말 날짜 테이블                                            ║
# ╚══════════════════════════════════════════════════════════════╝

# 각 분기의 마지막 거래일 (또는 그에 가장 가까운 날짜)
# 공휴일/주말이면 LOOKBACK_DAYS 범위 내 직전 영업일 자동 탐색
QUARTER_END_DATES = {
    # 2022
    "22Q1": "2022-03-31",
    "22Q2": "2022-06-30",
    "22Q3": "2022-09-30",
    "22Q4": "2022-12-30",
    # 2023
    "23Q1": "2023-03-31",
    "23Q2": "2023-06-30",
    "23Q3": "2023-09-27",   # 추석 연휴 → 9/27 마지막 거래일
    "23Q4": "2023-12-29",
    # 2024
    "24Q1": "2024-03-29",
    "24Q2": "2024-06-28",
    "24Q3": "2024-09-30",
    "24Q4": "2024-12-30",
    # 2025
    "25Q1": "2025-03-31",
    "25Q2": "2025-06-30",
    "25Q3": "2025-09-30",
}


# ╔══════════════════════════════════════════════════════════════╗
# ║  종가 조회 함수                                                ║
# ╚══════════════════════════════════════════════════════════════╝

def get_quarter_close(ticker: str, date_str: str) -> float | None:
    """
    분기말 종가 반환.
    해당일이 비영업일(공휴일/주말)이면 직전 영업일 종가 반환.
    실패 시 None.
    """
    end_dt   = pd.Timestamp(date_str)
    start_dt = end_dt - pd.Timedelta(days=LOOKBACK_DAYS)
    try:
        df = fdr.DataReader(
            ticker,
            start=start_dt.strftime("%Y-%m-%d"),
            end=end_dt.strftime("%Y-%m-%d"),
        )
        if df.empty:
            return None
        return float(df["Close"].iloc[-1])
    except Exception:
        return None


# ╔══════════════════════════════════════════════════════════════╗
# ║  입력 파일 로드 & (ticker, quarter) 쌍 추출                     ║
# ╚══════════════════════════════════════════════════════════════╝

def load_ticker_quarter_pairs():
    """
    모든 입력 파일에서 (ticker, quarter) 유니크 쌍 추출.
    quarter 형식: "22Q1", "23Q4" 등
    """
    all_pairs = set()

    for label, filepath in INPUT_FILES:
        if not os.path.exists(filepath):
            print(f"  [SKIP] 파일 없음: {filepath}")
            continue

        ext = os.path.splitext(filepath)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(filepath, dtype={"ticker": str}, usecols=lambda c: c in ["ticker", "quarter"])
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(filepath, dtype={"ticker": str})
        else:
            print(f"  [SKIP] 지원하지 않는 형식: {filepath}")
            continue

        if "ticker" not in df.columns:
            print(f"  [SKIP] ticker 컬럼 없음: {filepath}")
            continue

        df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)

        if "quarter" in df.columns:
            df["quarter"] = df["quarter"].astype(str).str.strip()
            pairs = set(zip(df["ticker"], df["quarter"]))
        else:
            # quarter 컬럼 없으면 → 전체 분기로 확장
            tickers = df["ticker"].unique()
            pairs = {(t, q) for t in tickers for q in QUARTER_END_DATES}

        all_pairs.update(pairs)
        print(f"  [{label}] {filepath}: {len(pairs)}쌍 추가")

    # QUARTER_END_DATES에 있는 분기만 필터
    valid_pairs = {
        (t, q) for t, q in all_pairs
        if q in QUARTER_END_DATES
    }

    return sorted(valid_pairs, key=lambda x: (x[1], x[0]))


# ╔══════════════════════════════════════════════════════════════╗
# ║  메인                                                        ║
# ╚══════════════════════════════════════════════════════════════╝

def main():
    start_time = datetime.now()

    print("=" * 60)
    print("  분기말 종가 통합 수집")
    print("=" * 60)

    # ── 캐시 확인 ──
    cached = {}
    if os.path.exists(CACHE_FILE):
        cache_df = pd.read_csv(CACHE_FILE, dtype={"ticker": str})
        cache_df["ticker"] = cache_df["ticker"].str.zfill(6)
        cached = {
            (row["ticker"], row["quarter"]): row["price"]
            for _, row in cache_df.iterrows()
            if pd.notna(row["price"])
        }
        print(f"\n  캐시 로드: {len(cached)}건 (재수집 생략)")

    # ── (ticker, quarter) 쌍 로드 ──
    print(f"\n  입력 파일 로드 중...")
    pairs = load_ticker_quarter_pairs()
    print(f"\n  총 {len(pairs)}개 (ticker, quarter) 쌍")

    # 캐시에 없는 것만 수집 대상
    to_fetch = [(t, q) for t, q in pairs if (t, q) not in cached]
    print(f"  신규 수집 대상: {len(to_fetch)}건")
    if cached:
        print(f"  캐시 재사용: {len(pairs) - len(to_fetch)}건")

    if not to_fetch:
        print("\n  모든 데이터가 캐시에 있음 → 수집 생략")
    else:
        est_min = len(to_fetch) * REQUEST_DELAY / 60
        print(f"  예상 소요: 약 {est_min:.0f}분\n")

        # ── 수집 ──
        ckpt_file = "price_checkpoint.json"
        results = dict(cached)  # 캐시 포함
        fails = []
        start_idx = 0

        # 체크포인트 로드
        if os.path.exists(ckpt_file):
            with open(ckpt_file, "r", encoding="utf-8") as f:
                ckpt = json.load(f)
                extra_results = ckpt.get("results", {})
                # key를 tuple로 복원
                for k, v in extra_results.items():
                    t, q = k.split("|")
                    results[(t, q)] = v
                start_idx = ckpt.get("next_idx", 0)
                print(f"  체크포인트 로드: idx={start_idx}부터 재개")

        for i in range(start_idx, len(to_fetch)):
            ticker, quarter = to_fetch[i]
            date_str = QUARTER_END_DATES[quarter]

            price = get_quarter_close(ticker, date_str)

            if price is not None:
                results[(ticker, quarter)] = price
            else:
                fails.append({"ticker": ticker, "quarter": quarter, "date": date_str})

            # 진행률 출력
            done = i + 1
            if done % 200 == 0 or done == len(to_fetch):
                pct = done / len(to_fetch) * 100
                print(f"  [{done:>6}/{len(to_fetch)}] ({pct:.1f}%)  "
                      f"성공: {len(results) - len(cached)}  실패: {len(fails)}")

            time.sleep(REQUEST_DELAY)

            # 체크포인트 저장
            if done % CHECKPOINT_EVERY == 0:
                ckpt_data = {
                    "results": {f"{t}|{q}": v for (t, q), v in results.items()},
                    "next_idx": done,
                }
                with open(ckpt_file, "w", encoding="utf-8") as f:
                    json.dump(ckpt_data, f)

        # 체크포인트 정리
        if os.path.exists(ckpt_file):
            os.remove(ckpt_file)

        print(f"\n  수집 완료: 성공 {len(results)}건 / 실패 {len(fails)}건")

    # ── 결과 저장 ──
    # 전체 결과 (캐시 + 신규)
    all_results = cached.copy()
    if to_fetch:
        all_results.update({k: v for k, v in results.items()})

    out_rows = [
        {"ticker": t, "quarter": q, "price": p}
        for (t, q), p in sorted(all_results.items(), key=lambda x: (x[0][1], x[0][0]))
    ]
    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n  저장: {OUTPUT_FILE} ({len(out_df)}건)")

    # 캐시 갱신
    out_df.to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")

    # 실패 목록
    if to_fetch:
        if fails:
            fail_df = pd.DataFrame(fails)
            fail_df.to_csv(FAIL_FILE, index=False, encoding="utf-8-sig")
            print(f"  저장: {FAIL_FILE} ({len(fails)}건)")

    # ── 분기별 요약 ──
    print(f"\n{'='*60}")
    print("  분기별 수집 현황")
    print("=" * 60)

    if not out_df.empty:
        for q in sorted(QUARTER_END_DATES.keys()):
            q_data = out_df[out_df["quarter"] == q]
            total_pairs = sum(1 for _, qq in pairs if qq == q)
            if total_pairs > 0:
                print(f"  {q}: {len(q_data):>5}/{total_pairs:>5}건 수집")

    elapsed = datetime.now() - start_time
    print(f"\n  소요시간: {elapsed}")


if __name__ == "__main__":
    main()
