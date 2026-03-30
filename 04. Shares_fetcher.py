"""
분기별 발행주식수(shares) 통합 수집 스크립트
=============================================
- 대상: KOSPI / KOSDAQ
- 기간: 2022Q1 ~ 2025Q3 (설정 가능)
- 소스: DART API stockTotqySttus (주식의 총수 현황)
- 로직: 보통주 (발행총수 - 감소총수) = 유통주식수
- 출력: shares_quarter.csv         ← (ticker, quarter, shares) lookup 테이블
        shares_null_report.csv     ← 수집 실패 종목

입력 파일 요구사항:
    - ticker 컬럼 (6자리 종목코드)
    - corp_code 컬럼 (8자리 DART 고유번호)
    - quarter 컬럼 (예: "22Q1", "23Q4", "25Q3")

사용법:
    pip install requests pandas openpyxl
    python unified_shares_fetcher.py
"""

import pandas as pd
import requests
import time
import os
import json
import zipfile
import io
import xml.etree.ElementTree as ET
from datetime import datetime


# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIG                                                      ║
# ╚══════════════════════════════════════════════════════════════╝

API_KEY = ""   # ← DART API 인증키 붙여넣기

# 입력 파일 목록
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

OUTPUT_FILE = "shares_quarter.csv"
FAIL_FILE   = "shares_null_report.csv"
CACHE_FILE  = "shares_quarter_cache.csv"

# corp_code 컬럼이 없을 때 DART에서 자동 매핑
AUTO_CORP_CODE = True

# API 호출 간격 (초) — DART 분당 제한 준수
SLEEP_SEC = 1.0

# 체크포인트 저장 주기
CHECKPOINT_EVERY = 50


# ╔══════════════════════════════════════════════════════════════╗
# ║  분기 → 보고서코드 / 사업연도 매핑                               ║
# ╚══════════════════════════════════════════════════════════════╝

DART_STOCK_URL = "https://opendart.fss.or.kr/api/stockTotqySttus.json"

REPRT_CODES = {
    1: "11013",   # 1분기보고서
    2: "11012",   # 반기보고서
    3: "11014",   # 3분기보고서
    4: "11011",   # 사업보고서
}

def parse_quarter(quarter_str: str) -> tuple[int, int]:
    """
    "22Q1" → (2022, 1)
    "25Q3" → (2025, 3)
    """
    q = quarter_str.strip().upper()
    year = 2000 + int(q[:2])
    qnum = int(q[-1])
    return year, qnum


# ╔══════════════════════════════════════════════════════════════╗
# ║  유틸리티                                                     ║
# ╚══════════════════════════════════════════════════════════════╝

def parse_int(val):
    if val is None or str(val).strip() in ("", "-", "None"):
        return None
    try:
        return int(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def load_corp_code_xml(api_key):
    """DART corpCode.xml → {stock_code(6): corp_code(8)} dict"""
    print("  DART corpCode.xml 다운로드 중...")
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    res = requests.get(url, params={"crtfc_key": api_key}, timeout=60)
    res.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(res.content)) as zf:
        xml_bytes = zf.read(zf.namelist()[0])

    root = ET.fromstring(xml_bytes)
    mapping = {}
    for item in root.iter("list"):
        sc = (item.findtext("stock_code") or "").strip()
        cc = (item.findtext("corp_code") or "").strip()
        if sc and cc:
            mapping[sc] = cc

    print(f"  → 상장사 {len(mapping)}개 매핑 완료")
    return mapping


# ╔══════════════════════════════════════════════════════════════╗
# ║  DART API 호출: 발행주식수 조회                                  ║
# ╚══════════════════════════════════════════════════════════════╝

def fetch_shares(api_key: str, corp_code: str, year: int, quarter: int) -> int | None:
    """
    DART stockTotqySttus API로 보통주 발행주식수 조회.
    보통주의 (현재까지 발행한 주식의 총수 - 현재까지 감소한 주식의 총수) 반환.
    실패 시 None.
    """
    reprt_code = REPRT_CODES.get(quarter)
    if not reprt_code:
        return None

    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": str(year),
        "reprt_code": reprt_code,
    }
    try:
        resp = requests.get(DART_STOCK_URL, params=params, timeout=30)
        data = resp.json()
    except Exception:
        return None

    if data.get("status") != "000":
        return None

    items = data.get("list", [])

    # 1순위: "보통주" 행
    for item in items:
        se = (item.get("se") or "").strip()
        if "보통주" in se:
            issued = parse_int(item.get("now_to_isu_stock_totqy"))
            decreased = parse_int(item.get("now_to_dcrs_stock_totqy"))
            if issued is not None:
                return issued - (decreased or 0)

    # 2순위: 보통주 행이 없으면 첫 번째 행 사용 (일부 기업 구분 없음)
    if items:
        issued = parse_int(items[0].get("now_to_isu_stock_totqy"))
        decreased = parse_int(items[0].get("now_to_dcrs_stock_totqy"))
        if issued is not None:
            return issued - (decreased or 0)

    return None


# ╔══════════════════════════════════════════════════════════════╗
# ║  입력 파일 로드 & (ticker, quarter, corp_code) 추출              ║
# ╚══════════════════════════════════════════════════════════════╝

def load_ticker_quarter_pairs(corp_code_xml=None):
    """
    모든 입력 파일에서 (ticker, quarter, corp_code) 유니크 쌍 추출.
    """
    all_data = {}  # (ticker, quarter) → corp_code

    for label, filepath in INPUT_FILES:
        if not os.path.exists(filepath):
            print(f"  [SKIP] 파일 없음: {filepath}")
            continue

        ext = os.path.splitext(filepath)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(filepath, dtype={"ticker": str})
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(filepath, dtype={"ticker": str})
        else:
            print(f"  [SKIP] 지원하지 않는 형식: {filepath}")
            continue

        if "ticker" not in df.columns:
            print(f"  [SKIP] ticker 컬럼 없음: {filepath}")
            continue

        df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)

        # quarter 컬럼 확인
        if "quarter" not in df.columns:
            print(f"  [SKIP] quarter 컬럼 없음: {filepath}")
            continue

        df["quarter"] = df["quarter"].astype(str).str.strip()

        # corp_code 처리
        has_corp_code = False
        if "corp_code" in df.columns:
            df["corp_code"] = df["corp_code"].astype(str).str.strip().str.zfill(8)
            df.loc[df["corp_code"].str.contains("nan", case=False, na=True), "corp_code"] = ""
            has_corp_code = (df["corp_code"] != "").any()

        count = 0
        for _, row in df.iterrows():
            ticker = row["ticker"]
            quarter = row["quarter"]

            # corp_code 결정
            cc = ""
            if has_corp_code and str(row.get("corp_code", "")).strip() not in ("", "nan", "00000000"):
                cc = str(row["corp_code"]).strip().zfill(8)
            elif corp_code_xml and ticker in corp_code_xml:
                cc = corp_code_xml[ticker]

            if cc and (ticker, quarter) not in all_data:
                all_data[(ticker, quarter)] = cc
                count += 1

        print(f"  [{label}] {filepath}: {count}건 추가")

    # 유효한 분기만 필터 (parse 가능한 것)
    valid_data = {}
    for (ticker, quarter), corp_code in all_data.items():
        try:
            year, qnum = parse_quarter(quarter)
            if 2022 <= year <= 2025 and 1 <= qnum <= 4:
                valid_data[(ticker, quarter)] = corp_code
        except (ValueError, IndexError):
            pass

    return valid_data


# ╔══════════════════════════════════════════════════════════════╗
# ║  메인                                                        ║
# ╚══════════════════════════════════════════════════════════════╝

def main():
    start_time = datetime.now()

    # API KEY 확인
    if not API_KEY:
        print("ERROR: 스크립트 상단의 API_KEY를 설정해주세요!")
        return

    print("=" * 60)
    print("  분기별 발행주식수 통합 수집")
    print("=" * 60)

    # ── corp_code 자동매핑용 XML ──
    corp_code_xml = None
    if AUTO_CORP_CODE:
        corp_code_xml = load_corp_code_xml(API_KEY)

    # ── 캐시 확인 ──
    cached = {}
    if os.path.exists(CACHE_FILE):
        cache_df = pd.read_csv(CACHE_FILE, dtype={"ticker": str})
        cache_df["ticker"] = cache_df["ticker"].str.zfill(6)
        cached = {
            (row["ticker"], row["quarter"]): int(row["shares"])
            for _, row in cache_df.iterrows()
            if pd.notna(row["shares"])
        }
        print(f"\n  캐시 로드: {len(cached)}건 (재수집 생략)")

    # ── (ticker, quarter, corp_code) 로드 ──
    print(f"\n  입력 파일 로드 중...")
    all_data = load_ticker_quarter_pairs(corp_code_xml)
    print(f"\n  총 {len(all_data)}개 (ticker, quarter) 쌍")

    # 캐시에 없는 것만 수집 대상
    to_fetch = {
        k: v for k, v in all_data.items()
        if k not in cached
    }
    print(f"  신규 수집 대상: {len(to_fetch)}건")
    if cached:
        print(f"  캐시 재사용: {len(all_data) - len(to_fetch)}건")

    if not to_fetch:
        print("\n  모든 데이터가 캐시에 있음 → 수집 생략")
    else:
        est_min = len(to_fetch) * SLEEP_SEC / 60
        print(f"  예상 소요: 약 {est_min:.0f}분\n")

        # ── 수집 ──
        ckpt_file = "shares_checkpoint.json"
        results = dict(cached)  # 캐시 포함
        fails = []

        fetch_list = sorted(to_fetch.items(), key=lambda x: (x[0][1], x[0][0]))
        start_idx = 0

        # 체크포인트 로드
        if os.path.exists(ckpt_file):
            with open(ckpt_file, "r", encoding="utf-8") as f:
                ckpt = json.load(f)
                extra = ckpt.get("results", {})
                for k, v in extra.items():
                    t, q = k.split("|")
                    results[(t, q)] = v
                start_idx = ckpt.get("next_idx", 0)
                print(f"  체크포인트 로드: idx={start_idx}부터 재개")

        for i in range(start_idx, len(fetch_list)):
            (ticker, quarter), corp_code = fetch_list[i]
            year, qnum = parse_quarter(quarter)

            shares = fetch_shares(API_KEY, corp_code, year, qnum)

            if shares is not None:
                results[(ticker, quarter)] = shares
            else:
                fails.append({
                    "ticker": ticker,
                    "quarter": quarter,
                    "corp_code": corp_code,
                })

            # 진행률
            done = i + 1
            if done % 100 == 0 or done == len(fetch_list):
                pct = done / len(fetch_list) * 100
                print(f"  [{done:>6}/{len(fetch_list)}] ({pct:.1f}%)  "
                      f"성공: {len(results) - len(cached)}  실패: {len(fails)}")

            time.sleep(SLEEP_SEC)

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
    all_results = dict(cached)
    if to_fetch:
        all_results.update({k: v for k, v in results.items()})

    out_rows = [
        {"ticker": t, "quarter": q, "shares": s}
        for (t, q), s in sorted(all_results.items(), key=lambda x: (x[0][1], x[0][0]))
    ]
    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"\n  저장: {OUTPUT_FILE} ({len(out_df)}건)")

    # 캐시 갱신
    out_df.to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")

    # 실패 목록
    if to_fetch and fails:
        fail_df = pd.DataFrame(fails)
        fail_df.to_csv(FAIL_FILE, index=False, encoding="utf-8-sig")
        print(f"  저장: {FAIL_FILE} ({len(fails)}건)")

    # ── 분기별 요약 ──
    print(f"\n{'='*60}")
    print("  분기별 수집 현황")
    print("=" * 60)

    if not out_df.empty:
        quarters_in_data = sorted(out_df["quarter"].unique())
        for q in quarters_in_data:
            q_count = len(out_df[out_df["quarter"] == q])
            total_in_input = sum(1 for (_, qq) in all_data if qq == q)
            print(f"  {q}: {q_count:>5}/{total_in_input:>5}건")

    elapsed = datetime.now() - start_time
    print(f"\n  소요시간: {elapsed}")


if __name__ == "__main__":
    main()
