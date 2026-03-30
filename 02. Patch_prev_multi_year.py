"""
revenue_prev / op_income_prev 멀티연도 패치 스크립트
- 2022~2024년 Q1~Q4를 DART에서 수집 (매출액, 영업이익만)
- 누적 차감 적용 → 각 연도 개별 분기 금액 산출
- 아래 CSV들의 revenue_prev, op_income_prev에 매핑
    * 2023년 Q1~Q4  ← 2022년 값 매핑
    * 2024년 Q1~Q4  ← 2023년 값 매핑
    * 2025년 Q1~Q3  ← 2024년 값 매핑
- 2022년 CSV는 패치하지 않음 (2023년 prev용 데이터로만 사용)

사용법:
    1. 아래 API_KEY에 DART 인증키 붙여넣기
    2. python patch_prev_multi_year.py
"""

import json
import os
import time

import pandas as pd
import requests

# ──────────────────────────────────────────────
# 설정 ★ 여기에 API 키 붙여넣기
# ──────────────────────────────────────────────
API_KEY = "여기에_DART_API키_붙여넣기"

BASE_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
INPUT_FILE = "2023KOSDAQ기준.xlsx"
QUARTER_DIR = "output_quarters"

REPRT_CODES = {
    1: "11013",  # 1분기보고서
    2: "11012",  # 반기보고서
    3: "11014",  # 3분기보고서
    4: "11011",  # 사업보고서
}

# 어떤 연도의 CSV를 어떤 전년도 데이터로 패치할지
PATCH_PLAN = {
    2023: {"prev_year": 2022, "quarters": [1, 2, 3, 4]},
    2024: {"prev_year": 2023, "quarters": [1, 2, 3, 4]},
    2025: {"prev_year": 2024, "quarters": [1, 2, 3]},
}

# prev_year로 필요한 연도들만 수집
FETCH_YEARS = sorted({cfg["prev_year"] for cfg in PATCH_PLAN.values()})
FETCH_QUARTERS = [1, 2, 3, 4]

FS_DIVS = ["CFS", "OFS"]
SJ_DIVS_IS = ["IS", "CIS"]
SLEEP_SEC = 1.2
CHECKPOINT_EVERY = 50

# 매출/영업이익 계정명 변형
REVENUE_NAMES = [
    "매출액", "수익(매출액)", "영업수익", "매출", "매출액(수익)",
    "순매출액", "I.매출액", "Ⅰ.매출액", "매출액 (수익)",
]
OP_INCOME_NAMES = [
    "영업이익", "영업이익(손실)", "영업손익", "영업이익(영업손실)",
    "Ⅲ.영업이익", "III.영업이익", "영업이익 (손실)",
]


def parse_amount(val):
    if val is None or val == "" or val == "-":
        return None
    try:
        return int(str(val).replace(",", ""))
    except ValueError:
        return None



def extract_amount(items, account_names):
    lookup = {}
    for item in items:
        key = (item.get("sj_div", ""), item.get("account_nm", "").strip())
        lookup[key] = item

    # 1차: 정확 일치
    for sj in SJ_DIVS_IS:
        for nm in account_names:
            item = lookup.get((sj, nm))
            if item:
                return parse_amount(item.get("thstrm_amount"))

    # 2차: 부분 일치 fallback
    for item in items:
        if item.get("sj_div") not in SJ_DIVS_IS:
            continue
        account_nm = item.get("account_nm", "").strip()
        for nm in account_names:
            if nm in account_nm:
                return parse_amount(item.get("thstrm_amount"))

    return None



def fetch_financials(api_key, corp_code, year, quarter):
    """지정 연도/분기의 thstrm_amount(누적값)를 추출"""
    reprt_code = REPRT_CODES[quarter]

    for fs_div in FS_DIVS:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": reprt_code,
            "fs_div": fs_div,
        }
        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            data = resp.json()
        except Exception as e:
            print(f"    [ERROR] {corp_code} {year} Q{quarter}: {e}")
            continue

        status = data.get("status")
        if status == "000":
            items = data.get("list", [])
            return {
                "revenue": extract_amount(items, REVENUE_NAMES),
                "op_income": extract_amount(items, OP_INCOME_NAMES),
            }

        # 013: 조회된 데이트가 없습니다. → CFS 실패 시 OFS 재시도
        if status == "013":
            continue

    return {}



def subtract(curr, prev):
    if curr is None:
        return None
    if prev is None:
        return curr
    return curr - prev



def to_quarter_amounts(raw_year_results):
    """연간 raw(누적값) -> 개별 분기값 변환"""
    adjusted = {q: {} for q in FETCH_QUARTERS}

    all_tickers = set()
    for q in FETCH_QUARTERS:
        all_tickers.update(raw_year_results.get(q, {}).keys())

    for ticker in all_tickers:
        for idx, q in enumerate(FETCH_QUARTERS):
            curr = raw_year_results.get(q, {}).get(ticker, {})
            if q == 1:
                adjusted[q][ticker] = dict(curr)
            else:
                prev_q = FETCH_QUARTERS[idx - 1]
                prev = raw_year_results.get(prev_q, {}).get(ticker, {})
                adjusted[q][ticker] = {
                    "revenue": subtract(curr.get("revenue"), prev.get("revenue")),
                    "op_income": subtract(curr.get("op_income"), prev.get("op_income")),
                }

    return adjusted



def load_companies(input_file):
    df = pd.read_excel(input_file)
    companies = df[["ticker", "corp_name", "corp_code"]].copy()
    companies["ticker"] = companies["ticker"].astype(str).str.zfill(6)
    companies["corp_code"] = companies["corp_code"].astype(int).astype(str).str.zfill(8)
    return companies



def collect_raw_year(api_key, companies, year):
    total = len(companies)
    year_results = {}

    for q in FETCH_QUARTERS:
        print(f"{'=' * 60}")
        print(f"  {year}년 {q}분기 수집 (reprt_code: {REPRT_CODES[q]})")
        print(f"{'=' * 60}")

        ckpt_file = os.path.join(QUARTER_DIR, f"prev_checkpoint_{year}_Q{q}.json")
        results = {}
        start = 0

        if os.path.exists(ckpt_file):
            with open(ckpt_file, "r", encoding="utf-8") as f:
                ckpt = json.load(f)
                results = ckpt.get("results", {})
                start = ckpt.get("next_idx", 0)
                print(f"  체크포인트: {len(results)}건, idx={start}부터 재개")

        for i in range(start, total):
            row = companies.iloc[i]
            ticker = row["ticker"]
            corp_code = row["corp_code"]
            corp_name = row["corp_name"]

            print(f"  [{i + 1}/{total}] {ticker} {corp_name} ({year} Q{q})...", end=" ")

            data = fetch_financials(api_key, corp_code, year, q)
            if data:
                results[ticker] = data
                rev = data.get("revenue")
                op = data.get("op_income")
                print(f"매출={'OK' if rev is not None else '-'} 영업이익={'OK' if op is not None else '-'}")
            else:
                results[ticker] = {}
                print("데이터 없음")

            time.sleep(SLEEP_SEC)

            if (i + 1) % CHECKPOINT_EVERY == 0:
                with open(ckpt_file, "w", encoding="utf-8") as f:
                    json.dump({"results": results, "next_idx": i + 1}, f, ensure_ascii=False)
                print(f"  --- 체크포인트 ({i + 1}/{total}) ---")

        year_results[q] = results

        if os.path.exists(ckpt_file):
            os.remove(ckpt_file)

    return year_results



def patch_csv(target_year, quarters, prev_year, adjusted_prev_data):
    print(f"\n{'=' * 60}")
    print(f"  {target_year}년 CSV 패치 (prev_year={prev_year})")
    print(f"{'=' * 60}")

    for q in quarters:
        csv_path = os.path.join(QUARTER_DIR, f"quarter_{q}_{target_year}.csv")
        if not os.path.exists(csv_path):
            print(f"  Q{q}: {csv_path} 없음, 건너뜀")
            continue

        df_q = pd.read_csv(csv_path, dtype={"ticker": str})
        df_q["ticker"] = df_q["ticker"].astype(str).str.zfill(6)

        rev_map = {ticker: values.get("revenue") for ticker, values in adjusted_prev_data[q].items()}
        op_map = {ticker: values.get("op_income") for ticker, values in adjusted_prev_data[q].items()}

        df_q["revenue_prev"] = df_q["ticker"].map(rev_map)
        df_q["op_income_prev"] = df_q["ticker"].map(op_map)

        df_q.to_csv(csv_path, index=False, encoding="utf-8-sig")

        rev_filled = df_q["revenue_prev"].notna().sum()
        op_filled = df_q["op_income_prev"].notna().sum()
        print(f"  Q{q}: revenue_prev={rev_filled}건, op_income_prev={op_filled}건 / {len(df_q)}건")



def main():
    if API_KEY == "여기에_DART_API키_붙여넣기":
        print("ERROR: API_KEY를 설정해주세요!")
        return

    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: 입력 파일 없음 -> {INPUT_FILE}")
        return

    companies = load_companies(INPUT_FILE)
    total = len(companies)
    print(f"총 {total}개 기업 / prev용 연도({FETCH_YEARS}) 매출·영업이익 수집 시작\n")

    # Phase 1: 필요한 prev_year들 raw(누적값) 수집
    raw_by_year = {}
    for year in FETCH_YEARS:
        raw_by_year[year] = collect_raw_year(API_KEY, companies, year)

    # Phase 2: 각 연도 누적 차감 → 개별 분기
    print(f"\n{'=' * 60}")
    print("  누적 차감 적용 (개별 분기 산출)")
    print(f"{'=' * 60}")

    adjusted_by_year = {}
    for year in FETCH_YEARS:
        adjusted_by_year[year] = to_quarter_amounts(raw_by_year[year])
        print(f"  {year}년 완료")

    # Phase 3: 타겟 연도별 CSV 패치
    for target_year, cfg in PATCH_PLAN.items():
        patch_csv(
            target_year=target_year,
            quarters=cfg["quarters"],
            prev_year=cfg["prev_year"],
            adjusted_prev_data=adjusted_by_year[cfg["prev_year"]],
        )

    print(f"\n{'=' * 60}")
    print("  패치 완료! 기존 CSV 덮어쓰기 완료.")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
