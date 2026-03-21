"""
revenue_prev / op_income_prev 패치 스크립트
- 2022년 Q1~Q4를 DART에서 수집 (매출액, 영업이익만)
- 누적 차감 적용 → 2022년 개별 분기 금액 산출
- 기존 quarter_1~4_2023.csv의 revenue_prev, op_income_prev에 매핑

사용법:
    1. 아래 API_KEY에 DART 인증키 붙여넣기
    2. python patch_prev.py
"""

import requests
import pandas as pd
import time
import json
import os

# ──────────────────────────────────────────────
# 설정 ★ 여기에 API 키 붙여넣기
# ──────────────────────────────────────────────
API_KEY = "여기에_DART_API키_붙여넣기"

BASE_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
BSNS_YEAR = "2022"  # ← 전년도 수집

INPUT_FILE = "2023KOSDAQ기준.xlsx"
QUARTER_DIR = "output_quarters"       # 기존 2023 CSV가 있는 폴더
OUTPUT_DIR = "output_quarters"        # 같은 폴더에 덮어쓰기

REPRT_CODES = {
    1: "11013",
    2: "11012",
    3: "11014",
    4: "11011",
}

FS_DIVS = ["CFS", "OFS"]
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
SJ_DIVS_IS = ["IS", "CIS"]


def parse_amount(val):
    if val is None or val == "" or val == "-":
        return None
    try:
        return int(str(val).replace(",", ""))
    except ValueError:
        return None


def fetch_prev(api_key, corp_code, quarter):
    """2022년 해당 분기에서 매출액/영업이익의 thstrm_amount만 추출"""
    reprt_code = REPRT_CODES[quarter]
    for fs_div in FS_DIVS:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bsns_year": BSNS_YEAR,
            "reprt_code": reprt_code,
            "fs_div": fs_div,
        }
        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            data = resp.json()
        except Exception as e:
            print(f"    [ERROR] {corp_code} Q{quarter}: {e}")
            continue

        if data.get("status") == "000":
            items = data.get("list", [])
            row = {}

            # lookup: (sj_div, account_nm) → item
            lookup = {}
            for item in items:
                key = (item.get("sj_div", ""), item.get("account_nm", "").strip())
                lookup[key] = item

            # 매출액
            for sj in SJ_DIVS_IS:
                for nm in REVENUE_NAMES:
                    item = lookup.get((sj, nm))
                    if item:
                        row["revenue_2022"] = parse_amount(item.get("thstrm_amount"))
                        break
                if "revenue_2022" in row:
                    break
            # 부분매칭 fallback
            if "revenue_2022" not in row:
                for item in items:
                    if item.get("sj_div") in SJ_DIVS_IS:
                        for nm in REVENUE_NAMES:
                            if nm in item.get("account_nm", "").strip():
                                row["revenue_2022"] = parse_amount(item.get("thstrm_amount"))
                                break
                    if "revenue_2022" in row:
                        break

            # 영업이익
            for sj in SJ_DIVS_IS:
                for nm in OP_INCOME_NAMES:
                    item = lookup.get((sj, nm))
                    if item:
                        row["op_income_2022"] = parse_amount(item.get("thstrm_amount"))
                        break
                if "op_income_2022" in row:
                    break
            if "op_income_2022" not in row:
                for item in items:
                    if item.get("sj_div") in SJ_DIVS_IS:
                        for nm in OP_INCOME_NAMES:
                            if nm in item.get("account_nm", "").strip():
                                row["op_income_2022"] = parse_amount(item.get("thstrm_amount"))
                                break
                    if "op_income_2022" in row:
                        break

            return row
        elif data.get("status") == "013":
            continue
    return {}


def subtract(curr, prev):
    if curr is None:
        return None
    if prev is None:
        return curr
    return curr - prev


def main():
    if API_KEY == "여기에_DART_API키_붙여넣기":
        print("ERROR: API_KEY를 설정해주세요!")
        return

    df = pd.read_excel(INPUT_FILE)
    companies = df[["ticker", "corp_name", "corp_code"]].copy()
    companies["corp_code"] = companies["corp_code"].astype(int).astype(str).str.zfill(8)
    total = len(companies)
    print(f"총 {total}개 기업 / 2022년 매출·영업이익 수집 시작\n")

    # ──────────────────────────────────────────
    # Phase 1: 2022년 Q1~Q4 raw(누적) 수집
    # ──────────────────────────────────────────
    raw_2022 = {}

    for q in [1, 2, 3, 4]:
        print(f"{'='*60}")
        print(f"  2022년 {q}분기 수집 (reprt_code: {REPRT_CODES[q]})")
        print(f"{'='*60}")

        ckpt_file = os.path.join(QUARTER_DIR, f"prev_checkpoint_Q{q}.json")
        results = {}
        start = 0

        if os.path.exists(ckpt_file):
            with open(ckpt_file, "r") as f:
                ckpt = json.load(f)
                results = ckpt.get("results", {})
                start = ckpt.get("next_idx", 0)
                print(f"  체크포인트: {len(results)}건, idx={start}부터 재개")

        for i in range(start, total):
            row = companies.iloc[i]
            ticker = str(row["ticker"])
            corp_code = row["corp_code"]

            print(f"  [{i+1}/{total}] {ticker} {row['corp_name']} (2022 Q{q})...", end=" ")

            data = fetch_prev(API_KEY, corp_code, q)
            if data:
                results[ticker] = data
                rev = data.get("revenue_2022")
                op = data.get("op_income_2022")
                print(f"매출={'OK' if rev is not None else '-'} 영업이익={'OK' if op is not None else '-'}")
            else:
                results[ticker] = {}
                print("데이터 없음")

            time.sleep(SLEEP_SEC)

            if (i + 1) % CHECKPOINT_EVERY == 0:
                with open(ckpt_file, "w") as f:
                    json.dump({"results": results, "next_idx": i + 1}, f, ensure_ascii=False)
                print(f"  --- 체크포인트 ({i+1}/{total}) ---")

        raw_2022[q] = results
        if os.path.exists(ckpt_file):
            os.remove(ckpt_file)

    # ──────────────────────────────────────────
    # Phase 2: 누적 차감 → 개별 2022 분기
    # ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  누적 차감 적용 (2022 개별 분기 산출)")
    print(f"{'='*60}")

    adj_2022 = {q: {} for q in [1, 2, 3, 4]}
    all_tickers = set()
    for q in [1, 2, 3, 4]:
        all_tickers.update(raw_2022[q].keys())

    for ticker in all_tickers:
        for qi, q in enumerate([1, 2, 3, 4]):
            raw_curr = raw_2022.get(q, {}).get(ticker, {})
            if q == 1:
                adj_2022[q][ticker] = dict(raw_curr)
            else:
                prev_q = [1, 2, 3, 4][qi - 1]
                raw_prev = raw_2022.get(prev_q, {}).get(ticker, {})
                adj_2022[q][ticker] = {
                    "revenue_2022": subtract(
                        raw_curr.get("revenue_2022"),
                        raw_prev.get("revenue_2022")),
                    "op_income_2022": subtract(
                        raw_curr.get("op_income_2022"),
                        raw_prev.get("op_income_2022")),
                }

    # ──────────────────────────────────────────
    # Phase 3: 기존 2023 CSV에 prev 매핑
    # ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  기존 2023 CSV에 revenue_prev / op_income_prev 패치")
    print(f"{'='*60}")

    for q in [1, 2, 3, 4]:
        csv_path = os.path.join(QUARTER_DIR, f"quarter_{q}_2023.csv")
        if not os.path.exists(csv_path):
            print(f"  Q{q}: {csv_path} 없음, 건너뜀")
            continue

        df_q = pd.read_csv(csv_path, dtype={"ticker": str})

        # 매핑
        rev_map = {t: v.get("revenue_2022") for t, v in adj_2022[q].items()}
        op_map = {t: v.get("op_income_2022") for t, v in adj_2022[q].items()}

        df_q["revenue_prev"] = df_q["ticker"].map(rev_map)
        df_q["op_income_prev"] = df_q["ticker"].map(op_map)

        # 저장
        df_q.to_csv(csv_path, index=False, encoding="utf-8-sig")

        rev_filled = df_q["revenue_prev"].notna().sum()
        op_filled = df_q["op_income_prev"].notna().sum()
        print(f"  Q{q}: revenue_prev={rev_filled}건, op_income_prev={op_filled}건 / {len(df_q)}건")

    print(f"\n{'='*60}")
    print(f"  패치 완료! 기존 CSV 덮어쓰기 완료.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
