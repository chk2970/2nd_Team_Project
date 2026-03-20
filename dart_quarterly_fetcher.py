"""
DART API 분기별 재무데이터 수집 스크립트 v2
- 대상: 2023KOSDAQ기준.xlsx의 1041개 기업
- 기간: 2023년 1Q, 2Q, 3Q, 4Q
- 핵심: 손익계산서(IS)/현금흐름표(CF) 누적금액 → 개별 분기 자동 차감
- 출력: quarter_1_2023.csv ~ quarter_4_2023.csv (순수 분기 금액)

사용법:
    1. pip install requests pandas openpyxl
    2. 아래 API_KEY에 DART 인증키 붙여넣기
    3. python dart_quarterly_fetcher.py
"""

import requests
import pandas as pd
import time
import json
import os
from pathlib import Path

# ──────────────────────────────────────────────
# 설정 ★ 여기에 API 키 붙여넣기
# ──────────────────────────────────────────────
API_KEY = "b5e77be8fad95c3e33f35eaa8d297271a4ae8119"

BASE_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
BSNS_YEAR = "2023"
INPUT_FILE = "2023KOSDAQ기준.xlsx"
OUTPUT_DIR = "output_quarters"

REPRT_CODES = {
    1: "11013",   # 1분기
    2: "11012",   # 반기(2분기)
    3: "11014",   # 3분기
    4: "11011",   # 사업보고서(4분기)
}

FS_DIVS = ["CFS", "OFS"]
SLEEP_SEC = 1.2
CHECKPOINT_EVERY = 50

# ──────────────────────────────────────────────
# 누적 vs 시점 컬럼 분류
# ──────────────────────────────────────────────
# IS/CF 항목 → DART가 누적으로 제공 → 전분기 차감 필요
CUMULATIVE_COLS = [
    "revenue_curr", "revenue_prev",
    "op_income_curr", "op_income_prev",
    "net_income", "interest", "cf_oper",
]

# BS 항목 → 시점 잔액 → 그대로 사용
POINT_IN_TIME_COLS = [
    "assets", "liabilities", "equity",
    "cur_assets", "cur_liab", "retained_earnings",
    "short_liab", "treasury", "capital_increase",
]

# ──────────────────────────────────────────────
# DART 계정과목 → 컬럼명 매핑
# sj_divs: IS/CIS 모두 탐색 (많은 기업이 CIS로 제출)
# names: 계정명 변형을 최대한 포함
# ──────────────────────────────────────────────
ACCOUNT_MAP = {
    "revenue": {
        "names": ["매출액", "수익(매출액)", "영업수익", "매출", "매출액(수익)",
                  "순매출액", "매출및지분법이익", "매출 및 지분법이익",
                  "I.매출액", "Ⅰ.매출액", "매출액 (수익)"],
        "sj_divs": ["IS", "CIS"],
        "curr_col": "thstrm_amount",
        "prev_col": "frmtrm_amount",
        "target_curr": "revenue_curr",
        "target_prev": "revenue_prev",
    },
    "op_income": {
        "names": ["영업이익", "영업이익(손실)", "영업손익", "영업이익(영업손실)",
                  "Ⅲ.영업이익", "III.영업이익", "영업이익 (손실)"],
        "sj_divs": ["IS", "CIS"],
        "curr_col": "thstrm_amount",
        "prev_col": "frmtrm_amount",
        "target_curr": "op_income_curr",
        "target_prev": "op_income_prev",
    },
    "net_income": {
        "names": ["당기순이익", "당기순이익(손실)", "분기순이익", "분기순이익(손실)",
                  "당기순손익", "반기순이익", "반기순이익(손실)", "연결당기순이익",
                  "당기순이익(당기순손실)", "당기순이익 (손실)",
                  "분기순이익 (손실)", "반기순이익 (손실)"],
        "sj_divs": ["IS", "CIS"],
        "curr_col": "thstrm_amount",
        "target": "net_income",
    },
    "interest": {
        "names": ["이자비용", "이자비용(금융비용)", "금융비용", "금융원가",
                  "이자비용 (금융비용)", "금융이자비용", "차입금이자"],
        "sj_divs": ["IS", "CIS"],
        "curr_col": "thstrm_amount",
        "target": "interest",
    },
    "assets": {
        "names": ["자산총계", "자 산 총 계"],
        "sj_divs": ["BS"],
        "curr_col": "thstrm_amount",
        "target": "assets",
    },
    "liabilities": {
        "names": ["부채총계", "부 채 총 계"],
        "sj_divs": ["BS"],
        "curr_col": "thstrm_amount",
        "target": "liabilities",
    },
    "equity": {
        "names": ["자본총계", "자 본 총 계"],
        "sj_divs": ["BS"],
        "curr_col": "thstrm_amount",
        "target": "equity",
    },
    "cur_assets": {
        "names": ["유동자산", "Ⅰ.유동자산", "I.유동자산", "유 동 자 산"],
        "sj_divs": ["BS"],
        "curr_col": "thstrm_amount",
        "target": "cur_assets",
    },
    "cur_liab": {
        "names": ["유동부채", "Ⅰ.유동부채", "I.유동부채", "유 동 부 채"],
        "sj_divs": ["BS"],
        "curr_col": "thstrm_amount",
        "target": "cur_liab",
    },
    "retained_earnings": {
        "names": ["이익잉여금", "이익잉여금(결손금)", "이익잉여금 (결손금)",
                  "미처분이익잉여금", "미처분이익잉여금(미처리결손금)"],
        "sj_divs": ["BS"],
        "curr_col": "thstrm_amount",
        "target": "retained_earnings",
    },
    "short_liab": {
        "names": ["단기차입금", "단기사채", "단기차입금및단기사채",
                  "단기차입금 및 단기사채", "유동성장기차입금",
                  "단기금융부채", "단기차입금및유동성장기부채"],
        "sj_divs": ["BS"],
        "curr_col": "thstrm_amount",
        "target": "short_liab",
    },
    "treasury": {
        "names": ["자기주식", "자기주식(-)"],
        "sj_divs": ["BS"],
        "curr_col": "thstrm_amount",
        "target": "treasury",
    },
    "cf_oper": {
        "names": ["영업활동현금흐름", "영업활동으로인한현금흐름",
                  "영업활동 현금흐름", "Ⅰ.영업활동현금흐름",
                  "I.영업활동현금흐름", "영업활동으로 인한 현금흐름"],
        "sj_divs": ["CF"],
        "curr_col": "thstrm_amount",
        "target": "cf_oper",
    },
    "capital_increase": {
        "names": ["자본금", "납입자본금", "보통주자본금"],
        "sj_divs": ["BS"],
        "curr_col": "thstrm_amount",
        "target": "capital_increase",
    },
}


def parse_amount(val):
    if val is None or val == "" or val == "-":
        return None
    try:
        return int(str(val).replace(",", ""))
    except ValueError:
        return None


def extract_fields(items):
    """API 응답에서 필요한 컬럼값 추출 — sj_div 복수 탐색 + 부분 매칭 fallback"""
    row = {}

    # (sj_div, account_nm) → item 룩업
    lookup = {}
    for item in items:
        key = (item.get("sj_div", ""), item.get("account_nm", "").strip())
        lookup[key] = item

    # account_nm만으로 역색인 (sj_div 무시 fallback용)
    name_lookup = {}
    for item in items:
        nm = item.get("account_nm", "").strip()
        if nm not in name_lookup:
            name_lookup[nm] = item

    for field_key, mapping in ACCOUNT_MAP.items():
        sj_divs = mapping["sj_divs"]
        found = False

        # 1차: (sj_div, account_nm) 정확 매칭
        for sj in sj_divs:
            for name_candidate in mapping["names"]:
                item = lookup.get((sj, name_candidate))
                if item:
                    _apply_item(row, item, mapping)
                    found = True
                    break
            if found:
                break

        # 2차: sj_div 무시하고 account_nm만으로 매칭
        if not found:
            for name_candidate in mapping["names"]:
                item = name_lookup.get(name_candidate)
                if item:
                    _apply_item(row, item, mapping)
                    found = True
                    break

        # 3차: 부분 문자열 매칭 (계정명에 핵심 키워드 포함)
        if not found:
            for name_candidate in mapping["names"]:
                for item in items:
                    actual_nm = item.get("account_nm", "").strip()
                    item_sj = item.get("sj_div", "")
                    if (name_candidate in actual_nm or actual_nm in name_candidate) \
                            and item_sj in sj_divs:
                        _apply_item(row, item, mapping)
                        found = True
                        break
                if found:
                    break

        # 못 찾으면 None
        if not found:
            if "target_curr" in mapping:
                row[mapping["target_curr"]] = None
            if "target_prev" in mapping:
                row[mapping["target_prev"]] = None
            if "target" in mapping:
                row[mapping["target"]] = None

    return row


def _apply_item(row, item, mapping):
    """매칭된 item에서 금액을 row에 적용"""
    curr_col = mapping.get("curr_col", "thstrm_amount")
    if "target_curr" in mapping:
        row[mapping["target_curr"]] = parse_amount(item.get(curr_col))
    if "target_prev" in mapping:
        prev_col = mapping.get("prev_col", "frmtrm_amount")
        row[mapping["target_prev"]] = parse_amount(item.get(prev_col))
    if "target" in mapping:
        row[mapping["target"]] = parse_amount(item.get(curr_col))


def fetch_company(api_key, corp_code, quarter):
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
            print(f"    [ERROR] {corp_code} Q{quarter} {fs_div}: {e}")
            continue

        status = data.get("status")
        if status == "000":
            result = extract_fields(data.get("list", []))
            result["fs_div"] = fs_div
            return result
        elif status == "013":
            continue
        else:
            msg = data.get("message", "unknown")
            print(f"    [WARN] {corp_code} Q{quarter} {fs_div}: {status} - {msg}")
            continue
    return {}


# ──────────────────────────────────────────────
# 핵심: 누적 → 개별 분기 차감 로직
# ──────────────────────────────────────────────
def subtract_cumulative(curr_val, prev_val):
    """curr_val(당분기 누적) - prev_val(전분기 누적) = 순수 분기 금액"""
    if curr_val is None:
        return None
    if prev_val is None:
        return curr_val
    return curr_val - prev_val


def apply_quarter_adjustment(raw_data_by_quarter):
    """
    4개 분기의 raw(누적) 데이터를 받아서 개별 분기 금액으로 변환.

    차감 규칙 (IS/CF 누적 컬럼만):
        Q1: 그대로 (1분기 단독)
        Q2: Q2누적 - Q1누적
        Q3: Q3누적 - Q2누적
        Q4: Q4누적 - Q3누적

    BS 컬럼은 시점 잔액이므로 그대로 유지.
    """
    adjusted = {q: {} for q in [1, 2, 3, 4]}
    quarter_order = [1, 2, 3, 4]

    all_tickers = set()
    for q in quarter_order:
        if q in raw_data_by_quarter:
            all_tickers.update(raw_data_by_quarter[q].keys())

    for ticker in all_tickers:
        for qi, q in enumerate(quarter_order):
            raw_curr = raw_data_by_quarter.get(q, {}).get(ticker, {})
            if not raw_curr:
                adjusted[q][ticker] = {}
                continue

            row = dict(raw_curr)

            if q == 1:
                adjusted[q][ticker] = row
            else:
                prev_q = quarter_order[qi - 1]
                raw_prev = raw_data_by_quarter.get(prev_q, {}).get(ticker, {})

                for col in CUMULATIVE_COLS:
                    curr_val = raw_curr.get(col)
                    prev_val = raw_prev.get(col) if raw_prev else None
                    row[col] = subtract_cumulative(curr_val, prev_val)

                adjusted[q][ticker] = row

    return adjusted


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────
def main():
    if API_KEY == "여기에_DART_API키_붙여넣기":
        print("ERROR: 스크립트 상단의 API_KEY를 실제 DART 인증키로 변경해주세요!")
        return

    df = pd.read_excel(INPUT_FILE)
    companies = df[["ticker", "corp_name", "sector", "corp_code"]].copy()
    companies["corp_code"] = companies["corp_code"].astype(int).astype(str).str.zfill(8)
    total = len(companies)
    print(f"총 {total}개 기업 로드 완료\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    TARGET_COLS = [
        "ticker", "corp_name", "sector", "corp_code",
        "revenue_curr", "revenue_prev",
        "op_income_curr", "op_income_prev",
        "net_income", "assets", "liabilities", "equity",
        "cur_assets", "cur_liab", "retained_earnings",
        "interest", "cf_oper", "capital_increase",
        "short_liab", "treasury", "fs_div",
    ]

    # ──────────────────────────────────────────
    # Phase 1: 4분기 raw(누적) 데이터 수집
    # ──────────────────────────────────────────
    raw_data = {}

    for q in [1, 2, 3, 4]:
        print(f"\n{'='*60}")
        print(f"  [Phase 1] 2023년 {q}분기 RAW 수집 (reprt_code: {REPRT_CODES[q]})")
        print(f"{'='*60}")

        checkpoint_file = os.path.join(OUTPUT_DIR, f"raw_checkpoint_Q{q}.json")
        results = {}
        start = 0

        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, "r") as f:
                ckpt = json.load(f)
                results = ckpt.get("results", {})
                start = ckpt.get("next_idx", 0)
                print(f"  체크포인트 로드: {len(results)}건, idx={start}부터 재개")

        for i in range(start, total):
            row = companies.iloc[i]
            ticker = str(row["ticker"])
            name = row["corp_name"]
            corp_code = row["corp_code"]

            print(f"  [{i+1}/{total}] {ticker} {name} (Q{q})...", end=" ")

            data = fetch_company(API_KEY, corp_code, q)

            if data:
                data["ticker"] = ticker
                data["corp_name"] = name
                data["sector"] = row["sector"]
                data["corp_code"] = corp_code
                results[ticker] = data
                filled = sum(1 for k, v in data.items()
                             if k not in ("ticker","corp_name","sector","corp_code","fs_div")
                             and v is not None)
                print(f"OK ({filled}개 항목)")
            else:
                results[ticker] = {
                    "ticker": ticker, "corp_name": name,
                    "sector": row["sector"], "corp_code": corp_code,
                }
                print("데이터 없음")

            time.sleep(SLEEP_SEC)

            if (i + 1) % CHECKPOINT_EVERY == 0:
                with open(checkpoint_file, "w") as f:
                    json.dump({"results": results, "next_idx": i + 1},
                              f, ensure_ascii=False)
                print(f"  --- 체크포인트 저장 ({i+1}/{total}) ---")

        raw_data[q] = results

        # RAW 원본도 별도 저장 (검증용)
        raw_file = os.path.join(OUTPUT_DIR, f"raw_quarter_{q}_2023.csv")
        raw_df = pd.DataFrame(list(results.values()))
        for col in TARGET_COLS:
            if col not in raw_df.columns:
                raw_df[col] = None
        raw_df[TARGET_COLS].to_csv(raw_file, index=False, encoding="utf-8-sig")
        print(f"  → RAW 저장: {raw_file} ({len(results)}건)")

        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)

    # ──────────────────────────────────────────
    # Phase 2: 누적 → 개별 분기 차감
    # ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  [Phase 2] IS/CF 누적금액 → 개별 분기 차감 적용")
    print(f"{'='*60}")
    print(f"  대상 컬럼: {CUMULATIVE_COLS}")
    print(f"  차감 규칙: Q1=그대로, Q2=Q2-Q1, Q3=Q3-Q2, Q4=Q4-Q3\n")

    adjusted = apply_quarter_adjustment(raw_data)

    for q in [1, 2, 3, 4]:
        rows = list(adjusted[q].values())
        if not rows:
            print(f"  Q{q}: 데이터 없음, 건너뜀")
            continue

        out_df = pd.DataFrame(rows)
        for col in TARGET_COLS:
            if col not in out_df.columns:
                out_df[col] = None
        out_df = out_df[TARGET_COLS]

        output_file = os.path.join(OUTPUT_DIR, f"quarter_{q}_2023.csv")
        out_df.to_csv(output_file, index=False, encoding="utf-8-sig")

        # 차감 전후 비교 검증 (샘플)
        if q > 1:
            prev_q = q - 1
            sample_tickers = [t for t in list(adjusted[q].keys())
                              if adjusted[q][t].get("revenue_curr") is not None][:2]
            if sample_tickers:
                print(f"  Q{q} 차감 검증 (샘플):")
                for t in sample_tickers:
                    raw_v = raw_data.get(q, {}).get(t, {}).get("revenue_curr")
                    prev_v = raw_data.get(prev_q, {}).get(t, {}).get("revenue_curr")
                    adj_v = adjusted[q][t].get("revenue_curr")
                    if all(v is not None for v in [raw_v, prev_v, adj_v]):
                        print(f"    {t}: 매출누적={raw_v:>15,} - 전분기누적={prev_v:>15,} = 순수분기={adj_v:>15,}")
                    else:
                        print(f"    {t}: 일부 데이터 없음 (raw={raw_v}, prev={prev_v})")

        print(f"  → 최종 저장: {output_file} ({len(out_df)}건)")

    # ──────────────────────────────────────────
    # 완료 요약
    # ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  전체 완료!")
    print(f"{'='*60}")
    print(f"  출력 디렉토리: {OUTPUT_DIR}/")
    print(f"  ├── raw_quarter_1~4_2023.csv  (원본 누적 데이터 - 검증용)")
    print(f"  └── quarter_1~4_2023.csv      (개별 분기 차감 완료 - 최종)")
    print(f"\n  IS/CF 차감 적용 컬럼 (7개):")
    for col in CUMULATIVE_COLS:
        print(f"    ✓ {col}")
    print(f"\n  BS 시점잔액 유지 컬럼 (9개):")
    for col in POINT_IN_TIME_COLS:
        print(f"    · {col}")


if __name__ == "__main__":
    main()