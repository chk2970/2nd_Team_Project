"""
DART API 통합 재무데이터 수집 스크립트
======================================
- 대상: KOSPI / KOSDAQ (입력 엑셀 기반)
- 기간: 2022~2025 (설정 가능)
- 핵심 로직:
    · CFS 우선 → OFS 폴백
    · IS > CIS sj_div 우선순위 + 정확매칭 > contains 폴백
    · IS/CF 누적금액 → 개별 분기 자동 차감
    · frmtrm_amount로 전년동기(revenue_prev, op_income_prev) 자동 추출
    · 체크포인트 저장/재개 (중단 후 이어서 수집 가능)
- 출력: {OUTPUT_DIR}/{market}_{year}_Q{q}.csv  (분기별)
        {OUTPUT_DIR}/{market}_{year}_merged.csv (연간 합본)

사용법:
    1. pip install requests pandas openpyxl
    2. 아래 CONFIG 섹션 수정 (API_KEY, 입력파일 등)
    3. python unified_dart_fetcher.py

결합 출처:
    · dart_quarterly_fetcher.py  → 기본 구조, 체크포인트, 누적차감
    · dart_fetcher_2022.py       → ACCOUNT_MAP, 3단계 매칭 로직
    · 2024_4th_v2.py             → sj_div 필터, 정확매칭 우선
    · kospi결측기업재추출.py     → CFS→OFS 폴백, 정교한 계정명 매핑
    · KOSPI_2025_재무API.py      → corpCode.xml 자동 매핑
"""

import requests
import pandas as pd
import time
import json
import os
import zipfile
import io
import xml.etree.ElementTree as ET
from datetime import datetime


# ╔══════════════════════════════════════════════════════════════╗
# ║  CONFIG — 여기만 수정하면 됨                                   ║
# ╚══════════════════════════════════════════════════════════════╝

API_KEY = ""   # ← DART API 인증키 붙여넣기

OUTPUT_DIR = "dart_output"

# 수집 대상 정의: (market_label, input_file, year, quarters)
# input_file 컬럼 요구사항: ticker, corp_name, sector, corp_code
# corp_code가 없으면 DART corpCode.xml에서 자동 매핑 (AUTO_CORP_CODE = True)
JOBS = [
    # ── KOSDAQ ──
    ("KOSDAQ", "2023KOSDAQ기준.xlsx", 2022, [1, 2, 3, 4]),
    ("KOSDAQ", "2023KOSDAQ기준.xlsx", 2023, [1, 2, 3, 4]),
    ("KOSDAQ", "2023KOSDAQ기준.xlsx", 2024, [1, 2, 3, 4]),
    ("KOSDAQ", "2023KOSDAQ기준.xlsx", 2025, [1, 2, 3]),

    # ── KOSPI ──
    ("KOSPI", "2023KOSPI기준.xlsx", 2022, [1, 2, 3, 4]),
    ("KOSPI", "2023KOSPI기준.xlsx", 2023, [1, 2, 3, 4]),
    ("KOSPI", "2023KOSPI기준.xlsx", 2024, [1, 2, 3, 4]),
    ("KOSPI", "2023KOSPI기준.xlsx", 2025, [1, 2, 3]),
]

# corp_code 컬럼이 입력파일에 없을 때 DART에서 자동 매핑할지 여부
AUTO_CORP_CODE = True

# API 호출 간격 (초) — DART 분당 제한 준수
SLEEP_SEC = 1.0

# 체크포인트 저장 주기 (N개 기업마다)
CHECKPOINT_EVERY = 50


# ╔══════════════════════════════════════════════════════════════╗
# ║  보고서 코드 / 컬럼 분류 / 계정 매핑                            ║
# ╚══════════════════════════════════════════════════════════════╝

BASE_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
FS_DIVS = ["CFS", "OFS"]

REPRT_CODES = {
    1: "11013",   # 1분기보고서
    2: "11012",   # 반기보고서
    3: "11014",   # 3분기보고서
    4: "11011",   # 사업보고서 (연간)
}

# IS/CF 항목 → DART가 누적으로 제공 → 전분기 누적 차감 필요
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

# DART 계정과목 → 우리 컬럼명 매핑
# sj_divs: 탐색할 재무제표 구분 (우선순위순)
# names: 계정명 변형 (정확도 높은 순서)
ACCOUNT_MAP = {
    "revenue": {
        "names": [
            "매출액", "수익(매출액)", "영업수익", "매출액(수익)",
            "순매출액", "매출", "매출및지분법이익", "매출 및 지분법이익",
            "영업수익(매출액)", "I.매출액", "Ⅰ.매출액", "매출액 (수익)",
        ],
        "sj_divs": ["IS", "CIS"],
        "curr_col": "thstrm_amount",
        "prev_col": "frmtrm_amount",
        "target_curr": "revenue_curr",
        "target_prev": "revenue_prev",
    },
    "op_income": {
        "names": [
            "영업이익", "영업이익(손실)", "영업손익",
            "영업이익(영업손실)", "영업이익 (손실)",
            "Ⅲ.영업이익", "III.영업이익",
        ],
        "sj_divs": ["IS", "CIS"],
        "curr_col": "thstrm_amount",
        "prev_col": "frmtrm_amount",
        "target_curr": "op_income_curr",
        "target_prev": "op_income_prev",
    },
    "net_income": {
        "names": [
            "당기순이익", "당기순이익(손실)", "분기순이익", "분기순이익(손실)",
            "당기순손익", "반기순이익", "반기순이익(손실)", "연결당기순이익",
            "당기순이익(당기순손실)", "당기순이익 (손실)",
            "분기순이익 (손실)", "반기순이익 (손실)",
            "지배기업의소유주에게귀속되는당기순이익",
            "지배기업의소유주에게귀속되는분기순이익",
            "지배기업소유주지분순이익",
        ],
        "sj_divs": ["IS", "CIS"],
        "curr_col": "thstrm_amount",
        "target": "net_income",
    },
    "interest": {
        "names": [
            "이자비용", "이자비용(금융비용)", "금융비용", "금융원가",
            "이자비용 (금융비용)", "금융이자비용", "차입금이자",
        ],
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
        "names": [
            "이익잉여금", "이익잉여금(결손금)", "이익잉여금 (결손금)",
            "미처분이익잉여금", "미처분이익잉여금(미처리결손금)",
        ],
        "sj_divs": ["BS"],
        "curr_col": "thstrm_amount",
        "target": "retained_earnings",
    },
    "short_liab": {
        "names": [
            "단기차입금", "단기사채", "단기차입금및단기사채",
            "단기차입금 및 단기사채", "유동성장기차입금",
            "단기금융부채", "단기차입금및유동성장기부채",
        ],
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
        "names": [
            "영업활동현금흐름", "영업활동으로인한현금흐름",
            "영업활동 현금흐름", "Ⅰ.영업활동현금흐름",
            "I.영업활동현금흐름", "영업활동으로 인한 현금흐름",
            "영업활동으로인한순현금흐름",
        ],
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

# 최종 출력 컬럼 순서
OUTPUT_COLS = [
    "quarter", "ticker", "corp_name", "sector",
    "revenue_curr", "revenue_prev",
    "op_income_curr", "op_income_prev",
    "net_income", "assets", "liabilities", "equity",
    "cur_assets", "cur_liab", "retained_earnings",
    "interest", "cf_oper", "capital_increase",
    "short_liab", "treasury", "fs_div",
]


# ╔══════════════════════════════════════════════════════════════╗
# ║  유틸리티 함수                                                 ║
# ╚══════════════════════════════════════════════════════════════╝

def parse_amount(val):
    """DART 금액 문자열 → int 변환. 실패 시 None."""
    if val is None or str(val).strip() in ("", "-", "None"):
        return None
    try:
        return int(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def load_corp_code_xml(api_key):
    """DART corpCode.xml → {stock_code: corp_code} dict"""
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


def load_companies(input_file, corp_code_xml=None):
    """
    입력 엑셀 로드 → DataFrame (ticker, corp_name, sector, corp_code).
    corp_code 컬럼이 없으면 corp_code_xml로 자동 매핑.
    """
    df = pd.read_excel(input_file, dtype={"ticker": str})

    # ticker 정규화
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)
    else:
        raise ValueError(f"입력 파일에 'ticker' 컬럼이 없습니다: {input_file}")

    # 필수 컬럼 확인
    for col in ["corp_name", "sector"]:
        if col not in df.columns:
            df[col] = ""

    # corp_code 처리
    if "corp_code" in df.columns:
        df["corp_code"] = df["corp_code"].astype(str).str.strip().str.zfill(8)
        # 'nan' 등 무효값 정리
        df.loc[df["corp_code"].str.contains("nan", case=False, na=True), "corp_code"] = ""
    elif corp_code_xml:
        df["corp_code"] = df["ticker"].map(corp_code_xml).fillna("")
        mapped = (df["corp_code"] != "").sum()
        print(f"  → corp_code 자동매핑: {mapped}/{len(df)}개 성공")
    else:
        raise ValueError(
            f"입력 파일에 'corp_code' 컬럼이 없고 AUTO_CORP_CODE도 비활성입니다: {input_file}"
        )

    # corp_code 없는 행 제거
    df = df[df["corp_code"] != ""].copy()
    df["corp_code"] = df["corp_code"].str.zfill(8)

    # 중복 제거
    df = df.drop_duplicates(subset=["ticker"]).reset_index(drop=True)

    return df[["ticker", "corp_name", "sector", "corp_code"]]


# ╔══════════════════════════════════════════════════════════════╗
# ║  DART API 호출 & 계정 추출                                     ║
# ╚══════════════════════════════════════════════════════════════╝

def extract_fields(items):
    """
    API 응답 list에서 필요한 컬럼값 추출.
    3단계 매칭: ① (sj_div, 정확매칭) → ② (sj_div 무시, 정확매칭) → ③ (부분매칭)
    sj_div 우선순위: IS > CIS (매출/영업이익), BS, CF
    """
    row = {}

    # (sj_div, account_nm) → item 룩업 테이블
    lookup = {}
    for item in items:
        key = (item.get("sj_div", ""), item.get("account_nm", "").strip())
        if key not in lookup:       # 첫 번째 매칭 우선
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

        # ── 1차: (sj_div, account_nm) 정확 매칭 ──
        for sj in sj_divs:
            for name_candidate in mapping["names"]:
                item = lookup.get((sj, name_candidate))
                if item:
                    _apply_item(row, item, mapping)
                    found = True
                    break
            if found:
                break

        # ── 2차: sj_div 무시, account_nm 정확 매칭 ──
        if not found:
            for name_candidate in mapping["names"]:
                item = name_lookup.get(name_candidate)
                if item:
                    _apply_item(row, item, mapping)
                    found = True
                    break

        # ── 3차: 부분 문자열 매칭 (contains) ──
        if not found:
            for name_candidate in mapping["names"]:
                for item in items:
                    actual_nm = item.get("account_nm", "").strip()
                    item_sj = item.get("sj_div", "")
                    if item_sj in sj_divs and (
                        name_candidate in actual_nm or actual_nm in name_candidate
                    ):
                        _apply_item(row, item, mapping)
                        found = True
                        break
                if found:
                    break

        # ── 못 찾으면 None ──
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


def fetch_company(api_key, corp_code, year, quarter):
    """
    한 기업의 한 분기 재무데이터 수집.
    CFS 우선, 없으면 OFS 폴백.
    반환: dict (컬럼명: 값) or {}
    """
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
            continue

        status = data.get("status")
        if status == "000":
            result = extract_fields(data.get("list", []))
            result["fs_div"] = fs_div
            return result
        elif status == "013":
            # 데이터 없음 → 다음 fs_div 시도
            continue
        # 기타 에러도 다음 fs_div 시도

    return {}


# ╔══════════════════════════════════════════════════════════════╗
# ║  누적 → 개별 분기 차감                                         ║
# ╚══════════════════════════════════════════════════════════════╝

def subtract_cumulative(curr_val, prev_val):
    """curr(당분기 누적) - prev(전분기 누적) = 순수 분기 금액"""
    if curr_val is None:
        return None
    if prev_val is None:
        return curr_val
    return curr_val - prev_val


def apply_quarter_adjustment(raw_data_by_quarter, quarters):
    """
    여러 분기의 raw(누적) 데이터 → 개별 분기 금액으로 변환.
    
    차감 규칙 (IS/CF 누적 컬럼만):
        Q1: 그대로 (1분기 단독)
        Q2: Q2누적 - Q1누적
        Q3: Q3누적 - Q2누적
        Q4: Q4누적(=연간) - Q3누적
    
    BS 컬럼은 시점 잔액이므로 그대로 유지.
    """
    quarters_sorted = sorted(quarters)
    adjusted = {q: {} for q in quarters_sorted}

    # 모든 ticker 수집
    all_tickers = set()
    for q in quarters_sorted:
        if q in raw_data_by_quarter:
            all_tickers.update(raw_data_by_quarter[q].keys())

    for ticker in all_tickers:
        for qi, q in enumerate(quarters_sorted):
            raw_curr = raw_data_by_quarter.get(q, {}).get(ticker, {})
            if not raw_curr:
                adjusted[q][ticker] = {}
                continue

            row = dict(raw_curr)

            if qi == 0:
                # 해당 연도 첫 번째 분기 → 그대로
                adjusted[q][ticker] = row
            else:
                prev_q = quarters_sorted[qi - 1]
                raw_prev = raw_data_by_quarter.get(prev_q, {}).get(ticker, {})

                for col in CUMULATIVE_COLS:
                    curr_val = raw_curr.get(col)
                    prev_val = raw_prev.get(col) if raw_prev else None
                    row[col] = subtract_cumulative(curr_val, prev_val)

                adjusted[q][ticker] = row

    return adjusted


# ╔══════════════════════════════════════════════════════════════╗
# ║  메인 수집 루프                                                ║
# ╚══════════════════════════════════════════════════════════════╝

def collect_year(market, companies, year, quarters, output_dir):
    """
    한 시장의 한 해 전체 분기를 수집하고 CSV 저장.
    
    1단계(Phase 1): 각 분기 RAW(누적) 데이터 수집 + 체크포인트
    2단계(Phase 2): IS/CF 누적 → 개별 분기 차감
    3단계(Phase 3): 분기별 CSV + 연간 병합 CSV 저장
    """
    total = len(companies)
    year_short = str(year)[2:]   # 25, 24, 23, 22

    # ──────────────────────────────────────────
    # Phase 1: RAW 수집
    # ──────────────────────────────────────────
    raw_data = {}

    for q in quarters:
        print(f"\n{'='*60}")
        print(f"  [{market} {year}] Q{q} RAW 수집 (reprt_code: {REPRT_CODES[q]})")
        print(f"{'='*60}")

        ckpt_file = os.path.join(output_dir, f"ckpt_{market}_{year}_Q{q}.json")
        results = {}
        start = 0

        # 체크포인트 로드
        if os.path.exists(ckpt_file):
            with open(ckpt_file, "r", encoding="utf-8") as f:
                ckpt = json.load(f)
                results = ckpt.get("results", {})
                start = ckpt.get("next_idx", 0)
                print(f"  체크포인트 로드: {len(results)}건 완료, idx={start}부터 재개")

        for i in range(start, total):
            row = companies.iloc[i]
            ticker = row["ticker"]
            name = row["corp_name"]
            corp_code = row["corp_code"]

            data = fetch_company(API_KEY, corp_code, year, q)

            if data:
                data["ticker"] = ticker
                data["corp_name"] = name
                data["sector"] = row["sector"]
                results[ticker] = data

                filled = sum(
                    1 for k, v in data.items()
                    if k not in ("ticker", "corp_name", "sector", "fs_div")
                    and v is not None
                )
                if (i + 1) % 50 == 0 or i == 0:
                    print(f"  [{i+1}/{total}] {ticker} {name} → OK ({filled}개 항목)")
            else:
                results[ticker] = {
                    "ticker": ticker, "corp_name": name, "sector": row["sector"],
                }
                if (i + 1) % 50 == 0 or i == 0:
                    print(f"  [{i+1}/{total}] {ticker} {name} → 데이터 없음")

            time.sleep(SLEEP_SEC)

            # 체크포인트 저장
            if (i + 1) % CHECKPOINT_EVERY == 0:
                with open(ckpt_file, "w", encoding="utf-8") as f:
                    json.dump({"results": results, "next_idx": i + 1},
                              f, ensure_ascii=False)

        raw_data[q] = results

        # RAW 원본 저장 (디버깅/검증용)
        raw_df = pd.DataFrame(list(results.values()))
        raw_file = os.path.join(output_dir, f"raw_{market}_{year}_Q{q}.csv")
        _save_df(raw_df, raw_file)
        print(f"  → RAW 저장: {raw_file} ({len(results)}건)")

        # 체크포인트 정리
        if os.path.exists(ckpt_file):
            os.remove(ckpt_file)

    # ──────────────────────────────────────────
    # Phase 2: 누적 → 개별 분기 차감
    # ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  [{market} {year}] IS/CF 누적 → 개별 분기 차감")
    print(f"  대상: {CUMULATIVE_COLS}")
    print(f"  규칙: Q1=그대로, Q2=Q2-Q1, Q3=Q3-Q2, Q4=Q4-Q3")
    print(f"{'='*60}")

    adjusted = apply_quarter_adjustment(raw_data, quarters)

    # ──────────────────────────────────────────
    # Phase 3: CSV 저장
    # ──────────────────────────────────────────
    all_quarter_dfs = []

    for q in quarters:
        rows = list(adjusted[q].values())
        if not rows:
            print(f"  Q{q}: 데이터 없음, 건너뜀")
            continue

        out_df = pd.DataFrame(rows)
        out_df["quarter"] = f"{year_short}Q{q}"

        # 컬럼 정렬
        for col in OUTPUT_COLS:
            if col not in out_df.columns:
                out_df[col] = None
        out_df = out_df[OUTPUT_COLS]

        # 분기별 저장
        q_file = os.path.join(output_dir, f"{market}_{year}_Q{q}.csv")
        _save_df(out_df, q_file)
        print(f"  → {q_file} ({len(out_df)}건)")

        all_quarter_dfs.append(out_df)

        # 차감 검증 (샘플)
        if q > min(quarters):
            _print_verification_sample(raw_data, adjusted, q, quarters)

    # 연간 병합 저장
    if all_quarter_dfs:
        merged = pd.concat(all_quarter_dfs, ignore_index=True)
        merged_file = os.path.join(output_dir, f"{market}_{year}_merged.csv")
        _save_df(merged, merged_file)
        print(f"  → 병합: {merged_file} ({len(merged)}건, {len(all_quarter_dfs)}개 분기)")


def _save_df(df, path):
    """DataFrame → CSV 저장 (utf-8-sig)"""
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _print_verification_sample(raw_data, adjusted, q, quarters):
    """차감 검증 샘플 출력"""
    quarters_sorted = sorted(quarters)
    qi = quarters_sorted.index(q)
    prev_q = quarters_sorted[qi - 1]

    sample_tickers = [
        t for t in list(adjusted[q].keys())
        if adjusted[q][t].get("revenue_curr") is not None
    ][:2]

    if sample_tickers:
        for t in sample_tickers:
            raw_v = raw_data.get(q, {}).get(t, {}).get("revenue_curr")
            prev_v = raw_data.get(prev_q, {}).get(t, {}).get("revenue_curr")
            adj_v = adjusted[q][t].get("revenue_curr")
            if all(v is not None for v in [raw_v, prev_v, adj_v]):
                print(f"    검증 {t}: Q{q}누적={raw_v:>15,} - Q{prev_q}누적={prev_v:>15,} = 단일분기={adj_v:>15,}")


# ╔══════════════════════════════════════════════════════════════╗
# ║  실행                                                        ║
# ╚══════════════════════════════════════════════════════════════╝

def main():
    start_time = datetime.now()

    # API KEY 확인
    if not API_KEY or API_KEY == "여기에_DART_API키_붙여넣기":
        print("ERROR: 스크립트 상단의 API_KEY를 실제 DART 인증키로 설정해주세요!")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # corp_code 자동매핑용 XML 다운로드 (필요 시 1회)
    corp_code_xml = None
    if AUTO_CORP_CODE:
        corp_code_xml = load_corp_code_xml(API_KEY)

    # 입력파일 캐시 (같은 파일 중복 로드 방지)
    company_cache = {}

    # Job별 실행
    for job_idx, (market, input_file, year, quarters) in enumerate(JOBS):
        print(f"\n\n{'#'*60}")
        print(f"  JOB [{job_idx+1}/{len(JOBS)}]  {market} {year}년 Q{min(quarters)}~Q{max(quarters)}")
        print(f"  입력: {input_file}")
        print(f"{'#'*60}")

        # 입력파일 존재 확인
        if not os.path.exists(input_file):
            print(f"  ⚠ 입력파일 없음: {input_file} → 건너뜀")
            continue

        # 기업 목록 로드 (캐시)
        cache_key = input_file
        if cache_key not in company_cache:
            print(f"  기업 목록 로드 중: {input_file}")
            company_cache[cache_key] = load_companies(input_file, corp_code_xml)
        companies = company_cache[cache_key]
        print(f"  대상: {len(companies)}개 기업")

        # 시장별 출력 디렉토리
        market_dir = os.path.join(OUTPUT_DIR, market)
        os.makedirs(market_dir, exist_ok=True)

        # 수집 실행
        collect_year(market, companies, year, quarters, market_dir)

    # 완료 요약
    elapsed = datetime.now() - start_time
    print(f"\n\n{'='*60}")
    print(f"  전체 완료!")
    print(f"  소요시간: {elapsed}")
    print(f"  출력: {OUTPUT_DIR}/")
    print(f"{'='*60}")

    # 출력 파일 목록
    for root, dirs, files in os.walk(OUTPUT_DIR):
        for f in sorted(files):
            if f.endswith(".csv") and not f.startswith("raw_"):
                fpath = os.path.join(root, f)
                try:
                    df = pd.read_csv(fpath, nrows=1)
                    rows = len(pd.read_csv(fpath))
                    print(f"  {os.path.relpath(fpath, OUTPUT_DIR):40s} {rows:>6}행")
                except:
                    print(f"  {os.path.relpath(fpath, OUTPUT_DIR):40s} (읽기 실패)")


if __name__ == "__main__":
    main()
