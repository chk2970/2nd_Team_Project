from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests

"""
06. Revenue 예외 재수집 템플릿
=============================
역할
- 특정 연도 데이터에서 revenue_curr가 0 또는 NaN인 행만 골라 DART로 재수집
- thstrm_add_amount 우선, 없으면 누적 차감 방식으로 단일분기 revenue_curr 재계산
- 원본 파일에 revenue_curr를 패치한 새 파일 저장

주의
- 기본 파이프라인 필수 단계는 아님
- 01~05 이후에도 revenue_curr 예외값이 남을 때만 사용
- corp_code, quarter, ticker 컬럼이 있어야 함
"""

API_KEY = ""
INPUT_FILE = "patched_output/KOSPI_2022_merged_patched.csv"
OUTPUT_FILE = "patched_output/KOSPI_2022_merged_patched_revenue_fixed.csv"
TARGET_YEAR = 2022
SLEEP_SEC = 0.6
ONLY_PATCH_MISSING_OR_ZERO = True

BASE_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
REVENUE_NAMES = [
    "매출액", "수익(매출액)", "영업수익", "매출액(수익)", "순매출액", "매출",
    "영업수익(매출액)", "I.매출액", "Ⅰ.매출액", "매출액 (수익)",
]
REPORT_CODES = {1: "11013", 2: "11012", 3: "11014", 4: "11011"}


def load_table(path: str) -> pd.DataFrame:
    ext = Path(path).suffix.lower()
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype={"ticker": str, "corp_code": str})
    if ext == ".csv":
        try:
            return pd.read_csv(path, dtype={"ticker": str, "corp_code": str}, encoding="utf-8-sig")
        except UnicodeDecodeError:
            return pd.read_csv(path, dtype={"ticker": str, "corp_code": str}, encoding="cp949")
    raise ValueError(f"지원하지 않는 파일 형식: {path}")



def save_table(df: pd.DataFrame, path: str) -> None:
    ext = Path(path).suffix.lower()
    if ext in {".xlsx", ".xls"}:
        df.to_excel(path, index=False)
    elif ext == ".csv":
        df.to_csv(path, index=False, encoding="utf-8-sig")
    else:
        raise ValueError(f"지원하지 않는 저장 형식: {path}")



def parse_amount(val) -> Optional[int]:
    if val is None or str(val).strip() in {"", "-", "None"}:
        return None
    try:
        return int(str(val).replace(",", "").strip())
    except Exception:
        return None



def parse_quarter(text: str) -> Optional[int]:
    s = str(text).strip().upper()
    if "Q1" in s:
        return 1
    if "Q2" in s:
        return 2
    if "Q3" in s:
        return 3
    if "Q4" in s:
        return 4
    return None



def fetch_revenue_raw(api_key: str, corp_code: str, year: int, quarter: int) -> Tuple[Optional[int], Optional[int], Optional[str]]:
    reprt_code = REPORT_CODES[quarter]
    corp_code = str(corp_code).zfill(8)
    for fs_div in ["CFS", "OFS"]:
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
        except Exception:
            continue

        if data.get("status") != "000":
            continue

        items = data.get("list", [])
        is_items = [x for x in items if x.get("sj_div") in {"IS", "CIS"}]

        # exact match
        for name in REVENUE_NAMES:
            for item in is_items:
                if str(item.get("account_nm", "")).strip() == name:
                    return parse_amount(item.get("thstrm_amount")), parse_amount(item.get("thstrm_add_amount")), fs_div

        # contains fallback
        for item in is_items:
            acc = str(item.get("account_nm", "")).strip()
            if any(name in acc for name in REVENUE_NAMES):
                return parse_amount(item.get("thstrm_amount")), parse_amount(item.get("thstrm_add_amount")), fs_div

    return None, None, None



def compute_single_quarter(raw: Dict[int, Tuple[Optional[int], Optional[int], Optional[str]]], quarter: int) -> Optional[int]:
    amt, add_amt, _ = raw.get(quarter, (None, None, None))
    if quarter == 1:
        return amt
    if add_amt is not None:
        return add_amt
    prev_amt, _, _ = raw.get(quarter - 1, (None, None, None))
    if amt is not None and prev_amt is not None:
        return amt - prev_amt
    return None



def main() -> None:
    if not API_KEY:
        raise ValueError("API_KEY를 입력하세요.")
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"입력 파일이 없습니다: {INPUT_FILE}")

    df = load_table(INPUT_FILE)
    required = ["ticker", "corp_code", "quarter", "revenue_curr"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"필수 컬럼이 없습니다: {missing}")

    df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)
    df["quarter"] = df["quarter"].astype(str).str.strip().str.upper()
    df["corp_code"] = df["corp_code"].astype(str).str.replace(".0", "", regex=False).str.zfill(8)
    df["qnum"] = df["quarter"].apply(parse_quarter)
    df["year"] = pd.to_numeric(df["quarter"].str.extract(r"^(\d{2})", expand=False), errors="coerce") + 2000

    work = df[df["year"] == TARGET_YEAR].copy()
    if ONLY_PATCH_MISSING_OR_ZERO:
        work = work[work["revenue_curr"].isna() | (pd.to_numeric(work["revenue_curr"], errors="coerce") == 0)].copy()

    targets = work[["ticker", "corp_code"]].drop_duplicates()
    print(f"대상 기업 수: {len(targets)}")

    patch_rows = []
    for i, row in enumerate(targets.itertuples(index=False), start=1):
        ticker = row.ticker
        corp_code = row.corp_code
        print(f"[{i}/{len(targets)}] {ticker} / corp_code={corp_code}")

        raw = {}
        for q in [1, 2, 3, 4]:
            amt, add_amt, fs_div = fetch_revenue_raw(API_KEY, corp_code, TARGET_YEAR, q)
            raw[q] = (amt, add_amt, fs_div)
            time.sleep(SLEEP_SEC)

        for q in [1, 2, 3, 4]:
            value = compute_single_quarter(raw, q)
            patch_rows.append({
                "ticker": ticker,
                "quarter": f"{str(TARGET_YEAR)[2:]}Q{q}",
                "revenue_curr_refetched": value,
            })

    patch_df = pd.DataFrame(patch_rows)
    merged = df.merge(patch_df, on=["ticker", "quarter"], how="left")
    old_val = pd.to_numeric(merged["revenue_curr"], errors="coerce")
    new_val = pd.to_numeric(merged["revenue_curr_refetched"], errors="coerce")

    if ONLY_PATCH_MISSING_OR_ZERO:
        merged["revenue_curr"] = np.where(old_val.isna() | (old_val == 0), new_val, old_val)
    else:
        merged["revenue_curr"] = new_val.combine_first(old_val)

    save_table(merged.drop(columns=["qnum", "year"], errors="ignore"), OUTPUT_FILE)
    patch_log = str(Path(OUTPUT_FILE).with_name(Path(OUTPUT_FILE).stem + "_patch_log.csv"))
    patch_df.to_csv(patch_log, index=False, encoding="utf-8-sig")

    print(f"\n저장 완료: {OUTPUT_FILE}")
    print(f"패치 로그: {patch_log}")


if __name__ == "__main__":
    import numpy as np
    main()
