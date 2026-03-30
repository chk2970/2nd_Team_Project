from __future__ import annotations

import glob
import os
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

"""
08. Master builder + 지표 생성
==============================
역할
- 연도별 patched 재무파일을 시장별로 이어붙여 master 파일 생성
- sector 통일
- 주요 분석 지표 계산
  · oper_margin
  · liab_ratio
  · curr_ratio
  · interest_coverage
  · revenue_qoq
  · oper_income_qoq
  · market_cap
  · insolvency_flag
  · div_ratio
  · z_score

권장 입력
- 05_Patch_price_shares.py 산출물
  예: patched_output/KOSPI_2022_merged_patched.csv
"""

# ============================================================
# CONFIG
# ============================================================
MARKETS = {
    "KOSPI": {
        "patterns": ["patched_output/KOSPI_*_merged_patched.csv"],
        "output": "master_kospi.csv",
    },
    "KOSDAQ": {
        "patterns": ["patched_output/KOSDAQ_*_merged_patched.csv"],
        "output": "master_kosdaq.csv",
    },
}

STANDARD_ORDER = [
    "quarter", "ticker", "corp_name", "sector", "price", "shares",
    "revenue_curr", "revenue_prev", "op_income_curr", "op_income_prev",
    "net_income", "assets", "liabilities", "equity", "cur_assets",
    "cur_liab", "retained_earnings", "interest", "cf_oper",
    "capital_increase", "short_liab", "treasury", "dividend",
    "div_yield", "oper_margin", "liab_ratio", "curr_ratio",
    "interest_coverage", "revenue_qoq", "oper_income_qoq",
    "market_cap", "insolvency_flag", "div_ratio", "z_score",
]

NUMERIC_BASE_COLS = [
    "price", "shares", "revenue_curr", "revenue_prev", "op_income_curr", "op_income_prev",
    "net_income", "assets", "liabilities", "equity", "cur_assets", "cur_liab",
    "retained_earnings", "interest", "cf_oper", "capital_increase", "short_liab",
    "treasury", "dividend", "div_yield",
]


# ============================================================
# UTIL
# ============================================================
def load_table(path: str) -> pd.DataFrame:
    ext = Path(path).suffix.lower()
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype={"ticker": str})
    if ext == ".csv":
        try:
            return pd.read_csv(path, dtype={"ticker": str}, encoding="utf-8-sig")
        except UnicodeDecodeError:
            return pd.read_csv(path, dtype={"ticker": str}, encoding="cp949")
    raise ValueError(f"지원하지 않는 파일 형식: {path}")



def save_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False, encoding="utf-8-sig")



def safe_div(a, b):
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return np.where((b == 0) | pd.isna(a) | pd.isna(b), np.nan, a / b)



def categorize_sector(val) -> str:
    t = str(val).lower()
    if "통신" in t:
        return "통신"
    if "it" in t or "소프트웨어" in t or "정보서비스" in t:
        return "IT 서비스"
    if "은행" in t:
        return "은행"
    if "증권" in t:
        return "증권"
    if "보험" in t:
        return "보험"
    if "금융" in t or "신탁" in t or "지주" in t or "투자" in t:
        return "기타금융"
    if "자동차" in t or "부품" in t or "조선" in t or "운송장비" in t:
        return "운송장비·부품"
    if "전자" in t or "반도체" in t or "정밀" in t or "전선" in t or "전기장비" in t or "케이블" in t:
        return "전기·전자"
    if "화학" in t or "석유" in t or "에너지" in t or "정제" in t:
        return "화학"
    if "제약" in t or "바이오" in t or "의약" in t:
        return "제약"
    if "기계" in t:
        return "기계·장비"
    if "금속" in t or "철강" in t:
        return "금속"
    if "비금속" in t:
        return "비금속"
    if "종이" in t or "목재" in t:
        return "종이·목재"
    if "부동산" in t or "리츠" in t:
        return "부동산"
    if "서비스" in t or "컨설팅" in t:
        return "일반서비스"
    if "섬유" in t or "의류" in t or "직물" in t or "피혁" in t or "가죽" in t:
        return "섬유·의류"
    if "음식" in t or "식품" in t or "담배" in t:
        return "음식료·담배"
    if "유통" in t or "도매" in t or "소매" in t or "판매" in t:
        return "유통"
    if "건설" in t:
        return "건설"
    if "오락" in t or "문화" in t or "스포츠" in t:
        return "오락·문화"
    if "전기" in t or "가스" in t:
        return "전기·가스"
    if "운송" in t or "창고" in t:
        return "운송·창고"
    if "농업" in t or "임업" in t or "어업" in t:
        return "농업·임업·어업"
    return "기타제조"



def gather_files(patterns: List[str]) -> List[str]:
    paths: List[str] = []
    for pattern in patterns:
        paths.extend(glob.glob(pattern))
    return sorted(set(paths))



def normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ticker" in out.columns:
        out["ticker"] = out["ticker"].astype(str).str.strip().str.zfill(6)
    if "quarter" in out.columns:
        out["quarter"] = out["quarter"].astype(str).str.strip().str.upper()
    return out



def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in STANDARD_ORDER:
        if col not in out.columns:
            out[col] = np.nan
    return out



def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in NUMERIC_BASE_COLS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    out["oper_margin"] = safe_div(out["op_income_curr"], out["revenue_curr"]) * 100
    out["liab_ratio"] = safe_div(out["liabilities"], out["equity"]) * 100
    out["curr_ratio"] = safe_div(out["cur_assets"], out["cur_liab"]) * 100
    out["interest_coverage"] = safe_div(out["op_income_curr"], out["interest"])
    out["revenue_qoq"] = safe_div(out["revenue_curr"] - out["revenue_prev"], out["revenue_prev"]) * 100
    out["oper_income_qoq"] = safe_div(out["op_income_curr"] - out["op_income_prev"], out["op_income_prev"]) * 100
    out["market_cap"] = out["price"] * out["shares"]
    out["insolvency_flag"] = safe_div(out["cf_oper"], out["net_income"])
    out["div_ratio"] = np.where(
        (pd.isna(out["dividend"])) | (pd.isna(out["net_income"])) | (out["net_income"] <= 0),
        np.nan,
        (out["dividend"] / out["net_income"]) * 100,
    )

    # Altman Z-score (분석용 lowercase z_score)
    out["z_score"] = np.where(
        (pd.isna(out["assets"])) | (pd.isna(out["liabilities"])) | (out["assets"] == 0) | (out["liabilities"] == 0),
        np.nan,
        1.2 * ((out["cur_assets"] - out["cur_liab"]) / out["assets"])
        + 1.4 * (out["retained_earnings"] / out["assets"])
        + 3.3 * (out["op_income_curr"] / out["assets"])
        + 0.6 * (out["equity"] / out["liabilities"])
        + 1.0 * (out["revenue_curr"] / out["assets"])
    )

    return out



def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    preferred = [c for c in STANDARD_ORDER if c in df.columns]
    rest = [c for c in df.columns if c not in preferred]
    return df[preferred + rest]



def build_market_master(label: str, cfg: Dict[str, object]) -> None:
    files = gather_files(cfg["patterns"])
    if not files:
        print(f"[SKIP] {label}: 입력 파일이 없습니다. patterns={cfg['patterns']}")
        return

    print("=" * 72)
    print(f"08. {label} master 생성")
    print("=" * 72)
    for path in files:
        print(f"  - {path}")

    frames = [normalize(load_table(path)) for path in files]
    df = pd.concat(frames, ignore_index=True)
    df = ensure_columns(df)

    if "sector" in df.columns:
        df["sector"] = df["sector"].apply(categorize_sector)

    df = compute_metrics(df)
    df = reorder_columns(df)

    output_path = cfg["output"]
    save_csv(df, output_path)

    print(f"\n저장 완료: {output_path}")
    print(f"  행 수: {len(df)} | 컬럼 수: {len(df.columns)}")
    check_cols = [c for c in ["price", "shares", "oper_margin", "market_cap", "z_score"] if c in df.columns]
    if check_cols:
        print(df[[c for c in ["quarter", "ticker", "corp_name", "sector"] if c in df.columns] + check_cols].head(5).to_string(index=False))


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    for label, cfg in MARKETS.items():
        build_market_master(label, cfg)


if __name__ == "__main__":
    main()
