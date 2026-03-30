"""
C 파일: 이상치 범위 처리 + 조건부 NaN 처리
=====================================================
역할
1) 숫자형 컬럼 강제 변환
2) 조건부 NaN 처리
   - equity < 0        -> liab_ratio NaN
   - interest == 0     -> interest_coverage NaN
   - net_income <= 0   -> insolvency_flag NaN
   - net_income < 0    -> div_ratio NaN
3) 이상치 범위 클리핑
4) NaN은 그대로 유지
"""

from __future__ import annotations

import os
from typing import Dict, List

import numpy as np
import pandas as pd

# ============================================================
# 설정
# ============================================================
DATASETS = [
    {
        "label": "KOSPI",
        "input": "master_kospi_preclean.csv",
        "output": "master_kospi_outlier_nan.csv",
    },
    {
        "label": "KOSDAQ",
        "input": "master_kosdaq_preclean.csv",
        "output": "master_kosdaq_outlier_nan.csv",
    },
]

CLIP_RULES = {
    "oper_margin": (-200.0, 100.0),
    "liab_ratio": (0.0, 1000.0),
    "curr_ratio": (0.0, 2000.0),
    "interest_coverage": (-100.0, 100.0),
    "revenue_qoq": (-100.0, 500.0),
    "oper_income_qoq": (-500.0, 500.0),
    "insolvency_flag": (-10.0, 10.0),
    "div_ratio": (0.0, 200.0),
    "z_score": (-10.0, 20.0),
}

CHECK_COLS = list(CLIP_RULES.keys()) + ["market_cap"]
CONDITION_COLS = ["equity", "interest", "net_income"]


# ============================================================
# 유틸
# ============================================================
def load_table(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path, dtype={"ticker": str})
    if ext == ".csv":
        try:
            return pd.read_csv(path, dtype={"ticker": str}, encoding="utf-8-sig")
        except UnicodeDecodeError:
            return pd.read_csv(path, dtype={"ticker": str}, encoding="cp949")
    raise ValueError(f"지원하지 않는 파일 형식: {path}")


def save_table(df: pd.DataFrame, path: str) -> None:
    ext = os.path.splitext(path)[1].lower()
    if ext in {".xlsx", ".xls"}:
        df.to_excel(path, index=False)
        return
    if ext == ".csv":
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return
    raise ValueError(f"지원하지 않는 저장 형식: {path}")


def print_null_status(df: pd.DataFrame, title: str):
    print(f"\n[{title}]")
    for col in CHECK_COLS:
        if col in df.columns:
            null_cnt = int(df[col].isna().sum())
            print(f"  {col:<18}: null {null_cnt:>6}개 ({null_cnt / len(df) * 100:.1f}%)")


# ============================================================
# 이상치 처리 본체
# ============================================================
def process_dataset(cfg: Dict[str, str]) -> None:
    label = cfg["label"]
    input_path = cfg["input"]
    output_path = cfg["output"]

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"[{label}] 입력 파일 없음: {input_path}")

    df = load_table(input_path)
    print(f"\n{'=' * 68}")
    print(f"[{label}] 로드 완료: {input_path}")
    print(f"  -> {len(df)}행 / {len(df.columns)}컬럼")

    # 숫자형 변환
    for col in set(CHECK_COLS + CONDITION_COLS):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in [c for c in CHECK_COLS if c in df.columns]:
        inf_cnt = int(np.isinf(df[col]).sum())
        if inf_cnt:
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            print(f"  {col}: inf {inf_cnt}개 -> NaN")

    print_null_status(df, f"{label} 처리 전 결측 현황")

    # 조건부 NaN 처리
    if all(c in df.columns for c in ["equity", "liab_ratio"]):
        mask = df["equity"] < 0
        df.loc[mask, "liab_ratio"] = np.nan
        print(f"  liab_ratio: equity<0 {int(mask.sum())}행 -> NaN")

    if all(c in df.columns for c in ["interest", "interest_coverage"]):
        mask = df["interest"] == 0
        df.loc[mask, "interest_coverage"] = np.nan
        print(f"  interest_coverage: interest=0 {int(mask.sum())}행 -> NaN")

    if all(c in df.columns for c in ["net_income", "insolvency_flag"]):
        mask = df["net_income"] <= 0
        df.loc[mask, "insolvency_flag"] = np.nan
        print(f"  insolvency_flag: net_income<=0 {int(mask.sum())}행 -> NaN")

    if all(c in df.columns for c in ["net_income", "div_ratio"]):
        mask = df["net_income"] < 0
        df.loc[mask, "div_ratio"] = np.nan
        print(f"  div_ratio: net_income<0 {int(mask.sum())}행 -> NaN")

    # 범위 클리핑 (NaN 유지)
    print("\n  이상치 범위 처리")
    for col, (low, high) in CLIP_RULES.items():
        if col not in df.columns:
            continue
        before = int(((df[col] < low) | (df[col] > high)).sum())
        df[col] = df[col].clip(lower=low, upper=high)
        if before:
            print(f"    - {col}: {before}개 클리핑 ({low}, {high})")

    save_table(df, output_path)
    print_null_status(df, f"{label} 처리 후 결측 현황")
    print(f"\n  저장 완료: {output_path}")

    sample_cols = [c for c in ["quarter", "period", "ticker", "corp_name"] + CHECK_COLS if c in df.columns]
    if sample_cols:
        print("\n  샘플 확인")
        print(df[sample_cols].head(6).to_string(index=False))


# ============================================================
# 실행
# ============================================================
def main():
    for cfg in DATASETS:
        process_dataset(cfg)


if __name__ == "__main__":
    main()
