from __future__ import annotations

import os
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

"""
12. F-score 합산 / 그룹화
=========================
역할
- Derived_pipeline.py가 만든 9개 F-score T/F 컬럼을 합산
- fscore_total 생성
- fscore_group 생성 (HIGH / MID / LOW)

기본 규칙
- HIGH: 7~9점
- MID : 4~6점
- LOW : 0~3점
"""

INPUT_FILES = [
    "derived_output_Fscore.xlsx",
]
OUTPUT_SUFFIX = "_grouped"
FSCORE_COLS = ["ROA", "ΔROA", "CFO", "ACCRUAL", "ΔLEVER", "ΔLIQUID", "EQ_OFFER", "ΔMARGIN", "ΔTURN"]


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



def save_table(df: pd.DataFrame, path: str) -> None:
    ext = Path(path).suffix.lower()
    if ext in {".xlsx", ".xls"}:
        df.to_excel(path, index=False)
    elif ext == ".csv":
        df.to_csv(path, index=False, encoding="utf-8-sig")
    else:
        raise ValueError(f"지원하지 않는 저장 형식: {path}")



def output_path_for(input_path: str) -> str:
    p = Path(input_path)
    return str(p.with_name(p.stem + OUTPUT_SUFFIX + p.suffix))



def classify_group(total: pd.Series) -> pd.Series:
    return np.select(
        [total >= 7, total >= 4, total < 4],
        ["HIGH", "MID", "LOW"],
        default="LOW",
    )



def process_one(path: str) -> None:
    if not os.path.exists(path):
        print(f"[SKIP] 파일 없음: {path}")
        return

    df = load_table(path)
    missing = [c for c in FSCORE_COLS if c not in df.columns]
    if missing:
        raise KeyError(f"{path}에 F-score 컬럼이 없습니다: {missing}")

    for col in FSCORE_COLS:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        df[col] = np.where(df[col] >= 1, 1, 0)

    df["fscore_total"] = df[FSCORE_COLS].sum(axis=1).astype(int)
    df["fscore_group"] = classify_group(df["fscore_total"])

    out_path = output_path_for(path)
    save_table(df, out_path)

    print("=" * 72)
    print(f"12. F-score 그룹화 완료: {path}")
    print(f"저장: {out_path}")
    print(df["fscore_group"].value_counts(dropna=False).to_string())

    sample_cols = [c for c in ["quarter", "period", "ticker", "corp_name"] if c in df.columns] + ["fscore_total", "fscore_group"]
    print(df[sample_cols].head(8).to_string(index=False))



def main() -> None:
    for path in INPUT_FILES:
        process_one(path)


if __name__ == "__main__":
    main()
