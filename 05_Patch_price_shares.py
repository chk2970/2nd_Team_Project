from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List

import pandas as pd

"""
05. price / shares lookup 병합
==============================
역할
- Price_fetcher.py가 만든 (ticker, quarter, price) lookup 병합
- Shares_fetcher.py가 만든 (ticker, quarter, shares) lookup 병합
- DART_API_Fetcher.py의 연간 merged 파일에 price / shares 컬럼을 실제 반영

입력 예시
- dart_output/KOSPI/KOSPI_2022_merged.csv
- dart_output/KOSPI/KOSPI_2023_merged.csv
- dart_output/KOSDAQ/KOSDAQ_2024_merged.csv
- price_quarter.csv
- shares_quarter.csv

출력 예시
- patched_output/KOSPI_2022_merged_patched.csv
"""

# ============================================================
# CONFIG
# ============================================================
INPUT_FILES = [
    "dart_output/KOSPI/KOSPI_2022_merged.csv",
    "dart_output/KOSPI/KOSPI_2023_merged.csv",
    "dart_output/KOSPI/KOSPI_2024_merged.csv",
    "dart_output/KOSPI/KOSPI_2025_merged.csv",
    "dart_output/KOSDAQ/KOSDAQ_2022_merged.csv",
    "dart_output/KOSDAQ/KOSDAQ_2023_merged.csv",
    "dart_output/KOSDAQ/KOSDAQ_2024_merged.csv",
    "dart_output/KOSDAQ/KOSDAQ_2025_merged.csv",
]

PRICE_FILE = "price_quarter.csv"
SHARES_FILE = "shares_quarter.csv"
OUTPUT_DIR = "patched_output"
OVERWRITE_EXISTING_COLUMNS = True


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
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")



def normalize_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ticker" not in out.columns or "quarter" not in out.columns:
        raise KeyError("ticker, quarter 컬럼이 모두 필요합니다.")
    out["ticker"] = out["ticker"].astype(str).str.strip().str.zfill(6)
    out["quarter"] = out["quarter"].astype(str).str.strip().str.upper()
    return out



def load_lookup(path: str, value_col: str) -> pd.DataFrame:
    df = load_table(path)
    required = {"ticker", "quarter", value_col}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"{path}에 필요한 컬럼이 없습니다: {missing}")

    df = normalize_keys(df)
    df = df[["ticker", "quarter", value_col]].copy()
    df = df.drop_duplicates(["ticker", "quarter"], keep="last")
    return df



def patch_one_file(input_path: str, price_df: pd.DataFrame, shares_df: pd.DataFrame) -> Dict[str, object]:
    if not os.path.exists(input_path):
        return {
            "input": input_path,
            "status": "missing_input",
        }

    df = load_table(input_path)
    df = normalize_keys(df)

    before_price = int(df["price"].notna().sum()) if "price" in df.columns else 0
    before_shares = int(df["shares"].notna().sum()) if "shares" in df.columns else 0

    # 기존 컬럼 처리
    if OVERWRITE_EXISTING_COLUMNS:
        if "price" in df.columns:
            df = df.drop(columns=["price"])
        if "shares" in df.columns:
            df = df.drop(columns=["shares"])

    df = df.merge(price_df, on=["ticker", "quarter"], how="left")
    df = df.merge(shares_df, on=["ticker", "quarter"], how="left")

    after_price = int(df["price"].notna().sum()) if "price" in df.columns else 0
    after_shares = int(df["shares"].notna().sum()) if "shares" in df.columns else 0

    out_name = Path(input_path).stem + "_patched.csv"
    output_path = str(Path(OUTPUT_DIR) / out_name)
    save_csv(df, output_path)

    return {
        "input": input_path,
        "output": output_path,
        "rows": len(df),
        "price_before": before_price,
        "price_after": after_price,
        "shares_before": before_shares,
        "shares_after": after_shares,
        "status": "ok",
    }


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    if not os.path.exists(PRICE_FILE):
        raise FileNotFoundError(f"price lookup 파일이 없습니다: {PRICE_FILE}")
    if not os.path.exists(SHARES_FILE):
        raise FileNotFoundError(f"shares lookup 파일이 없습니다: {SHARES_FILE}")

    price_df = load_lookup(PRICE_FILE, "price")
    shares_df = load_lookup(SHARES_FILE, "shares")

    print("=" * 72)
    print("05. price / shares 병합 시작")
    print("=" * 72)
    print(f"price lookup : {len(price_df)}건")
    print(f"shares lookup: {len(shares_df)}건")

    results: List[Dict[str, object]] = []
    for path in INPUT_FILES:
        result = patch_one_file(path, price_df, shares_df)
        results.append(result)
        if result["status"] == "missing_input":
            print(f"[SKIP] 입력 없음: {path}")
            continue
        print(
            f"[OK] {Path(path).name} | "
            f"price {result['price_before']} -> {result['price_after']} | "
            f"shares {result['shares_before']} -> {result['shares_after']} | "
            f"저장: {result['output']}"
        )

    summary = pd.DataFrame(results)
    save_csv(summary, str(Path(OUTPUT_DIR) / "patch_price_shares_summary.csv"))

    print("\n완료")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
