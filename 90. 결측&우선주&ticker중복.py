import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

# ============================================================
# 데이터 품질 검증 통합 스크립트
# - check_missing_values.py
# - ticker_check_duplicates.py
# - 검산.py
# ============================================================
# 역할:
# 1) 결측치 요약 / 핵심 결측 행 상세 / 분기별 결측 패턴
# 2) 파일 내부 중복(ticker × quarter) / 시장 간 중복 ticker 점검
# 3) 우선주 의심 ticker(끝자리 5, 7) 점검
# 4) 필요 시 결측 행을 CSV로 저장
#
# 주의:
# - 이 파일은 "검증(QA) 전용"입니다.
# - 원본 데이터를 수정하지 않습니다.
# ============================================================

# [1] 기본 설정 -------------------------------------------------
DATASETS: Dict[str, str] = {
    "KOSPI": "KOSPI_우선주_리츠_전처리.xlsx",
    "KOSDAQ": "KOSDAQ_우선주_리츠_전처리.xlsx",
}

# 추가로 "특정 파일의 결측 행만 CSV로 저장"하고 싶을 때 사용
# 예시:
# EXPORT_TARGETS = [
#     {
#         "label": "KOSDAQ_2024_original",
#         "path": "04_KOSDAQ_2024_original.xlsx",
#         "output": "missing_rows_kosdaq_2024.csv",
#     }
# ]
EXPORT_TARGETS: List[Dict[str, str]] = []

OUTPUT_DIR = "qa_outputs"
SAVE_DATASET_MISSING_ROWS = False  # True면 KOSPI/KOSDAQ 핵심 결측 행도 CSV 저장

CRITICAL_COLS = [
    "price", "shares", "revenue_curr", "op_income_curr",
    "net_income", "assets", "equity",
]

EXPORT_MISSING_COLS = [
    "price", "shares", "revenue_curr", "revenue_prev",
    "op_income_curr", "op_income_prev", "net_income", "assets",
    "liabilities", "equity", "cur_assets", "cur_liab",
    "retained_earnings", "cf_oper", "oper_margin",
]

PREFERRED_STOCK_ENDINGS = ("5", "7")


# [2] 공통 함수 -------------------------------------------------
def ensure_output_dir(path: str) -> Path:
    out = Path(path)
    out.mkdir(parents=True, exist_ok=True)
    return out


def load_table(path: str) -> pd.DataFrame:
    ext = Path(path).suffix.lower()
    if ext in {".xlsx", ".xlsm", ".xls"}:
        df = pd.read_excel(path, dtype={"ticker": str})
    elif ext == ".csv":
        try:
            df = pd.read_csv(path, dtype={"ticker": str}, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(path, dtype={"ticker": str}, encoding="cp949")
    else:
        raise ValueError(f"지원하지 않는 파일 형식입니다: {path}")

    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)
    return df


def replace_blank_with_na(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    df = df.copy()
    existing_cols = [c for c in cols if c in df.columns]
    if existing_cols:
        df[existing_cols] = df[existing_cols].replace(r"^\s*$", pd.NA, regex=True)
    return df


def get_period_col(df: pd.DataFrame) -> Optional[str]:
    if "quarter" in df.columns:
        return "quarter"
    if "period" in df.columns:
        return "period"
    return None


def existing_cols(df: pd.DataFrame, cols: Iterable[str]) -> List[str]:
    return [c for c in cols if c in df.columns]


# [3] 결측치 점검 ------------------------------------------------
def missing_summary(df: pd.DataFrame, label: str) -> pd.DataFrame:
    missing = df.isnull().sum()
    missing_pct = (missing / len(df) * 100).round(2) if len(df) > 0 else 0
    result = pd.DataFrame({
        "결측수": missing,
        "결측률(%)": missing_pct,
    })
    result = result.query("결측수 > 0").sort_values("결측률(%)", ascending=False)

    print(f"\n{'=' * 60}")
    print(f"[{label}] 전체 결측치 현황 | 총 {len(df)}행 | 결측 있는 컬럼 {len(result)}개")
    print(f"{'=' * 60}")
    if result.empty:
        print("결측치 없음 ✓")
    else:
        print(result.to_string())
    return result


def critical_missing_detail(df: pd.DataFrame, label: str) -> pd.DataFrame:
    cols = existing_cols(df, CRITICAL_COLS)
    if not cols:
        print(f"\n[{label}] 핵심 컬럼이 없어 상세 결측 점검을 건너뜁니다.")
        return pd.DataFrame()

    mask = df[cols].isnull().any(axis=1)
    id_cols = [c for c in [get_period_col(df), "ticker", "corp_name"] if c and c in df.columns]
    bad = df.loc[mask, id_cols + cols].copy()

    print(f"\n=== 핵심 컬럼 결측 종목 상세: {label} ===")
    unique_tickers = bad["ticker"].nunique() if "ticker" in bad.columns and not bad.empty else 0
    print(f"{mask.sum()}행 / {unique_tickers}개 종목")
    if bad.empty:
        print("없음 ✓")
    else:
        print(bad.to_string(index=False))
    return bad


def quarter_missing_pattern(df: pd.DataFrame, label: str) -> None:
    period_col = get_period_col(df)
    print(f"\n=== 분기별 결측 패턴: {label} ===")
    if period_col is None:
        print("quarter/period 컬럼이 없어 점검을 건너뜁니다.")
        return

    found = False
    for col in df.columns:
        null_rows = df[df[col].isnull()]
        if null_rows.empty:
            continue
        found = True
        null_by_q = null_rows.groupby(period_col)["ticker"].count() if "ticker" in df.columns else null_rows.groupby(period_col).size()
        print(f"{col:25s}: {null_by_q.to_dict()}")

    if not found:
        print("결측 패턴 없음 ✓")


def infer_missing_causes(df: pd.DataFrame, label: str) -> None:
    print(f"\n=== 결측 원인 추정: {label} ===")

    if {"dividend", "div_yield"}.issubset(df.columns):
        no_div = df[(df["dividend"] == 0) & df["div_yield"].isnull()]
        print(f"무배당(dividend=0) + div_yield 결측 : {len(no_div)}행")
    else:
        print("무배당/div_yield 점검 불가 (관련 컬럼 없음)")

    if "equity" in df.columns:
        insol = df[df["equity"] <= 0]
        print(f"자본잠식 의심 (equity ≤ 0)          : {len(insol)}행")
    else:
        print("자본잠식 점검 불가 (equity 컬럼 없음)")

    if "price" in df.columns:
        no_price = df[df["price"].isnull()]
        print(f"price 결측                          : {len(no_price)}행")
        if not no_price.empty and "corp_name" in no_price.columns:
            names = pd.Series(no_price["corp_name"].dropna().unique()).head(10).tolist()
            print(f"  → 예시 종목: {names}")
    else:
        print("price 결측 점검 불가 (price 컬럼 없음)")


# [4] 중복/코드 점검 ---------------------------------------------
def duplicate_within_file(df: pd.DataFrame, label: str) -> pd.DataFrame:
    period_col = get_period_col(df)
    print(f"\n=== 파일 내부 중복 (ticker × quarter/period): {label} ===")
    if period_col is None or "ticker" not in df.columns:
        print("ticker 또는 quarter/period 컬럼이 없어 점검을 건너뜁니다.")
        return pd.DataFrame()

    dup = df[df.duplicated(["ticker", period_col], keep=False)].copy()
    if dup.empty:
        print("없음 ✓")
    else:
        show_cols = [c for c in [period_col, "ticker", "corp_name"] if c in dup.columns]
        print(f"{len(dup)}행 중복 발견 ↓")
        print(dup[show_cols].sort_values(["ticker", period_col]).to_string(index=False))
    return dup


def cross_market_duplicates(datasets: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    print("\n=== 시장 간 중복 ticker ===")
    if len(datasets) < 2 or any("ticker" not in df.columns for df in datasets.values()):
        print("비교 가능한 데이터셋이 부족합니다.")
        return pd.DataFrame()

    labels = list(datasets.keys())
    if len(labels) != 2:
        print("현재는 2개 시장 비교 기준으로 출력합니다.")

    left, right = labels[0], labels[1]
    left_df, right_df = datasets[left], datasets[right]
    left_tickers = set(left_df["ticker"].dropna().unique())
    right_tickers = set(right_df["ticker"].dropna().unique())
    cross_dup = left_tickers & right_tickers

    if not cross_dup:
        print("없음 ✓")
        return pd.DataFrame()

    rows = []
    for ticker in sorted(cross_dup):
        left_name = left_df.loc[left_df["ticker"] == ticker, "corp_name"].iloc[0] if "corp_name" in left_df.columns else ""
        right_name = right_df.loc[right_df["ticker"] == ticker, "corp_name"].iloc[0] if "corp_name" in right_df.columns else ""
        rows.append({
            "ticker": ticker,
            f"{left}_corp": left_name,
            f"{right}_corp": right_name,
        })

    result = pd.DataFrame(rows)
    print(result.to_string(index=False))
    return result


def preferred_stock_suspects(df: pd.DataFrame, label: str) -> pd.DataFrame:
    print(f"\n=== 우선주 의심 ticker (끝자리 {'/'.join(PREFERRED_STOCK_ENDINGS)}): {label} ===")
    if "ticker" not in df.columns:
        print("ticker 컬럼이 없어 점검을 건너뜁니다.")
        return pd.DataFrame()

    pref = df[df["ticker"].str[-1].isin(PREFERRED_STOCK_ENDINGS)]
    cols = [c for c in ["ticker", "corp_name"] if c in pref.columns]
    pref = pref[cols].drop_duplicates()
    if pref.empty:
        print("없음 ✓")
    else:
        preview = pref["ticker"].tolist()[:10]
        suffix = "..." if len(pref) > 10 else ""
        print(f"{len(pref)}개: {preview}{suffix}")
    return pref


# [5] 결측 행 CSV 저장 -------------------------------------------
def export_missing_rows(df: pd.DataFrame, label: str, output_path: str, cols: Iterable[str]) -> pd.DataFrame:
    df = replace_blank_with_na(df, cols)
    use_cols = existing_cols(df, cols)
    if not use_cols:
        print(f"\n[{label}] 저장 대상 결측 컬럼이 없어 CSV 저장을 건너뜁니다.")
        return pd.DataFrame()

    df_missing = df[df[use_cols].isnull().any(axis=1)].copy()
    id_cols = [c for c in [get_period_col(df), "ticker", "corp_name"] if c and c in df.columns]
    ordered_cols = id_cols + [c for c in use_cols if c not in id_cols]

    df_missing[ordered_cols].to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n[{label}] 전체: {len(df)}행 / 결측 포함: {len(df_missing)}행")
    print(f"CSV 저장 완료: {output_path}")
    return df_missing


# [6] 실행 ------------------------------------------------------
def main() -> None:
    out_dir = ensure_output_dir(OUTPUT_DIR)

    loaded: Dict[str, pd.DataFrame] = {}
    for label, path in DATASETS.items():
        if not os.path.exists(path):
            print(f"[경고] 파일 없음 - {label}: {path}")
            continue
        df = load_table(path)
        df = replace_blank_with_na(df, CRITICAL_COLS + EXPORT_MISSING_COLS)
        loaded[label] = df

    if not loaded:
        print("검사할 데이터셋을 불러오지 못했습니다. 파일 경로를 확인하세요.")
        return

    # 1) 결측치/중복/우선주 의심 점검
    for label, df in loaded.items():
        missing_summary(df, label)
        critical_missing_detail(df, label)
        quarter_missing_pattern(df, label)
        infer_missing_causes(df, label)
        duplicate_within_file(df, label)
        preferred_stock_suspects(df, label)

        if SAVE_DATASET_MISSING_ROWS:
            file_name = f"missing_rows_{label.lower()}.csv"
            export_missing_rows(df, label, str(out_dir / file_name), EXPORT_MISSING_COLS)

    # 2) 시장 간 중복 ticker 점검
    cross_market_duplicates(loaded)

    # 3) 검산.py 역할: 특정 파일의 결측 행 CSV 저장
    if EXPORT_TARGETS:
        print("\n" + "=" * 60)
        print("선택 저장 대상 결측 행 추출")
        print("=" * 60)
        for target in EXPORT_TARGETS:
            label = target["label"]
            path = target["path"]
            output = target.get("output") or f"missing_rows_{label}.csv"
            output_path = str(out_dir / output)

            if not os.path.exists(path):
                print(f"[경고] 저장 대상 파일 없음 - {label}: {path}")
                continue

            df = load_table(path)
            export_missing_rows(df, label, output_path, EXPORT_MISSING_COLS)

    print("\n" + "=" * 60)
    print("데이터 품질 점검 완료")
    print(f"출력 폴더: {out_dir.resolve()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
