import os
from pathlib import Path

import numpy as np
import pandas as pd

# ============================================================
#  파생변수 통합 파이프라인
#  - separate 모드: 당기 파일 + 전년동기 파일 2개를 매칭
#  - single 모드: 하나의 파일 안에서 year/Q를 읽어 전년동기 매칭
# ============================================================

# -------------------------------
# 실행 모드 설정
# -------------------------------
MODE = "separate"   # "separate" or "single"

# separate 모드용
CURRENT_FILE = "01_KOSPI_2025_분석용.xlsx"   # 당기 파일
PREV_FILE = "kospi_2024.xlsx"              # 전년동기 파일

# single 모드용
SINGLE_FILE = "삼성전자.xlsx"

# 공통 출력 설정
OUTPUT_DIR = "."
OUTPUT_PREFIX = "derived_output"
SAVE_INTERMEDIATE = True
FINAL_OUTPUT = None   # None이면 자동: {OUTPUT_PREFIX}_Fscore.xlsx

# ============================================================
# 공통 상수
# ============================================================
KEY_COLS = ["ticker", "period"]
TEXT_COL_CANDIDATES = ["market", "ticker", "sector", "corp_name", "period", "fscore_group", "Z_ZONE"]

FTABLE_COLS = [
    "roa_curr", "roa_prev", "cfo_ratio", "accrual",
    "lever_curr", "lever_prev", "liquid_curr", "liquid_prev",
    "eq_offer", "margin_curr", "margin_prev", "turn_curr", "turn_prev",
]

FSCORE_COLS = ["ROA", "ΔROA", "CFO", "ACCRUAL", "ΔLEVER", "ΔLIQUID", "EQ_OFFER", "ΔMARGIN", "ΔTURN"]

CLIP_RULES = {
    "roa_curr": (-1, 1),
    "cfo_ratio": (-1, 1),
    "accrual": (-1, 1),
    "margin_curr": (-500, 300),
    "turn_curr": (-1, 3),
}

CLIPNAN_RULES = {
    "roa_prev": (-1, 1),
    "lever_prev": (0, 2),
    "liquid_curr": (0, 50),
    "liquid_prev": (0, 50),
    "margin_prev": (-500, 300),
    "turn_prev": (-1, 3),
}

REQUIRED_BASE_COLS = {
    "ticker", "period", "corp_name",
    "op_income_curr", "revenue_curr", "net_income", "assets", "cf_oper",
    "liabilities", "cur_assets", "cur_liab", "capital_increase",
}

REQUIRED_PREV_SOURCE_COLS = {
    "ticker", "period", "net_income", "liabilities", "cur_assets", "cur_liab",
    "revenue_curr",
}


# ============================================================
# 유틸
# ============================================================
def safe_div(a, b):
    return np.where((b == 0) | pd.isna(b) | pd.isna(a), np.nan, a / b)


def ensure_dir(path_str):
    Path(path_str).mkdir(parents=True, exist_ok=True)


def detect_extension(path_str):
    return Path(path_str).suffix.lower()


def load_table(path_str):
    ext = detect_extension(path_str)
    if ext in [".xlsx", ".xlsm", ".xls"]:
        return pd.read_excel(path_str, dtype={"ticker": str})
    if ext == ".csv":
        return pd.read_csv(path_str, dtype={"ticker": str})
    raise ValueError(f"지원하지 않는 파일 형식입니다: {path_str}")


def save_table(df, path_str):
    ext = detect_extension(path_str)
    if ext in [".xlsx", ".xlsm", ".xls"]:
        df.to_excel(path_str, index=False)
    elif ext == ".csv":
        df.to_csv(path_str, index=False, encoding="utf-8-sig")
    else:
        raise ValueError(f"지원하지 않는 저장 형식입니다: {path_str}")


def make_output_path(name):
    ensure_dir(OUTPUT_DIR)
    return str(Path(OUTPUT_DIR) / name)


def normalize_ticker(df):
    if "ticker" in df.columns:
        df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)
    return df


def normalize_period_cols(df):
    if "period" not in df.columns:
        raise KeyError("period 컬럼이 없습니다.")

    df["period"] = df["period"].astype(str).str.strip()
    df["match_q"] = df["period"].str.extract(r"(Q\d)")
    year_4 = df["period"].str.extract(r"((?:19|20)\d{2})", expand=False)
    year_2 = df["period"].str.extract(r"(^\d{2})(?=Q\d)", expand=False)
    df["year"] = np.where(
        year_4.notna(),
        pd.to_numeric(year_4, errors="coerce"),
        pd.to_numeric(year_2, errors="coerce") + 2000,
    )
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    return df


def coerce_numeric(df):
    text_cols = {c for c in TEXT_COL_CANDIDATES if c in df.columns}
    for col in df.columns:
        if col not in text_cols:
            df[col] = pd.to_numeric(df[col], errors="ignore")
    return df


def validate_columns(df, required_cols, label):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"{label}에 필요한 컬럼이 없습니다: {missing}")


# ============================================================
# 1단계: oper_margin
# ============================================================
def add_oper_margin(df):
    df = df.copy()
    df["oper_margin"] = safe_div(df["op_income_curr"], df["revenue_curr"]) * 100
    return df


# ============================================================
# 전년동기 매칭
# ============================================================
def merge_prev_from_separate(df_curr, df_prev):
    validate_columns(df_curr, REQUIRED_BASE_COLS, "당기 파일")
    validate_columns(df_prev, REQUIRED_PREV_SOURCE_COLS, "전기 파일")

    df_curr = normalize_ticker(df_curr.copy())
    df_prev = normalize_ticker(df_prev.copy())
    df_curr = normalize_period_cols(df_curr)
    df_prev = normalize_period_cols(df_prev)

    if "oper_margin" not in df_prev.columns:
        df_prev = add_oper_margin(df_prev)

    prev_cols_map = {
        "net_income": "prev_net_income",
        "liabilities": "prev_liabilities",
        "cur_assets": "prev_cur_assets",
        "cur_liab": "prev_cur_liab",
        "oper_margin": "prev_oper_margin",
        "revenue_curr": "prev_revenue",
    }

    df_prev_match = df_prev[["ticker", "match_q"] + list(prev_cols_map.keys())].copy()
    df_prev_match = df_prev_match.rename(columns=prev_cols_map)

    merged = df_curr.merge(df_prev_match, on=["ticker", "match_q"], how="left")
    return merged


def merge_prev_from_single(df):
    validate_columns(df, REQUIRED_BASE_COLS, "단일 파일")

    df = normalize_ticker(df.copy())
    df = normalize_period_cols(df)

    if df["year"].isna().all():
        raise ValueError("single 모드에서는 period에서 연도를 추출할 수 있어야 합니다. 예: 25Q1, 2025Q1")

    if "oper_margin" not in df.columns:
        df = add_oper_margin(df)

    prev_cols_map = {
        "net_income": "prev_net_income",
        "liabilities": "prev_liabilities",
        "cur_assets": "prev_cur_assets",
        "cur_liab": "prev_cur_liab",
        "oper_margin": "prev_oper_margin",
        "revenue_curr": "prev_revenue",
    }

    df_prev = df[["ticker", "match_q", "year"] + list(prev_cols_map.keys())].copy()
    df_prev = df_prev.rename(columns=prev_cols_map)
    df_prev["year"] = df_prev["year"] + 1

    merged = df.merge(df_prev, on=["ticker", "match_q", "year"], how="left")
    return merged


# ============================================================
# 2단계: F-Table
# ============================================================
def add_ftable(df):
    df = df.copy()

    df["roa_curr"] = safe_div(df["net_income"], df["assets"])
    df["cfo_ratio"] = safe_div(df["cf_oper"], df["assets"])
    df["accrual"] = df["cfo_ratio"] - df["roa_curr"]
    df["lever_curr"] = safe_div(df["liabilities"], df["assets"])
    df["liquid_curr"] = safe_div(df["cur_assets"], df["cur_liab"])
    df["eq_offer"] = df["capital_increase"].fillna(0)
    df["margin_curr"] = df["oper_margin"]
    df["turn_curr"] = safe_div(df["revenue_curr"], df["assets"])

    df["roa_prev"] = safe_div(df["prev_net_income"], df["assets"])
    df["lever_prev"] = safe_div(df["prev_liabilities"], df["assets"])
    df["liquid_prev"] = safe_div(df["prev_cur_assets"], df["cur_liab"])
    df["margin_prev"] = df["prev_oper_margin"]
    df["turn_prev"] = safe_div(df["prev_revenue"], df["assets"])

    return df


# ============================================================
# 3단계: 클리핑
# ============================================================
def clip_outliers(df):
    df = df.copy()

    for col in df.select_dtypes(include=[np.number]).columns:
        df[col] = df[col].replace([np.inf, -np.inf], np.nan)

    for col, (lo, hi) in CLIP_RULES.items():
        if col in df.columns:
            df[col] = df[col].clip(lo, hi)

    for col, (lo, hi) in CLIPNAN_RULES.items():
        if col in df.columns:
            mask = (df[col] < lo) | (df[col] > hi)
            df.loc[mask, col] = np.nan

    return df


# ============================================================
# 4단계: F-Score
# ============================================================
def add_fscore(df):
    df = df.copy()

    df["ROA"] = np.where(df["roa_curr"].isna(), np.nan, np.where(df["roa_curr"] > 0, 1, 0))
    df["ΔROA"] = np.where(df["roa_curr"].isna() | df["roa_prev"].isna(), np.nan,
                          np.where(df["roa_curr"] > df["roa_prev"], 1, 0))
    df["CFO"] = np.where(df["cfo_ratio"].isna(), np.nan, np.where(df["cfo_ratio"] > 0, 1, 0))
    df["ACCRUAL"] = np.where(df["accrual"].isna(), np.nan, np.where(df["accrual"] > 0, 1, 0))
    df["ΔLEVER"] = np.where(df["lever_curr"].isna() | df["lever_prev"].isna(), np.nan,
                            np.where(df["lever_curr"] < df["lever_prev"], 1, 0))
    df["ΔLIQUID"] = np.where(df["liquid_curr"].isna() | df["liquid_prev"].isna(), np.nan,
                             np.where(df["liquid_curr"] > df["liquid_prev"], 1, 0))
    df["EQ_OFFER"] = np.where(df["eq_offer"].isna(), np.nan, np.where(df["eq_offer"] == 0, 1, 0))
    df["ΔMARGIN"] = np.where(df["margin_curr"].isna() | df["margin_prev"].isna(), np.nan,
                             np.where(df["margin_curr"] > df["margin_prev"], 1, 0))
    df["ΔTURN"] = np.where(df["turn_curr"].isna() | df["turn_prev"].isna(), np.nan,
                           np.where(df["turn_curr"] > df["turn_prev"], 1, 0))

    for col in FSCORE_COLS:
        df[col] = df[col].fillna(0)

    return df


# ============================================================
# 후처리 / 출력
# ============================================================
def drop_temp_cols(df):
    temp_cols = [
        "match_q", "year",
        "prev_net_income", "prev_liabilities", "prev_cur_assets",
        "prev_cur_liab", "prev_oper_margin", "prev_revenue",
    ]
    return df.drop(columns=[c for c in temp_cols if c in df.columns], errors="ignore")


def print_null_ratio(df, cols, title):
    print(f"\n[{title} null 비율]")
    for col in cols:
        if col in df.columns:
            pct = df[col].isna().mean() * 100
            print(f"  {col:<15}: {pct:6.1f}%")


def print_fscore_summary(df):
    print("\n[F-Score 요약]")
    print(f"{'지표':<10} {'T(1)':>8} {'F(0)':>8} {'T비율':>8}")
    print("-" * 38)
    for col in FSCORE_COLS:
        t = int((df[col] == 1).sum())
        f = int((df[col] == 0).sum())
        pct = round(t / (t + f) * 100, 1) if (t + f) else 0.0
        print(f"{col:<10} {t:>8} {f:>8} {pct:>7.1f}%")


def build_output_paths(base_ext=".xlsx"):
    step1 = make_output_path(f"{OUTPUT_PREFIX}_oper_margin{base_ext}")
    step2 = make_output_path(f"{OUTPUT_PREFIX}_Ftable{base_ext}")
    step3 = make_output_path(f"{OUTPUT_PREFIX}_Ftable_clipped{base_ext}")
    step4 = FINAL_OUTPUT or make_output_path(f"{OUTPUT_PREFIX}_Fscore{base_ext}")
    return step1, step2, step3, step4


# ============================================================
# 메인
# ============================================================
def main():
    print("=" * 70)
    print("파생변수 통합 파이프라인 시작")
    print(f"MODE = {MODE}")
    print("=" * 70)

    if MODE == "separate":
        base_ext = detect_extension(CURRENT_FILE) or ".xlsx"
        step1_out, step2_out, step3_out, step4_out = build_output_paths(base_ext)

        df_curr = load_table(CURRENT_FILE)
        df_prev = load_table(PREV_FILE)
        df_curr = normalize_ticker(coerce_numeric(df_curr))
        df_prev = normalize_ticker(coerce_numeric(df_prev))

        validate_columns(df_curr, REQUIRED_BASE_COLS, "당기 파일")
        validate_columns(df_prev, REQUIRED_PREV_SOURCE_COLS, "전기 파일")

        print(f"당기 파일: {CURRENT_FILE} ({len(df_curr)}행)")
        print(f"전기 파일: {PREV_FILE} ({len(df_prev)}행)")

        # 1단계
        df_step1 = add_oper_margin(df_curr)
        if SAVE_INTERMEDIATE:
            save_table(df_step1, step1_out)
            print(f"[1단계 완료] oper_margin 저장: {step1_out}")

        # 2단계
        df_merged = merge_prev_from_separate(df_step1, df_prev)
        df_step2 = add_ftable(df_merged)
        print_null_ratio(df_step2, FTABLE_COLS, "F-Table")
        if SAVE_INTERMEDIATE:
            save_table(df_step2, step2_out)
            print(f"[2단계 완료] F-Table 저장: {step2_out}")

    elif MODE == "single":
        base_ext = detect_extension(SINGLE_FILE) or ".xlsx"
        step1_out, step2_out, step3_out, step4_out = build_output_paths(base_ext)

        df = load_table(SINGLE_FILE)
        df = normalize_ticker(coerce_numeric(df))
        validate_columns(df, REQUIRED_BASE_COLS, "단일 파일")

        print(f"단일 파일: {SINGLE_FILE} ({len(df)}행)")

        # 1단계
        df_step1 = add_oper_margin(df)
        if SAVE_INTERMEDIATE:
            save_table(df_step1, step1_out)
            print(f"[1단계 완료] oper_margin 저장: {step1_out}")

        # 2단계
        df_merged = merge_prev_from_single(df_step1)
        df_step2 = add_ftable(df_merged)
        print_null_ratio(df_step2, FTABLE_COLS, "F-Table")
        if SAVE_INTERMEDIATE:
            save_table(df_step2, step2_out)
            print(f"[2단계 완료] F-Table 저장: {step2_out}")

    else:
        raise ValueError("MODE는 'separate' 또는 'single' 이어야 합니다.")

    # 3단계
    df_step3 = clip_outliers(df_step2)
    if SAVE_INTERMEDIATE:
        save_table(df_step3, step3_out)
        print(f"[3단계 완료] 클리핑 저장: {step3_out}")

    # 4단계
    df_step4 = add_fscore(df_step3)
    df_step4 = drop_temp_cols(df_step4)
    save_table(df_step4, step4_out)
    print(f"[4단계 완료] 최종 저장: {step4_out}")

    print_fscore_summary(df_step4)

    preview_cols = [c for c in ["period", "ticker", "corp_name"] + FTABLE_COLS + FSCORE_COLS if c in df_step4.columns]
    print("\n[미리보기]")
    print(df_step4[preview_cols].head(8).to_string(index=False))


if __name__ == "__main__":
    main()
