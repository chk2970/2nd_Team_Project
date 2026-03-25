import pandas as pd
import numpy as np
from scipy import stats

# =========================
# 1. 설정
# =========================
ORIGINAL_PATH = "merged_original.xlsx"
KFOCUS_PATH = "merged_kfocus.xlsx"

OUT_ORIGINAL_DETAIL = "alpha_result_original.xlsx"
OUT_ORIGINAL_SUMMARY = "alpha_summary_original.xlsx"

OUT_KFOCUS_DETAIL = "alpha_result_kfocus.xlsx"
OUT_KFOCUS_SUMMARY = "alpha_summary_kfocus.xlsx"


# =========================
# 2. 보조 함수
# =========================
def period_to_sort_key(period):
    """
    예: 23Q1 -> 20231, 24Q4 -> 20244
    """
    period = str(period).strip().upper()
    year = int("20" + period[:2])
    quarter = int(period[-1])
    return year * 10 + quarter

def load_and_prepare(filepath):
    df = pd.read_excel(filepath)

    # 컬럼명 정리
    df.columns = [str(c).strip() for c in df.columns]

    # 핵심 컬럼 확인
    required_cols = ["period", "ticker", "sector", "price", "fscore_group", "fscore_total"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{filepath}에 필요한 컬럼이 없습니다: {missing}")

    # ticker를 문자열로 통일
    df["ticker"] = df["ticker"].astype(str).str.strip().str.zfill(6)

    # period 정리
    df["period"] = df["period"].astype(str).str.strip().str.upper()
    df["period_sort"] = df["period"].apply(period_to_sort_key)

    # price 숫자형
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # sector 정리
    df["sector"] = df["sector"].astype(str).str.strip()

    # fscoregroup 정리
    df["fscore_group"] = df["fscore_group"].astype(str).str.strip().str.upper()

    # 가격 없는 행 제거
    df = df.dropna(subset=["price"]).copy()

    return df


def calculate_returns_and_alpha(df):
    # 종목별 시계열 정렬
    df = df.sort_values(["ticker", "period_sort"]).copy()

    # 다음 분기 가격
    df["next_price"] = df.groupby("ticker")["price"].shift(-1)
    df["next_period"] = df.groupby("ticker")["period"].shift(-1)
    df["next_period_sort"] = df.groupby("ticker")["period_sort"].shift(-1)

    # 인접 분기인지 확인 (예: 23Q1 -> 23Q2)
    df["is_next_quarter"] = (df["next_period_sort"] - df["period_sort"] == 1)

    # 종목 수익률
    df["stock_return"] = np.where(
        df["is_next_quarter"],
        (df["next_price"] - df["price"]) / df["price"],
        np.nan
    )

    # 같은 분기, 같은 섹터의 평균 종목수익률 = sector_return
    sector_return = (
        df.groupby(["period", "sector"], dropna=False)["stock_return"]
        .mean()
        .reset_index()
        .rename(columns={"stock_return": "sector_return"})
    )

    df = df.merge(sector_return, on=["period", "sector"], how="left")

    # 알파
    df["alpha"] = df["stock_return"] - df["sector_return"]

    return df


def make_group_summary(df):
    # 분석 가능한 행만 사용
    use = df.dropna(subset=["stock_return", "alpha", "fscore_group"]).copy()

    # 그룹별 요약
    group_summary = (
        use.groupby("fscore_group")
        .agg(
            n=("ticker", "count"),
            mean_stock_return=("stock_return", "mean"),
            mean_sector_return=("sector_return", "mean"),
            mean_alpha=("alpha", "mean"),
            median_alpha=("alpha", "median"),
            std_alpha=("alpha", "std")
        )
        .reset_index()
        .sort_values("fscore_group")
    )

    # HIGH vs LOW t-test
    high_alpha = use.loc[use["fscore_group"] == "HIGH", "alpha"].dropna()
    low_alpha = use.loc[use["fscore_group"] == "LOW", "alpha"].dropna()

    if len(high_alpha) >= 2 and len(low_alpha) >= 2:
        t_stat, p_value = stats.ttest_ind(high_alpha, low_alpha, equal_var=False, nan_policy="omit")
        high_mean = high_alpha.mean()
        low_mean = low_alpha.mean()
        diff = high_mean - low_mean
    else:
        t_stat, p_value, high_mean, low_mean, diff = [np.nan] * 5

    test_summary = pd.DataFrame({
        "comparison": ["HIGH - LOW"],
        "high_n": [len(high_alpha)],
        "low_n": [len(low_alpha)],
        "high_mean_alpha": [high_mean],
        "low_mean_alpha": [low_mean],
        "diff_mean_alpha": [diff],
        "t_stat": [t_stat],
        "p_value": [p_value]
    })

    return group_summary, test_summary


def save_results(detail_df, group_summary, test_summary, detail_path, summary_path):
    # 보기 좋은 상세 결과
    detail_cols = [
        "market", "period", "ticker", "corpname", "sector",
        "price", "next_price", "next_period",
        "fscore_total", "fscore_group",
        "stock_return", "sector_return", "alpha",
        "Z_SCORE", "Z_ZONE"
    ]
    detail_cols = [c for c in detail_cols if c in detail_df.columns]

    detail_out = detail_df[detail_cols].copy()

    with pd.ExcelWriter(detail_path, engine="openpyxl") as writer:
        detail_out.to_excel(writer, sheet_name="detail", index=False)

    with pd.ExcelWriter(summary_path, engine="openpyxl") as writer:
        group_summary.to_excel(writer, sheet_name="group_summary", index=False)
        test_summary.to_excel(writer, sheet_name="t_test", index=False)


def run_pipeline(filepath, detail_path, summary_path):
    df = load_and_prepare(filepath)
    df = calculate_returns_and_alpha(df)
    group_summary, test_summary = make_group_summary(df)
    save_results(df, group_summary, test_summary, detail_path, summary_path)
    return df, group_summary, test_summary


# =========================
# 3. 실행
# =========================
df_original, summary_original, test_original = run_pipeline(
    ORIGINAL_PATH,
    OUT_ORIGINAL_DETAIL,
    OUT_ORIGINAL_SUMMARY
)

df_kfocus, summary_kfocus, test_kfocus = run_pipeline(
    KFOCUS_PATH,
    OUT_KFOCUS_DETAIL,
    OUT_KFOCUS_SUMMARY
)

print("완료:")
print("-", OUT_ORIGINAL_DETAIL)
print("-", OUT_ORIGINAL_SUMMARY)
print("-", OUT_KFOCUS_DETAIL)
print("-", OUT_KFOCUS_SUMMARY)

print("\n[Original] 그룹 요약")
print(summary_original)

print("\n[Original] HIGH-LOW t-test")
print(test_original)

print("\n[KFocus] 그룹 요약")
print(summary_kfocus)

print("\n[KFocus] HIGH-LOW t-test")
print(test_kfocus)