# oper_margin 구하기(2025년)
import pandas as pd

df = pd.read_excel("01_KOSPI_2025_분석용.xlsx", dtype={"ticker": str})
df["oper_margin"] = (df["op_income_curr"] / df["revenue_curr"]) * 100
df.to_excel("01_KOSPI_2025_분석용.xlsx(oper_margin추가).xlsx", index=False)
print(f"완료! oper_margin 추가 ({df['oper_margin'].notna().sum()}개 계산)")


#Ftable 추출ㄱ
"""
KOSPI 2025 F-Table 파생변수 13개 추가
- 당기 = 2025 분자 / 2025 분모
- 전년동기/전기 = 2024 같은 분기 분자 / 2025 분모
"""
import pandas as pd
import numpy as np

# ──────────────────────────────────────────────
# 설정 (파일 경로를 본인 환경에 맞게 수정)
# ──────────────────────────────────────────────
FILE_2025 = "01_KOSPI_2025_분석용.xlsx(oper_margin추가).xlsx"
FILE_2024 = "kospi_2024.xlsx"
OUTPUT_FILE = "kospi_2025_Ftable.csv"


def safe_div(a, b):
    return np.where((b == 0) | pd.isna(b) | pd.isna(a), np.nan, a / b)


def main():
    print("=" * 60)
    print("  KOSPI 2025 F-Table 파생변수 추가 (13개 컬럼)")
    print("  당기=2025, 전년동기/전기=2024 분자 / 2025 분모")
    print("=" * 60)

    df25 = pd.read_excel(FILE_2025, dtype={"ticker": str})
    df24 = pd.read_excel(FILE_2024, dtype={"ticker": str})
    print(f"  2025: {len(df25)}행, 2024: {len(df24)}행")

    df25["ticker"] = df25["ticker"].str.strip().str.zfill(6)
    df24["ticker"] = df24["ticker"].str.strip().str.zfill(6)

    # ──────────────────────────────────────────
    # period 형식 통일 (매칭용)
    # 2025: "25Q1" → "Q1", 2024: "Q1" 그대로
    # ──────────────────────────────────────────
    df25["match_q"] = df25["period"].str.extract(r'(Q\d)')
    df24["match_q"] = df24["period"].str.extract(r'(Q\d)')

    print(f"  2025 분기: {df25['match_q'].unique()}")
    print(f"  2024 분기: {df24['match_q'].unique()}")

    df = df25.copy()

    # ──────────────────────────────────────────
    # 2024 같은 분기 데이터 가져오기
    # ──────────────────────────────────────────
    print("  2024 데이터 매칭 중...")

    df24_match = df24[[
        "ticker", "match_q",
        "net_income", "assets", "liabilities",
        "cur_assets", "cur_liab",
        "oper_margin", "revenue_curr",
    ]].copy()

    df24_match = df24_match.rename(columns={
        "net_income":   "prev_net_income",
        "assets":       "prev_assets",
        "liabilities":  "prev_liabilities",
        "cur_assets":   "prev_cur_assets",
        "cur_liab":     "prev_cur_liab",
        "oper_margin":  "prev_oper_margin",
        "revenue_curr": "prev_revenue",
    })

    df = df.merge(df24_match, on=["ticker", "match_q"], how="left")

    # ──────────────────────────────────────────
    # 당기 변수 (2025 / 2025)
    # ──────────────────────────────────────────
    print("  당기 변수 계산...")

    df["roa_curr"] = safe_div(df["net_income"], df["assets"])
    df["cfo_ratio"] = safe_div(df["cf_oper"], df["assets"])
    df["accrual"] = df["cfo_ratio"] - df["roa_curr"]
    df["lever_curr"] = safe_div(df["liabilities"], df["assets"])
    df["liquid_curr"] = safe_div(df["cur_assets"], df["cur_liab"])
    df["eq_offer"] = df["capital_increase"].fillna(0)
    df["margin_curr"] = df["oper_margin"]
    df["turn_curr"] = safe_div(df["revenue_curr"], df["assets"])

    # ──────────────────────────────────────────
    # 전년동기/전기 변수 (2024 같은 분기 분자 / 2025 분모)
    # ──────────────────────────────────────────
    print("  전년동기/전기 변수 계산...")

    df["roa_prev"] = safe_div(df["prev_net_income"], df["assets"])
    df["lever_prev"] = safe_div(df["prev_liabilities"], df["assets"])
    df["liquid_prev"] = safe_div(df["prev_cur_assets"], df["cur_liab"])
    df["margin_prev"] = df["prev_oper_margin"]
    df["turn_prev"] = safe_div(df["prev_revenue"], df["assets"])

    # ──────────────────────────────────────────
    # 임시 컬럼 삭제, 저장
    # ──────────────────────────────────────────
    temp_cols = [
        "prev_net_income", "prev_assets", "prev_liabilities",
        "prev_cur_assets", "prev_cur_liab",
        "prev_oper_margin", "prev_revenue",
        "match_q",
    ]
    df.drop(columns=temp_cols, inplace=True)

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    # ──────────────────────────────────────────
    # 결과 요약
    # ──────────────────────────────────────────
    new_cols = [
        "roa_curr", "roa_prev", "cfo_ratio", "accrual",
        "lever_curr", "lever_prev", "liquid_curr", "liquid_prev",
        "eq_offer", "margin_curr", "margin_prev", "turn_curr", "turn_prev",
    ]

    print(f"\n{'=' * 60}")
    print(f"  완료! 저장: {OUTPUT_FILE}")
    print(f"  원본 컬럼 + 추가 13개 = 총 {len(df.columns)}개 컬럼")
    print(f"{'=' * 60}")

    print(f"\n  추가된 컬럼별 null 비율:")
    for col in new_cols:
        pct = df[col].isna().sum() / len(df) * 100
        print(f"    {col:<15}: {pct:.1f}%")

    print(f"\n  샘플 (첫 기업):")
    sample = df[df["ticker"] == df["ticker"].iloc[0]]
    print(sample[["period", "ticker", "corp_name"] + new_cols].to_string())


if __name__ == "__main__":
    main()


# 이상치 클리핑
import pandas as pd
import numpy as np

# ──────────────────────────────────────────────
# 설정 (파일 경로를 본인 환경에 맞게 수정)
# ──────────────────────────────────────────────
INPUT_FILE = "kospi_2025_Ftable.xlsx"
OUTPUT_FILE = "kospi_2025_Ftable_clipped.xlsx"

df = pd.read_excel(INPUT_FILE, dtype={"ticker": str})
print(f"로드: {len(df)}행\n")

# ──────────────────────────────────────────────
# 클리핑 적용
# ──────────────────────────────────────────────

# 1. inf → NaN (모든 컬럼)
for col in df.select_dtypes(include=[np.number]).columns:
    inf_cnt = np.isinf(df[col]).sum()
    if inf_cnt > 0:
        df[col] = df[col].replace([np.inf, -np.inf], np.nan)
        print(f"  {col}: inf {inf_cnt}개 → NaN")

# 2. Clip (당기: 범위 밖 → 경계값)
clip_rules = {
    "roa_curr":    (-1, 1),
    "cfo_ratio":   (-1, 1),
    "accrual":     (-1, 1),
    "margin_curr": (-500, 300),
    "turn_curr":   (-1, 3),
}
for col, (lo, hi) in clip_rules.items():
    before = ((df[col] < lo) | (df[col] > hi)).sum()
    df[col] = df[col].clip(lo, hi)
    if before > 0:
        print(f"  {col}: {before}개 클리핑 ({lo}, {hi})")

# 3. Clip+NaN (전년: 범위 밖 → NaN)
clipnan_rules = {
    "roa_prev":    (-1, 1),
    "lever_prev":  (0, 2),
    "liquid_curr": (0, 50),
    "liquid_prev": (0, 50),
    "margin_prev": (-500, 300),
    "turn_prev":   (-1, 3),
}
for col, (lo, hi) in clipnan_rules.items():
    before = ((df[col] < lo) | (df[col] > hi)).sum()
    df.loc[(df[col] < lo) | (df[col] > hi), col] = np.nan
    if before > 0:
        print(f"  {col}: {before}개 → NaN ({lo}, {hi})")

# ──────────────────────────────────────────────
# 저장
# ──────────────────────────────────────────────
df.to_excel(OUTPUT_FILE, index=False)

# ──────────────────────────────────────────────
# 결과 확인
# ──────────────────────────────────────────────
print(f"\n완료! 저장: {OUTPUT_FILE}\n")

ftable_cols = ['roa_curr','roa_prev','cfo_ratio','accrual','lever_curr','lever_prev',
               'liquid_curr','liquid_prev','margin_curr','margin_prev','turn_curr','turn_prev']

print("클리핑 후 확인:")
for col in ftable_cols:
    if col in df.columns:
        v = df[col].replace([np.inf,-np.inf], np.nan).dropna()
        extreme = (v.abs() > 1000).sum()
        print(f"  {col:<15}: min={v.min():.4f}, max={v.max():.4f}, |값|>1000={extreme}개")


# F-score 활용변수
import pandas as pd
import numpy as np

INPUT_FILE = "kospi_2025_Ftable_clipped.xlsx"
OUTPUT_FILE = "kospi_2025_Fscore.xlsx"

df = pd.read_excel(INPUT_FILE, dtype={"ticker": str})
print(f"로드: {len(df)}행\n")

# ──────────────────────────────────────────────
# F-Score 9개 T/F 판정
# ──────────────────────────────────────────────

# 수익성
df["ROA"] = np.where(df["roa_curr"].isna(), np.nan,
            np.where(df["roa_curr"] > 0, 1, 0))

df["ΔROA"] = np.where(df["roa_curr"].isna() | df["roa_prev"].isna(), np.nan,
             np.where(df["roa_curr"] > df["roa_prev"], 1, 0))

df["CFO"] = np.where(df["cfo_ratio"].isna(), np.nan,
            np.where(df["cfo_ratio"] > 0, 1, 0))

df["ACCRUAL"] = np.where(df["accrual"].isna(), np.nan,
                np.where(df["accrual"] > 0, 1, 0))

# 유동성
df["ΔLEVER"] = np.where(df["lever_curr"].isna() | df["lever_prev"].isna(), np.nan,
               np.where(df["lever_curr"] < df["lever_prev"], 1, 0))

df["ΔLIQUID"] = np.where(df["liquid_curr"].isna() | df["liquid_prev"].isna(), np.nan,
                np.where(df["liquid_curr"] > df["liquid_prev"], 1, 0))

df["EQ_OFFER"] = np.where(df["eq_offer"].isna(), np.nan,
                 np.where(df["eq_offer"] == 0, 1, 0))

# 운영 효율성
df["ΔMARGIN"] = np.where(df["margin_curr"].isna() | df["margin_prev"].isna(), np.nan,
                np.where(df["margin_curr"] > df["margin_prev"], 1, 0))

df["ΔTURN"] = np.where(df["turn_curr"].isna() | df["turn_prev"].isna(), np.nan,
              np.where(df["turn_curr"] > df["turn_prev"], 1, 0))

# ──────────────────────────────────────────────
# NaN → 0점(F) 처리 (prev 없는 기업)
# ──────────────────────────────────────────────
f_cols = ["ROA", "ΔROA", "CFO", "ACCRUAL", "ΔLEVER", "ΔLIQUID", "EQ_OFFER", "ΔMARGIN", "ΔTURN"]
for col in f_cols:
    df[col] = df[col].fillna(0)

# ──────────────────────────────────────────────
# 저장
# ──────────────────────────────────────────────
df.to_excel(OUTPUT_FILE, index=False)

# ──────────────────────────────────────────────
# 결과 요약
# ──────────────────────────────────────────────
print(f"완료! 저장: {OUTPUT_FILE}\n")
print(f"{'지표':<10} {'T(1)':>8} {'F(0)':>8} {'T비율':>8}")
print("-" * 35)
for col in f_cols:
    t = int((df[col] == 1).sum())
    f = int((df[col] == 0).sum())
    pct = round(t / (t + f) * 100, 1)
    print(f"{col:<10} {t:>8} {f:>8} {pct:>7.1f}%")

print(f"\n샘플:")
print(df[["period", "ticker", "corp_name"] + f_cols].head(8).to_string())