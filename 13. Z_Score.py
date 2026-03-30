# z-score 추출
import pandas as pd
import numpy as np

INPUT_FILE = "merged_kfocus.xlsx"
OUTPUT_FILE = "merged_kfocus_z_score.xlsx"

df = pd.read_excel(INPUT_FILE, dtype={"ticker": str})
print(f"로드: {len(df)}행\n")

# ──────────────────────────────────────────────
# Altman Z-Score 변수 계산
# ──────────────────────────────────────────────

# X1: 운전자본 비율 = (유동자산 - 유동부채) / 총자산
df["X1"] = (df["cur_assets"] - df["cur_liab"]) / df["assets"]

# X2: 이익잉여금 비율 = 이익잉여금 / 총자산
df["X2"] = df["retained_earnings"] / df["assets"]

# X3: EBIT/총자산 = 영업이익 / 총자산
df["X3"] = df["op_income_curr"] / df["assets"]

# X4: 시가총액/총부채
df["X4"] = df["market_cap"] / df["liabilities"]

# X5: 자산회전율 = 매출액 / 총자산
df["X5"] = df["revenue_curr"] / df["assets"]

# ──────────────────────────────────────────────
# Z-Score 산출
# ──────────────────────────────────────────────
df["Z_SCORE"] = (1.2 * df["X1"]
               + 1.4 * df["X2"]
               + 3.3 * df["X3"]
               + 0.6 * df["X4"]
               + 1.0 * df["X5"])

# ──────────────────────────────────────────────
# 구간 판정 (Altman 기준)
# ──────────────────────────────────────────────
# Z > 2.99  → 안전(Safe)
# 1.81 ≤ Z ≤ 2.99 → 회색지대(Grey)
# Z < 1.81  → 부도위험(Distress)
df["Z_ZONE"] = np.where(df["Z_SCORE"].isna(), np.nan,
               np.where(df["Z_SCORE"] > 2.99, "Safe",
               np.where(df["Z_SCORE"] >= 1.81, "Grey", "Distress")))

# ──────────────────────────────────────────────
# 저장
# ──────────────────────────────────────────────
df.to_excel(OUTPUT_FILE, index=False)

# ──────────────────────────────────────────────
# 결과 요약
# ──────────────────────────────────────────────
print(f"완료! 저장: {OUTPUT_FILE}\n")

print(f"Z-Score 통계:")
print(f"  평균: {df['Z_SCORE'].mean():.4f}")
print(f"  중앙값: {df['Z_SCORE'].median():.4f}")
print(f"  최소: {df['Z_SCORE'].min():.4f}")
print(f"  최대: {df['Z_SCORE'].max():.4f}")
print(f"  NaN: {df['Z_SCORE'].isna().sum()}개")

print(f"\nZ-Zone 분포:")
for zone in ["Safe", "Grey", "Distress"]:
    cnt = (df["Z_ZONE"] == zone).sum()
    pct = cnt / len(df) * 100
    print(f"  {zone:<10}: {cnt:>6}개 ({pct:.1f}%)")

print(f"\n샘플:")
print(df[["period", "ticker", "corp_name", "X1", "X2", "X3", "X4", "X5", "Z_SCORE", "Z_ZONE"]].head(8).to_string())