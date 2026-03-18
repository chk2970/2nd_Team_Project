# kosdaq_quarter_data 2,3,4.py 합친 csv

"""
A, C, D CSV 병합 스크립트
- A: kosdaq_2025_quarter_data_1(14).csv  → 재무 14개
- C: kosdaq_2025_quarter_data_2(5).csv   → cf_oper 등 5개
- D: kosdaq_2025_quarter_data_3(2).csv   → price, shares
- 기준: ticker + 분기
"""

import pandas as pd
import os

# ─────────────────────────────────────────
# 파일 읽기
# ─────────────────────────────────────────
def read_csv_auto(path):
    for enc in ["utf-8-sig", "cp949", "utf-8"]:
        try:
            df = pd.read_csv(path, encoding=enc)
            if len(df.columns) > 1:
                print(f"  -> {os.path.basename(path)}: {len(df)}행 / {len(df.columns)}컬럼 / enc={enc}")
                return df
        except:
            pass
    raise Exception(f"파일 읽기 실패: {path}")

print("=" * 50)
print("파일 로드 중...")
print("=" * 50)

df_a = read_csv_auto("kosdaq_2025_quarter_data_1(14).csv")
df_c = read_csv_auto("kosdaq_2025_quarter_data_2(5).csv")
df_d = read_csv_auto("kosdaq_2025_quarter_data_3(2).csv")

# ─────────────────────────────────────────
# ticker 6자리 통일
# ─────────────────────────────────────────
for df in [df_a, df_c, df_d]:
    df["ticker"] = df["ticker"].astype(str).str.zfill(6)

# ─────────────────────────────────────────
# 분기 컬럼명 통일 (quarter → 분기)
# ─────────────────────────────────────────
if "quarter" in df_c.columns:
    df_c = df_c.rename(columns={"quarter": "분기"})
if "quarter" in df_d.columns:
    df_d = df_d.rename(columns={"quarter": "분기"})

print(f"\nA 분기값: {df_a['분기'].unique()}")
print(f"C 분기값: {df_c['분기'].unique()}")
print(f"D 분기값: {df_d['분기'].unique()}")

# ─────────────────────────────────────────
# C에서 필요한 컬럼만 추출
# ─────────────────────────────────────────
c_cols = ["ticker", "분기", "cf_oper", "capital_increase", "short_liab", "treasury", "dividend"]
df_c_slim = df_c[[c for c in c_cols if c in df_c.columns]]

# ─────────────────────────────────────────
# D에서 필요한 컬럼만 추출
# ─────────────────────────────────────────
d_cols = ["ticker", "분기", "price", "shares"]
df_d_slim = df_d[[c for c in d_cols if c in df_d.columns]]

# ─────────────────────────────────────────
# 병합: A 기준으로 C, D 붙이기
# ─────────────────────────────────────────
print("\n병합 중...")
df = pd.merge(df_a, df_c_slim, on=["ticker", "분기"], how="left")
df = pd.merge(df,   df_d_slim, on=["ticker", "분기"], how="left")

# ─────────────────────────────────────────
# 최종 컬럼 순서
# ─────────────────────────────────────────
col_order = [
    "ticker", "corp_name", "sector", "corp_code", "분기",
    "price", "shares",
    "revenue_curr", "revenue_prev",
    "op_income_curr", "op_income_prev",
    "net_income",
    "assets", "liabilities", "equity",
    "cur_assets", "cur_liab",
    "retained_earnings",
    "interest",
    "cf_oper", "capital_increase", "short_liab", "treasury", "dividend",
]
df = df[[c for c in col_order if c in df.columns]]

# ─────────────────────────────────────────
# 저장
# ─────────────────────────────────────────
output = "kosdaq_2025_final.csv"
df.to_csv(output, index=False, encoding="utf-8-sig")

print(f"\n✅ 완료! 저장: {os.path.abspath(output)}")
print(f"   총 {len(df)}행 / {len(df.columns)}컬럼")
print(f"   종목 수: {df['ticker'].nunique()}개")
print("\nnull rate (%):")
print((df.isnull().sum() / len(df) * 100).round(1).to_string())
print()
print(df.head(6).to_string())