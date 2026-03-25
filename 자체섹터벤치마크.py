import pandas as pd
import numpy as np

file_path = "전시장개별종목주가_완료.xlsx"
df = pd.read_excel(file_path, sheet_name="Sheet1")

periods = ["23Q1","23Q2","23Q3","23Q4","24Q1","24Q2","24Q3","24Q4","25Q1","25Q2","25Q3"]

long_frames = []

base_cols = ["market", "ticker", "corp_name", "sector"]

for p in periods:
    temp = df[base_cols + [f"{p}_start", f"{p}_end"]].copy()
    temp.columns = base_cols + ["start_price", "end_price"]
    temp["period"] = p
    temp["stock_return"] = (temp["end_price"] / temp["start_price"]) - 1
    long_frames.append(temp)

df_long = pd.concat(long_frames, ignore_index=True)

df_long = df_long.replace([np.inf, -np.inf], np.nan)
df_long = df_long.dropna(subset=["sector", "start_price", "end_price", "stock_return"])

df_sector_bm = (
    df_long
    .groupby(["sector", "period"], as_index=False)
    .agg(
        n_stocks=("ticker", "count"),
        sector_return=("stock_return", "mean")
    )
)

df_final = df_long.merge(df_sector_bm, on=["sector", "period"], how="left")
df_final["alpha"] = df_final["stock_return"] - df_final["sector_return"]

# Excel 파일로 저장 (한국어 파일명)
df_long.to_excel("종목별_분기수익률.xlsx", index=False, engine='openpyxl')
df_sector_bm.to_excel("섹터_벤치마크.xlsx", index=False, engine='openpyxl')
df_final.to_excel("종목별_알파_최종.xlsx", index=False, engine='openpyxl')

print("파일 생성 완료:")
print("- 종목별_분기수익률.xlsx")
print("- 섹터_벤치마크.xlsx")
print("- 종목별_알파_최종.xlsx")