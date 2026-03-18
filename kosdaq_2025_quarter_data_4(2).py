# price, shares 컬럼만 출력된 csv
import FinanceDataReader as fdr
import pandas as pd
import time
import os
from tqdm import tqdm

QUARTERS = [
    ("20250331", "1분기"),
    ("20250630", "2분기"),
    ("20250930", "3분기"),
]

# ─────────────────────────────────────────
# STEP 1: KOSDAQ stock list + sector
# ─────────────────────────────────────────
def get_kosdaq_list():
    print("Loading KOSDAQ stock list...")
    df = fdr.StockListing("KOSDAQ")
    print(f"  -> raw columns: {list(df.columns)}")

    df = df.rename(columns={
        "Code":   "ticker",
        "ISU_CD": "corp_code",
        "Name":   "corp_name",
        "Dept":   "sector",
        "Stocks": "shares_listed",
    })

    df["ticker"] = df["ticker"].astype(str).str.zfill(6)

    keep = ["ticker", "corp_name", "sector", "corp_code", "shares_listed"]
    df = df[[c for c in keep if c in df.columns]].drop_duplicates("ticker").reset_index(drop=True)
    print(f"  -> {len(df)} stocks loaded")
    return df

# ─────────────────────────────────────────
# STEP 2: price (분기말 종가)
# ─────────────────────────────────────────
def get_price(ticker, date):
    try:
        end = f"{date[:4]}-{date[4:6]}-{date[6:]}"
        df  = fdr.DataReader(ticker, f"{date[:4]}-01-01", end)
        if df.empty:
            return None
        return int(df["Close"].iloc[-1])
    except Exception:
        return None

# ─────────────────────────────────────────
# STEP 3: main
# ─────────────────────────────────────────
def main():
    df_list           = get_kosdaq_list()
    has_shares_listed = "shares_listed" in df_list.columns
    all_rows          = []

    for _, corp in tqdm(df_list.iterrows(), total=len(df_list), desc="Processing"):
        ticker = corp["ticker"]

        row = {
            "ticker":    ticker,
            "corp_name": corp["corp_name"],
            "sector":    corp.get("sector"),
            "corp_code": corp.get("corp_code"),
        }

        for date, qname in QUARTERS:
            # price
            row[f"{qname}_price"] = get_price(ticker, date)

            # shares: StockListing의 Stocks 컬럼 사용
            if has_shares_listed:
                v = corp.get("shares_listed")
                row[f"{qname}_shares"] = int(v) if pd.notna(v) and v != 0 else None
            else:
                row[f"{qname}_shares"] = None

            time.sleep(0.02)

        all_rows.append(row)

    # wide format
    df = pd.DataFrame(all_rows)
    id_cols  = ["ticker", "corp_name", "sector", "corp_code"]
    val_cols = []
    for _, qname in QUARTERS:
        val_cols += [f"{qname}_price", f"{qname}_shares"]

    df = df[[c for c in id_cols + val_cols if c in df.columns]]
    wide_path = "kosdaq_2025_krx_wide.csv"
    df.to_csv(wide_path, index=False, encoding="utf-8-sig")
    print(f"\nSaved wide: {os.path.abspath(wide_path)}")

    # long format
    rows_long = []
    for _, r in df.iterrows():
        for _, qname in QUARTERS:
            rows_long.append({
                "ticker":    r["ticker"],
                "corp_name": r["corp_name"],
                "sector":    r.get("sector"),
                "corp_code": r.get("corp_code"),
                "quarter":   qname,
                "price":     r.get(f"{qname}_price"),
                "shares":    r.get(f"{qname}_shares"),
            })

    df_long = pd.DataFrame(rows_long)
    long_path = "kosdaq_2025_quarter_data_3(2).csv"
    df_long.to_csv(long_path, index=False, encoding="utf-8-sig")
    print(f"Saved long: {os.path.abspath(long_path)}")

    print(f"\nNull rate (%):")
    print((df_long.isnull().sum() / len(df_long) * 100).round(1).to_string())
    print(f"\nSummary:")
    print(f"  wide: {len(df)} rows / {len(df.columns)} cols")
    print(f"  long: {len(df_long)} rows / {len(df_long.columns)} cols")
    print(df_long.head(6).to_string())


if __name__ == "__main__":
    main()
