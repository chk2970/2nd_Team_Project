"""
이상값 처리 스크립트
- 입력: KOSDAQ_전처리완료_1차.xlsx
- 출력: KOSDAQ_전처리완료_2차.csv
"""

import pandas as pd
import numpy as np
import os

INPUT_FILE  = "KOSDAQ_전처리완료_1차.xlsx"
OUTPUT_FILE = "KOSDAQ_outlier_data.csv"

def main():
    print(f"파일 로드 중: {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)
    print(f"  -> {len(df)}행 / {len(df.columns)}컬럼 로드 완료")

    # 이상값 처리 전 현황
    print("\n[처리 전 이상값 현황]")
    indicators = [
        "oper_margin", "liab_ratio", "curr_ratio", "interest_coverage",
        "revenue_qoq", "oper_income_qoq", "market_cap",
        "insolvency_flag", "div_ratio", "z_score"
    ]
    for col in indicators:
        if col in df.columns:
            null_cnt = df[col].isnull().sum()
            print(f"  {col}: null {null_cnt}개 ({null_cnt/len(df)*100:.1f}%)")

    print("\n[이상값 처리 시작]")

    # ─────────────────────────────────────────
    # 1. oper_margin (영업이익률): Clipping -200% ~ 100%
    # ─────────────────────────────────────────
    df["oper_margin"] = df["oper_margin"].clip(-200, 100)
    print("✅ oper_margin: -200 ~ 100 클리핑 완료")

    # ─────────────────────────────────────────
    # 2. liab_ratio (부채비율): 자본잠식(equity<0) → NaN, 나머지 0~1000 클리핑
    # ─────────────────────────────────────────
    df.loc[df["equity"] < 0, "liab_ratio"] = np.nan
    df["liab_ratio"] = df["liab_ratio"].clip(0, 1000)
    print("✅ liab_ratio: 자본잠식 NaN + 0~1000 클리핑 완료")

    # ─────────────────────────────────────────
    # 3. curr_ratio (유동비율): Clipping 0% ~ 2000%
    # ─────────────────────────────────────────
    df["curr_ratio"] = df["curr_ratio"].clip(0, 2000)
    print("✅ curr_ratio: 0 ~ 2000 클리핑 완료")

    # ─────────────────────────────────────────
    # 4. interest_coverage (이자보상배율): 이자비용=0 → NaN, 나머지 -100~100 클리핑
    # ─────────────────────────────────────────
    df.loc[df["interest"] == 0, "interest_coverage"] = np.nan
    df["interest_coverage"] = df["interest_coverage"].clip(-100, 100)
    print("✅ interest_coverage: 이자비용=0 NaN + -100~100 클리핑 완료")

    # ─────────────────────────────────────────
    # 5. revenue_qoq (매출 QoQ): Clipping -100% ~ 500%
    # ─────────────────────────────────────────
    df["revenue_qoq"] = df["revenue_qoq"].clip(-100, 500)
    print("✅ revenue_qoq: -100 ~ 500 클리핑 완료")

    # ─────────────────────────────────────────
    # 6. oper_income_qoq (영업이익 QoQ): Clipping -500% ~ 500%
    # ─────────────────────────────────────────
    df["oper_income_qoq"] = df["oper_income_qoq"].clip(-500, 500)
    print("✅ oper_income_qoq: -500 ~ 500 클리핑 완료")

    # ─────────────────────────────────────────
    # 7. market_cap (시가총액): 유지 (처리 없음)
    # ─────────────────────────────────────────
    print("✅ market_cap: 유지 (처리 없음)")

    # ─────────────────────────────────────────
    # 8. insolvency_flag (흑자도산감지): 순이익<=0 → NaN, 나머지 -10~10 클리핑
    # ─────────────────────────────────────────
    df.loc[df["net_income"] <= 0, "insolvency_flag"] = np.nan
    df["insolvency_flag"] = df["insolvency_flag"].clip(-10, 10)
    print("✅ insolvency_flag: 순이익<=0 NaN + -10~10 클리핑 완료")

    # ─────────────────────────────────────────
    # 9. div_ratio (배당성향): 순이익<0 → NaN, 나머지 0~200 클리핑
    # ─────────────────────────────────────────
    df.loc[df["net_income"] < 0, "div_ratio"] = np.nan
    df["div_ratio"] = df["div_ratio"].clip(0, 200)
    print("✅ div_ratio: 순이익<0 NaN + 0~200 클리핑 완료")

    # ─────────────────────────────────────────
    # 10. z_score: Clipping -10 ~ 20
    # ─────────────────────────────────────────
    df["z_score"] = df["z_score"].clip(-10, 20)
    print("✅ z_score: -10 ~ 20 클리핑 완료")

    # ─────────────────────────────────────────
    # 저장
    # ─────────────────────────────────────────
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"\n✅ 완료! 저장: {os.path.abspath(OUTPUT_FILE)}")
    print(f"   총 {len(df)}행 / {len(df.columns)}컬럼")

    print("\n[처리 후 이상값 현황]")
    for col in indicators:
        if col in df.columns:
            null_cnt = df[col].isnull().sum()
            print(f"  {col}: null {null_cnt}개 ({null_cnt/len(df)*100:.1f}%)")

    print("\n[샘플 확인]")
    print(df[["quarter","ticker","corp_name"] + [c for c in indicators if c in df.columns]].head(6).to_string())

if __name__ == "__main__":
    main()