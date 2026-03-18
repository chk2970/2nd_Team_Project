import pandas as pd
import requests
import time

DART_API_KEY = ""  # API 키 입력

# ── 설정 ──────────────────────────────────────────
BSNS_YEAR = "2024"       # 사업연도 (결산배당 기준)
REPRT_CODE = "11011"     # 11011=사업보고서(연간), 11012=반기, 11013=1분기, 11014=3분기
INPUT_FILE = "코스닥재무제표최종_SPAC제거버전.xlsx"
OUTPUT_FILE = "kosdaq_div_yield.xlsx"
SLEEP_SEC = 0.3          # 호출 간격 (초)
# ──────────────────────────────────────────────────

def fetch_dps(corp_code: str) -> float | None:
    """DART dvSttus API → 주당배당금(DPS) 반환. 없으면 None."""
    url = "https://opendart.fss.or.kr/api/dvSttus.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": str(corp_code).zfill(8),
        "bsns_year": BSNS_YEAR,
        "reprt_code": REPRT_CODE,
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if data.get("status") != "000" or not data.get("list"):
            return None

        # 보통주 현금배당 DPS 추출
        for item in data["list"]:
            if "보통주" in item.get("se", "") and "현금" in item.get("se", ""):
                raw = item.get("dps", "").replace(",", "").strip()
                return float(raw) if raw else None
        return None

    except Exception:
        return None


def main():
    df_all = pd.read_excel(INPUT_FILE, sheet_name="kosdaq_2025_merged_final")

    # 종목 유니크 추출 + price는 분기 중 최신값(3분기) 우선 사용
    price_map = (
        df_all.sort_values("분기")
        .groupby("ticker")["price"]
        .last()
        .reset_index()
    )
    uniq = (
        df_all[["ticker", "corp_code", "corp_name"]]
        .drop_duplicates("ticker")
        .merge(price_map, on="ticker", how="left")
        .reset_index(drop=True)
    )
    uniq["corp_code"] = uniq["corp_code"].astype(str).str.zfill(8)

    print(f"총 {len(uniq)}개 종목 처리 시작...")

    dps_list = []
    for i, row in uniq.iterrows():
        dps = fetch_dps(row["corp_code"])
        dps_list.append(dps)

        if (i + 1) % 100 == 0:
            done = sum(1 for x in dps_list if x is not None)
            print(f"  [{i+1}/{len(uniq)}] 배당 데이터 있는 종목: {done}개")

        time.sleep(SLEEP_SEC)

    uniq["dps"] = dps_list

    # div_yield = DPS / 주가 (price가 없거나 0이면 NaN)
    uniq["div_yield"] = uniq.apply(
        lambda r: round(r["dps"] / r["price"], 4)
        if pd.notna(r["dps"]) and pd.notna(r["price"]) and r["price"] > 0
        else None,
        axis=1,
    )

    result = uniq[["ticker", "corp_code", "corp_name", "div_yield"]].copy()

    filled = result["div_yield"].notna().sum()
    print(f"\n완료: {filled}/{len(result)}개 종목 div_yield 계산됨")
    print(result[result["div_yield"].notna()].head(10))

    result.to_excel(OUTPUT_FILE, index=False)
    print(f"\n저장 완료 → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
