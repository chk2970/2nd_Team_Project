# pip install pykrx
from pykrx import stock
import pandas as pd

# 1. 엑셀 로드
df_target = pd.read_excel(r'C:\workspaces\Basic\WebConn\6_2nd_Project\KOSDAQ_신규상장및거래중지.xlsx')

# 2. 코스닥 전체 종목 코드/이름
tickers = stock.get_market_ticker_list(market="KOSDAQ")
ticker_names = {t: stock.get_market_ticker_name(t) for t in tickers}
# 이름 → 코드 역매핑
name_to_code = {v: k for k, v in ticker_names.items()}

# 3. 상장일 조회 (첫 거래일 = 상장일)
results = []
for _, row in df_target.iterrows():
    name = row['기업명']
    code = name_to_code.get(name, None)
    listing_date = None

    if code:
        try:
            ohlcv = stock.get_market_ohlcv("19900101", "20261231", code)
            if len(ohlcv) > 0:
                listing_date = ohlcv.index[0].strftime("%Y-%m-%d")
        except:
            pass

    results.append({'기업명': name, '종목코드': code, '분류': row['분류'], '상장일': listing_date})
    print(f"  {name} → {code} → {listing_date}")

# 4. 저장
result_df = pd.DataFrame(results)
result_df.to_csv('kosdaq_상장일_매칭결과.csv', index=False, encoding='utf-8-sig')
print(f'\n총 {len(result_df)}건 / 성공: {result_df["상장일"].notna().sum()}건 / 실패: {result_df["상장일"].isna().sum()}건')