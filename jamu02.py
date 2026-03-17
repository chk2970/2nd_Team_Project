# 거래소에서 실시간 섹터와 종가를 내리스트 합치는 코드
import FinanceDataReader as fdr
import pandas as pd
import os

# 1. 파일 경로 설정 (사용자님 환경 확인)
base_file = "kospi_with_corpcode.csv"  # 만약 seconpj 폴더 안에 있다면 "../seconpj/kospi_with_corpcode.csv"

# 2. 시장 데이터 가져오기 (컬럼명 에러 방지 버전)
print("📡 거래소에서 종가 및 섹터 정보를 실시간으로 가져옵니다...")
try:
    krx = fdr.StockListing("KRX")

    # [수정] 라이브러리마다 컬럼명이 다를 수 있어 존재 여부를 확인합니다.
    # 보통 'Code', 'Name', 'Sector'(또는 'Industry'), 'Close', 'Stocks' 등이 들어있습니다.
    cols_mapping = {
        'Code': 'stock_code',
        'Sector': '섹터',
        'Industry': '섹터',  # Sector 대신 Industry일 경우 대비
        'Close': '종가',
        'Stocks': '발행주식수'
    }

    # 있는 컬럼만 골라서 이름을 바꿉니다.
    target_cols = [c for c in krx.columns if c in cols_mapping]
    krx = krx[target_cols].rename(columns=cols_mapping)

    # 만약 '섹터' 컬럼이 두 개 생겼다면 하나로 합칩니다.
    if isinstance(krx.get('섹터'), pd.DataFrame):
        krx['섹터'] = krx['섹터'].iloc[:, 0]

    krx['stock_code'] = krx['stock_code'].astype(str).str.zfill(6)
    print("✅ 시장 데이터 로드 성공!")
except Exception as e:
    print(f"❌ 시장 데이터 로드 중 오류: {e}")
    exit()

# 3. 기초 데이터 로드 및 병합
print("📂 기초 리스트 로드 및 병합 중...")
if os.path.exists(base_file):
    merged = pd.read_csv(base_file)
    merged['stock_code'] = merged['stock_code'].astype(str).str.zfill(6)

    # 시장 데이터(섹터, 종가)를 기초 데이터에 수혈
    merged = pd.merge(merged, krx, on='stock_code', how='left')
    print("✅ 데이터 준비 완료! 이제 루프를 돌며 재무제표를 채우면 됩니다.")
else:
    print(f"❌ {base_file} 파일이 없습니다. 경로를 확인하세요.")
    exit()