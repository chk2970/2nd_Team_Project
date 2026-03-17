# 빈컬럼 값 채우는 코드('이자': '이자비용당기','영업': '영업활동현금흐름당기',증자': '유상증자당기',차입': '단기차입금증가당기',)

import pandas as pd
import os
import re
import requests
import time
import zipfile
import io
import xml.etree.ElementTree as ET

DART_API_KEY = "ec997a45483798421cff59d3e9c916aa050ff7bd"
target_csvs = ["fixed_2025_1분기_25컬럼_최종.csv", "fixed_2025_2분기_25컬럼_최종.csv", "fixed_2025_3분기_25컬럼_최종.csv"]


def force_clean(x):
    clean = re.sub(r'[^0-9]', '', str(x))
    return clean[-6:].zfill(6) if clean else "000000"


# 1. 고유번호 매핑
res = requests.get(f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}")
z = zipfile.ZipFile(io.BytesIO(res.content))
tree = ET.fromstring(z.read('CORPCODE.xml'))
code_to_corp = {row.findtext('stock_code'): row.findtext('corp_code') for row in tree.findall('list') if
                row.findtext('stock_code')}

# 2. 대상 수집
all_stock_codes = set()
for csv_f in target_csvs:
    if os.path.exists(csv_f):
        all_stock_codes.update(pd.read_csv(csv_f).iloc[:, 0].apply(force_clean).tolist())

master_fin_data = {}
# 매칭 범위를 더 넓힌 키워드
keywords = {
    '이자': '이자비용당기',
    '영업': '영업활동현금흐름당기',
    '증자': '유상증자당기',
    '차입': '단기차입금증가당기',
    '주식': '자기주식취득당기',
    '배당': '배당금 지급'
}

print(f"🚀 {len(all_stock_codes)}개 종목 초정밀 분석 시작 (연결+별도 통합)...")

for idx, stk_code in enumerate(all_stock_codes):
    corp_code = code_to_corp.get(stk_code)
    if not corp_code: continue

    # 연결(CFS)과 별도(OFS)를 둘 다 시도해서 데이터가 있는 쪽을 취함
    for fs_div in ['CFS', 'OFS']:
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
        params = {'crtfc_key': DART_API_KEY, 'corp_code': corp_code, 'bsns_year': '2024', 'reprt_code': '11011',
                  'fs_div': fs_div}

        try:
            res = requests.get(url, params=params).json()
            items = res.get('list', [])
            if not items: continue

            if stk_code not in master_fin_data: master_fin_data[stk_code] = {}

            for item in items:
                nm = str(item.get('account_nm', '')).replace(' ', '')
                val = pd.to_numeric(str(item.get('thstrm_amount', '0')).replace(',', ''), errors='coerce') or 0

                for kw_key, col_name in keywords.items():
                    if kw_key in nm:
                        # 더 큰 값이나 0이 아닌 값을 우선 저장
                        master_fin_data[stk_code][col_name] = max(master_fin_data[stk_code].get(col_name, 0), val)
        except:
            continue

    if (idx + 1) % 10 == 0:
        print(f"📡 분석 중... ({idx + 1}/{len(all_stock_codes)})")
    time.sleep(0.1)  # 4만 건 한도이므로 조금 더 빠르게 진행 가능

# 3. CSV 저장
for csv_f in target_csvs:
    if os.path.exists(csv_f):
        df = pd.read_csv(csv_f)
        df['key'] = df.iloc[:, 0].apply(force_clean)
        for col in keywords.values():
            df[col] = df['key'].map(lambda x: master_fin_data.get(x, {}).get(col, 0))

        if '종가' in df.columns:
            df['배당수익률'] = (df['배당금 지급'] / df['종가'] * 100).fillna(0)

        df.drop(columns=['key']).to_csv(csv_f, index=False, encoding='utf-8-sig')
        print(f"✅ {csv_f} 보정 완료!")

print("\n✨ 이제 0이었던 데이터들이 최대한 채워졌을 겁니다! 확인해 보세요.")