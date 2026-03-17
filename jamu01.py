#재무 1분기 2분기 3분기 뽑는거

import pandas as pd
import os
import re
import requests
import time
import zipfile
import io
import xml.etree.ElementTree as ET

# 1. 인증키 설정
DART_API_KEY = "ec997a45483798421cff59d3e9c916aa050ff7bd"
target_csvs = ["fixed_2025_1분기_25컬럼_최종.csv", "fixed_2025_2분기_25컬럼_최종.csv", "fixed_2025_3분기_25컬럼_최종.csv"]


def force_clean(x):
    clean = re.sub(r'[^0-9]', '', str(x))
    return clean[-6:].zfill(6) if clean else "000000"


# --- [1단계] DART 고유번호(corp_code) 맵핑 데이터 확보 (쿼리 1건 소모) ---
print("📡 DART 고유번호 목록을 내려받아 매핑 중입니다... (잠시만 기다려주세요)")
corp_code_url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}"
res = requests.get(corp_code_url)
z = zipfile.ZipFile(io.BytesIO(res.content))
xml_data = z.read('CORPCODE.xml')
tree = ET.fromstring(xml_data)

# {종목코드: 고유번호} 사전 만들기
code_to_corp = {row.findtext('stock_code'): row.findtext('corp_code')
                for row in tree.findall('list') if row.findtext('stock_code')}

# --- [2단계] CSV에서 대상 종목코드 수집 ---
all_stock_codes = set()
for csv_f in target_csvs:
    if os.path.exists(csv_f):
        df_tmp = pd.read_csv(csv_f)
        all_stock_codes.update(df_tmp.iloc[:, 0].apply(force_clean).tolist())

# DART 고유번호로 변환 (매핑되는 것만 골라냄)
corp_list = [code_to_corp[s] for s in all_stock_codes if s in code_to_corp]
print(f"📊 대상 종목 {len(all_stock_codes)}개 중 {len(corp_list)}개의 고유번호를 찾았습니다.")

# --- [3단계] 데이터 호출 및 주입 ---
master_fin_data = {}
keywords = {'이자': '이자비용당기', '영업활동현금흐름': '영업활동현금흐름당기', '유상증자': '유상증자당기',
            '차입금': '단기차입금증가당기', '자기주식': '자기주식취득당기', '배당': '배당금 지급'}

for i in range(0, len(corp_list), 100):
    chunk = corp_list[i:i + 100]
    url = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
    params = {
        'crtfc_key': DART_API_KEY,
        'corp_code': ",".join(chunk),
        'bsns_year': '2024',
        'reprt_code': '11011'  # 사업보고서
    }

    res = requests.get(url, params=params).json()
    items = res.get('list', [])
    if items:
        for item in items:
            stk_code = force_clean(item.get('stock_code', ''))
            nm = str(item.get('account_nm', '')).replace(' ', '')
            val = pd.to_numeric(str(item.get('thstrm_amount', '0')).replace(',', ''), errors='coerce') or 0

            if stk_code not in master_fin_data: master_fin_data[stk_code] = {}
            for kw, col in keywords.items():
                if kw in nm:
                    master_fin_data[stk_code][col] = max(master_fin_data[stk_code].get(col, 0), val)

    print(f"📡 API 호출 중... ({min(i + 100, len(corp_list))}/{len(corp_list)})")
    time.sleep(0.6)

# --- [4단계] CSV 저장 ---
for csv_f in target_csvs:
    if os.path.exists(csv_f):
        df = pd.read_csv(csv_f)
        df['key'] = df.iloc[:, 0].apply(force_clean)
        for col in keywords.values():
            df[col] = df['key'].map(lambda x: master_fin_data.get(x, {}).get(col, 0))
        if '종가' in df.columns:
            df['배당수익률'] = (df['배당금 지급'] / df['종가'] * 100).fillna(0)
        df.drop(columns=['key']).to_csv(csv_f, index=False, encoding='utf-8-sig')
        print(f"✅ {csv_f} 업데이트 완료!")

print("\n✨ 이제 데이터가 정상적으로 들어갔을 겁니다. CSV를 확인해 보세요!")