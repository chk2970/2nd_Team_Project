# 1.2.3. 분기 재무 뽑는 코드

import pandas as pd
import os
import requests
import time
import zipfile
import io
import xml.etree.ElementTree as ET
import FinanceDataReader as fdr

# ==========================================
# [1] 설정 및 경로
# ==========================================
DART_API_KEY = "ec997a45483798421cff59d3e9c916aa050ff7bd"
SAVE_DIR = r"C:\hrfile\project-phy\seconproject"
YEAR = '2025'
REPORT_CODE = '11013'  # 1분기

# 🌟 [초정밀] 전체 재무제표에서 찾아낼 키워드들
keywords = {
    '매출액(당기)': ['영업수익', '매출액', '수익(매출액)', '수익'],
    '영업이익(당기)': ['영업이익'],
    '당기순이익': ['분기순이익', '당기순이익'],
    '자산총계': ['자산총계'],
    '부채총계': ['부채총계'],
    '자본총계': ['자본총계'],
    '유동자산': ['유동자산'],
    '유동부채': ['유동부채'],
    '비유동부채': ['비유동부채'],
    '이익잉여금': ['이익잉여금'],
    '이자비용': ['이자비용', '금융비용(이자비용)'],
    '단기차입금': ['단기차입금'],
    '영업활동현금흐름': ['영업활동으로 인한 현금흐름', '영업활동현금흐름'],
    '유상증자': ['유상증자', '자본금의 증가'],
    '자기주식취득': ['자기주식의 취득', '자기주식취득'],
    '배당금 지급': ['배당금의 지급', '현금배당금의 지급', '배당금지급']
}

# ==========================================
# [2] 기초 데이터 준비
# ==========================================
print("📡 KOSPI 종목 및 DART 고유번호 매핑 중...")
df_krx = fdr.StockListing('KOSPI')
res = requests.get(f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}")
z = zipfile.ZipFile(io.BytesIO(res.content))
tree = ET.fromstring(z.read('CORPCODE.xml'))
corp_map = {row.findtext('stock_code'): row.findtext('corp_code')
            for row in tree.findall('list') if row.findtext('stock_code') in df_krx['Code'].tolist()}

# ==========================================
# [3] 정밀 수집 루프 (약 10~15분 소요)
# ==========================================
final_rows = []
total = len(corp_map)
print(f"🚀 총 {total}개 기업 정밀 수집 시작... (보고서 전체를 뒤집니다)")

for idx, (s_code, c_code) in enumerate(corp_map.items()):
    info = df_krx[df_krx['Code'] == s_code].iloc[0]
    data = {k: 0 for k in keywords}
    data['매출액(전기)'] = 0
    data['영업이익(전기)'] = 0

    # 🌟 SinglAcntAll API 호출 (보고서 전체 항목 가져오기)
    url = f"https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?crtfc_key={DART_API_KEY}&corp_code={c_code}&bsns_year={YEAR}&reprt_code={REPORT_CODE}&fs_div=OFS"
    try:
        resp = requests.get(url).json()
        if resp.get('status') == '000':
            for item in resp.get('list', []):
                nm = item.get('account_nm', '').replace(' ', '')
                val = pd.to_numeric(item.get('thstrm_amount', '0').replace(',', ''), errors='coerce') or 0
                prev = pd.to_numeric(item.get('frmtrm_amount', '0').replace(',', ''), errors='coerce') or 0

                for key, syns in keywords.items():
                    if any(s in nm for s in syns):
                        if data[key] == 0:
                            data[key] = val
                            if key == '매출액(당기)': data['매출액(전기)'] = prev
                            if key == '영업이익(당기)': data['영업이익(전기)'] = prev
    except:
        pass

    # [4] 34개 컬럼 순서 고정
    final_rows.append({
        '종목코드': s_code, '기업명': info['Name'], '섹터': info.get('Sector', '미분류'), '종가': info.get('Close', 0),
        '발행주식수': info.get('Stocks', 0),
        '매출액(당기)': data['매출액(당기)'], '매출액(전기)': data['매출액(전기)'],
        '영업이익(당기)': data['영업이익(당기)'], '영업이익(전기)': data['영업이익(전기)'], '당기순이익': data['당기순이익'],
        '자산총계': data['자산총계'], '부채총계': data['부채총계'], '자본총계': data['자본총계'], '유동자산': data['유동자산'],
        '유동부채': data['유동부채'], '비유동부채': data['비유동부채'], '이익잉여금': data['이익잉여금'], '이자비용': data['이자비용'],
        '단기차입금': data['단기차입금'],
        '영업활동현금흐름': data['영업활동현금흐름'], '유상증자': data['유상증자'], '자기주식취득': data['자기주식취득'], '배당금 지급': data['배당금 지급'],
        '배당수익률': round(data['배당금 지급'] / info['Marcap'] * 100, 2) if info['Marcap'] > 0 else 0,
        '영업이익률': 0, '부채비율': 0, '유동비율': 0, '이자보상배율': 0, '매출QoQ': 0,
        '영업이익': data['영업이익(당기)'], '시가총액': info.get('Marcap', 0),
        '흑자도산 감지': 0, '배당성향': 0
    })

    if idx % 50 == 0: print(f"📊 진행률: {idx}/{total} 완료...")
    time.sleep(0.6)  # API 속도 제한 준수

# [5] 저장
pd.DataFrame(final_rows).to_csv(os.path.join(SAVE_DIR, "KOSPI_최종데이터_1분기.csv"), index=False, encoding='utf-8-sig')
print("\n🎉 드디어 정밀 수집이 끝났습니다! 이제 엑셀을 확인해 보세요.")