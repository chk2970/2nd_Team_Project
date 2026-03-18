# Code(종목코드), Name(기업명), corp_code(DART기업코드) 추출 csv
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile
from tqdm import tqdm
import os
import FinanceDataReader as fdr

API_KEY = "c1b55b3c944500631f2199d8985a47eddf32d451"

# ============================================================
# 설정: 수집할 분기 목록
# ============================================================
q_dict = {"11013": "1분기", "11012": "2분기", "11014": "3분기", "11011": "4분기"}
quarters = [
    ("2025", "11013"),  # 1분기
    ("2025", "11012"),  # 2분기
    ("2025", "11014"),  # 3분기
]

# ============================================================
# STEP 1. DART CORPCODE.xml 다운로드 및 코스닥 필터링
# ============================================================
print("=" * 50)
print("STEP 1. DART CORPCODE.xml 다운로드 중...")
print("=" * 50)

url = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={API_KEY}"
with urlopen(url) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall("corp_num")

tree = ET.parse("corp_num/CORPCODE.xml")
root = tree.getroot()

mapping = []
for item in root.findall(".//list"):
    corp_code = item.findtext("corp_code")
    stock_code = item.findtext("stock_code")
    corp_name = item.findtext("corp_name")
    if stock_code and len(stock_code) == 6:
        mapping.append({
            "stock_code": stock_code,
            "corp_code":  corp_code,
            "corp_name":  corp_name,
        })

map_df = pd.DataFrame(mapping)
print(f"전체 상장기업: {len(map_df)}개")

print("\nSTEP 2. 코스닥 기업 필터링 중 (corp_cls == K)...")
kosdaq_rows = []
for _, row in tqdm(map_df.iterrows(), total=len(map_df)):
    try:
        r = requests.get(
            "https://opendart.fss.or.kr/api/company.json",
            params={"crtfc_key": API_KEY, "corp_code": row["corp_code"]},
            timeout=10,
        )
        data = r.json()
        if data.get("status") == "000" and data.get("corp_cls") == "K":
            kosdaq_rows.append({
                "Code":      row["stock_code"],
                "Name":      row["corp_name"],
                "corp_code": str(row["corp_code"]).zfill(8),
            })
    except:
        continue

merged = pd.DataFrame(kosdaq_rows)
merged["Code"] = merged["Code"].astype(str).str.zfill(6)
print(f"\n코스닥 종목 수: {len(merged)}개")
merged.to_csv("kosdaq_with_corpcode.csv", index=False, encoding="utf-8-sig")
print("저장 완료: kosdaq_with_corpcode.csv")