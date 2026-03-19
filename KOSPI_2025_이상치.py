# 이상치 범위 설정
import pandas as pd
import numpy as np

"""
# 1. 파일 읽기 (파일명은 실제 파일로 수정하세요)
file_path = 'KOSPI_정제(우선주,리츠).csv'
try:
    # 소수점 . 이 포함된 데이터를 숫자로 인식하며 읽어옵니다.
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(f"✅ '{file_path}' 로드 성공!")
except Exception as e:
    print(f"❌ 파일을 읽을 수 없습니다: {e}")
"""

# 1. 엑셀 파일 읽기
# 파이참 프로젝트 폴더 안에 있는 실제 엑셀 파일명을 적어주세요.
file_path = '02_KOSPI_결측치보완.xlsx'

try:
    # 엑셀 읽기 (엔진으로 openpyxl 사용)
    df = pd.read_excel(file_path)
    print(f"✅ '{file_path}' 파일을 성공적으로 불러왔습니다.")
except Exception as e:
    print(f"❌ 파일을 읽을 수 없습니다. (pip install openpyxl 확인): {e}")

def apply_exact_clipping(df):
    cdf = df.copy()

    # [수정된 11개 지표와 정확한 수치 범위]
    # 상한가 넘으면 상한값으로, 하한가 안되면 하한값으로 무조건 대체
    specs = {
        'oper_margin': (-200.0, 100.0),  # 1 & 11. 영업이익률 (강조하신 수치!)
        'liab_ratio': (0.0, 1000.0),  # 2. 부채비율
        'curr_ratio': (0.0, 2000.0),  # 3. 유동비율
        'interest_coverage': (-100.0, 100.0),  # 4. 이자보상배율
        'revenue_qoq': (-100.0, 500.0),  # 5. 매출 QoQ
        'oper_income_qoq': (-500.0, 500.0),  # 6. 영업이익 QoQ
        'insolvency_flag': (-10.0, 10.0),  # 8. 흑자도산감지
        'div_ratio': (0.0, 200.0),  # 9. 배당성향 (무조건 0~200 사이로 대체)
        'z_score': (-10.0, 20.0)  # 10. Z-Score
    }

    for col, (low, high) in specs.items():
        if col in cdf.columns:
            # Step A: 숫자가 아닌 형태를 강제로 숫자로 변환 (정제 실패 방지)
            cdf[col] = pd.to_numeric(cdf[col], errors='coerce')

            # Step B: 범위 대체 (Clipping)
            # 하한가보다 작으면 low로, 상한가보다 크면 high로 딱 맞춤
            cdf[col] = cdf[col].clip(lower=low, upper=high)

            # Step C: 기존에 비어있던 널(Null) 값들이 있다면 0으로 채우기
            # 만약 빈칸을 그대로 두고 싶다면 아래 줄을 삭제하세요.
            cdf[col] = cdf[col].fillna(0)

    # 마지막 확인: 11번 oper_margin 정제 재적용
    if 'oper_margin' in cdf.columns:
        cdf['oper_margin'] = cdf['oper_margin'].clip(-200.0, 100.0)

    return cdf


# 2. 정제 실행
cleaned_df = apply_exact_clipping(df)

# 3. 결과 저장
output_file = '정제완료_확정본.csv'
cleaned_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"✨ 정제가 완료되었습니다! 모든 수치가 지정한 범위 내로 대체되었습니다.")
print(f"결과 파일: {output_file}")

# [검증] 정제된 데이터의 최소/최대값 출력해서 확인하기
print("\n--- 실제 정제 결과 수치 확인 ---")
print(cleaned_df[['oper_margin', 'div_ratio', 'liab_ratio']].agg(['min', 'max']))