# 우선주(5,7,7) 리츠,스팩 제외 코스피
import pandas as pd
import os

# [1] 경로 및 파일 설정
base_path = r"C:\workspaces\Basic\WebConn\seconproject"
input_file = "KOSPI_2025_재무제표(컬럼영문).csv"
output_file = "KOSPI_2025_보통주_최종_정제본.csv"

full_input_path = os.path.join(base_path, input_file)
full_output_path = os.path.join(base_path, output_file)


def filter_data_final():
    if not os.path.exists(full_input_path):
        print(f"❌ 파일을 찾을 수 없습니다: {full_input_path}")
        return

    print(f"🚀 {input_file} 정제 작업을 시작합니다...")

    # 1. 데이터 로드 (인코딩 에러 방지)
    try:
        df = pd.read_csv(full_input_path, dtype={'ticker': str, '종목코드': str}, encoding='utf-8-sig')
    except:
        df = pd.read_csv(full_input_path, dtype={'ticker': str, '종목코드': str}, encoding='cp949')

    # 2. 중복 헤더 제거
    if '종목코드' in str(df.iloc[0, 0]):
        df = df.iloc[1:].reset_index(drop=True)

    original_count = len(df)

    # 3. 우선주 제외 (끝자리가 5, 7, 9인 종목코드)
    code_col = 'ticker' if 'ticker' in df.columns else '종목코드'
    df = df[~df[code_col].str.endswith(('5', '7', '9'))]
    pref_filtered = original_count - len(df)

    # 4. 리츠 및 스팩 제외 (메리츠는 살리고 '리츠', '스팩'만 제거)
    name_col = 'corp_name' if 'corp_name' in df.columns else '기업명'

    # [핵심 로직] (?<!메)리츠 : '메'가 앞에 붙지 않은 '리츠'만 찾습니다.
    # 즉, '메리츠'는 통과시키고 '신한알파리츠' 등은 제거합니다.
    exclusion_pattern = r'(?<!메)리츠|스팩'
    df = df[~df[name_col].str.contains(exclusion_pattern, na=False)]

    final_count = len(df)
    etc_filtered = (original_count - pref_filtered) - final_count

    # [결과 출력]
    print(f"\n📊 정제 결과 보고:")
    print(f"   - 원본 전체 종목: {original_count}개")
    print(f"   - 제외된 우선주: {pref_filtered}개")
    print(f"   - 제외된 리츠 및 스팩: {etc_filtered}개 (메리츠는 제외 안 됨)")
    print(f"   - 최종 남은 보통주: {final_count}개")

    # 5. 새로운 파일로 저장
    df.to_csv(full_output_path, index=False, encoding='utf-8-sig')
    print(f"\n✨ 작업 완료! 새 파일 생성됨: {output_file}")


if __name__ == "__main__":
    filter_data_final()
"""
# 1. 모든 숫자형(Float) 컬럼을 소수점 2자리로 반올림
df = df.round(2)

# 2. (선택사항) 특정 컬럼만 정밀하게 챙기고 싶다면?
# 예를 들어 insolvency_flag와 div_ratio만 딱 2자리로 만들고 싶을 때
# df['insolvency_flag'] = df['insolvency_flag'].round(2)
# df['div_ratio'] = df['div_ratio'].round(2)

# 3. 저장 (한글 깨짐 방지 옵션 포함)
df.to_csv("최종_깔끔한_결과물.csv", index=False, encoding='utf-8-sig')

print("✨ 모든 소수점이 2자리로 정리되었습니다! ㅋ")
"""