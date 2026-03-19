# 섹터 통일 및 5대 계산 및 소수점2자리 코드
import pandas as pd
import numpy as np
import os

# [1] 파일 설정
TARGET_FILE = "KOSPI_Final_Standard_Ready.csv"

if not os.path.exists(TARGET_FILE):
    print(f"❌ 파일을 찾을 수 없습니다: {TARGET_FILE}")
else:
    try:
        df = pd.read_csv(TARGET_FILE, encoding='utf-8-sig')
    except UnicodeDecodeError:
        df = pd.read_csv(TARGET_FILE, encoding='cp949')

    # [2] 섹터 분류 함수 정의 (보내주신 로직 그대로)
    def categorize_sector(val):
        t = str(val).lower()
        if '통신' in t: return '통신'
        if 'it' in t or '소프트웨어' in t or '정보서비스' in t: return 'IT 서비스'
        if '은행' in t: return '은행'
        if '증권' in t: return '증권'
        if '보험' in t: return '보험'
        if '금융' in t or '신탁' in t or '지주' in t or '투자' in t: return '기타금융'
        if '자동차' in t or '부품' in t or '조선' in t or '운송장비' in t: return '운송장비·부품'
        if '전자' in t or '반도체' in t or '정밀' in t or '전선' in t or '전기장비' in t or '케이블' in t: return '전기·전자'
        if '화학' in t or '석유' in t or '에너지' in t or '정제' in t: return '화학'
        if '제약' in t or '바이오' in t or '의약' in t: return '제약'
        if '기계' in t: return '기계·장비'
        if '금속' in t or '철강' in t: return '금속'
        if '비금속' in t: return '비금속'
        if '종이' in t or '목재' in t: return '종이·목재'
        if '부동산' in t or '리츠' in t: return '부동산'
        if '서비스' in t or '컨설팅' in t: return '일반서비스'
        if '섬유' in t or '의류' in t or '직물' in t or '피혁' in t or '가죽' in t: return '섬유·의류'
        if '음식' in t or '식품' in t or '담배' in t: return '음식료·담배'
        if '유통' in t or '도매' in t or '소매' in t or '판매' in t: return '유통'
        if '건설' in t: return '건설'
        if '오락' in t or '문화' in t or '스포츠' in t: return '오락·문화'
        if '전기' in t or '가스' in t: return '전기·가스'
        if '운송' in t or '창고' in t: return '운송·창고'
        if '농업' in t or '임업' in t or '어업' in t: return '농업, 임업 및 어업'
        return '기타제조'

    # [3] 컬럼명 강제 교정 (기존 로직)
    en_headers = [
        'quarter', 'ticker', 'corp_name', 'sector', 'price', 'shares',
        'revenue_curr', 'revenue_prev', 'op_income_curr', 'op_income_prev',
        'net_income', 'assets', 'liabilities', 'equity', 'cur_assets',
        'cur_liab', 'retained_earnings', 'interest', 'cf_oper',
        'capital_increase', 'short_liab', 'treasury', 'dividend',
        'div_yield', 'oper_margin', 'liab_ratio', 'curr_ratio',
        'interest_coverage', 'revenue_qoq', 'oper_income_qoq',
        'market_cap', 'insolvency_flag', 'div_ratio', 'z_score'
    ]
    if df.iloc[0, 0] in ['quarter', 'ticker', '분기', '종목코드']:
        df.columns = df.iloc[0]
        df = df.drop(df.index[0]).reset_index(drop=True)
    if 'op_income_curr' not in df.columns:
        df.columns = en_headers[:len(df.columns)]

    # [4] ★ 섹터 분류 재적용 (계산 전에 실행!) ★
    df['sector'] = df['sector'].apply(categorize_sector)

    # [5] 지표 계산 함수 (0으로 나누기 방지)
    def set_opermarg(opin_c, rev_c): return (opin_c / rev_c * 100) if rev_c != 0 else 0
    def set_liabrat(liab, equi): return (liab / equi * 100) if equi != 0 else 0
    def set_currrat(c_asset, c_liab): return (c_asset / c_liab * 100) if c_liab != 0 else 0
    def set_interecover(opin_c, intere): return (opin_c / intere) if intere != 0 else 0
    def set_revqoq(rev_c, rev_p): return ((rev_c - rev_p) / rev_p * 100) if rev_p != 0 else 0
    def set_opinqoq(opin_c, opin_p): return ((opin_c - opin_p) / opin_p * 100) if opin_p != 0 else 0
    def set_mkcap(price, share): return price * share
    def set_insolvflag(cf_oper, netin): return (cf_oper / netin) if netin != 0 else 0
    def set_divrat(div, netin): return (div / netin * 100) if netin > 0 else 0
    def set_z(c_asset, c_liab, asset, ret_earn, opin_c, equi, liab, rev_c):
        if asset == 0 or liab == 0: return 0
        return (1.2 * ((c_asset - c_liab) / asset) + 1.4 * (ret_earn / asset) +
                3.3 * (opin_c / asset) + 0.6 * (equi / liab) + 1.0 * (rev_c / asset))

    # [6] 숫자형 변환
    numeric_cols = ['revenue_curr', 'revenue_prev', 'op_income_curr', 'op_income_prev', 'net_income',
                    'assets', 'liabilities', 'equity', 'cur_assets', 'cur_liab', 'retained_earnings',
                    'interest', 'cf_oper', 'dividend', 'price', 'shares']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # [7] 지표 계산 적용
    df['oper_margin'] = df.apply(lambda r: set_opermarg(r['op_income_curr'], r['revenue_curr']), axis=1)
    df['liab_ratio'] = df.apply(lambda r: set_liabrat(r['liabilities'], r['equity']), axis=1)
    df['curr_ratio'] = df.apply(lambda r: set_currrat(r['cur_assets'], r['cur_liab']), axis=1)
    df['interest_coverage'] = df.apply(lambda r: set_interecover(r['op_income_curr'], r['interest']), axis=1)
    df['revenue_qoq'] = df.apply(lambda r: set_revqoq(r['revenue_curr'], r['revenue_prev']), axis=1)
    df['oper_income_qoq'] = df.apply(lambda r: set_opinqoq(r['op_income_curr'], r['op_income_prev']), axis=1)
    df['market_cap'] = df.apply(lambda r: set_mkcap(r['price'], r['shares']), axis=1)
    df['insolvency_flag'] = df.apply(lambda r: set_insolvflag(r['cf_oper'], r['net_income']), axis=1)
    df['div_ratio'] = df.apply(lambda r: set_divrat(r['dividend'], r['net_income']), axis=1)
    df['z_score'] = df.apply(lambda r: set_z(r['cur_assets'], r['cur_liab'], r['assets'], r['retained_earnings'],
                                           r['op_income_curr'], r['equity'], r['liabilities'], r['revenue_curr']), axis=1)

    # [8] 소수점 정리 및 저장
    float_cols = ['oper_margin', 'liab_ratio', 'curr_ratio', 'interest_coverage', 'revenue_qoq', 'oper_income_qoq', 'z_score', 'div_ratio']
    df[float_cols] = df[float_cols].round(2)

    df.to_csv("KOSPI_최종_분기별_계산완료.csv", index=False, encoding='utf-8-sig')
    print("✅ 섹터와 지표가 모두 포함된 최종 파일이 생성되었습니다!")
    print(df[['quarter', 'corp_name', 'sector', 'oper_margin', 'z_score']].head())

    # 1. 모든 숫자형(Float) 컬럼을 소수점 2자리로 반올림
    df = df.round(2)

    # 2. (선택사항) 특정 컬럼만 정밀하게 챙기고 싶다면?
    # 예를 들어 insolvency_flag와 div_ratio만 딱 2자리로 만들고 싶을 때
    # df['insolvency_flag'] = df['insolvency_flag'].round(2)
    # df['div_ratio'] = df['div_ratio'].round(2)

    # 3. 저장 (한글 깨짐 방지 옵션 포함)
    df.to_csv("KOSPI_2025_재무제표(컬럼영문).xlsx", index=False, encoding='utf-8-sig')

    print("✨ 모든 소수점이 2자리로 정리되었습니다! ㅋ")