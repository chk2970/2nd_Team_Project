import pandas as pd

kospi  = pd.read_excel('KOSPI_우선주_리츠_전처리.xlsx',  dtype={'ticker': str})
kosdaq = pd.read_excel('KOSDAQ_우선주_리츠_전처리.xlsx', dtype={'ticker': str})

# ── 1. 전체 결측치 현황 ────────────────────────────────────────────────────
def missing_summary(df, label):
    missing     = df.isnull().sum()
    missing_pct = (missing / len(df) * 100).round(2)
    result = pd.DataFrame({
        '결측수': missing,
        '결측률(%)': missing_pct
    }).query('결측수 > 0').sort_values('결측률(%)', ascending=False)

    print(f"\n{'='*52}")
    print(f"  [{label}]  총 {len(df)}행 / 결측 있는 컬럼 {len(result)}개")
    print(f"{'='*52}")
    if result.empty:
        print("  결측치 없음 ✓")
    else:
        print(result.to_string())
    return result

kospi_miss  = missing_summary(kospi,  'KOSPI')
kosdaq_miss = missing_summary(kosdaq, 'KOSDAQ')

# ── 2. 핵심 컬럼 결측 종목 추출 ───────────────────────────────────────────
CRITICAL = ['price', 'shares', 'revenue_curr', 'op_income_curr',
            'net_income', 'assets', 'equity']

print("\n\n=== 핵심 컬럼 결측 종목 상세 ===")
for label, df in [('KOSPI', kospi), ('KOSDAQ', kosdaq)]:
    mask = df[CRITICAL].isnull().any(axis=1)
    bad  = df[mask][['quarter', 'ticker', 'corp_name'] + CRITICAL]
    print(f"\n[{label}]  {mask.sum()}행 / {df[mask]['ticker'].nunique()}개 종목")
    if not bad.empty:
        print(bad.to_string(index=False))

# ── 3. 분기별 결측 패턴 (특정 분기만 결측인지) ────────────────────────────
print("\n\n=== 분기별 결측 패턴 ===")
for label, df in [('KOSPI', kospi), ('KOSDAQ', kosdaq)]:
    print(f"\n[{label}]")
    for col in df.columns:
        null_by_q = df[df[col].isnull()].groupby('quarter')['ticker'].count()
        if null_by_q.sum() > 0:
            print(f"  {col:25s}: {null_by_q.to_dict()}")

# ── 4. 결측치 원인 분류 ────────────────────────────────────────────────────
print("\n\n=== 결측 원인 추정 ===")
for label, df in [('KOSPI', kospi), ('KOSDAQ', kosdaq)]:
    print(f"\n[{label}]")

    # ① 무배당 기업 (dividend=0인데 div_yield 결측)
    no_div = df[(df['dividend'] == 0) & df['div_yield'].isnull()]
    print(f"  무배당(dividend=0) + div_yield 결측 : {len(no_div)}행")

    # ② 자본잠식 의심 (equity <= 0)
    insol = df[df['equity'] <= 0] if 'equity' in df.columns else pd.DataFrame()
    print(f"  자본잠식 의심 (equity ≤ 0)          : {len(insol)}행")

    # ③ price 결측 (신규상장 등)
    no_price = df[df['price'].isnull()]
    print(f"  price 결측                          : {len(no_price)}행")
    if not no_price.empty:
        print(f"    → {no_price['corp_name'].unique()[:10].tolist()}")