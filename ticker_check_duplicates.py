import pandas as pd

kospi  = pd.read_excel('KOSPI_우선주_리츠_전처리.xlsx',  dtype={'ticker': str})
kosdaq = pd.read_excel('KOSDAQ_우선주_리츠_전처리.xlsx', dtype={'ticker': str})

# ── 1. 파일 내부 중복 (같은 ticker가 같은 분기에 2행 이상) ──────────────
print("=== 파일 내부 중복 (ticker × quarter) ===")
for label, df in [('KOSPI', kospi), ('KOSDAQ', kosdaq)]:
    dup = df[df.duplicated(['ticker', 'quarter'], keep=False)]
    if dup.empty:
        print(f"  [{label}] 없음 ✓")
    else:
        print(f"  [{label}] {len(dup)}행 중복 발견 ↓")
        print(dup[['quarter', 'ticker', 'corp_name']].sort_values(['ticker','quarter']).to_string())

# ── 2. KOSPI ↔ KOSDAQ 시장 간 중복 ────────────────────────────────────────
print("\n=== 시장 간 중복 ticker ===")
kospi_tickers  = set(kospi['ticker'].unique())
kosdaq_tickers = set(kosdaq['ticker'].unique())
cross_dup      = kospi_tickers & kosdaq_tickers

if not cross_dup:
    print("  없음 ✓")
else:
    print(f"  {len(cross_dup)}개 발견:")
    rows = []
    for t in sorted(cross_dup):
        kp_name = kospi[kospi['ticker'] == t]['corp_name'].iloc[0]
        kd_name = kosdaq[kosdaq['ticker'] == t]['corp_name'].iloc[0]
        rows.append({'ticker': t, 'KOSPI_corp': kp_name, 'KOSDAQ_corp': kd_name})
    print(pd.DataFrame(rows).to_string(index=False))

# ── 3. 우선주 패턴 확인 (끝자리 5 또는 7) ─────────────────────────────────
print("\n=== 우선주 의심 ticker (끝자리 5 or 7) ===")
for label, df in [('KOSPI', kospi), ('KOSDAQ', kosdaq)]:
    pref = df[df['ticker'].str[-1].isin(['5', '7'])][['ticker','corp_name']].drop_duplicates()
    print(f"  [{label}] {len(pref)}개: {pref['ticker'].tolist()[:10]}{'...' if len(pref)>10 else ''}")