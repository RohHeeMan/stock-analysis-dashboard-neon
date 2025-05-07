import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def compute_ratios(df_raw: pd.DataFrame, ticker: str) -> Dict[str, float]:
    """
    재무제표 raw DataFrame에서 주요 비율 계산:
    - 영업이익률(%) = 영업이익 / 매출액 * 100
    - ROE(%)       = 당기순이익 / 자본총계 * 100
    - 부채비율(총자산 대비 %) = 총부채 / (총부채 + 자본총계) * 100
    - 부채대자본비율(%)     = 총부채 / 자본총계 * 100
    - 지배주주 D/E 비율(%) = 총부채 / 지배주주지분 * 100
    """
    df = df_raw.copy()
    df['thstrm_amount'] = pd.to_numeric(df['thstrm_amount'], errors='coerce')

    # 태그 후보 (IFRS + DART 별도)
    LIABILITY_IDS = [
        'ifrs-full_Liabilities',
        'ifrs-full_CurrentLiabilities',
        'ifrs-full_NoncurrentLiabilities',
        'dart_CurrentLiabilities',
        'dart_NoncurrentLiabilities',
        'dart_Liabilities',
    ]
    EQUITY_IDS = [
        'ifrs-full_Equity',
        'ifrs-full_EquityAttributableToOwnersOfParent',
    ]

    # 계정명 기반 키 (한글 + 영어)
    DEBT_NM_KEYS = ['부채총계', '총부채', '부채', 'liabilities']
    EQUITY_NM_KEYS = ['자본총계', '총자본', '자본', 'equity']

    # Pivot by account_id and account_nm
    pivot_id = df.groupby('account_id')['thstrm_amount'].sum()
    pivot_nm = df.groupby('account_nm')['thstrm_amount'].sum()

    def get_metric(id_keys, nm_keys):
        # 1) ID 정확 매칭
        for ik in id_keys:
            if ik in pivot_id.index and pivot_id[ik] != 0:
                return float(pivot_id[ik])
        # 2) 이름 정확 매칭
        for nk in nm_keys:
            if nk in pivot_nm.index and pivot_nm[nk] != 0:
                return float(pivot_nm[nk])
        # 3) 이름 부분 매칭
        for nk in nm_keys:
            for acct, val in pivot_nm.items():
                if nk.lower() in acct.lower() and val != 0:
                    return float(val)
        return None

    # 총부채 집계
    liab_val = sum(get_metric([i], []) or 0 for i in LIABILITY_IDS)
    if liab_val == 0:
        fallback = get_metric([], DEBT_NM_KEYS)
        if fallback:
            liab_val = fallback

    # 전체 자본 집계
    eq_total = sum(get_metric([i], []) or 0 for i in EQUITY_IDS)
    if eq_total == 0:
        fallback_eq = get_metric([], EQUITY_NM_KEYS)
        if fallback_eq:
            eq_total = fallback_eq

    # 지배주주지분
    eq_parent = get_metric(['ifrs-full_EquityAttributableToOwnersOfParent'], ['지배기업 소유주지분']) or eq_total

    # 매출, 영업이익, 당기순이익
    sales_val = get_metric(['ifrs-full_Revenue'], ['매출액', '수익'])
    op_val    = get_metric(['dart_OperatingIncomeLoss', 'ifrs-full_OperatingProfitLoss'], ['영업이익', '영업손익'])
    net_val   = get_metric(['ifrs-full_ProfitLoss'], ['당기순이익', '순이익'])

    logger.debug(
        f"{ticker} ▶ sales={sales_val}, op={op_val}, net={net_val}, "
        f"debt={liab_val}, eq_total={eq_total}, eq_parent={eq_parent}"
    )

    # 비율 계산
    ratios = {
        '영업이익률(%)': (round(op_val / sales_val * 100, 2) if op_val and sales_val else None),
        'ROE(%)':       (round(net_val / eq_total * 100, 2) if net_val and eq_total else None),
        '부채비율(총자산 대비 %)': (round(liab_val / (liab_val + eq_total) * 100, 2) if (liab_val + eq_total) > 0 else None),
        '부채대자본비율(자본 대비 %)': (round(liab_val / eq_total * 100, 2) if eq_total > 0 else None),
        '지배주주 D/E 비율(%)': (round(liab_val / eq_parent * 100, 2) if eq_parent > 0 else None),
    }

    return ratios
