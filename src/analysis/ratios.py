# src/analysis/ratios.py

import pandas as pd
import logging
from typing import Dict

logger = logging.getLogger(__name__)

def compute_ratios(df_raw: pd.DataFrame, ticker: str) -> Dict[str, float]:
    """
    재무제표 raw DataFrame에서 주요 비율 계산:
      - operating_margin    : 영업이익률 (%) = 영업이익 / 매출액 * 100
      - roe                 : ROE (%)      = 당기순이익 / 자본총계 * 100
      - debt_ratio          : 부채비율 (%)   = 총부채 / (총부채 + 자본총계) * 100
      - controlling_debt_ratio : 지배주주 D/E (%) = 총부채 / 지배주주지분 * 100
    """

    df = df_raw.copy()
    df['amount'] = pd.to_numeric(df['thstrm_amount'], errors='coerce')

    # 계정 코드별 합계
    pivot_id = df.groupby('account_id')['amount'].sum()
    pivot_nm = df.groupby('account_nm')['amount'].sum()

    def get_metric(id_keys, nm_keys):
        # 우선 account_id
        for ik in id_keys:
            if ik in pivot_id and pivot_id[ik] != 0:
                return float(pivot_id[ik])
        # 다음 account_nm (완전일치 → 부분일치)
        for nk in nm_keys:
            if nk in pivot_nm and pivot_nm[nk] != 0:
                return float(pivot_nm[nk])
        for nk in nm_keys:
            for acct, val in pivot_nm.items():
                if nk.lower() in acct.lower() and val != 0:
                    return float(val)
        return 0.0  # 없으면 0으로 내려보냄

    # key lists
    LIABILITY_IDS = [
        'ifrs-full_Liabilities',
        'ifrs-full_CurrentLiabilities',
        'ifrs-full_NoncurrentLiabilities',
        'dart_Liabilities',
    ]
    EQUITY_IDS = [
        'ifrs-full_Equity',
        'ifrs-full_EquityAttributableToOwnersOfParent',
    ]

    # 값 추출
    sales_val = get_metric(['ifrs-full_Revenue'], ['매출액', '수익'])
    op_val    = get_metric(['ifrs-full_OperatingProfitLoss', 'dart_OperatingIncomeLoss'], ['영업이익'])
    net_val   = get_metric(['ifrs-full_ProfitLoss'], ['당기순이익', '순이익'])
    debt_val  = sum(get_metric([l], []) for l in LIABILITY_IDS)
    eq_total  = sum(get_metric([e], []) for e in EQUITY_IDS)
    # 지배주주지분: 만약 별도 항목 없으면 전체 자본 사용
    eq_parent = get_metric(['ifrs-full_EquityAttributableToOwnersOfParent'], []) or eq_total

    # 비율 계산
    operating_margin       = round(op_val  / sales_val * 100, 2) if sales_val else None
    roe                    = round(net_val / eq_total * 100,     2) if eq_total else None
    debt_ratio             = round(debt_val / (debt_val + eq_total) * 100, 2) if (debt_val + eq_total) else None
    controlling_debt_ratio = round(debt_val / eq_parent * 100, 2)             if eq_parent else None

    logger.debug(
        f"{ticker} ratios → OM:{operating_margin}, ROE:{roe}, "
        f"DR:{debt_ratio}, C-DR:{controlling_debt_ratio}"
    )

    return {
        "operating_margin":       operating_margin,
        "roe":                    roe,
        "debt_ratio":             debt_ratio,
        "controlling_debt_ratio": controlling_debt_ratio
    }
