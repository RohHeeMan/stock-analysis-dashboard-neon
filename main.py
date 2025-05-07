# src/main.py

import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import logging
import pandas as pd
import requests
from sqlalchemy import create_engine, text

from src.data_collection.dart_api import (
    fetch_all_corp_codes,
    fetch_latest_for_year,
    REPORT_CODE,
    FS_PRIORITY
)
from src.utils.db import execute_query
from src.analysis.ratios import compute_ratios

# --- 1. 환경 변수 및 로깅 설정 -----------------------------------------
load_dotenv(override=True)
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise RuntimeError("환경변수 DATABASE_URL이 설정되지 않았습니다.")

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# --- 2. DB 연결 ---------------------------------------------------------
engine = create_engine(DATABASE_URL)
kst    = ZoneInfo("Asia/Seoul")

# --- 3. 한글명 매핑 -----------------------------------------------------
RPT_MAP = {
    "11011": "연간사업보고서",
    "11014": "3분기보고서",
    "11012": "반기보고서",
    "11013": "1분기보고서"
}
FS_MAP = {
    "CFS": "연결재무제표",
    "OFS": "개별재무제표"
}

# --- 4. 캐시 조회/저장 함수 -----------------------------------------------
def load_cached(corp_code, year):
    """
    dart_cache에서 (corp_code, year)의 캐시를 조회합니다.
    """
    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT stock_code, report_code, fs_div, recs
              FROM dart_cache
             WHERE corp_code = :c AND year = :y
        """), {"c": corp_code, "y": year}).fetchone()

    if not r:
        return None

    recs = r.recs if isinstance(r.recs, (list, dict)) else json.loads(r.recs)
    return {
        "stock_code":  r.stock_code,
        "report_code": r.report_code,
        "fs_div":      r.fs_div,
        "recs":        recs
    }

def save_cache(corp_code, corp_name, stock_code, year,
               recs, report_code, fs_div):
    """
    dart_cache에 데이터를 upsert 합니다.
    """
    payload = json.dumps(recs, ensure_ascii=False)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dart_cache(
              corp_code, stock_code, year,
              report_code, fs_div, recs
            ) VALUES (
              :c, :s, :y,
              :r, :f, :j
            )
            ON CONFLICT(corp_code, year, stock_code)
            DO UPDATE SET
              report_code  = EXCLUDED.report_code,
              fs_div        = EXCLUDED.fs_div,
              recs          = EXCLUDED.recs,
              last_updated  = NOW()
        """), {
            "c": corp_code,
            "s": stock_code,
            "y": year,
            "r": report_code,
            "f": fs_div,
            "j": payload
        })

    fs_name  = FS_MAP.get(fs_div, fs_div)
    rpt_name = RPT_MAP.get(report_code, report_code)
    logger.info(
        f"▷ 캐시 저장/업데이트: {corp_name}({stock_code})-{year} "
        f"[{fs_name}, {rpt_name}]"
    )

# --- 5. 메인 로직 --------------------------------------------------------
def main():
    start = datetime.now(kst)
    logger.info(f"[시작] 재무 데이터 수집 - {start.isoformat()}")

    # 전체 corp_code 목록 조회
    codes = fetch_all_corp_codes()
    df = pd.DataFrame(codes)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)

    # TARGET_TICKERS 환경변수 필터링 (없으면 전체)
    targets = [t.strip() for t in os.getenv("TARGET_TICKERS","").split(",") if t.strip()]
    if targets:
        df = df[df["stock_code"].isin(targets)]

    mapping = df.set_index("stock_code")["corp_code"].to_dict()
    names   = df.set_index("stock_code")["corp_name"].to_dict()
    tickers = sorted(mapping.keys())
    years   = list(range(datetime.now(kst).year - 1,
                         datetime.now(kst).year - 6, -1))

    for idx, tkr in enumerate(tickers, start=1):
        corp = mapping[tkr]
        name = names[tkr]

        for yr in years:
            logger.info(f"=== [{idx}/{len(tickers)}] {tkr} ({name}) ===")
            logger.info(f" ▶ {tkr} | 사업연도 {yr} ({name})")

            # 캐시 확인
            cached = load_cached(corp, yr)
            if cached and cached["fs_div"] == "CFS":
                logger.info("    ✓ CFS 캐시 존재 → 스킵")
                continue

            # DART API 호출
            recs, rpt, fdiv = fetch_latest_for_year(corp, tkr, yr)

            # 결과 헤더: report_code, fs_div 한글명
            rpt_name = RPT_MAP.get(rpt, rpt)
            fs_name  = FS_MAP.get(fdiv, fdiv)
            logger.info(f"    ▶ [{rpt_name}, {fs_name}] 조회 완료")

            if not recs:
                logger.warning("    ■ 공시 없음")
                continue

            # 캐시 저장
            save_cache(corp, name, tkr, yr, recs, rpt, fdiv)

            # RAW upsert
            raw_sql = """
                INSERT INTO raw_financials(
                  corp_name, ticker, year, report_code, fs_div,
                  account_id, account_nm,
                  thstrm_amount, frmtrm_amount, bfefrm_amount,
                  created_at
                ) VALUES (
                  :cn, :tk, :yr, :rp, :fd,
                  :aid, :anm,
                  :ta, :fa, :ba,
                  NOW()
                )
                ON CONFLICT(ticker, year, report_code, fs_div, account_id)
                DO NOTHING;
            """
            cnt = 0
            with engine.begin() as conn:
                for r in recs:
                    conn.execute(text(raw_sql), {
                        "cn": name, "tk": tkr, "yr": yr,
                        "rp": rpt, "fd": fdiv,
                        "aid": r["account_id"],
                        "anm": r["account_nm"],
                        "ta":  r.get("thstrm_amount") or None,
                        "fa":  r.get("frmtrm_amount") or None,
                        "ba":  r.get("bfefrm_amount") or None
                    })
                    cnt += 1
            logger.info(f"    ✓ RAW upsert 완료 ({cnt}건)")

            # SUMMARY upsert
            df_r = pd.DataFrame(recs)
            df_r["amount"] = pd.to_numeric(df_r["thstrm_amount"], errors="coerce")
            liab = df_r[df_r["account_nm"].str.contains(r"부채|Liabilities", na=False)]["amount"].sum()
            eq   = df_r[(df_r["account_id"] == "ifrs-full_Equity") |
                        (df_r["account_nm"].str.contains(r"자본총계|Equity", na=False))]["amount"].sum()
            eqp  = df_r[df_r["account_nm"].str.contains(r"지배기업", na=False)]["amount"].sum() or eq
            ops  = compute_ratios(df_r, tkr)
            dr   = (liab / (liab + eq) * 100) if (liab + eq) > 0 else None
            cr   = (liab / eqp * 100)      if eqp > 0      else None

            summary_sql = """
                INSERT INTO summary_financials(
                  corp_name, ticker, year, report_code, fs_div,
                  operating_margin, roe, debt_ratio,
                  controlling_debt_ratio, created_at
                ) VALUES (
                  :cn, :tk, :yr, :rp, :fd,
                  :om, :roe, :dr,
                  :cr, NOW()
                )
                ON CONFLICT(ticker, year, report_code, fs_div)
                DO NOTHING;
            """
            execute_query(summary_sql, {
                "cn":  name, "tk": tkr, "yr": yr,
                "rp":  rpt, "fd": fdiv,
                "om":  float(ops.get("영업이익률(%)") or 0),
                "roe": float(ops.get("ROE(%)") or 0),
                "dr":  dr,
                "cr":  cr
            })
            logger.info("    ✓ SUMMARY upsert 완료\n")

    elapsed = datetime.now(kst) - start
    logger.info(f"[완료] 전체 소요 시간: {elapsed}")

if __name__ == "__main__":
    main()
