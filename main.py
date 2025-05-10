import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import logging
import pandas as pd
import requests
from sqlalchemy import create_engine, text
import sys

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
    level=logging.INFO  # 로그 레벨을 INFO로 설정
)
logger = logging.getLogger(__name__)  # 로거 객체 생성
logger.setLevel(logging.DEBUG)  # 디버그 레벨의 로그도 출력하도록 설정

# --- 2. DB 연결 ---------------------------------------------------------
engine = create_engine(DATABASE_URL)  # SQLAlchemy 엔진을 이용한 DB 연결
kst = ZoneInfo("Asia/Seoul")  # 한국 표준시 설정

# --- 3. 한글명 매핑 -----------------------------------------------------
# 보고서 종류와 재무제표 종류를 한글명으로 매핑
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
    이미 캐시된 데이터가 있는지 확인하고, 있으면 반환합니다.
    """
    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT stock_code, report_code, fs_div, recs
              FROM dart_cache
             WHERE corp_code = :c AND year = :y
        """), {"c": corp_code, "y": year}).fetchone()

    if not r:  # 캐시가 없으면 None을 반환
        return None

    recs = r.recs if isinstance(r.recs, (list, dict)) else json.loads(r.recs)
    return {
        "stock_code": r.stock_code,
        "report_code": r.report_code,
        "fs_div": r.fs_div,
        "recs": recs
    }

def save_cache(corp_code, corp_name, stock_code, year,
               recs, report_code, fs_div):
    """
    dart_cache에 데이터를 upsert 합니다.
    캐시를 저장 또는 업데이트하여 추후 동일 데이터를 중복 조회하지 않도록 합니다.
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

# --- 5. Corp Codes 로딩 및 DB 저장 함수 ---------------------------
def load_corp_codes_from_csv(file_path="corp_codes.csv"):
    """
    corp_codes.csv 파일이 없으면 DART에서 XML 다운로드 후,
    corp_code, stock_code, corp_name을 추출해 저장하고 DataFrame 반환
    """
    if not os.path.exists(file_path):
        logger.info("▷ corp_codes.csv 파일이 없습니다. DART로부터 다운로드를 시작합니다.")

        url = "https://opendart.fss.or.kr/api/corpCode.xml"
        api_key = os.getenv("DART_API_KEY")
        if not api_key:
            raise RuntimeError("환경변수 DART_API_KEY가 설정되지 않았습니다.")

        res = requests.get(url, params={"crtfc_key": api_key})
        if res.status_code != 200:
            raise RuntimeError("DART API에서 기업코드 파일을 가져오지 못했습니다.")

        # XML은 압축파일로 제공됨 → 저장하고 압축 해제
        zip_path = "corp_code.zip"
        with open(zip_path, "wb") as f:
            f.write(res.content)

        import zipfile
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(".")

        # XML 파싱
        import xml.etree.ElementTree as ET
        tree = ET.parse("CORPCODE.xml")
        root = tree.getroot()

        records = []
        for child in root.findall("list"):
            corp_code  = child.findtext("corp_code")   # 기업 고유코드
            stock_code = child.findtext("stock_code")  # 종목코드
            corp_name  = child.findtext("corp_name")   # 회사명

            # stock_code가 존재할 때만 수집
            if stock_code and stock_code.strip():
                records.append({
                    "corp_code": corp_code.zfill(8),       # 반드시 8자리
                    "stock_code": stock_code.zfill(6),     # 6자리 종목코드
                    "corp_name": corp_name
                })

        # DataFrame 저장
        df = pd.DataFrame(records)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        logger.info(f"▷ DART로부터 corp_codes.csv 생성 완료 ({len(df)}건)")

    # CSV 로드 (문자열로 강제 변환하여 앞자리 0 유지)
    df = pd.read_csv(file_path, dtype={"corp_code": str, "stock_code": str})
    df["corp_code"] = df["corp_code"].astype(str).str.zfill(8)
    df["stock_code"] = df["stock_code"].astype(str).str.zfill(6)
    return df


def insert_corp_codes_to_db(df):
    """
    DataFrame을 DB에 삽입합니다.
    corp_code가 기본키 또는 unique로 설정되어 있어야 ON CONFLICT가 작동합니다.
    """
    insert_sql = """
        INSERT INTO corp_codes (corp_code, stock_code, corp_name)
        VALUES (:corp_code, :stock_code, :corp_name)
        ON CONFLICT(corp_code) DO NOTHING;
    """
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text(insert_sql), {
                "corp_code": row["corp_code"],
                "stock_code": row["stock_code"],
                "corp_name": row["corp_name"]
            })
    logger.info("▷ corp_codes DB에 저장 완료")

# --- 6. 메인 로직 --------------------------------------------------------
def main():
    start = datetime.now(kst)
    logger.info(f"[시작] 재무 데이터 수집 - {start.isoformat()}")

    # DB에 corp_codes가 없다면, corp_codes.csv 파일을 다운로드하여 DB에 저장
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM corp_codes")).fetchone()
        if result[0] == 0:
            logger.info("▷ DB에 corp_codes 테이블이 비어있습니다. csv 파일에서 로드하여 저장합니다.")
            corp_codes_df = load_corp_codes_from_csv()  # 필요에 따라 파일 경로 수정
            insert_corp_codes_to_db(corp_codes_df)
    
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

    # 주식 데이터를 수집
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

            # 분석 지표 계산
            logger.info("▷ 재무 분석 지표 계산 시작")
            
            # SUMMARY upsert
            # recs 데이터를 pandas DataFrame으로 변환
            df_r = pd.DataFrame(recs)
            df_r["amount"] = pd.to_numeric(df_r["thstrm_amount"], errors="coerce")

            # 부채 총액 계산 (Liabilities 관련 계정)
            liab = df_r[df_r["account_nm"].str.contains(r"부채|Liabilities", na=False)]["amount"].sum()

            # 자본 총액 계산 (Equity 관련 계정)
            eq = df_r[(df_r["account_id"] == "ifrs-full_Equity") |
                      (df_r["account_nm"].str.contains(r"자본총계|Equity", na=False))]["amount"].sum()

            # 지배기업 자본 총액 계산 (지배기업 관련 계정, 없으면 기본 자본 총액으로 대체)
            eqp = df_r[df_r["account_nm"].str.contains(r"지배기업", na=False)]["amount"].sum() or eq

            # 재무 비율 계산 (영업이익률, ROE 등)
            ops = compute_ratios(df_r, tkr)

            # 부채비율 계산 (자본 대비 부채 비율)
            dr = (liab / (liab + eq) * 100) if (liab + eq) > 0 else None

            # 지배기업 부채비율 계산 (지배기업 자본 대비 부채 비율)
            
            cr = (liab / eqp * 100) if eqp > 0 else None
            # numpy 타입일 경우 float 변환
            if dr is not None: dr = float(dr)
            if cr is not None: cr = float(cr)
            om = ops.get("operating_margin")
            roe = ops.get("roe")
            
            # SUMMARY upsert (update or insert)
            update_sql = text("""
                UPDATE summary_financials
                SET operating_margin = :om,
                    roe = :roe,
                    debt_ratio = :dr,
                    controlling_debt_ratio = :cr
                WHERE ticker = :tk AND year = :yr AND report_code = :rp AND fs_div = :fd
            """)
            insert_sql = text("""
                INSERT INTO summary_financials(
                corp_name, ticker, year, report_code, fs_div,
                operating_margin, roe, debt_ratio, controlling_debt_ratio, created_at
                ) VALUES (
                :cn, :tk, :yr, :rp, :fd,
                :om, :roe, :dr, :cr, NOW()
                )
            """)
            with engine.begin() as conn:
                # 먼저 업데이트 시도
                result = conn.execute(update_sql, {
                    "om": om, "roe": roe, "dr": dr, "cr": cr,
                    "tk": tkr, "yr": yr, "rp": rpt, "fd": fdiv
                })
                # 행이 없으면 새로 삽입
                if result.rowcount == 0:
                    conn.execute(insert_sql, {
                        "cn": name, "tk": tkr, "yr": yr,
                        "rp": rpt, "fd": fdiv,
                        "om": om, "roe": roe, "dr": dr, "cr": cr
                    })

    end = datetime.now(kst)
    logger.info(f"[완료] 재무 데이터 수집 - {end.isoformat()} (소요 시간: {end - start})")

# main 함수 실행
if __name__ == "__main__":
    try:
        main()
    except Exception:
        logging.exception("스크립트 수행 중 에러 발생")
        sys.exit(0)   # 예외가 나도 exit code 0으로 종료
