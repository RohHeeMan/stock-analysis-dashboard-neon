import os
import io
import zipfile
import logging
import xml.etree.ElementTree as ET
import json

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Tuple

import requests
from sqlalchemy import create_engine, text

from src.utils.db import fetch_dataframe, execute_query
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

# ─── 환경 변수 및 DB 연결 ──────────────────────────────────────────
DART_API_KEY = os.getenv('DART_API_KEY')
DATABASE_URL = os.getenv('DATABASE_URL')
if not DART_API_KEY or not DATABASE_URL:
    raise RuntimeError("DART_API_KEY 또는 DATABASE_URL이 설정되지 않았습니다.")

engine = create_engine(DATABASE_URL)
kst    = ZoneInfo("Asia/Seoul")

# ─── DART OpenAPI 엔드포인트 & 우선순위 ───────────────────────────
CORP_CODE_URL       = 'https://opendart.fss.or.kr/api/corpCode.xml'
DART_LIST_ENDPOINT  = 'https://opendart.fss.or.kr/api/list.json'
DART_ENDPOINT       = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
REPORT_CODE         = '11011'         # 연간사업보고서 코드
FS_PRIORITY         = ['CFS', 'OFS']  # 연결 우선 → 개별
# 하루 최대 호출 수
MAX_CALLS = int(os.getenv('MAX_CALLS', 19000))

# 재무제표 종류, 공시 코드
#REPORT_CODE = '11011'         # 연간사업보고서
#FS_PRIORITY = ['CFS', 'OFS']  # 연결재무제표 우선 → 개별재무제표

# 하루 최대 호출 수(20,000번 이지만 안전을 위해서 19,000만 수집, 다음날 다시 수집하면 dart_state이용 처리 하니까 하루 호출수 감소)
# 정확한 호출수 계산되니 19,000까지만 돌려도 상관없음.
#MAX_CALLS = int(os.getenv('MAX_CALLS', 19000))

def init_today_counter():
    """
    스크립트 시작 시 오늘 날짜의 카운터 레코드를 생성합니다.단 가상컴퓨터(서버)시간으로 체크
    """
    today = datetime.now(kst).date().isoformat()
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO dart_state(date, used_calls) VALUES (:d, 0) "
            "ON CONFLICT(date) DO NOTHING"
        )

def fetch(url: str, **kwargs) -> requests.Response:
    """
    API 호출 전 used_calls < MAX_CALLS 확인 & +1,
    호출 실패 시 -1 롤백 처리 (원자적 관리)
    """
    # 날짜가 변경되었으면 오늘 레코드 초기화
    today = datetime.now(kst).date().isoformat()
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dart_state(date, used_calls)
            VALUES(:d, 0)
            ON CONFLICT(date) DO NOTHING
        """)
                     
    # 슬롯 확보
    with engine.begin() as conn:
        row = conn.execute(text(f"""
            UPDATE dart_state
               SET used_calls = used_calls + 1
             WHERE date = :d
               AND used_calls < {MAX_CALLS}
         RETURNING used_calls
        """), {"d": today}).fetchone()
        if row is None:
            raise RuntimeError(f"DART API 일일 호출 한도({MAX_CALLS}) 초과: date={today}")

    try:
        resp = requests.get(url, **kwargs)
        resp.raise_for_status()
        return resp
    except Exception:
        # 실패 시 롤백
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE dart_state SET used_calls = used_calls - 1 WHERE date = :d"
            ), {"d": today})
        raise


def init_corp_codes():
    """
    최초 1회만 DART 법인코드 ZIP을 다운로드하여
    corp_codes 테이블에 upsert 합니다.
    """
    count = fetch_dataframe("SELECT COUNT(*) FROM corp_codes").iloc[0, 0]
    if count > 0:
        logger.info("▷ corp_codes 이미 초기화됨 → 스킵")
        return
    logger.info("▷ corp_codes 테이블 초기화 시작")
    resp = fetch(CORP_CODE_URL, params={'crtfc_key': DART_API_KEY}, timeout=30)
    bio = io.BytesIO(resp.content)
    with zipfile.ZipFile(bio) as zf:
        xml_bytes = zf.read(zf.namelist()[0])
    root = ET.fromstring(xml_bytes)
    sql = """
        INSERT INTO corp_codes(corp_code, stock_code, corp_name)
        VALUES (:corp, :stock, :name)
        ON CONFLICT (corp_code) DO NOTHING
    """
    for elem in root.findall('list'):
        execute_query(sql, {
            'corp':  elem.findtext('corp_code', ''),
            'stock': elem.findtext('stock_code', '').zfill(6),
            'name':  elem.findtext('corp_name', ''),
        })
    logger.info("▷ corp_codes 테이블 초기화 완료")


def fetch_all_corp_codes() -> List[Dict[str, str]]:
    """
    DB의 corp_codes 테이블에서 법인코드 목록을 조회합니다.
    (비어 있거나 잘못된 stock_code는 필터링)
    """
    df = fetch_dataframe("SELECT corp_code, stock_code, corp_name FROM corp_codes")
    df['stock_code'] = df['stock_code'].fillna('').astype(str).str.strip()
    df = df[(df['stock_code'].str.upper() != 'EMPTY') & (df['stock_code'] != '000000')]
    df['stock_code'] = df['stock_code'].str.extract(r'(\d+)')[0].str.zfill(6)
    return df.to_dict(orient='records')


def list_reports_for_year(
    corp_code: str,
    year: int,
    last_reprt_at: str = "",
    reprt_code: str = "",
    fs_div: str = ""
) -> List[Dict]:
    """
    DART list.json 으로 공시 메타를 조회합니다.
    """
    metas = []
    page = 1
    while True:
        resp = fetch(
            DART_LIST_ENDPOINT,
            params={
                'crtfc_key':     DART_API_KEY,
                'corp_code':     corp_code,
                'bgn_de':        f"{year}0101",
                'end_de':        f"{year}1231",
                'last_reprt_at': last_reprt_at,
                'corp_cls':      'Y',
                'reprt_code':    reprt_code,
                'fs_div':        fs_div,
                'page_no':       page,
                'page_count':    100,
                'sort':          'date',
                'sort_mthd':     'D',
            }, timeout=15
        )
        data = resp.json()
        items = data.get('list', [])
        if not items:
            break
        for it in items:
            metas.append({
                'report_nm':     it.get('report_nm'),
                'rprt_code':     it.get('reprt_code'),
                'fs_div':        it.get('fs_div'),
                'rcept_no':      it.get('rcept_no'),
                'rcept_dt':      it.get('rcept_dt'),
                'flr_nm':        it.get('flr_nm'),
                'rm':            it.get('rm'),
                'last_reprt_at': it.get('last_reprt_at'),
            })
        total_page = int(data.get('total_page', 1))
        if page >= total_page:
            break
        page += 1
    return metas


def save_cache(
    corp_code: str,
    stock_code: str,
    year: int,
    recs: List[Dict],
    report_code: str,
    fs_div: str
):
    """
    dart_cache 테이블에 JSONB 형태로 재무제표를 upsert 합니다.
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
            'c': corp_code,
            's': stock_code,
            'y': year,
            'r': report_code,
            'f': fs_div,
            'j': payload
        })


def fetch_latest_for_year(corp_code: str, stock_code: str, year: int) -> Tuple[List[Dict], str, str]:
    """
    1) CFS 캐시 조회 후 존재 시 스킵
    2) CFS→OFS 순으로 재무제표 API 호출(fetch 사용 → 카운트 증가)
    """
    # 1) 캐시 조회 (corp_code, stock_code, year 로 통일)
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT report_code, fs_div, recs
              FROM dart_cache
             WHERE corp_code = :c
               AND stock_code = :s
               AND year = :y
        """), {'c': corp_code, 's': stock_code, 'y': year}).fetchone()

    if row and row.fs_div == 'CFS':
        logger.info("    ✓ CFS 캐시 존재 → 스킵")
        recs = row.recs if isinstance(row.recs, (list, dict)) else json.loads(row.recs)
        return recs, row.report_code, row.fs_div

    # 2) API 호출
    for fs_div in FS_PRIORITY:
        resp = fetch(
            DART_ENDPOINT,
            params={
                'crtfc_key':  DART_API_KEY,
                'corp_code':  corp_code,
                'bsns_year':  year,
                'reprt_code': REPORT_CODE,
                'fs_div':     fs_div,
            },
            timeout=15
        )
        data = resp.json()
        items = data.get('list') or []
        if data.get('status') in ('000', '013') and items:
            recs = [{
                'account_id':    it.get('account_id', ''),
                'account_nm':    it.get('account_nm', ''),
                'thstrm_amount': it.get('thstrm_amount', ''),
                'frmtrm_amount': it.get('frmtrm_amount', ''),
                'bfefrm_amount': it.get('bfefrm_amount', ''),
            } for it in items]

            # 3) 캐시 저장 (동일한 stock_code 사용)
            save_cache(corp_code, stock_code, year, recs, REPORT_CODE, fs_div)
            return recs, REPORT_CODE, fs_div

    logger.warning(f"{corp_code} {year}: 연간사업보고서 CFS/OFS 모두 없음")
    return [], '', ''
