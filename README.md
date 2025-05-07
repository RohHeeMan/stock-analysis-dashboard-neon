# stock-analysis-dashboard-neon
파이선 재무제표 수집

# 📊 Stock Analysis Dashboard

이 프로젝트는 한국 주식의 재무제표 데이터를 DART API를 통해 수집하고, GPT로 요약 분석하며 ROE, 부채비율 등의 재무비율을 시각화하는 Streamlit 대시보드입니다.

버전 Python 3.13.3

## 구성

- `main.py`: DART API를 통해 재무제표 데이터 수집
- `app.py`: Streamlit 대시보드 실행
- `src/data_collection/dart_api.py`: DART API 호출
- `src/analysis/financial_summary.py`: GPT 요약 생성
- `src/analysis/ratios.py`: 재무비율 계산
- `components/`: UI 구성 요소

## 실행 방법

### 1. `.env` 파일에 API 키를 설정
### 2. `main.py` 실행하여 CSV 수집
### 3. `streamlit run app.py`로 대시보드 실행

## 자료 수집 처리 순서 방법
### 1. 명령을 실행하면, 아래 동작이 순서대로 수행됩니다:
### 1-1. corp_codes 테이블에서 종목 매핑 조회
### 1-2. 과거 5개년의 DART 재무제표(raw) 수집
### 1-3. raw_financials 테이블에 신규 항목만 증분 INSERT
### 1-4. summary_financials 테이블에 비율 요약 증분 INSERT
### 1-5. python main.py 로 실행

## 자료 처리(웹구동) 처리 순서 방법
### 2. 브라우저가 자동으로 열리면
### 2-1. http://localhost:8501 (또는 출력된 URL) 로 접속하여 대시보드를 사용합니다.
### 2-2. 사이드바에서 연도와 종목을 선택하면, DB에 증분 저장된 최신 데이터를 바로 확인할 수 있습니다.
### 2-3. streamlit run app.py 로 웹구동

** 이 두 단계만으로 “수집 → DB 저장 → 대시보드 표시”가 모두 자동으로 이루어집니다. 문제가 있으면 알려주세요! **

## 자동 설치
### pip install -r requirements.txt


## main.py
### DART에서 자동으로 corp_codes 테이블을 갱신하고, 그 매핑을 바탕으로 raw_financials와 summary_financials에 ### 신규 데이터만 삽입합니다.

## dart_api.py
### load_corp_codes() 가 최초 실행 시 한 번만 DART에서 내려받고 DB에 저장
### 그 후에는 COUNT(*)>0 체크로 곧바로 DB 조회로 넘어가서 초고속 매핑 반환
### init_corp_codes_table(): 테이블이 없으면 생성
### load_corp_codes():
### DART의 corpCode.xml ZIP을 받아 파싱
### 각 레코드를 업서트(upsert)
### 최종 corp_codes 전체를 리턴

### 이후 main.py 에서는 load_corp_codes() 호출만으로 언제든 최신 매핑을 DB에서 가져오므로, CSV 파일은 전혀 필요 없습니다.


### python main.py를 실행만 하면, 다음과 같은 일이 자동으로 일어납니다:

### 1. corp_codes 테이블 확인
###    비어 있으면 한 번만 전체 상장사 코드를 DART API에서 받아와 채우고
###    이미 데이터가 있으면 그대로 넘어갑니다.

### 2. 상장사 필터링
### 테이블에 있는 코드 중 실제 상장 종목(6자리 종목코드)만 추려냅니다.

### 3. 조회 대상 결정
### 첫 로딩이거나 PROCESS_ALL=true 설정 시 → 전체 상장사
### 그 외에는 → .env의 TARGET_TICKERS 리스트에 있는 종목만

### 5개년 재무제표 수집

### 각 회사·연도별로 DART API 호출

### raw_financials와 summary_financials 테이블에 ON CONFLICT DO NOTHING 옵션으로 중복 없이 저장

### 덕분에,
### 초기 한 번만 전체 상장사 코드를 DB에 채운 뒤에는
### 매번 원하는 시점에 python main.py 만 실행하면
### 신규 상장사가 있으면 자동으로 코드가 추가되고,
### 신규 보고서만 증분으로 수집되어 반영됩니다.

### 즉, 스크립트를 여러 번 돌려도 무방하며, 항상 최신 재무 데이터를 안전하게 가져올 수 있습니다!


## ==== main.py의 핵심 처리 흐름을 간략히 요약하면 다음과 같습니다: ====

### 네, 방금 드린 main.py와 dart_api.py 조합은 요청하신 모든 요건을 충족합니다:

### 환경·DB·로깅 초기화

### .env에서 DART_API_KEY, DATABASE_URL 로드

### logging 설정, ZoneInfo('Asia/Seoul')로 한국시간 사용

### 일일 호출 카운터 관리

### dart_state 테이블에서 오늘(date)의 used_calls 불러오기

### check_new_day()로 자정 지나면 자동 0 초기화

### 캐시 활용 + 변경 감지

### load_cached()로 dart_cache(corp_code, year)에서 report_code, fs_div, recs 조회

### CFS 캐시가 있으면 스킵, OFS만 있으면 CFS 재조회

### DART 메타데이터(list_reports_for_year) → 전체 재무제표(fetch_latest_for_year)

### fetch_latest_for_year() 내부에서

### 연간사업보고서(11011) + CFS 호출

### CFS 수집 없으면 연간사업보고서 + OFS 호출

### (CFS 수집 없을 경우만 OFS수집, status 코드 검사 포함 하여 하루 20000번 이상 돌아가지 않도록 처리)

### RAW upsert(부분 커밋)

### raw_financials에 10건 단위로 conn.begin() → tx.commit() 반복

### 지표 계산 & SUMMARY upsert

### compute_ratios()로 영업이익률·ROE 계산

### 총부채·총자본·지배주주지분 기준 부채비율 계산

### summary_financials에 ON CONFLICT … DO UPDATE

### 로깅

### 메타조회, 캐시 스킵/갱신, RAW 부분 커밋, SUMMARY 완료 등 단계별 logger.info()

### API 호출 최소화

### 캐시가 없거나 변경된 연도·종목만 fetch_latest_for_year() 호출 → 대다수 종목은 한 번만 호출

### 하루 최대 호출량 MAX_CALLS=15000로 안전 관리

### 따라서 “한 번 수집한 데이터는 스킵, 변경된 부분만 업데이트”에 최적화되어 있고, 연간사업보고서 CFS 우선→OFS ### fallback 로직도 정확히 구현되어 있습니다. 다른 점 없이 요청하신 구조대로 적용된 상태입니다.

### 사용 테이블
### SELECT * FROM corp_codes;

### select * from dart_state;

### select * from dart_cache;

### select * from raw_financials;

### select * from summary_financials;

### -- 회사 코드
### CREATE TABLE corp_codes (
###     corp_code character varying(20),
###     stock_code character varying(6),
###     corp_name text
### );
### 
### 
### -- 1) 원시 재무제표(raw) 테이블
### CREATE TABLE raw_financials (
###     id bigint,
###     corp_name text,
###     ticker character varying(6),
###     year integer,
###     report_code character varying(10),
###     fs_div character varying(10),
###     account_id text,
###     account_nm text,
###     thstrm_amount numeric(30,2),
###     frmtrm_amount numeric(30,2),
###     bfefrm_amount numeric(30,2),
###     created_at timestamp with time zone
### );
### 
### -- 2) 요약 재무제표(summary) 테이블
### CREATE TABLE summary_financials (
###     id bigint,
###     corp_name text,
###     ticker character varying(6),
###     year integer,
###     report_code character varying(10),
###     fs_div character varying(10),
###     operating_margin numeric(14,2),
###     roe numeric(14,2),
###     debt_ratio numeric(14,2),
###     created_at timestamp with time zone,
###     controlling_debt_ratio numeric(14,2)
### );
### 
### --3. DART 호출 결과 캐시 테이블
### CREATE TABLE dart_cache (
###   corp_code      TEXT       NOT NULL,
###   stock_code     VARCHAR(6) NOT NULL,
###   year           INTEGER    NOT NULL,
###   report_code    VARCHAR(10) NOT NULL,
###   fs_div         VARCHAR(3) NOT NULL,
###   recs           JSONB      NOT NULL,
###   last_updated   TIMESTAMPTZ DEFAULT NOW(),
###   PRIMARY KEY(corp_code, year, stock_code)
### );
### 
### --4. 일별 호출 횟수 관리 테이블
### CREATE TABLE IF NOT EXISTS dart_state (
###     date       DATE PRIMARY KEY,
###     used_calls INTEGER NOT NULL
### );
