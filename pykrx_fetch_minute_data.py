import argparse
import logging
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

from pykrx import stock

from database.db_manager_upper import DatabaseManager
from utils.date_utils import DateUtils

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_minute_data_by_pykrx(ticker: str, date_str: str) -> List[Dict[str, Any]]:
    """pykrx를 사용하여 특정일의 전체 분봉 데이터를 가져옵니다."""
    try:
        # pykrx는 YYYYMMDD 형식의 문자열을 사용합니다.
        # get_market_ohlcv는 fromdate, todate가 필수입니다.
        df = stock.get_market_ohlcv(fromdate=date_str, todate=date_str, ticker=ticker, interval='m')
        if df.empty:
            return []

        # 데이터 포맷 변환
        formatted_data = []
        for index, row in df.iterrows():
            formatted_data.append({
                'datetime': index.to_pydatetime(), # Timestamp를 datetime 객체로 변환
                'price': int(row['종가'])
            })
        return formatted_data
    except Exception as e:
        # 데이터가 없는 경우 pykrx에서 예외가 발생할 수 있습니다.
        logging.warning(f"pykrx 데이터 조회 중 오류 또는 데이터 없음 (ticker: {ticker}, date: {date_str}): {e}")
        return []

def process_stock(stock_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    """단일 종목에 대해 D+0 ~ D+7 분봉 데이터를 수집하고 DB 저장 형태로 가공합니다."""
    ticker = stock_info['ticker']
    name = stock_info['name']
    high_rise_date = stock_info['date']
    trade_session_id = stock_info['id']

    all_days_data = []
    # D+0부터 D+7까지 반복
    for i in range(8):
        fetch_date = DateUtils.get_target_date(high_rise_date, later=i)
        if not fetch_date:
            logging.warning(f"{name}({ticker}): {high_rise_date}의 D+{i} 영업일을 계산할 수 없어 건너뜁니다.")
            continue

        fetch_date_str = fetch_date.strftime("%Y%m%d")
        logging.info(f"{name}({ticker}): D+{i} ({fetch_date.strftime('%Y-%m-%d')}) 분봉 데이터 수집 시도...")
        
        time.sleep(0.5) # pykrx API 요청 간 지연
        minute_data = fetch_minute_data_by_pykrx(ticker, fetch_date_str)

        if not minute_data:
            logging.warning(f"{name}({ticker}): {fetch_date_str} 분봉 데이터가 없습니다.")
            continue

        # 데이터베이스에 저장할 형태로 가공
        for data_point in minute_data:
            all_days_data.append({
                'trade_session_id': trade_session_id,
                'high_rise_date': high_rise_date,
                'ticker': ticker,
                'name': name,
                'datetime': data_point['datetime'],
                'price': data_point['price']
            })
    
    if all_days_data:
        logging.info(f"{name}({ticker}): 총 {len(all_days_data)}건의 분봉 데이터 수집 완료.")
    
    return all_days_data

def fetch_and_save_minute_data(start_date_str: str, end_date_str: str):
    """지정된 기간 동안의 급등주에 대해 D+0일의 분봉 데이터를 병렬로 가져와 저장합니다."""
    all_minute_data = []

    # 날짜 문자열을 datetime 객체로 변환
    try:
        start_date = datetime.strptime(start_date_str, "%Y%m%d").date()
        end_date = datetime.strptime(end_date_str, "%Y%m%d").date()
    except ValueError:
        logging.error("날짜 형식이 잘못되었습니다. YYYYMMDD 형식으로 입력해주세요.")
        return
    
    with DatabaseManager() as db:
        stocks_to_process = db.get_selected_pykrx_upper_stocks_by_date_range(start_date, end_date)

    if not stocks_to_process:
        logging.info("지정된 기간에 해당하는 급등주 데이터가 없습니다.")
        return

    logging.info(f"총 {len(stocks_to_process)}개 종목에 대해 분봉 데이터 수집을 시작합니다.")

    with ThreadPoolExecutor(max_workers=5) as executor: # pykrx는 동시 요청에 민감할 수 있어 worker 수를 줄임
        future_to_stock = {executor.submit(process_stock, stock): stock for stock in stocks_to_process}
        for future in as_completed(future_to_stock):
            stock = future_to_stock[future]
            try:
                data = future.result()
                if data:
                    all_minute_data.extend(data)
            except Exception as exc:
                logging.error(f"{stock['name']}({stock['ticker']}) 처리 중 오류 발생: {exc}")

    if all_minute_data:
        logging.info(f"총 {len(all_minute_data)}건의 분봉 데이터를 데이터베이스에 저장합니다.")
        with DatabaseManager() as db:
            db.save_minute_prices(all_minute_data)
    else:
        logging.info("저장할 분봉 데이터가 없습니다.")

if __name__ == "__main__":
    # --- 데이터 수집 기간 설정 ---
    START_DATE = "20250605"  # 시작일 (YYYYMMDD)
    END_DATE = "20250605"    # 종료일 (YYYYMMDD)
    # --------------------------

    # 로그 파일 설정 (필요 시 경로 지정)
    LOG_FILE = None # 예: 'logs/pykrx_fetch.log'

    # 로그 파일이 지정된 경우, 로깅 설정을 변경합니다.
    if LOG_FILE:
        # 기존 핸들러를 제거하고 파일 핸들러를 추가합니다.
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            filename=LOG_FILE,
                            filemode='w')
        print(f"모든 로그를 {LOG_FILE} 파일에 저장합니다.")

    logging.info(f"pykrx_fetch_minute_data.py 스크립트 실행 시작: {START_DATE} ~ {END_DATE}")
    try:
        fetch_and_save_minute_data(START_DATE, END_DATE)
    except Exception as e:
        logging.error(f"메인 실행 중 오류 발생: {e}", exc_info=True)
    finally:
        logging.info("pykrx_fetch_minute_data.py 스크립트 실행 종료")
