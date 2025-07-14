import pandas as pd
import requests
from datetime import datetime, timedelta, date
import time
import logging
import os
import sys
from typing import List, Dict, Any

print("pykrx_fetch_minute_data.py 스크립트 실행 시작")

# 상위 디렉토리를 import path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.kis_api import KISApi
from database.db_manager_upper import DatabaseManager
from utils.date_utils import DateUtils

kis_api = KISApi()

def get_trade_dates_to_fetch(start_date: date, count: int) -> List[date]:
    """주어진 날짜(start_date)를 포함하여 과거 또는 오늘까지의 실제 거래 가능한 영업일을 반환합니다."""
    trade_dates = []
    current_date = start_date
    today = datetime.now().date()

    while len(trade_dates) < count:
        # 오늘 날짜를 초과하는 미래 날짜는 조회하지 않음
        if current_date > today:
            break

        if DateUtils.is_business_day(current_date):
            trade_dates.append(current_date)
        
        current_date += timedelta(days=1)

    return trade_dates

def fetch_full_day_minute_data(ticker: str, base_date: str) -> List[Dict[str, Any]]:
    """한국투자증권 API를 사용하여 특정일의 전체 분봉 데이터(390개)를 가져옵니다."""
    all_data = []
    # 장 마감 시간부터 30분씩 13번 반복하여 과거 데이터 조회
    # 15:30 -> 15:01, 15:00 -> 14:31, ...
    end_time = datetime.strptime(f"{base_date} 15:30:00", "%Y%m%d %H:%M:%S")

    for i in range(13):
        query_time = end_time - timedelta(minutes=30 * i)
        query_time_str = query_time.strftime("%H%M%S")

        try:
            # KIS API를 통해 분봉 데이터 요청
            response = kis_api.get_minute_chart(ticker=ticker, date=base_date, time=query_time_str)
            if response and response.get('output2'):
                all_data.extend(response['output2'])
            else:
                logging.warning(f"  - {base_date} {query_time_str} 데이터 없음.")
            time.sleep(0.2)  # API 호출 간격
        except Exception as e:
            logging.error(f"  - KIS API 호출 중 오류 발생: {e}")
            continue

    if not all_data:
        return []

    # 중복 제거 및 정렬
    df = pd.DataFrame(all_data)
    df = df.drop_duplicates(subset=['stck_cntg_hour'])
    df = df.sort_values(by='stck_cntg_hour').reset_index(drop=True)

    # 데이터 포맷 변환
    formatted_data = []
    for _, row in df.iterrows():
        dt_str = f"{base_date}{row['stck_cntg_hour']}"
        dt_obj = datetime.strptime(dt_str, "%Y%m%d%H%M%S")
        formatted_data.append({
            'datetime': dt_obj,
            'price': int(row['stck_prpr'])
        })
    return formatted_data

def fetch_and_save_minute_data(start_date_str: str, end_date_str: str):
    """지정된 기간 동안의 급등주에 대해 D+5일까지의 분봉 데이터를 가져와 저장합니다."""

    # DB 조회를 위해 날짜 형식을 'YYYY-MM-DD'로 변환
    start_date = datetime.strptime(start_date_str, "%Y%m%d").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d").strftime("%Y-%m-%d")
    
    with DatabaseManager() as db:
        # 해당 기간의 급등주 목록 가져오기
        print(f"DB에서 급등주 조회 시작: {start_date} ~ {end_date}")
        upper_stocks = db.get_selected_pykrx_upper_stocks(start_date, end_date)
        print(f"DB에서 조회된 급등주 수: {len(upper_stocks)}")

    if not upper_stocks:
        print(f"{start_date_str} ~ {end_date_str} 기간에 해당하는 급등주 정보가 없습니다.")
        return

    print(f"총 {len(upper_stocks)}개의 급등주에 대한 분봉 데이터 수집을 시작합니다.")

    for stock_info in upper_stocks:
        high_rise_date_dt = stock_info['date']
        ticker = stock_info['ticker']
        name = stock_info['name']
        
        print(f"종목: {name}({ticker}), 급등일: {high_rise_date_dt.strftime('%Y-%m-%d')}")

        # D+0(급등일)부터 D+5까지 총 7일의 거래일 계산
        trade_dates = get_trade_dates_to_fetch(high_rise_date_dt, 7)

        # D+5까지의 데이터가 모두 존재하는지 확인 (총 7일치)
        if len(trade_dates) < 7:
            print(f"  - {name}({ticker})는 D+5까지의 영업일 데이터가 부족하여 건너뜁니다. (수집된 영업일 수: {len(trade_dates)}개)")
            continue

        print(f"  - 수집 대상 거래일: {[d.strftime('%Y-%m-%d') for d in trade_dates]}")

        all_prices_to_save = []
        for trade_date_dt in trade_dates:
            trade_date_str = trade_date_dt.strftime("%Y%m%d")
            print(f"    - {trade_date_str} 데이터 수집 중...")
            
            minute_data = fetch_full_day_minute_data(ticker, trade_date_str)
            
            if minute_data:
                for data_point in minute_data:
                    all_prices_to_save.append({
                        'high_rise_date': high_rise_date_dt,
                        'ticker': ticker,
                        'name': name, # 종목명 추가
                        'datetime': data_point['datetime'],
                        'price': data_point['price']
                    })

        if all_prices_to_save:
            with DatabaseManager() as db:
                db.save_minute_prices(all_prices_to_save)

if __name__ == "__main__":
    try:
        # 아래 함수를 호출하여 특정 기간의 급등주에 대한 분봉 데이터를 가져옵니다.
        start_date = "20250701"
        end_date = "20250705"
        fetch_and_save_minute_data(start_date, end_date)
    except Exception as e:
        import traceback
        print(f"스크립트 실행 중 예외 발생: {e}")
        traceback.print_exc()
