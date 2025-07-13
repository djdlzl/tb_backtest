import sys
import os

# 프로젝트 루트 디렉토리를 sys.path에 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
import time
import mysql.connector
from config.config import DB_CONFIG
import logging

# 로깅 설정
# logging.basicConfig(level=print, format='%(asctime)s - %(levelname)s - %(message)s')


def create_db_connection():
    """데이터베이스 연결을 생성하고 반환합니다."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("데이터베이스에 성공적으로 연결되었습니다.")
        return conn
    except mysql.connector.Error as e:
        print(f"데이터베이스 연결 오류: {e}")
        return None

def create_table_if_not_exists(conn):
    """pykrx_upper_stocks 테이블이 없으면 생성합니다."""
    try:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pykrx_upper_stocks (
                `date` DATE,
                ticker VARCHAR(20),
                name VARCHAR(100),
                closing_price DECIMAL(10,2),
                fluctuation_rate DECIMAL(5,2),
                PRIMARY KEY (`date`, ticker)
            ) ENGINE=InnoDB
        ''')
        conn.commit()
        cursor.close()
        print("'pykrx_upper_stocks' 테이블이 준비되었습니다.")
    except mysql.connector.Error as e:
        print(f"테이블 생성 오류: {e}")


def save_stocks_to_db(conn, stocks_data):
    """조회된 주식 데이터를 데이터베이스에 저장합니다."""
    if not stocks_data:
        return

    try:
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT INTO pykrx_upper_stocks (date, ticker, name, closing_price, fluctuation_rate)
            VALUES (%(date)s, %(ticker)s, %(name)s, %(closing_price)s, %(fluctuation_rate)s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                closing_price = VALUES(closing_price),
                fluctuation_rate = VALUES(fluctuation_rate)
        ''', stocks_data)
        conn.commit()
        cursor.close()
        print(f"{len(stocks_data)}개의 주식 정보가 데이터베이스에 저장되었습니다.")
    except mysql.connector.Error as e:
        print(f"데이터 저장 오류: {e}")
        conn.rollback()


def find_daily_upper_stocks(start_date_str, end_date_str):
    """
    지정된 기간 동안 매일 20% 이상 상승한 종목을 찾아서 DB에 저장합니다.
    :param start_date_str: 'YYYYMMDD' 형식의 시작일
    :param end_date_str: 'YYYYMMDD' 형식의 종료일
    """
    conn = create_db_connection()
    if not conn:
        return

    create_table_if_not_exists(conn)

    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")
    
    print(f"조회 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        
        if current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue

        print(f"--- {current_date.strftime('%Y-%m-%d')} ---")
        
        stocks_to_save = []
        
        try:
            df = stock.get_market_ohlcv(date_str, market="ALL")
            
            if df.empty:
                print("거래일이 아니거나 데이터가 없습니다.")
                current_date += timedelta(days=1)
                time.sleep(1)
                continue

            prev_trading_date = stock.get_nearest_business_day_in_a_week(current_date.strftime("%Y%m%d"))
            prev_trading_date = datetime.strptime(prev_trading_date, "%Y%m%d") - timedelta(days=1)
            prev_trading_date_str = stock.get_nearest_business_day_in_a_week(prev_trading_date.strftime("%Y%m%d"))

            df_prev = stock.get_market_ohlcv(prev_trading_date_str, market="ALL")
            df_prev.rename(columns={'종가': '전일종가'}, inplace=True)

            df = df.join(df_prev['전일종가'])
            df.dropna(subset=['전일종가'], inplace=True)

            df = df[df['전일종가'] != 0].copy()

            df['등락률'] = ((df['종가'] - df['전일종가']) / df['전일종가']) * 100

            upper_stocks = df[df['등락률'] >= 17]

            if not upper_stocks.empty:
                for ticker, row in upper_stocks.iterrows():
                    stock_name = stock.get_market_ticker_name(ticker)
                    stocks_to_save.append({
                        'date': current_date.strftime('%Y-%m-%d'),
                        'ticker': ticker,
                        'name': stock_name,
                        'closing_price': row['종가'],
                        'fluctuation_rate': row['등락률']
                    })
                    print(f"  - {stock_name}({ticker}): {row['등락률']:.2f}% (전일 대비)")
            
            if not stocks_to_save:
                print("17% 이상 상승한 종목 없음")
            else:
                save_stocks_to_db(conn, stocks_to_save)

        except KeyError as e:
            print(f"데이터 처리 중 오류: 필요한 컬럼({e})이 없습니다.")
        except Exception as e:
            print(f"데이터 조회 중 오류 발생: {e}")

        time.sleep(1)
        current_date += timedelta(days=1)
    
    if conn and conn.is_connected():
        conn.close()
        print("데이터베이스 연결이 종료되었습니다.")


if __name__ == "__main__":
    # 오늘 날짜와 일주일 전 날짜를 기본값으로 설정
    today = datetime.now()
    one_week_ago = today - timedelta(days=7)
    
    start_date_default = one_week_ago.strftime("%Y%m%d")
    end_date_default = today.strftime("%Y%m%d")
    
    find_daily_upper_stocks(start_date_default, end_date_default)
