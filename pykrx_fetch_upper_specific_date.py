import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
import time
import logging
from typing import List, Dict, Any
from config.config import DB_CONFIG
from database.db_manager_upper import DatabaseManager

def find_daily_upper_stocks(start_date_str, end_date_str):
    """
    지정된 기간 동안 매일 20% 이상 상승한 종목을 찾아서 출력합니다.
    :param start_date_str: 'YYYYMMDD' 형식의 시작일
    :param end_date_str: 'YYYYMMDD' 형식의 종료일
    """
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")
    
    print(f"조회 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        
        # 주말(토:5, 일:6)은 건너뛰기
        if current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue

        print(f"--- {current_date.strftime('%Y-%m-%d')} ---")
        
        found_any = False
        stocks_to_save = []
        try:
            # OHLCV 데이터를 사용하여 등락률을 직접 계산
            df = stock.get_market_ohlcv(date_str, market="ALL")
            
            if df.empty:
                print("거래일이 아니거나 데이터가 없습니다.")
                current_date += timedelta(days=1)
                time.sleep(1)
                continue

            # 전일 종가 데이터를 얻기 위해, 실제 거래일 기준 하루 전 날짜를 찾음
            prev_trading_date = stock.get_nearest_business_day_in_a_week(current_date.strftime("%Y%m%d"))
            # 하루 더 이전으로
            prev_trading_date = datetime.strptime(prev_trading_date, "%Y%m%d") - timedelta(days=1)
            prev_trading_date_str = stock.get_nearest_business_day_in_a_week(prev_trading_date.strftime("%Y%m%d"))

            df_prev = stock.get_market_ohlcv(prev_trading_date_str, market="ALL")
            df_prev.rename(columns={'종가': '전일종가'}, inplace=True)

            # 현재 날짜의 데이터와 전일 종가 데이터를 병합
            df = df.join(df_prev['전일종가'])
            df.dropna(subset=['전일종가'], inplace=True)

            # 전일 종가가 0인 경우 제외
            df = df[df['전일종가'] != 0].copy()

            # 전일 종가 대비 등락률 계산
            df['등락률'] = ((df['종가'] - df['전일종가']) / df['전일종가']) * 100

            upper_stocks = df[df['등락률'] >= 17]

            if not upper_stocks.empty:
                found_any = True
                for ticker, row in upper_stocks.iterrows():
                    stock_name = stock.get_market_ticker_name(ticker)
                    print(f"  - {stock_name}({ticker}): 등락률({row['등락률']:.2f}%), 전일종가({row['전일종가']}), 당일종가({row['종가']})")
                    stocks_to_save.append({
                        'date': current_date.strftime('%Y-%m-%d'),
                        'ticker': ticker,
                        'name': stock_name,
                        'upper_rate': float(row['등락률']),
                        'closing_price': float(row['종가'])
                    })

                with DatabaseManager() as db:
                    db.save_pykrx_upper_stocks(stocks_to_save)
            
            if not found_any:
                print("17% 이상 상승한 종목 없음")

        except KeyError as e:
            print(f"데이터 처리 중 오류: 필요한 컬럼({e})이 없습니다.")
        except Exception as e:
            print(f"데이터 조회 중 오류 발생: {e}")

        time.sleep(1) # pykrx 서버 부하 감소를 위해 딜레이 추가
        current_date += timedelta(days=1)


if __name__ == "__main__":
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('upper_stocks.log')
        ]
    )
    
    # 오늘 날짜와 일주일 전 날짜를 기본값으로 설정
    today = datetime.now()
    one_week_ago = today - timedelta(days=7)
    
    start_date_default = "20250101"
    end_date_default = "20250709"

    find_daily_upper_stocks(start_date_default, end_date_default)
