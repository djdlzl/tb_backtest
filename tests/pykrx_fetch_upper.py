import sys
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from pykrx import stock
import time

# 프로젝트 루트 경로 설정
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 로깅 설정
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pykrx_fetch.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)

def fetch_and_save_upper_stocks():
    """
    최근 1주일간 20% 이상 상승한 모든 종목을 찾아 데이터베이스에 저장합니다.
    OHLCV 데이터를 사용하여 등락률을 직접 계산합니다.
    """
    from database.db_manager_upper import DatabaseManager

    today = datetime.now()
    # KRX 데이터가 보통 T+1일에 제공되므로, 어제 날짜를 기준으로 조회
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=7)

    end_date_str = end_date.strftime('%Y%m%d')
    start_date_str = start_date.strftime('%Y%m%d')

    logging.info(f"데이터 조회 기간: {start_date_str} ~ {end_date_str}")

    all_stocks_to_save = []

    try:
        # KOSPI와 KOSDAQ의 모든 티커 목록 가져오기
        tickers_kospi = stock.get_market_ticker_list(end_date_str, market="KOSPI")
        tickers_kosdaq = stock.get_market_ticker_list(end_date_str, market="KOSDAQ")
        all_tickers = tickers_kospi + tickers_kosdaq
        logging.info(f"총 {len(all_tickers)}개의 종목을 조회합니다 (KOSPI: {len(tickers_kospi)}, KOSDAQ: {len(tickers_kosdaq)}).")

        for i, ticker in enumerate(all_tickers):
            # KRX 서버 부하를 줄이기 위해 약간의 딜레이 추가
            time.sleep(0.1)
            
            df = stock.get_market_ohlcv(start_date_str, end_date_str, ticker)
            
            if len(df) < 2:
                # 데이터가 충분하지 않으면 건너뛰기
                continue

            # 기간 내 첫 거래일과 마지막 거래일의 종가
            first_trading_day = df.index[0]
            start_price = df.iloc[0]['종가']
            end_price = df.iloc[-1]['종가']

            if start_price == 0:
                continue # 거래 정지 등의 사유로 가격이 0인 경우 제외

            fluctuation_rate = ((end_price - start_price) / start_price) * 100

            if fluctuation_rate >= 20.0:
                stock_name = stock.get_market_ticker_name(ticker)
                stock_info = {
                    "date": end_date.strftime('%Y-%m-%d'),
                    "ticker": ticker,
                    "name": stock_name,
                    "closing_price": end_price,
                    "fluctuation_rate": round(fluctuation_rate, 2)
                }
                all_stocks_to_save.append(stock_info)
                # logging.info(f"[{i+1}/{len(all_tickers)}] 발견: {stock_name}({ticker}), {first_trading_day.strftime('%Y-%m-%d')}부터 등락률: {fluctuation_rate:.2f}%")
                print(f"{stock_name}({ticker}), {first_trading_day.strftime('%Y-%m-%d')}부터 등락률: {fluctuation_rate:.2f}%")

    except Exception as e:
        logging.error(f"데이터 조회 중 오류 발생: {e}", exc_info=True)

    if all_stocks_to_save:
        logging.info(f"총 {len(all_stocks_to_save)}개의 급등주를 데이터베이스에 저장합니다.")
        try:
            with DatabaseManager() as db:
                db.save_pykrx_upper_stocks(all_stocks_to_save)
        except Exception as e:
            logging.error(f"데이터베이스 저장 중 오류 발생: {e}", exc_info=True)
    else:
        logging.info("저장할 급등주 데이터가 없습니다.")

if __name__ == "__main__":
    fetch_and_save_upper_stocks()


