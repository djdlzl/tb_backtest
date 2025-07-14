import pandas as pd
import datetime
import time
from pykrx import stock
from utils.date_utils import DateUtils



class KRXApi:
    def __init__(self):
        self.date_utils = DateUtils()
        
    def get_OHLCV(self, ticker, days, end_date_str):
        time.sleep(1)
        end_date = datetime.datetime.strptime(end_date_str, '%Y%m%d')
        from_date = (end_date - datetime.timedelta(days=days*2)).strftime('%Y%m%d') # 주말, 휴일 고려
        df = stock.get_market_ohlcv_by_date(from_date, end_date_str, ticker)
        return df.tail(days)

    def get_listing_date(self, ticker):
        """
        특정 종목의 상장일을 조회합니다.
        stock.get_market_fundamental 함수를 사용하여 효율적으로 조회합니다.
        """
        try:
            # get_market_fundamental은 해당 날짜의 정보를 가져오므로 오늘 날짜를 기준으로 조회합니다.
            today_str = datetime.datetime.now().strftime('%Y%m%d')
            df = stock.get_market_fundamental(today_str, today_str, ticker)
            if not df.empty and '상장일' in df.columns:
                listing_date = df.iloc[0]['상장일']
                # pykrx 버전에 따라 datetime 객체 또는 Timestamp로 반환될 수 있음
                if isinstance(listing_date, (datetime.datetime, pd.Timestamp)):
                    return listing_date.strftime('%Y%m%d')
            # 만약 fundamental 정보에 상장일이 없다면, 이전 방식으로 조회
            return self.get_listing_date_from_ohlcv(ticker)
        except Exception as e:
            print(f"{ticker}의 상장일 조회 중 오류 발생(fundamental): {e}")
            return self.get_listing_date_from_ohlcv(ticker)

    def get_listing_date_from_ohlcv(self, ticker):
        """
        OHLCV 데이터 기반으로 상장일을 조회하는 대체 방법입니다.
        """
        try:
            df = stock.get_market_ohlcv("19900101", datetime.datetime.now().strftime('%Y%m%d'), ticker)
            if not df.empty:
                listing_date = df.index[0]
                return listing_date.strftime('%Y%m%d')
            return None
        except Exception as e:
            print(f"{ticker}의 상장일 조회 중 오류 발생(ohlcv): {e}")
            return None
