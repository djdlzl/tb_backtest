import logging
from datetime import datetime, timedelta
import pandas as pd
from database.db_manager_upper import DatabaseManager
from api.kis_api import KISApi
from api.krx_api import KRXApi
from trading.trading_upper import TradingUpper
from utils.date_utils import DateUtils
from config.condition import BUY_PERCENT_UPPER

class StockSelector:
    def __init__(self):
        self.db = DatabaseManager()
        self.kis_api = KISApi()
        self.krx_api = KRXApi()
        self.date_utils = DateUtils()
        self.trading_upper = TradingUpper()

    def select_and_save_all_stocks(self, start_date_str):
        """
        지정된 시작일 이후의 모든 급등주를 선별하여 DB에 저장합니다.
        """
        try:
            end_date_str = datetime.now().strftime('%Y%m%d')
            print(f"{start_date_str}부터 {end_date_str}까지의 모든 급등주를 대상으로 선별을 시작합니다.")

            with DatabaseManager() as db:
                stocks_to_check = db.get_pykrx_upper_stocks(start_date_str, end_date_str)
            
            selected_stocks = []
            today_str = datetime.now().strftime('%Y%m%d')

            for stock in stocks_to_check:
                # 조건 확인 (오늘 날짜를 기준으로 전달)
                result_possible, result_momentum = self.check_conditions(stock, today_str)
                if result_possible:
                    stock['trade_condition'] = 'strong_momentum' if result_momentum else 'normal'
                    selected_stocks.append(stock)
                    print(f"########## [선별 완료] {stock.get('date')} {stock.get('name')}({stock.get('ticker')}) - 조건: {stock['trade_condition']}")
            
            if selected_stocks:
                with DatabaseManager() as db:
                    db.save_selected_pykrx_upper_stocks(selected_stocks)
            else:
                print("선별된 종목이 없습니다.")

            return selected_stocks
        except Exception as e:
            logging.error(f"전체 종목 선별 및 저장 중 오류 발생: {e}")
            return []

    def check_conditions(self, stock, date_str):
        """
        개별 종목에 대한 모든 선별 조건을 확인합니다.
        """
        # --- 조건1: 상승일(D) 이전 기간에 20% 이상 상승 이력 체크 ---
        # 매수일(D+2) 기준 D+1까지의 데이터 15개를 가져옴 (기간 여유롭게)
        df = self.krx_api.get_OHLCV(stock.get('ticker'), 15, date_str)
        
        surge_date = stock.get('date')  # DB에서 이미 date 객체로 반환됨

        df.index = pd.to_datetime(df.index).date
        pre_surge_df = df[df.index < surge_date].copy()
        
        result_high_price = True
        if not pre_surge_df.empty:
            pre_surge_df['전일종가'] = pre_surge_df['종가'].shift(1)
            pre_surge_df['등락률'] = ((pre_surge_df['고가'] - pre_surge_df['전일종가']) / pre_surge_df['전일종가']) * 100
            if (pre_surge_df['등락률'] >= 20).any():
                result_high_price = False

        # --- 조건2: 하락률 조건 (임시 True) ---
        result_decline = True

        # --- 조건3: 거래량 조건 (임시 True) ---
        result_volume = True

        # --- 조건4: 상장일 1년 경과 ---
        result_lstg = self.check_listing_date(stock.get('ticker'))

        # --- 조건5: 시장 경고(과열/정지) 미지정 ---
        result_warning = self.check_market_warnings(stock.get('ticker'))

        # --- 최종 조건 통과 여부 ---
        all_conditions_met = result_high_price and result_decline and result_volume and result_lstg and result_warning

        # --- 💡 신규 로직: 강화된 모멘텀 식별 💡 ---
        is_strong_momentum = False
        if all_conditions_met and len(df[df.index >= surge_date]) >= 2:
            # D일과 D+1일 데이터 추출
            day_0_close = df.loc[surge_date]['종가']
            day_1_df = df[df.index > surge_date]
            if not day_1_df.empty:
                day_1_close = day_1_df.iloc[0]['종가']
                if day_0_close > 0:
                    day_1_return = (day_1_close - day_0_close) / day_0_close
                    if day_1_return >= 0.10:
                        is_strong_momentum = True

        print(stock.get('name'))
        print('조건1: 상승일 기준 10일 전까지 고가 20% 넘지 않은은 이력 여부 체크:',result_high_price)
        print('조건2: 상승일 고가 - 매수일 현재가 = -7.5% 체크:',result_decline)
        # print('조건3: 상승일 거래량 대비 다음날 거래량 20% 이상 체크:',result_volume)
        print('조건4: 상장일 이후 1년 체크:',result_lstg)
        print('조건5: 과열 종목 제외 체크:',result_warning)


        return all_conditions_met, is_strong_momentum

    def check_listing_date(self, ticker):
        """
        상장일이 1년 이상 경과했는지 확인합니다.
        """
        try:
            listing_date_str = self.krx_api.get_listing_date(ticker)
            if listing_date_str:
                listing_date = datetime.strptime(listing_date_str, '%Y%m%d').date()
                if (datetime.now().date() - listing_date) > timedelta(days=365):
                    return True
            return False
        except Exception as e:
            logging.error(f"{ticker} 상장일 확인 중 오류: {e}")
            return False

    def check_market_warnings(self, ticker):
        """
        과열 또는 거래 정지 종목인지 확인합니다.
        """
        try:
            stock_info = self.kis_api.get_stock_price(ticker)
            if stock_info and stock_info.get('output'):
                result_short_over_yn = stock_info['output'].get('short_over_yn', 'N') # 단기과열
                result_trht_yn = stock_info['output'].get('trht_yn', 'N') # 거래정지
                if result_short_over_yn == 'N' and result_trht_yn == 'N':
                    return True
            return False
        except Exception as e:
            logging.error(f"{ticker} 시장 경고 확인 중 오류: {e}")
            return False

if __name__ == "__main__":
    # 특정 날짜 이후 모든 급등주 선별 실행
    start_date = '20250701' # 원하는 시작 날짜를 입력하세요.
    selector = StockSelector()
    selector.select_and_save_all_stocks(start_date)
