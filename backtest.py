import pandas as pd
from datetime import datetime, time
from database.db_manager_upper import DatabaseManager
from config.condition import (
    SELLING_POINT_UPPER,
    RISK_MGMT_UPPER,
    RISK_MGMT_STRONG_MOMENTUM,
    TRAILING_STOP_PERCENTAGE,
    DAYS_LATER_UPPER,
    BACKTEST_BUY_TIME_1,
    BACKTEST_BUY_TIME_2,
    SELL_TIME_FOR_EXPIRATION,
    EXCLUDED_STOCKS
)
from utils.date_utils import DateUtils
import numpy as np

class Backtester:
    def __init__(self, start_date_str, end_date_str):
        """
        백테스터 초기화

        :param start_date_str: 백테스트 시작일 (YYYYMMDD)
        :param end_date_str: 백테스트 종료일 (YYYYMMDD)
        """
        self.db = DatabaseManager()
        self.start_date = datetime.strptime(start_date_str, "%Y%m%d").date()
        self.end_date = datetime.strptime(end_date_str, "%Y%m%d").date()
        self.trade_logs = []

    def run_backtest(self, excluded_tickers_from_ui=None):
        """백테스트 실행"""
        self.trade_logs = [] # 거래 기록 초기화
        print(f"백테스트를 시작합니다. 기간: {self.start_date} ~ {self.end_date}")

        # 1. 데이터 로드 (지정된 기간 내의 종목만)
        stocks = self.db.get_selected_pykrx_upper_stocks_by_date_range(self.start_date, self.end_date)
        if not stocks:
            print("지정된 기간 내에 백테스트할 데이터가 없습니다.")
            return

        print(f"총 {len(stocks)}개의 종목으로 백테스트를 진행합니다.")

        # 이름-티커 매핑 생성 및 최종 제외 목록 구성
        name_to_ticker_map = {stock['name']: stock['ticker'] for stock in stocks}
        final_excluded_tickers = set(EXCLUDED_STOCKS) # 설정 파일의 기본 제외 목록

        if excluded_tickers_from_ui:
            for item in excluded_tickers_from_ui:
                item = item.strip()
                if not item: continue

                # 입력값이 종목 이름인 경우 티커로 변환
                if item in name_to_ticker_map:
                    final_excluded_tickers.add(name_to_ticker_map[item])
                # 입력값이 티커인 경우 (6자리 숫자로 가정)
                elif item.isdigit() and len(item) == 6:
                    final_excluded_tickers.add(item)
                else:
                    print(f"경고: 제외 목록의 '{item}'은(는) 유효한 종목 이름이나 티커가 아니므로 무시합니다.")

        print(f"총 {len(stocks)}개의 종목으로 백테스트를 진행합니다.")

        for stock in stocks:
            # 최종 제외 목록에 있는지 확인
            if stock['ticker'] in final_excluded_tickers:
                print(f"종목 {stock['name']}({stock['ticker']})는 제외 목록에 있어 건너뜁니다.")
                continue

            # 데이터 로딩 시 이미 날짜 필터링이 완료되었으므로, 바로 시뮬레이션 진행
            # 데이터 로딩 시 이미 날짜 필터링이 완료되었으므로, 바로 시뮬레이션 진행
            self.simulate_trade(stock)

        # self.analyze_results() # app.py에서 별도로 호출하므로 주석 처리

    def simulate_trade(self, stock):
        """개별 종목에 대한 거래 시뮬레이션 (2회 분할 매수)"""
        trade_session_id = stock['id']
        ticker = stock['ticker']
        name = stock['name']
        trade_condition = stock['trade_condition']

        # D+0 ~ D+7 전체 분봉 데이터 로드
        minute_data = self.db.get_all_minute_prices_for_session(trade_session_id)

        if not minute_data:
            print(f"{name}({ticker}, {trade_session_id}): 분봉 데이터가 없어 건너뜁니다.")
            return

        df = pd.DataFrame(minute_data)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)

        # 설정 파일에서 매수 시간 파싱
        buy_hour_1, buy_minute_1 = map(int, BACKTEST_BUY_TIME_1.split(':'))
        buy_hour_2, buy_minute_2 = map(int, BACKTEST_BUY_TIME_2.split(':'))

        # 매수일 계산 (급등일 D-day 기준 D+2 영업일)
        high_rise_date = stock['date']
        buy_date = DateUtils.get_target_date(high_rise_date, later=DAYS_LATER_UPPER)
        if not buy_date:
            print(f"{name}({ticker}, {trade_session_id}): 매수일({high_rise_date}의 {DAYS_LATER_UPPER}일 후)이 유효하지 않아 건너뜁니다.")
            return

        # 1차 매수 시점 탐색
        buy_time_1 = datetime.combine(buy_date, time(buy_hour_1, buy_minute_1))
        try:
            first_buy_candle = df.loc[df.index >= buy_time_1].iloc[0]
            buy_price_1 = first_buy_candle['price']
            actual_buy_datetime_1 = first_buy_candle.name
        except IndexError:
            print(f"{name}({ticker}, {trade_session_id}): 1차 매수 시점({buy_time_1.strftime('%Y-%m-%d %H:%M')}) 데이터를 찾을 수 없어 건너뜁니다.")
            return

        # 2차 매수 시점 탐색
        buy_time_2 = datetime.combine(buy_date, time(buy_hour_2, buy_minute_2))
        try:
            second_buy_candle = df.loc[df.index >= buy_time_2].iloc[0]
            buy_price_2 = second_buy_candle['price']
            actual_buy_datetime_2 = second_buy_candle.name
        except IndexError:
            print(f"{name}({ticker}, {trade_session_id}): 2차 매수 시점({buy_time_2.strftime('%Y-%m-%d %H:%M')}) 데이터를 찾을 수 없어 건너뜁니다.")
            return

        # 매도 시뮬레이션
        self.find_sell_point(df, ticker, name, 
                             buy_price_1, actual_buy_datetime_1, 
                             buy_price_2, actual_buy_datetime_2, 
                             trade_condition, trade_session_id)

    def find_sell_point(self, df, ticker, name, 
                        buy_price_1, buy_datetime_1, 
                        buy_price_2, buy_datetime_2, 
                        trade_condition, trade_session_id):
        """매도 시점 탐색 및 거래 기록 (분할 매수 고려)"""
        # 최종 매수 정보 설정
        average_buy_price = (buy_price_1 + buy_price_2) / 2
        final_buy_datetime = buy_datetime_2 # 2차 매수 시점을 기준으로 필터링

        # 매수 이후 데이터만 필터링
        trade_df = df[df.index > final_buy_datetime]

        # 보유 기간 만료일 계산
        sell_due_date = DateUtils.get_target_date(final_buy_datetime.date(), later=DAYS_LATER_UPPER)
        
        sell_price = 0
        sell_datetime = None
        sell_reason = "기간만료"
        
        highest_profit_ratio = 1.0

        # 분 단위 시뮬레이션
        for current_datetime, row in trade_df.iterrows():
            current_price = row['price']
            current_reason = None

            # --- 매도 조건 확인 ---
            # 조건 1: 리스크 관리 (손절)
            risk_threshold = RISK_MGMT_STRONG_MOMENTUM if trade_condition == 'strong_momentum' else RISK_MGMT_UPPER
            if current_price < (average_buy_price * risk_threshold):
                current_reason = "리스크 관리 (손절)"

            # 조건 2: 익절 (트레일링 스탑)
            if current_reason is None:
                current_profit_ratio = current_price / average_buy_price
                if current_profit_ratio > highest_profit_ratio:
                    highest_profit_ratio = current_profit_ratio

                if highest_profit_ratio > SELLING_POINT_UPPER:
                    trailing_stop_price = average_buy_price * highest_profit_ratio * (1.0 - TRAILING_STOP_PERCENTAGE / 100.0)
                    if current_price < trailing_stop_price:
                        current_reason = f"익절 (고점 대비 -{TRAILING_STOP_PERCENTAGE}%)"
            
            # 조건 3: 기간 만료
            if current_reason is None:
                if sell_due_date and current_datetime.date() >= sell_due_date:
                    sell_time = time.fromisoformat(SELL_TIME_FOR_EXPIRATION)
                    if current_datetime.time() >= sell_time:
                        # 지정된 시간 또는 그 이후의 첫 번째 데이터 포인트에서 매도
                        try:
                            sell_candle = trade_df.loc[trade_df.index.time >= sell_time].iloc[0]
                            sell_price = sell_candle['price']
                            sell_datetime = sell_candle.name
                        except IndexError:
                            # 당일 장 마감까지 데이터가 없으면 마지막 데이터로 매도
                            last_candle = trade_df.iloc[-1]
                            sell_price = last_candle['price']
                            sell_datetime = last_candle.name
                        current_reason = "기간만료"

            if current_reason:
                if sell_price == 0: # 손절 또는 익절의 경우
                    sell_price = current_price
                    sell_datetime = current_datetime
                sell_reason = current_reason
                break
        
        if sell_price == 0 and not trade_df.empty:
            last_candle = trade_df.iloc[-1]
            sell_price = last_candle['price']
            sell_datetime = last_candle.name
            sell_reason = "데이터 종료 (강제 기간만료)"

        if sell_price > 0:
            profit_rate = (sell_price - average_buy_price) / average_buy_price
            self.trade_logs.append({
                'trade_session_id': trade_session_id,
                'ticker': ticker,
                'name': name,
                'buy_datetime': final_buy_datetime, # 최종 매수 시점
                'buy_price': average_buy_price,    # 평균 매수 단가
                'sell_datetime': sell_datetime,
                'sell_price': sell_price,
                'sell_reason': sell_reason,
                'profit_rate': profit_rate,
                'trade_condition': trade_condition
            })

    def analyze_results(self, initial_capital=10000000, return_df=False, start_date=None, end_date=None, excluded_tickers=None):
        """
        백테스트 결과 분석 및 출력.
        특정 기간 조회 및 특정 종목 제외 기능을 지원합니다.

        :param return_df: True일 경우, 필터링된 DataFrame과 통계 정보를 반환합니다.
        :param initial_capital: 초기 자본금
        :param start_date: 분석 시작일 (YYYY-MM-DD 형식)
        :param end_date: 분석 종료일 (YYYY-MM-DD 형식)
        :param excluded_tickers: 제외할 종목 목록
        :return: (logs_df, stats, monthly_returns_df) 튜플 또는 None
        """
        if not self.trade_logs:
            print("분석할 거래 기록이 없습니다.")
            if return_df:
                return pd.DataFrame(), {}, pd.DataFrame()
            return

        logs_df = pd.DataFrame(self.trade_logs)
        logs_df['buy_datetime'] = pd.to_datetime(logs_df['buy_datetime'])
        logs_df['sell_datetime'] = pd.to_datetime(logs_df['sell_datetime'])

        # 필터링
        if start_date:
            logs_df = logs_df[logs_df['sell_datetime'].dt.date >= pd.to_datetime(start_date).date()]
        if end_date:
            logs_df = logs_df[logs_df['sell_datetime'].dt.date <= pd.to_datetime(end_date).date()]
        # 종목 제외 필터링은 run_backtest에서 이미 수행되었으므로 여기서는 하지 않습니다.
        # excluded_tickers는 UI에 현재 필터링 상태를 표시하기 위해 stats에만 전달됩니다.

        if logs_df.empty:
            print("필터링된 거래 기록이 없습니다.")
            if return_df:
                return pd.DataFrame(), {}, pd.DataFrame()
            return

        # 월별 수익률 계산
        logs_df['sell_month'] = logs_df['sell_datetime'].dt.to_period('M')
        monthly_returns = logs_df.groupby('sell_month').apply(self.calculate_geometric_mean, include_groups=False)
        monthly_returns_df = monthly_returns.reset_index(name='monthly_gmean_return')
        monthly_returns_df['sell_month'] = monthly_returns_df['sell_month'].astype(str)

        # 전체 기간 통계
        total_trades = len(logs_df)
        winning_trades = len(logs_df[logs_df['profit_rate'] > 0])
        losing_trades = len(logs_df[logs_df['profit_rate'] <= 0])
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        total_gmean = self.calculate_geometric_mean(logs_df)
        total_return_factor = np.prod(1 + logs_df['profit_rate'])
        final_capital = initial_capital * total_return_factor

        # 콘솔 리포트 출력
        if not return_df: # 웹 UI 호출 시에는 콘솔에 출력하지 않음
            self.print_console_report(logs_df, monthly_returns_df, total_trades, win_rate, total_gmean, initial_capital, final_capital, total_return_factor)

        if return_df:
            stats = {
                'start_date': start_date,
                'end_date': end_date,
                'excluded_tickers': excluded_tickers,
                'total_trades': total_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': win_rate,
                'gmean_return_per_trade': total_gmean,
                'initial_capital': initial_capital,
                'final_capital': final_capital,
                'total_return': (total_return_factor - 1)
            }
            return logs_df, stats, monthly_returns_df

    def print_console_report(self, logs_df, monthly_returns_df, total_trades, win_rate, total_gmean, initial_capital, final_capital, total_return_factor):
        """백테스트 결과를 콘솔에 출력합니다."""
        print("\n--- 거래 기록 (필터링) ---")
        pd.options.display.float_format = '{:.2f}'.format
        display_df = logs_df[['trade_session_id', 'name', 'trade_condition', 'buy_datetime', 'buy_price', 'sell_datetime', 'sell_price', 'profit_rate', 'sell_reason']].copy()
        display_df['buy_datetime'] = display_df['buy_datetime'].dt.strftime('%Y-%m-%d %H:%M')
        display_df['sell_datetime'] = display_df['sell_datetime'].dt.strftime('%Y-%m-%d %H:%M')
        display_df['profit_rate'] = pd.to_numeric(display_df['profit_rate']).map('{:.2%}'.format)
        print(display_df)
        pd.reset_option('display.float_format')

        print("\n--- 매도 사유별 통계 ---")
        print(logs_df['sell_reason'].value_counts())

        print("\n--- 월별 기하평균 수익률 ---")
        monthly_display = monthly_returns_df.copy()
        monthly_display['monthly_gmean_return'] = monthly_display['monthly_gmean_return'].apply(lambda x: f"{x:.2%}")
        print(monthly_display)

        print("\n--- 최종 요약 ---")
        print(f"총 거래 횟수: {total_trades}")
        print(f"승률: {win_rate:.2f}%")
        print(f"기간 기하평균 수익률 (거래당): {total_gmean:.2%}")
        print(f"초기 자본: {initial_capital:,.0f}원")
        print(f"최종 자본: {final_capital:,.0f}원")
        print(f"총 수익률: {(total_return_factor - 1):.2%}")

    @staticmethod
    def calculate_geometric_mean(df):
        """데이터프레임의 수익률로 기하평균을 계산합니다."""
        if df.empty:
            return 0.0
        return (np.prod(1 + df['profit_rate'])) ** (1 / len(df)) - 1

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='지정된 기간 동안 주식 거래를 백테스트합니다.')
    parser.add_argument('--start-date', required=True, help='백테스트 시작일 (YYYYMMDD 형식)')
    parser.add_argument('--end-date', required=True, help='백테스트 종료일 (YYYYMMDD 형식)')
    args = parser.parse_args()

    backtester = Backtester(start_date_str=args.start_date, end_date_str=args.end_date)
    backtester.run_backtest()
    backtester.analyze_results(initial_capital=10000000)
