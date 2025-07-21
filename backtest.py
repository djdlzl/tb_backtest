"""
백테스트 시스템
매수/매도 조건을 기반으로 백테스트를 수행합니다.
"""

import logging
from datetime import datetime, date, time, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from database.db_manager_upper import DatabaseManager
from config.condition import (
    SELLING_POINT_UPPER, RISK_MGMT_UPPER, RISK_MGMT_STRONG_MOMENTUM,
    TRAILING_STOP_PERCENTAGE, BACKTEST_BUY_TIME_1, BACKTEST_BUY_TIME_2,
    SELL_TIME_FOR_EXPIRATION, STRONG_MOMENTUM
)


@dataclass
class BacktestResult:
    """백테스트 결과를 저장하는 데이터 클래스"""
    ticker: str
    name: str
    buy_time_1: str
    buy_time_2: str
    total_investment: int
    final_value: int
    profit_loss: int
    profit_rate: float
    buy_records: List[Dict[str, Any]]
    sell_records: List[Dict[str, Any]]
    trade_duration_days: int
    max_drawdown: float
    win_rate: float


class BacktestEngine:
    """백테스트 엔진"""
    
    def __init__(self):
        self.db_manager = None
        self.logger = logging.getLogger(__name__)
        
    def __enter__(self):
        self.db_manager = DatabaseManager()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_manager:
            self.db_manager.__exit__(exc_type, exc_val, exc_tb)
    
    def get_tick_interval(self, price: int) -> int:
        """호가 단위를 계산합니다."""
        if price < 2000:
            return 1
        elif price < 5000:
            return 5
        elif price < 20000:
            return 10
        elif price < 50000:
            return 50
        elif price < 200000:
            return 100
        elif price < 500000:
            return 500
        else:
            return 1000
    
    def calculate_target_price(self, current_price: int) -> int:
        """현재가 기준으로 매도 타겟 가격을 계산합니다."""
        tick_interval = self.get_tick_interval(current_price)
        target_price = max(current_price - tick_interval * 2, tick_interval)
        return target_price
    
    def should_sell(self, current_price: int, avg_price: int, current_date: date, 
                   target_date: date, current_time: time, trade_condition: str = "normal",
                   ticker_high_ratio: Dict[str, float] = None, ticker: str = "") -> Tuple[bool, str, Dict[str, Any]]:
        """매도 조건을 판단합니다."""
        
        if ticker_high_ratio is None:
            ticker_high_ratio = {}
            
        target_price = self.calculate_target_price(current_price)
        sell_reason = {"매도가": target_price}
        sell_reason_text = None
        
        # 조건1: 보유기간 만료로 매도 (15:10 이후)
        sell_time_threshold = time(15, 10)
        if current_date > target_date and current_time >= sell_time_threshold:
            sell_reason_text = "기간만료"
            sell_reason["매도목표일"] = target_date.strftime("%Y-%m-%d")
            return True, sell_reason_text, sell_reason
        
        # 조건2: 리스크 관리차 매도 (손절)
        if trade_condition == STRONG_MOMENTUM:
            risk_threshold = RISK_MGMT_STRONG_MOMENTUM
            reason = "주가 하락: 강력 모멘텀 리스크 관리"
        else:
            risk_threshold = RISK_MGMT_UPPER
            reason = "주가 하락: 리스크 관리차 매도"
        
        if target_price < (avg_price * risk_threshold):
            sell_reason_text = reason
            sell_reason["매도조건가"] = int(avg_price * risk_threshold)
            return True, sell_reason_text, sell_reason
        
        # 조건3: 트레일링스탑 로직 (익절)
        current_profit_ratio = target_price / avg_price
        
        # 최고 수익률 업데이트
        if ticker not in ticker_high_ratio or current_profit_ratio > ticker_high_ratio[ticker]:
            ticker_high_ratio[ticker] = current_profit_ratio
        
        high_ratio = ticker_high_ratio[ticker]
        selling_threshold = SELLING_POINT_UPPER  # 기준 수익률 (4%)
        trailing_threshold = 1 - (TRAILING_STOP_PERCENTAGE - 1)  # 트레일링스탑 비율
        
        # 수익률이 4% 이하일 때는 트레일링스탑 미적용
        if current_profit_ratio <= selling_threshold:
            pass
        # 수익률이 4~8% 사이일 때, 4% 미만으로 떨어지면 매도
        elif selling_threshold < high_ratio < (selling_threshold + (TRAILING_STOP_PERCENTAGE - 1)):
            if current_profit_ratio < selling_threshold:
                sell_reason_text = f"트레일링스탑: 고점 {high_ratio:.2%}에서 하락"
                sell_reason["최고수익률"] = f"{high_ratio:.2%}"
                return True, sell_reason_text, sell_reason
        # 수익률이 8% 이상일 때, 고점 대비 4%p 하락하면 매도
        elif high_ratio >= (selling_threshold + (TRAILING_STOP_PERCENTAGE - 1)):
            if current_profit_ratio < (high_ratio * trailing_threshold):
                sell_reason_text = f"트레일링스탑: 고점 {high_ratio:.2%}에서 {((1-trailing_threshold)*100):.0f}% 하락"
                sell_reason["최고수익률"] = f"{high_ratio:.2%}"
                return True, sell_reason_text, sell_reason
        
        return False, None, sell_reason
    
    def parse_time_string(self, time_str: str) -> time:
        """시간 문자열을 time 객체로 변환합니다."""
        try:
            hour, minute = map(int, time_str.split(':'))
            return time(hour, minute)
        except ValueError:
            self.logger.error(f"잘못된 시간 형식: {time_str}")
            return time(10, 40)  # 기본값
    
    def run_backtest(self, trade_session_id: int, buy_time_1: str = "10:40", 
                    buy_time_2: str = "11:30", investment_amount: int = 10000000,
                    target_date: Optional[date] = None) -> BacktestResult:
        """백테스트를 실행합니다."""
        
        # 분봉 데이터 조회
        minute_data = self.db_manager.get_all_minute_prices_for_session(trade_session_id)
        if not minute_data:
            self.logger.error(f"거래 세션 ID {trade_session_id}의 분봉 데이터가 없습니다.")
            return None
        
        # 기본 정보 설정
        ticker = minute_data[0].get('ticker', '')
        name = minute_data[0].get('name', '')
        trade_date = minute_data[0].get('datetime').date() if minute_data[0].get('datetime') else date.today()
        
        if target_date is None:
            target_date = trade_date + timedelta(days=1)  # 다음 날 매도
        
        # 매수 시간 설정
        buy_time_1_obj = self.parse_time_string(buy_time_1)
        buy_time_2_obj = self.parse_time_string(buy_time_2)
        
        # 백테스트 변수 초기화
        cash = investment_amount
        position = 0  # 보유 주식 수
        avg_price = 0  # 평균 매수가
        total_invested = 0
        buy_records = []
        sell_records = []
        ticker_high_ratio = {}
        
        is_position_closed = False
        max_value = investment_amount
        min_value = investment_amount
        
        self.logger.info(f"백테스트 시작: {ticker}({name}), 매수시간: {buy_time_1}/{buy_time_2}")
        
        # 분봉 데이터를 시간순으로 처리
        for data in sorted(minute_data, key=lambda x: x['datetime']):
            current_datetime = data['datetime']
            current_date = current_datetime.date()
            current_time = current_datetime.time()
            current_price = int(data['price'])
            
            current_value = cash + (position * current_price)
            max_value = max(max_value, current_value)
            min_value = min(min_value, current_value)
            
            # 매수 로직 (현금이 있고 아직 포지션이 없는 경우)
            if cash > 0 and not is_position_closed:
                should_buy = False
                buy_amount = 0
                
                # 매수 시간 확인
                if buy_time_1 == buy_time_2:  # 동일 시간이면 일괄 매수
                    if current_time >= buy_time_1_obj and len(buy_records) == 0:
                        should_buy = True
                        buy_amount = cash
                else:  # 다른 시간이면 분할 매수
                    if current_time >= buy_time_1_obj and len(buy_records) == 0:
                        should_buy = True
                        buy_amount = cash // 2
                    elif current_time >= buy_time_2_obj and len(buy_records) == 1:
                        should_buy = True
                        buy_amount = cash
                
                if should_buy and buy_amount > 0:
                    # 매수 가능 주식 수 계산 (호가 단위 고려)
                    target_price = self.calculate_target_price(current_price)
                    quantity = buy_amount // target_price
                    
                    if quantity > 0:
                        buy_price = target_price
                        buy_cost = quantity * buy_price
                        
                        # 평균 매수가 계산
                        if position == 0:
                            avg_price = buy_price
                        else:
                            avg_price = ((avg_price * position) + buy_cost) // (position + quantity)
                        
                        position += quantity
                        cash -= buy_cost
                        total_invested += buy_cost
                        
                        buy_record = {
                            'datetime': current_datetime,
                            'price': buy_price,
                            'quantity': quantity,
                            'amount': buy_cost,
                            'type': f'매수{len(buy_records) + 1}'
                        }
                        buy_records.append(buy_record)
                        
                        self.logger.info(f"매수 실행: {current_datetime}, 가격: {buy_price:,}원, 수량: {quantity:,}주, 금액: {buy_cost:,}원")
            
            # 매도 로직 (포지션이 있는 경우)
            if position > 0 and not is_position_closed:
                should_sell, sell_reason_text, sell_reason = self.should_sell(
                    current_price, avg_price, current_date, target_date, current_time,
                    trade_condition="normal", ticker_high_ratio=ticker_high_ratio, ticker=ticker
                )
                
                if should_sell:
                    sell_price = sell_reason["매도가"]
                    sell_amount = position * sell_price
                    profit_loss = sell_amount - total_invested
                    profit_rate = (profit_loss / total_invested) * 100 if total_invested > 0 else 0
                    
                    cash += sell_amount
                    
                    sell_record = {
                        'datetime': current_datetime,
                        'price': sell_price,
                        'quantity': position,
                        'amount': sell_amount,
                        'reason': sell_reason_text,
                        'profit_loss': profit_loss,
                        'profit_rate': profit_rate
                    }
                    sell_records.append(sell_record)
                    
                    self.logger.info(f"매도 실행: {current_datetime}, 가격: {sell_price:,}원, 수량: {position:,}주, 금액: {sell_amount:,}원, 수익률: {profit_rate:.2f}%")
                    
                    position = 0
                    is_position_closed = True
                    break
        
        # 백테스트 결과 계산
        final_value = cash + (position * current_price if position > 0 else 0)
        profit_loss = final_value - investment_amount
        profit_rate = (profit_loss / investment_amount) * 100 if investment_amount > 0 else 0
        
        # 최대 손실률 계산 (Maximum Drawdown)
        max_drawdown = ((max_value - min_value) / max_value) * 100 if max_value > 0 else 0
        
        # 승률 계산 (매도 기록이 있는 경우만)
        win_rate = 0
        if sell_records:
            winning_trades = sum(1 for record in sell_records if record['profit_loss'] > 0)
            win_rate = (winning_trades / len(sell_records)) * 100
        
        # 거래 기간 계산
        if buy_records and sell_records:
            trade_duration = (sell_records[-1]['datetime'].date() - buy_records[0]['datetime'].date()).days
        else:
            trade_duration = 0
        
        result = BacktestResult(
            ticker=ticker,
            name=name,
            buy_time_1=buy_time_1,
            buy_time_2=buy_time_2,
            total_investment=total_invested,
            final_value=final_value,
            profit_loss=profit_loss,
            profit_rate=profit_rate,
            buy_records=buy_records,
            sell_records=sell_records,
            trade_duration_days=trade_duration,
            max_drawdown=max_drawdown,
            win_rate=win_rate
        )
        
        self.logger.info(f"백테스트 완료: 수익률 {profit_rate:.2f}%, 최대손실률 {max_drawdown:.2f}%")
        return result
    
    def optimize_buy_times(self, trade_session_id: int, investment_amount: int = 10000000,
                          time_candidates: List[str] = None) -> List[BacktestResult]:
        """매수 시간을 최적화합니다."""
        
        if time_candidates is None:
            time_candidates = ["09:05", "09:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30", "13:00"]
        
        results = []
        
        # 모든 시간 조합에 대해 백테스트 실행
        for i, time1 in enumerate(time_candidates):
            for time2 in time_candidates[i:]:  # 중복 제거를 위해 i부터 시작
                try:
                    result = self.run_backtest(
                        trade_session_id=trade_session_id,
                        buy_time_1=time1,
                        buy_time_2=time2,
                        investment_amount=investment_amount
                    )
                    if result:
                        results.append(result)
                        self.logger.info(f"시간 조합 {time1}/{time2}: 수익률 {result.profit_rate:.2f}%")
                except Exception as e:
                    self.logger.error(f"백테스트 실행 중 오류 ({time1}/{time2}): {e}")
        
        # 수익률 기준으로 정렬
        results.sort(key=lambda x: x.profit_rate, reverse=True)
        return results
    
    def get_available_sessions(self) -> List[Dict[str, Any]]:
        """백테스트 가능한 거래 세션 목록을 반환합니다."""
        try:
            self.db_manager.cursor.execute('''
                SELECT DISTINCT trade_session_id, ticker, name, high_rise_date
                FROM minute_prices
                ORDER BY high_rise_date DESC, ticker
            ''')
            sessions = self.db_manager.cursor.fetchall()
            return sessions
        except Exception as e:
            self.logger.error(f"거래 세션 조회 중 오류: {e}")
            return []
    
    def run_bulk_backtest(self, buy_time_1: str = "10:40", buy_time_2: str = "11:30", 
                         investment_amount: int = 10000000) -> Dict[str, Any]:
        """모든 세션에 대해 백테스트를 실행하고 통합 결과를 반환합니다."""
        sessions = self.get_available_sessions()
        if not sessions:
            self.logger.error("백테스트 가능한 세션이 없습니다.")
            return None
        
        results = []
        total_investment = 0
        total_final_value = 0
        successful_trades = 0
        profitable_trades = 0
        
        self.logger.info(f"모든 세션 백테스트 시작 - {len(sessions)}개 세션, 매수시간: {buy_time_1}/{buy_time_2}")
        
        for i, session in enumerate(sessions):
            try:
                session_id = session['trade_session_id']
                result = self.run_backtest(
                    trade_session_id=session_id,
                    buy_time_1=buy_time_1,
                    buy_time_2=buy_time_2,
                    investment_amount=investment_amount
                )
                
                if result and result.total_investment > 0:
                    results.append(result)
                    total_investment += result.total_investment
                    total_final_value += result.final_value
                    successful_trades += 1
                    
                    if result.profit_loss > 0:
                        profitable_trades += 1
                
                # 진행률 표시
                if (i + 1) % 10 == 0:
                    self.logger.info(f"진행률: {i + 1}/{len(sessions)} ({(i + 1)/len(sessions)*100:.1f}%)")
                    
            except Exception as e:
                self.logger.error(f"세션 {session.get('trade_session_id', 'N/A')} 백테스트 중 오류: {e}")
                continue
        
        if not results:
            self.logger.error("성공한 백테스트 결과가 없습니다.")
            return None
        
        # 통계 계산
        profit_losses = [r.profit_loss for r in results]
        profit_rates = [r.profit_rate for r in results]
        max_drawdowns = [r.max_drawdown for r in results]
        trade_durations = [r.trade_duration_days for r in results]
        
        total_profit_loss = total_final_value - total_investment
        total_profit_rate = (total_profit_loss / total_investment * 100) if total_investment > 0 else 0
        win_rate = (profitable_trades / successful_trades * 100) if successful_trades > 0 else 0
        
        avg_profit_rate = sum(profit_rates) / len(profit_rates) if profit_rates else 0
        median_profit_rate = sorted(profit_rates)[len(profit_rates)//2] if profit_rates else 0
        avg_max_drawdown = sum(max_drawdowns) / len(max_drawdowns) if max_drawdowns else 0
        avg_trade_duration = sum(trade_durations) / len(trade_durations) if trade_durations else 0
        
        bulk_result = {
            'buy_time_1': buy_time_1,
            'buy_time_2': buy_time_2,
            'total_sessions': len(sessions),
            'successful_sessions': successful_trades,
            'profitable_sessions': profitable_trades,
            'total_investment': total_investment,
            'total_final_value': total_final_value,
            'total_profit_loss': total_profit_loss,
            'total_profit_rate': total_profit_rate,
            'win_rate': win_rate,
            'avg_profit_rate': avg_profit_rate,
            'median_profit_rate': median_profit_rate,
            'avg_max_drawdown': avg_max_drawdown,
            'avg_trade_duration': avg_trade_duration,
            'detailed_results': results,
            'best_result': max(results, key=lambda x: x.profit_rate) if results else None,
            'worst_result': min(results, key=lambda x: x.profit_rate) if results else None
        }
        
        self.logger.info(f"모든 세션 백테스트 완료 - 성공: {successful_trades}/{len(sessions)}, 평균수익률: {avg_profit_rate:.2f}%, 승률: {win_rate:.1f}%")
        return bulk_result
    
    def optimize_buy_times_for_all_sessions(self, investment_amount: int = 10000000,
                                          time_candidates: List[str] = None) -> List[Dict[str, Any]]:
        """모든 세션에 대해 매수 시간을 최적화합니다."""
        
        if time_candidates is None:
            time_candidates = ["09:05", "09:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30", "13:00", "13:30", "14:00", "14:30"]
        
        optimization_results = []
        total_combinations = len(time_candidates) * (len(time_candidates) + 1) // 2  # 중복 제거
        current_combination = 0
        
        self.logger.info(f"전체 세션 매수시간 최적화 시작 - {total_combinations}개 조합 테스트")
        
        # 모든 시간 조합에 대해 백테스트 실행
        for i, time1 in enumerate(time_candidates):
            for time2 in time_candidates[i:]:  # 중복 제거를 위해 i부터 시작
                current_combination += 1
                try:
                    bulk_result = self.run_bulk_backtest(
                        buy_time_1=time1,
                        buy_time_2=time2,
                        investment_amount=investment_amount
                    )
                    
                    if bulk_result:
                        optimization_results.append(bulk_result)
                        self.logger.info(
                            f"진행: {current_combination}/{total_combinations} - "
                            f"{time1}/{time2}: 평균수익률 {bulk_result['avg_profit_rate']:.2f}%, "
                            f"승률 {bulk_result['win_rate']:.1f}%"
                        )
                
                except Exception as e:
                    self.logger.error(f"시간 조합 {time1}/{time2} 최적화 중 오류: {e}")
                    continue
        
        # 평균 수익률 기준으로 정렬
        optimization_results.sort(key=lambda x: x['avg_profit_rate'], reverse=True)
        
        self.logger.info(f"전체 세션 최적화 완료 - {len(optimization_results)}개 조합 결과")
        return optimization_results


def main():
    """메인 함수 - 백테스트 실행 예시"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    with BacktestEngine() as engine:
        # 사용 가능한 세션 조회
        sessions = engine.get_available_sessions()
        if not sessions:
            print("백테스트 가능한 데이터가 없습니다.")
            return
        
        print(f"=== 백테스트 시스템 ===")
        print(f"총 {len(sessions)}개 거래 세션 발견")
        print(f"세션 예시 (상위 5개):")
        for i, session in enumerate(sessions[:5]):
            print(f"{i+1}. {session['ticker']}({session['name']}) - {session['high_rise_date']}")
        
        # 전체 세션 백테스트 (10:40/11:30)
        print(f"\n=== 전체 세션 백테스트 (10:40/11:30) ===")
        bulk_result = engine.run_bulk_backtest(buy_time_1="10:40", buy_time_2="11:30")
        
        if bulk_result:
            print(f"총 세션 수: {bulk_result['total_sessions']}")
            print(f"성공 세션: {bulk_result['successful_sessions']}")
            print(f"수익 세션: {bulk_result['profitable_sessions']}")
            print(f"승률: {bulk_result['win_rate']:.1f}%")
            print(f"전체 투자금액: {bulk_result['total_investment']:,}원")
            print(f"전체 최종가치: {bulk_result['total_final_value']:,}원")
            print(f"전체 수익률: {bulk_result['total_profit_rate']:.2f}%")
            print(f"평균 수익률: {bulk_result['avg_profit_rate']:.2f}%")
            print(f"중앙 수익률: {bulk_result['median_profit_rate']:.2f}%")
            print(f"평균 최대손실률: {bulk_result['avg_max_drawdown']:.2f}%")
            print(f"평균 거래기간: {bulk_result['avg_trade_duration']:.1f}일")
            
            if bulk_result['best_result']:
                best = bulk_result['best_result']
                print(f"\n최고 수익: {best.ticker}({best.name}) - {best.profit_rate:.2f}%")
            
            if bulk_result['worst_result']:
                worst = bulk_result['worst_result']
                print(f"최대 손실: {worst.ticker}({worst.name}) - {worst.profit_rate:.2f}%")
        
        # 전체 세션 매수시간 최적화 (제한된 시간 후보로 빠른 테스트)
        print(f"\n=== 전체 세션 매수시간 최적화 ===")
        print(f"빠른 테스트를 위해 제한된 시간 후보로 실행...")
        
        # 빠른 테스트를 위한 제한된 시간 후보
        limited_time_candidates = ["09:30", "10:00", "10:30", "11:00", "11:30", "12:00"]
        
        optimization_results = engine.optimize_buy_times_for_all_sessions(
            time_candidates=limited_time_candidates
        )
        
        if optimization_results:
            print(f"\n=== 최적화 결과 (상위 10개) ===")
            for i, result in enumerate(optimization_results[:10]):
                print(f"{i+1}. {result['buy_time_1']}/{result['buy_time_2']}: "
                      f"평균 {result['avg_profit_rate']:.2f}%, "
                      f"승률 {result['win_rate']:.1f}%, "
                      f"성공 {result['successful_sessions']}/{result['total_sessions']}")
        
        # 단일 세션 예시 (첫 번째 세션)
        if sessions:
            print(f"\n=== 단일 세션 예시 ===")
            session_id = sessions[0]['trade_session_id']
            print(f"세션: {sessions[0]['ticker']}({sessions[0]['name']}) - {sessions[0]['high_rise_date']}")
            
            result = engine.run_backtest(
                trade_session_id=session_id,
                buy_time_1="10:40",
                buy_time_2="11:30"
            )
            
            if result:
                print(f"매수시간: {result.buy_time_1}/{result.buy_time_2}")
                print(f"수익률: {result.profit_rate:.2f}%")
                print(f"손익: {result.profit_loss:,}원")
                print(f"최대손실률: {result.max_drawdown:.2f}%")


if __name__ == "__main__":
    main()
