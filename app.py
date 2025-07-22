from flask import Flask, render_template_string, request, jsonify
import pandas as pd
from backtest import BacktestEngine
from datetime import datetime, timedelta
import logging

app = Flask(__name__)

HTML_TEMPLATE = '''
<!doctype html>
<html lang="ko">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <title>백테스트 결과 - 분봉 데이터 기반</title>
    <style>
        body { font-family: 'Malgun Gothic', sans-serif; margin: 2rem; background-color: #f8f9fa; color: #212529; }
        h1, h2, h3 { text-align: center; color: #495057; }
        .container { max-width: 1600px; margin: auto; background: white; padding: 2rem; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
        th, td { padding: 0.75rem; text-align: left; border-bottom: 1px solid #dee2e6; }
        th { background-color: #f8f9fa; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
        .stat-card { background: #f8f9fa; padding: 1.5rem; border-radius: 8px; text-align: center; }
        .stat-card h3 { margin-top: 0; color: #007bff; }
        .stat-card p { font-size: 1.5rem; font-weight: bold; margin-bottom: 0; }
        .profit { color: #28a745; }
        .loss { color: #dc3545; }
        form { background-color: #e9ecef; padding: 1.5rem; border-radius: 8px; margin-bottom: 2rem; }
        .form-row { display: flex; flex-wrap: wrap; gap: 1rem; align-items: end; margin-bottom: 1rem; }
        .form-group { display: flex; flex-direction: column; min-width: 120px; }
        .form-group label { margin-bottom: 0.5rem; font-weight: bold; font-size: 0.9rem; }
        .form-group input, .form-group select, .form-group button { padding: 0.5rem; border-radius: 4px; border: 1px solid #ced4da; }
        .form-group button { background-color: #007bff; color: white; border-color: #007bff; cursor: pointer; }
        .form-group button:hover { background-color: #0056b3; }
        .form-group button.secondary { background-color: #6c757d; border-color: #6c757d; }
        .form-group button.secondary:hover { background-color: #5a6268; }
        .optimization-results { background-color: #f8f9fa; padding: 1rem; border-radius: 8px; margin-top: 1rem; }
        .session-info { background-color: #d1ecf1; padding: 1rem; border-radius: 8px; margin-bottom: 1rem; border-left: 4px solid #bee5eb; }
    </style>
</head>
<body>
    <div class="container">
        <h1>백테스트 결과 - 분봉 데이터 기반</h1>
        
        <form method="post">
            <div class="form-row">
                <div class="form-group">
                    <label for="session_id">거래 세션:</label>
                    <select id="session_id" name="session_id" required>
                        <option value="">세션을 선택하세요</option>
                        {% for session in available_sessions %}
                        <option value="{{ session.trade_session_id }}" 
                                {% if current_values.session_id == session.trade_session_id|string %}selected{% endif %}>
                            {{ session.ticker }}({{ session.name }}) - {{ session.high_rise_date }}
                        </option>
                        {% endfor %}
                    </select>
                </div>
                <div class="form-group">
                    <label for="buy_time_1">매수시간1:</label>
                    <input type="time" id="buy_time_1" name="buy_time_1" value="{{ current_values.buy_time_1 }}" required>
                </div>
                <div class="form-group">
                    <label for="buy_time_2">매수시간2:</label>
                    <input type="time" id="buy_time_2" name="buy_time_2" value="{{ current_values.buy_time_2 }}" required>
                </div>
                <div class="form-group">
                    <label for="investment_amount">투자금액:</label>
                    <input type="text" id="investment_amount" name="investment_amount" value="{{ current_values.investment_amount }}" placeholder="10000000">
                </div>
                <div class="form-group">
                    <button type="submit" name="action" value="single">단일 백테스트</button>
                </div>
                <div class="form-group">
                    <button type="submit" name="action" value="optimize" class="secondary">시간 최적화</button>
                </div>
                <div class="form-group">
                    <button type="submit" name="action" value="bulk" class="secondary" style="background-color: #28a745; border-color: #28a745;">전체 세션 백테스트</button>
                </div>
                <div class="form-group">
                    <button type="submit" name="action" value="bulk_optimize" class="secondary" style="background-color: #17a2b8; border-color: #17a2b8;">전체 최적화</button>
                </div>
            </div>
        </form>

        {% if session_info %}
        <div class="session-info">
            <h3>선택된 거래 세션 정보</h3>
            <p><strong>종목:</strong> {{ session_info.ticker }}({{ session_info.name }})</p>
            <p><strong>급등일:</strong> {{ session_info.high_rise_date }}</p>
            <p><strong>매수시간:</strong> {{ current_values.buy_time_1 }} / {{ current_values.buy_time_2 }}</p>
            <p><strong>투자금액:</strong> {{ current_values.investment_amount }}원</p>
        </div>
        {% endif %}

        {% if result %}
        <h2>백테스트 결과</h2>
        
        {% if result is mapping and 'total_sessions' in result %}
        <!-- 전체 세션 백테스트 결과 -->
        <div class="stats-grid">
            <div class="stat-card">
                <h3>총 세션 수</h3>
                <p>{{ result.total_sessions }}개</p>
            </div>
            <div class="stat-card">
                <h3>성공 세션</h3>
                <p>{{ result.successful_sessions }}개</p>
            </div>
            <div class="stat-card">
                <h3>수익 세션</h3>
                <p>{{ result.profitable_sessions }}개</p>
            </div>
            <div class="stat-card">
                <h3>승률</h3>
                <p class="{{ 'profit' if result.win_rate > 50 else 'loss' }}">{{ '%.1f'|format(result.win_rate) }}%</p>
            </div>
            <div class="stat-card">
                <h3>전체 투자금액</h3>
                <p>{{ '{:,.0f}'.format(result.total_investment) }}원</p>
            </div>
            <div class="stat-card">
                <h3>전체 최종가치</h3>
                <p>{{ '{:,.0f}'.format(result.total_final_value) }}원</p>
            </div>
            <div class="stat-card">
                <h3>전체 수익률</h3>
                <p class="{{ 'profit' if result.total_profit_rate > 0 else 'loss' }}">{{ '%.2f'|format(result.total_profit_rate) }}%</p>
            </div>
            <div class="stat-card">
                <h3>평균 수익률</h3>
                <p class="{{ 'profit' if result.avg_profit_rate > 0 else 'loss' }}">{{ '%.2f'|format(result.avg_profit_rate) }}%</p>
            </div>
            <div class="stat-card">
                <h3>중앙 수익률</h3>
                <p class="{{ 'profit' if result.median_profit_rate > 0 else 'loss' }}">{{ '%.2f'|format(result.median_profit_rate) }}%</p>
            </div>
            <div class="stat-card">
                <h3>평균 최대손실률</h3>
                <p class="loss">{{ '%.2f'|format(result.avg_max_drawdown) }}%</p>
            </div>
            <div class="stat-card">
                <h3>평균 거래기간</h3>
                <p>{{ '%.1f'|format(result.avg_trade_duration) }}일</p>
            </div>
        </div>
        
        {% if result.best_result %}
        <h3>최고 수익 사례</h3>
        <div class="session-info">
            <p><strong>종목:</strong> {{ result.best_result.ticker }}({{ result.best_result.name }})</p>
            <p><strong>수익률:</strong> <span class="profit">{{ '%.2f'|format(result.best_result.profit_rate) }}%</span></p>
            <p><strong>손익:</strong> <span class="profit">{{ '{:+,.0f}'.format(result.best_result.profit_loss) }}원</span></p>
        </div>
        {% endif %}
        
        {% if result.worst_result %}
        <h3>최대 손실 사례</h3>
        <div class="session-info">
            <p><strong>종목:</strong> {{ result.worst_result.ticker }}({{ result.worst_result.name }})</p>
            <p><strong>수익률:</strong> <span class="loss">{{ '%.2f'|format(result.worst_result.profit_rate) }}%</span></p>
            <p><strong>손익:</strong> <span class="loss">{{ '{:+,.0f}'.format(result.worst_result.profit_loss) }}원</span></p>
        </div>
        {% endif %}
        
        {% else %}
        <!-- 단일 세션 백테스트 결과 -->
        <div class="stats-grid">
            <div class="stat-card">
                <h3>총 투자금액</h3>
                <p>{{ '{:,.0f}'.format(result.total_investment) }}원</p>
            </div>
            <div class="stat-card">
                <h3>최종 가치</h3>
                <p>{{ '{:,.0f}'.format(result.final_value) }}원</p>
            </div>
            <div class="stat-card">
                <h3>손익</h3>
                <p class="{{ 'profit' if result.profit_loss > 0 else 'loss' }}">{{ '{:+,.0f}'.format(result.profit_loss) }}원</p>
            </div>
            <div class="stat-card">
                <h3>수익률</h3>
                <p class="{{ 'profit' if result.profit_rate > 0 else 'loss' }}">{{ '%.2f'|format(result.profit_rate) }}%</p>
            </div>
            <div class="stat-card">
                <h3>거래 기간</h3>
                <p>{{ result.trade_duration_days }}일</p>
            </div>
            <div class="stat-card">
                <h3>최대 손실률</h3>
                <p class="loss">{{ '%.2f'|format(result.max_drawdown) }}%</p>
            </div>
        </div>
        {% endif %}
        {% endif %}

        {% if optimization_results %}
        <div class="optimization-results">
            <h2>매수 시간 최적화 결과</h2>
            <p>총 {{ optimization_results|length }}개 조합에 대한 백테스트 결과 (수익률 높은 순)</p>
            <table>
                <thead>
                    <tr>
                        <th>순위</th>
                        <th>매수시간1</th>
                        <th>매수시간2</th>
                        <th>수익률</th>
                        <th>손익</th>
                        <th>최대 손실률</th>
                        <th>거래기간</th>
                    </tr>
                </thead>
                <tbody>
                    {% for result in optimization_results[:10] %}
                    <tr>
                        <td>{{ loop.index }}</td>
                        <td>{{ result.buy_time_1 }}</td>
                        <td>{{ result.buy_time_2 }}</td>
                        <td class="{{ 'profit' if result.profit_rate > 0 else 'loss' }}">{{ '%.2f'|format(result.profit_rate) }}%</td>
                        <td class="{{ 'profit' if result.profit_loss > 0 else 'loss' }}">{{ '{:+,.0f}'.format(result.profit_loss) }}원</td>
                        <td class="loss">{{ '%.2f'|format(result.max_drawdown) }}%</td>
                        <td>{{ result.trade_duration_days }}일</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% endif %}

        {% if result and result.buy_records %}
        <h2>매수 기록</h2>
        <table>
            <thead>
                <tr>
                    <th>기록</th>
                    <th>매수 시각</th>
                    <th>매수 가격</th>
                    <th>수량</th>
                    <th>금액</th>
                </tr>
            </thead>
            <tbody>
                {% for buy in result.buy_records %}
                <tr>
                    <td>{{ buy.type }}</td>
                    <td>{{ buy.datetime.strftime('%Y-%m-%d %H:%M') }}</td>
                    <td>{{ '{:,.0f}'.format(buy.price) }}원</td>
                    <td>{{ '{:,.0f}'.format(buy.quantity) }}주</td>
                    <td>{{ '{:,.0f}'.format(buy.amount) }}원</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% endif %}

        {% if result and result.sell_records %}
        <h2>매도 기록</h2>
        <table>
            <thead>
                <tr>
                    <th>매도 시각</th>
                    <th>매도 가격</th>
                    <th>수량</th>
                    <th>금액</th>
                    <th>수익률</th>
                    <th>매도 사유</th>
                </tr>
            </thead>
            <tbody>
                {% for sell in result.sell_records %}
                <tr>
                    <td>{{ sell.datetime.strftime('%Y-%m-%d %H:%M') }}</td>
                    <td>{{ '{:,.0f}'.format(sell.price) }}원</td>
                    <td>{{ '{:,.0f}'.format(sell.quantity) }}주</td>
                    <td>{{ '{:,.0f}'.format(sell.amount) }}원</td>
                    <td class="{{ 'profit' if sell.profit_rate > 0 else 'loss' }}">{{ '%.2f'|format(sell.profit_rate) }}%</td>
                    <td>{{ sell.reason }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% endif %}
    </div>
</body>
</html>
'''

@app.route('/', methods=['GET', 'POST'])
def backtest():
    """백테스트 메인 페이지"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 백테스트 엔진으로 사용 가능한 세션 조회
    with BacktestEngine() as engine:
        available_sessions = engine.get_available_sessions()
    
    # 기본값 설정
    current_values = {
        'session_id': '',
        'buy_time_1': '10:40',
        'buy_time_2': '11:30',
        'investment_amount': '10,000,000'
    }
    
    result = None
    session_info = None
    optimization_results = None
    
    if request.method == 'POST':
        try:
            # 입력값 처리
            session_id = int(request.form.get('session_id', 0))
            buy_time_1 = request.form.get('buy_time_1', '10:40')
            buy_time_2 = request.form.get('buy_time_2', '11:30')
            investment_amount = int(request.form.get('investment_amount', '10000000').replace(',', ''))
            action = request.form.get('action', 'single')
            
            # 현재 입력값 저장
            current_values = {
                'session_id': str(session_id),
                'buy_time_1': buy_time_1,
                'buy_time_2': buy_time_2,
                'investment_amount': f"{investment_amount:,}"
            }
            
            # bulk 액션에서는 세션 ID 검증 건너뛰기
            if action not in ['bulk', 'bulk_optimize'] and session_id <= 0:
                raise ValueError("거래 세션을 선택해주세요.")
            
            # 선택된 세션 정보 조회 (단일 세션 전용)
            if action not in ['bulk', 'bulk_optimize']:
                session_info = next((s for s in available_sessions if s['trade_session_id'] == session_id), None)
            
            with BacktestEngine() as engine:
                if action == 'bulk':
                    # 전체 세션 백테스트 실행
                    logging.info(f"전체 세션 백테스트 시작 - 매수시간: {buy_time_1}/{buy_time_2}")
                    bulk_result = engine.run_bulk_backtest(
                        buy_time_1=buy_time_1,
                        buy_time_2=buy_time_2,
                        investment_amount=investment_amount
                    )
                    
                    if bulk_result:
                        # bulk_result를 result 대신 전달
                        result = bulk_result
                        session_info = {
                            'ticker': f"전체 {bulk_result['total_sessions']}개 세션",
                            'name': f"성공: {bulk_result['successful_sessions']}개",
                            'high_rise_date': f"수익: {bulk_result['profitable_sessions']}개"
                        }
                        logging.info(f"전체 백테스트 완료 - 평균수익률: {bulk_result['avg_profit_rate']:.2f}%")
                
                elif action == 'bulk_optimize':
                    # 전체 세션 매수시간 최적화 실행
                    logging.info(f"전체 세션 매수시간 최적화 시작")
                    limited_time_candidates = ["09:30", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30", "13:00"]
                    
                    bulk_optimization_results = engine.optimize_buy_times_for_all_sessions(
                        investment_amount=investment_amount,
                        time_candidates=limited_time_candidates
                    )
                    
                    if bulk_optimization_results:
                        # 최적화 결과를 optimization_results에 직접 대입
                        optimization_results = []
                        for bulk_res in bulk_optimization_results:
                            # dict를 BacktestResult 유사한 객체로 변환
                            class BulkResult:
                                def __init__(self, data):
                                    self.buy_time_1 = data['buy_time_1']
                                    self.buy_time_2 = data['buy_time_2']
                                    self.profit_rate = data['avg_profit_rate']
                                    self.profit_loss = data['total_profit_loss']
                                    self.max_drawdown = data['avg_max_drawdown']
                                    self.trade_duration_days = data['avg_trade_duration']
                            
                            optimization_results.append(BulkResult(bulk_res))
                        
                        # 최상의 결과를 메인 결과로 설정 (전체 세션 결과로)
                        best_bulk = bulk_optimization_results[0]
                        result = best_bulk  # dict 그대로 사용
                        session_info = {
                            'ticker': f"최적 조합: {best_bulk['buy_time_1']}/{best_bulk['buy_time_2']}",
                            'name': f"전체 {best_bulk['total_sessions']}개 세션",
                            'high_rise_date': f"성공: {best_bulk['successful_sessions']}개"
                        }
                        current_values['buy_time_1'] = best_bulk['buy_time_1']
                        current_values['buy_time_2'] = best_bulk['buy_time_2']
                        logging.info(f"전체 최적화 완료 - 최상 {best_bulk['buy_time_1']}/{best_bulk['buy_time_2']}: {best_bulk['avg_profit_rate']:.2f}%")
                
                elif action == 'optimize':
                    # 단일 세션 매수 시간 최적화 실행
                    logging.info(f"매수 시간 최적화 시작 - 세션 ID: {session_id}")
                    optimization_results = engine.optimize_buy_times(
                        trade_session_id=session_id,
                        investment_amount=investment_amount
                    )
                    logging.info(f"최적화 완료 - {len(optimization_results)}개 조합 테스트")
                    
                    # 최상의 결과를 메인 결과로 설정
                    if optimization_results:
                        result = optimization_results[0]
                        current_values['buy_time_1'] = result.buy_time_1
                        current_values['buy_time_2'] = result.buy_time_2
                
                else:
                    # 단일 백테스트 실행
                    logging.info(f"백테스트 시작 - 세션 ID: {session_id}, 매수시간: {buy_time_1}/{buy_time_2}")
                    result = engine.run_backtest(
                        trade_session_id=session_id,
                        buy_time_1=buy_time_1,
                        buy_time_2=buy_time_2,
                        investment_amount=investment_amount
                    )
                    logging.info(f"백테스트 완료 - 수익률: {result.profit_rate:.2f}%" if result else "백테스트 실패")
        
        except ValueError as e:
            logging.error(f"입력값 오류: {e}")
            result = None
        except Exception as e:
            logging.error(f"백테스트 실행 중 오류: {e}")
            result = None
    
    return render_template_string(
        HTML_TEMPLATE,
        available_sessions=available_sessions,
        current_values=current_values,
        result=result,
        session_info=session_info,
        optimization_results=optimization_results
    )

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001, debug=True)
