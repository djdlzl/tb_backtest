from flask import Flask, render_template_string, request
import pandas as pd
from backtest import Backtester
from datetime import datetime, timedelta

app = Flask(__name__)

HTML_TEMPLATE = '''
<!doctype html>
<html lang="ko">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <title>백테스트 결과</title>
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
        form { background-color: #e9ecef; padding: 1.5rem; border-radius: 8px; margin-bottom: 2rem; display: flex; flex-wrap: wrap; gap: 1rem; align-items: center; }
        form div { display: flex; flex-direction: column; }
        form label { margin-bottom: 0.5rem; font-weight: bold; }
        form input, form button { padding: 0.5rem; border-radius: 4px; border: 1px solid #ced4da; }
        form button { background-color: #007bff; color: white; border-color: #007bff; cursor: pointer; }
        form button:hover { background-color: #0056b3; }
    </style>
</head>
<body>
    <div class="container">
        <h1>백테스트 결과</h1>
        
        <form method="post">
            <div>
                <label for="start_date">시작일:</label>
                <input type="date" id="start_date" name="start_date" value="{{ current_values.start_date }}">
            </div>
            <div>
                <label for="end_date">종료일:</label>
                <input type="date" id="end_date" name="end_date" value="{{ current_values.end_date }}">
            </div>
            <div>
                <label for="initial_capital">초기 자본:</label>
                <input type="text" id="initial_capital" name="initial_capital" value="{{ current_values.initial_capital }}">
            </div>
            <div>
                <label for="excluded_tickers">제외 종목 (종목명 또는 티커, 쉼표로 구분):</label>
                <input type="text" id="excluded_tickers" name="excluded_tickers" value="{{ current_values.excluded_tickers }}" placeholder="삼성전자, 005930" size="30">
            </div>
            <button type="submit">조회</button>
        </form>

        <h2>요약</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <h3>총 거래 횟수</h3>
                <p>{{ stats.total_trades | default(0) }}</p>
            </div>
            <div class="stat-card">
                <h3>승률</h3>
                <p class="{{ 'profit' if stats.win_rate|default(0) >= 50 else 'loss' }}">{{ '%.2f'|format(stats.win_rate|default(0)) }}%</p>
            </div>
            <div class="stat-card">
                <h3>기하평균 수익률/거래</h3>
                <p class="{{ 'profit' if stats.gmean_return_per_trade|default(0) > 0 else 'loss' }}">{{ '%.2f'|format(stats.gmean_return_per_trade|default(0) * 100) }}%</p>
            </div>
            <div class="stat-card">
                <h3>총 수익률</h3>
                <p class="{{ 'profit' if stats.total_return|default(0) > 0 else 'loss' }}">{{ '%.2f'|format(stats.total_return|default(0) * 100) }}%</p>
            </div>
            <div class="stat-card">
                <h3>초기 자본</h3>
                <p>{{ '{:,.0f}'.format(stats.initial_capital|default(0)) }}원</p>
            </div>
            <div class="stat-card">
                <h3>최종 자본</h3>
                <p>{{ '{:,.0f}'.format(stats.final_capital|default(0)) }}원</p>
            </div>
        </div>

        <h2>월별 수익률</h2>
        <table>
            <thead>
                <tr>
                    <th>월</th>
                    <th>기하평균 수익률</th>
                </tr>
            </thead>
            <tbody>
                {% for row in monthly_data %}
                <tr>
                    <td>{{ row.sell_month }}</td>
                    <td class="{{ 'profit' if row.monthly_gmean_return > 0 else 'loss' }}">{{ '%.2f'|format(row.monthly_gmean_return * 100) }}%</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>

        <h2>거래 기록</h2>
        <table>
            <thead>
                <tr>
                    <th>종목명</th>
                    <th>매매 조건</th>
                    <th>매수 시각</th>
                    <th>매수 가격</th>
                    <th>매도 시각</th>
                    <th>매도 가격</th>
                    <th>수익률</th>
                    <th>매도 사유</th>
                </tr>
            </thead>
            <tbody>
                {% for row in data %}
                <tr>
                    <td>{{ row.name }}</td>
                    <td>{{ row.trade_condition }}</td>
                    <td>{{ row.buy_datetime }}</td>
                    <td>{{ '{:,.0f}'.format(row.buy_price) }}</td>
                    <td>{{ row.sell_datetime }}</td>
                    <td>{{ '{:,.0f}'.format(row.sell_price) }}</td>
                    <td class="{{ 'profit' if row.profit_rate > 0 else 'loss' }}">{{ '%.2f'|format(row.profit_rate * 100) }}%</td>
                    <td>{{ row.sell_reason }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</body>
</html>
'''

@app.route('/', methods=['GET', 'POST'])
def backtest():
    today = datetime.today()
    if request.method == 'POST':
        start_date_str = request.form.get('start_date')
        end_date_str = request.form.get('end_date')
        initial_capital = float(request.form.get('initial_capital', '10000000').replace(',', ''))
        excluded_tickers_str = request.form.get('excluded_tickers')
        excluded_tickers = [ticker.strip() for ticker in excluded_tickers_str.split(',')] if excluded_tickers_str else None
    else:  # GET 요청 시 기본값 사용
        start_date_str = (today - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date_str = today.strftime('%Y-%m-%d')
        initial_capital = 10000000
        excluded_tickers = None
        excluded_tickers_str = ''

    # Backtester는 YYYYMMDD 형식을 사용하므로 변환
    start_date_yyyymmdd = start_date_str.replace('-', '')
    end_date_yyyymmdd = end_date_str.replace('-', '')

    # 1. Backtester 인스턴스 생성
    backtester = Backtester(start_date_str=start_date_yyyymmdd, end_date_str=end_date_yyyymmdd)
    
    # 2. 백테스트 실행
    backtester.run_backtest(excluded_tickers_from_ui=excluded_tickers)

    # 3. 결과 분석
    results_df, stats, monthly_returns_df = backtester.analyze_results(
        initial_capital=initial_capital, 
        return_df=True, 
        start_date=start_date_str, 
        end_date=end_date_str,
        excluded_tickers=excluded_tickers
    )

    # 거래 기록이 없을 경우 stats가 비어있을 수 있으므로 기본값을 설정
    if not stats:
        stats = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'win_rate': 0,
            'gmean_return_per_trade': 0,
            'initial_capital': initial_capital,
            'final_capital': initial_capital,
            'total_return': 0
        }

    # 템플릿에 전달할 데이터 가공
    data = []
    monthly_data = []
    display_columns = ['name', 'trade_condition', 'buy_datetime', 'buy_price', 'sell_datetime', 'sell_price', 'profit_rate', 'sell_reason']

    if not results_df.empty:
        results_df['buy_datetime'] = pd.to_datetime(results_df['buy_datetime']).dt.strftime('%Y-%m-%d %H:%M')
        results_df['sell_datetime'] = pd.to_datetime(results_df['sell_datetime']).dt.strftime('%Y-%m-%d %H:%M')
        data = results_df[display_columns].to_dict('records')
    
    if not monthly_returns_df.empty:
        monthly_data = monthly_returns_df.to_dict('records')

    # 웹 페이지의 입력 필드에 현재 설정값을 유지하기 위한 값
    current_values = {
        'start_date': start_date_str,
        'end_date': end_date_str,
        'initial_capital': f"{initial_capital:,.0f}",
        'excluded_tickers': excluded_tickers_str if excluded_tickers_str is not None else ''
    }

    return render_template_string(HTML_TEMPLATE, stats=stats, data=data, monthly_data=monthly_data, current_values=current_values)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001, debug=True)
