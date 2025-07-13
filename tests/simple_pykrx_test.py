# tests/simple_pykrx_test.py
import pandas as pd
from pykrx import stock
import traceback

def run_test():
    # 최근 영업일로 테스트 날짜를 지정합니다.
    # 2025년 7월 11일 (금요일)은 영업일일 가능성이 높습니다.
    test_date = "20240304"
    print(f"데이터 조회를 시도합니다: {test_date}")
    try:
        df = stock.get_market_price_change(test_date, market="KOSPI")
        print("데이터 조회 성공.")
        if df.empty:
            print("데이터프레임이 비어있습니다. 해당 날짜는 휴장일일 수 있습니다.")
        else:
            print("조회된 데이터:")
            print(df.head())
    except Exception as e:
        print("오류가 발생했습니다.")
        traceback.print_exc()

if __name__ == "__main__":
    run_test()
