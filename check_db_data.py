import sys
import os
import traceback

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_manager_upper import DatabaseManager
import pandas as pd

def check_latest_date(db_manager, table_name):
    """지정된 테이블의 가장 최신 날짜를 확인합니다."""
    try:
        db_manager.cursor.execute(f"SELECT MAX(date) FROM {table_name}")
        latest_date = db_manager.cursor.fetchone()
        
        if latest_date and latest_date['MAX(date)']:
            print(f"*** {table_name} 테이블의 최신 데이터 날짜: {latest_date['MAX(date)']} ***")
        else:
            print(f"*** {table_name} 테이블에 데이터가 없거나 날짜 정보가 없습니다. ***")
            
    except Exception as e:
        if "1146" in str(e): # MariaDB error code for "Table doesn't exist"
            print(f"*** {table_name} 테이블이 존재하지 않습니다. ***")
        else:
            print(f"*** {table_name} 테이블 최신 날짜 조회 중 오류 발생: ***")
            traceback.print_exc()

def check_table(db_manager, table_name):
    """지정된 테이블의 내용을 확인합니다."""
    try:
        # 데이터가 너무 많을 수 있으므로 최근 200개만 조회
        db_manager.cursor.execute(f"SELECT * FROM {table_name} ORDER BY date DESC LIMIT 200")
        data = db_manager.cursor.fetchall()
        
        if not data:
            print(f"\n--- {table_name} 테이블에 데이터가 없습니다. ---")
        else:
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 1000)
            df = pd.DataFrame(data)
            print(f"\n=============== {table_name} 테이블 데이터 (최신 200개) ===============")
            print(df)
            print("=" * 70)
            
    except Exception as e:
        # 테이블이 없는 경우 등 예외 처리
        if "1146" in str(e): # MariaDB error code for "Table doesn't exist"
            print(f"\n*** {table_name} 테이블이 존재하지 않습니다. ***")
        else:
            print(f"\n{table_name} 테이블 조회 중 오류 발생: {e}")

def check_all_data():
    """MariaDB의 주요 테이블 내용을 확인합니다.""" 
    db_manager = DatabaseManager()
    try:
        print("MariaDB의 주요 테이블 데이터 확인을 시작합니다...")
        print("-" * 40)
        check_latest_date(db_manager, "pykrx_upper_stocks")
        check_latest_date(db_manager, "selected_pykrx_upper_stocks")
        print("-" * 40)
        
        # 상세 데이터 확인이 필요할 경우 아래 주석을 해제하세요.
        # check_table(db_manager, "pykrx_upper_stocks")
        # check_table(db_manager, "selected_pykrx_upper_stocks")

    finally:
        db_manager.close()

if __name__ == '__main__':
    check_all_data()
