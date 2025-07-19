import sqlite3
import sys
import os

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from database.db_manager_upper import DatabaseManager

def migrate_data():
    """SQLite의 pykrx_upper_stocks 데이터를 MariaDB로 마이그레이션합니다."""
    # SQLite 연결
    sqlite_conn = sqlite3.connect('pykrx_temp.db')
    sqlite_cursor = sqlite_conn.cursor()

    # MariaDB 연결
    db_manager = DatabaseManager()

    try:
        # 1. MariaDB 연결 시 자동으로 테이블이 생성됩니다.

        # 2. SQLite에서 데이터 조회
        sqlite_cursor.execute('SELECT ticker, name, date, trade_condition FROM pykrx_upper_stocks')
        stocks_data = sqlite_cursor.fetchall()

        if not stocks_data:
            print("SQLite 데이터베이스에 마이그레이션할 데이터가 없습니다.")
            return

        # 3. MariaDB에 데이터 저장
        # 데이터를 딕셔너리 리스트로 변환
        stocks_to_save = [
            {'ticker': row[0], 'name': row[1], 'date': row[2], 'trade_condition': row[3]}
            for row in stocks_data
        ]
        
        # db_manager_upper.py에 MariaDB용 저장 함수를 추가해야 합니다.
        # 임시로 함수 이름을 save_pykrx_upper_stocks_to_mariadb로 가정합니다.
        db_manager.save_pykrx_upper_stocks(stocks_to_save)

        print(f"총 {len(stocks_data)}개의 데이터를 성공적으로 MariaDB로 마이그레이션했습니다.")

    except Exception as e:
        print(f"데이터 마이그레이션 중 오류 발생: {e}")
    finally:
        sqlite_conn.close()
        db_manager.close()

if __name__ == '__main__':
    migrate_data()
