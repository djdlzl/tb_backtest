import os
from dotenv import load_dotenv

# .env 파일의 절대 경로를 찾아서 환경 변수를 불러옵니다.
# 이렇게 하면 어디서 스크립트를 실행하든 항상 .env 파일을 찾을 수 있습니다.
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)


DB_CONFIG = {
    'host': os.getenv('DB_HOST'),      # 데이터베이스 서버 주소
    'user': os.getenv('DB_USER'),         # 데이터베이스 사용자 이름
    'password': os.getenv('DB_PASS'),  # 데이터베이스 비밀번호
    'database': os.getenv('DB_NAME'),  # 사용할 데이터베이스 이름
    'port': os.getenv('DB_PORT'),            # MariaDB 기본 포트
    'auth_plugin': 'mysql_native_password',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_general_ci'
}