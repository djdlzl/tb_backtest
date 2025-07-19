"""
일별분봉조회 테스트 모듈

한국투자증권 API를 사용하여 특정 종목의 일별 분봉 데이터를 조회하는 기능을 테스트합니다.
"""
import json
import requests
import pandas as pd
import sys
import os
from datetime import datetime, date
from zoneinfo import ZoneInfo

# 상위 디렉토리를 import path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.kis_api import KISApi

KST = ZoneInfo("Asia/Seoul")


class MinuteChartTest:
    """일별 분봉 조회 테스트를 위한 클래스"""
    
    @staticmethod
    def get_minute_chart(ticker, base_date, base_time):
        """
        특정 일자의 분봉 데이터를 조회합니다.
        
        Args:
            ticker (str): 종목 코드 (예: '217270')
            base_date (str): 기준 일자 (YYYYMMDD 형식)
            
        Returns:
            dict: 분봉 데이터
        """
        # KIS API 인스턴스 생성 및 분봉 데이터 요청
        kis_api = KISApi()
        print
        result = kis_api.get_minute_chart(ticker=ticker, date=base_date, time=base_time)
        
        if result:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        
        return result

    @staticmethod
    def get_minute_data(ticker, base_date, base_time):
        """
        특정 일자의 분봉 데이터를 조회하고 DataFrame으로 변환합니다.
        
        Args:
            ticker (str): 종목 코드 (예: '217270')
            base_date (str): 기준 일자 (YYYYMMDD 형식)
            
        Returns:
            pd.DataFrame: 분봉 데이터 DataFrame
        """
        result = MinuteChartTest.get_minute_chart(ticker, base_date, base_time)
        
        if not result or 'output2' not in result:
            print("데이터를 가져오는데 실패했거나 분봉 데이터가 없습니다.")
            return None
        
        # DataFrame으로 변환
        chart_data = result.get('output2', [])
        df = pd.DataFrame(chart_data)
        
        # 필요한 열만 선택하고 이름 변경
        if not df.empty:
            selected_columns = {
                'stck_cntg_hour': '시간',
                'stck_prpr': '현재가',
                'stck_oprc': '시가',
                'stck_hgpr': '고가',
                'stck_lwpr': '저가',
                'cntg_vol': '거래량',
                'acml_tr_pbmn': '거래대금'
            }
            
            # 존재하는 열만 선택
            existing_columns = {k: v for k, v in selected_columns.items() if k in df.columns}
            df = df[list(existing_columns.keys())]
            df = df.rename(columns=existing_columns)
            
            # 데이터 형식 변환
            numeric_columns = ['현재가', '시가', '고가', '저가', '거래량', '거래대금']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 시간 형식 변환 (HHMM -> HH:MM)
            if '시간' in df.columns:
                df['시간'] = df['시간'].apply(lambda x: f"{x[:2]}:{x[2:]}" if len(x) >= 4 else x)
                
            # 시간 기준으로 정렬
            df = df.sort_values(by='시간')
            
            return df
        else:
            print("분봉 데이터가 없습니다.")
            return None


def run_test():
    """테스트 실행 함수"""
    print("===== 일별분봉조회 테스트 시작 =====")
    
    # 7월 3일 이스트아시아홀딩스스(217270) 종목의 하루 분봉 조회
    ticker = "900110"  # 넵튠 종목코드
    base_time = "100000"
    base_date = "20250707"  # 7월 7일
    
    print(f"종목코드: {ticker}")
    print(f"조회일자: {base_date}")
    
    # 원시 API 응답 조회
    raw_data = MinuteChartTest.get_minute_chart(ticker, base_date, base_time)
    if raw_data:
        print("\n[API 응답 헤더]")
        if 'output2' in raw_data:
            for key, value in raw_data['output2'].items():
                print(f"{key}: {value}")
    
    # 분봉 데이터 DataFrame 조회
    df = MinuteChartTest.get_minute_data(ticker, base_date, base_time)
    if df is not None:
        print("\n[분봉 데이터]")
        print(df)
        
        # 요약 통계 출력
        print("\n[데이터 요약]")
        print(f"총 데이터 수: {len(df)}행")
        if '시간' in df.columns:
            print(f"거래 시간대: {df['시간'].iloc[0]} ~ {df['시간'].iloc[-1]}")
        if '현재가' in df.columns:
            print(f"종가: {df['현재가'].iloc[-1]:,}원")
            print(f"최고가: {df['고가'].max():,}원")
            print(f"최저가: {df['저가'].min():,}원")
        if '거래량' in df.columns:
            print(f"총 거래량: {df['거래량'].sum():,}주")
        if '거래대금' in df.columns:
            print(f"총 거래대금: {df['거래대금'].sum()/1000000:,.2f}백만원")
    
    print("\n===== 일별분봉조회 테스트 완료 =====")


if __name__ == "__main__":
    run_test()
