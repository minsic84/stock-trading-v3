"""
데이터 변환 유틸리티 (MySQL 다중 스키마 지원)
- OPT10001 기본정보 → daily_prices 테이블 형태 변환
- 당일 데이터 보완 로직
- 종목별 일봉 테이블 자동 생성 (MySQL)
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.database import get_database_service
from src.utils.trading_date import get_market_today

logger = logging.getLogger(__name__)


class DataConverter:
    """데이터 변환 및 테이블 관리 클래스 (MySQL 지원)"""

    def __init__(self):
        self.db_service = get_database_service()
        logger.info("데이터 변환기 초기화 완료")

    def create_daily_table_for_stock(self, stock_code: str) -> bool:
        """종목별 일봉 테이블 생성 (MySQL daily_prices_db 스키마에)"""
        try:
            table_name = f"daily_prices_{stock_code}"

            # MySQL daily_prices_db 스키마에 테이블 생성
            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            # 테이블 존재 여부 확인
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            if cursor.fetchone():
                logger.info(f"{stock_code}: 테이블 {table_name} 이미 존재")
                cursor.close()
                conn.close()
                return True

            # 종목별 일봉 테이블 생성 SQL
            create_sql = f"""
            CREATE TABLE {table_name} (
                id INT PRIMARY KEY AUTO_INCREMENT,
                date VARCHAR(8) NOT NULL COMMENT '일자(YYYYMMDD)',
                open_price INT COMMENT '시가',
                high_price INT COMMENT '고가', 
                low_price INT COMMENT '저가',
                close_price INT COMMENT '종가/현재가',
                volume BIGINT COMMENT '거래량',
                trading_value BIGINT COMMENT '거래대금',
                prev_day_diff INT DEFAULT 0 COMMENT '전일대비',
                change_rate INT DEFAULT 0 COMMENT '등락율(소수점2자리*100)',
                data_source VARCHAR(20) COMMENT '데이터 출처 (OPT10001/OPT10081)',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
                
                UNIQUE KEY idx_date (date),
                INDEX idx_close_price (close_price),
                INDEX idx_volume (volume),
                INDEX idx_created_at (created_at)
            ) ENGINE=InnoDB 
            CHARACTER SET utf8mb4 
            COLLATE utf8mb4_unicode_ci
            COMMENT='{stock_code} 종목 일봉 데이터'
            """

            cursor.execute(create_sql)
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"{stock_code}: 일봉 테이블 {table_name} 생성 완료")
            return True

        except Exception as e:
            logger.error(f"{stock_code}: 테이블 생성 실패 - {e}")
            return False

    def convert_stock_info_to_daily(self, stock_code: str) -> bool:
        """
        주식 기본정보(OPT10001)를 일봉 데이터로 변환하여 저장

        Args:
            stock_code: 종목코드

        Returns:
            bool: 변환 성공 여부
        """
        try:
            logger.info(f"{stock_code}: 기본정보 → 일봉 데이터 변환 시작")

            # 1. 테이블 생성 (필요시)
            if not self.create_daily_table_for_stock(stock_code):
                return False

            # 2. 기본정보에서 데이터 조회
            stock_info = self._get_stock_basic_info(stock_code)
            if not stock_info:
                logger.error(f"{stock_code}: 기본정보 조회 실패")
                return False

            # 3. 일봉 형태로 변환
            daily_data = self._convert_to_daily_format(stock_info)
            if not daily_data:
                logger.error(f"{stock_code}: 일봉 형태 변환 실패")
                return False

            # 4. 일봉 테이블에 저장
            success = self._save_daily_data(stock_code, daily_data)

            if success:
                logger.info(f"{stock_code}: 당일 데이터 변환 완료")
            else:
                logger.error(f"{stock_code}: 일봉 데이터 저장 실패")

            return success

        except Exception as e:
            logger.error(f"{stock_code}: 데이터 변환 중 오류 - {e}")
            return False

    def _get_stock_basic_info(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """stocks 테이블에서 기본정보 조회 (MySQL main 스키마에서)"""
        try:
            conn = self.db_service._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            cursor.execute("""
                SELECT code, name, current_price, prev_day_diff, change_rate,
                       volume, open_price, high_price, low_price
                FROM stocks 
                WHERE code = %s
            """, (stock_code,))

            result = cursor.fetchone()
            cursor.close()
            conn.close()

            if not result:
                logger.error(f"{stock_code}: stocks 테이블에 데이터 없음")
                return None

            return result

        except Exception as e:
            logger.error(f"{stock_code}: 기본정보 조회 실패 - {e}")
            return None

    def _convert_to_daily_format(self, stock_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """기본정보를 일봉 형태로 변환"""
        try:
            # 오늘 날짜 (YYYYMMDD 형태)
            today = get_market_today()
            date_str = today.strftime('%Y%m%d')

            # 일봉 데이터 형태로 변환
            daily_data = {
                'date': date_str,
                'open_price': stock_info.get('open_price', 0),
                'high_price': stock_info.get('high_price', 0),
                'low_price': stock_info.get('low_price', 0),
                'close_price': stock_info.get('current_price', 0),  # 현재가를 종가로
                'volume': stock_info.get('volume', 0),
                'trading_value': 0,  # 거래대금은 기본정보에 없음
                'prev_day_diff': stock_info.get('prev_day_diff', 0),
                'change_rate': stock_info.get('change_rate', 0),
                'data_source': 'OPT10001',
                'created_at': datetime.now()
            }

            logger.info(f"일봉 변환 완료: {date_str} - {daily_data['close_price']:,}원")
            return daily_data

        except Exception as e:
            logger.error(f"일봉 형태 변환 실패: {e}")
            return None

    def _save_daily_data(self, stock_code: str, daily_data: Dict[str, Any]) -> bool:
        """일봉 데이터를 종목별 테이블에 저장 (MySQL daily_prices_db 스키마에)"""
        try:
            table_name = f"daily_prices_{stock_code}"

            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            # INSERT ... ON DUPLICATE KEY UPDATE 쿼리 (MySQL 문법)
            insert_sql = f"""
            INSERT INTO {table_name} 
            (date, open_price, high_price, low_price, close_price, 
             volume, trading_value, prev_day_diff, change_rate, data_source, created_at)
            VALUES 
            (%(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
             %(volume)s, %(trading_value)s, %(prev_day_diff)s, %(change_rate)s, %(data_source)s, %(created_at)s)
            ON DUPLICATE KEY UPDATE
                open_price = VALUES(open_price),
                high_price = VALUES(high_price),
                low_price = VALUES(low_price),
                close_price = VALUES(close_price),
                volume = VALUES(volume),
                trading_value = VALUES(trading_value),
                prev_day_diff = VALUES(prev_day_diff),
                change_rate = VALUES(change_rate),
                data_source = VALUES(data_source),
                created_at = VALUES(created_at)
            """

            cursor.execute(insert_sql, daily_data)
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"{stock_code}: 일봉 데이터 저장 완료 - {daily_data['date']}")
            return True

        except Exception as e:
            logger.error(f"{stock_code}: 일봉 데이터 저장 실패 - {e}")
            return False


def get_data_converter() -> DataConverter:
    """데이터 변환기 인스턴스 반환 (편의 함수)"""
    return DataConverter()


# 편의 함수들
def create_daily_table(stock_code: str) -> bool:
    """종목별 일봉 테이블 생성 (편의 함수)"""
    return get_data_converter().create_daily_table_for_stock(stock_code)


def convert_today_data(stock_code: str) -> bool:
    """당일 데이터 변환 (편의 함수)"""
    return get_data_converter().convert_stock_info_to_daily(stock_code)