#!/usr/bin/env python3
"""
파일 경로: src/core/mysql_database.py

MySQL 다중 스키마 데이터베이스 서비스
- 기존 SQLite 기능을 MySQL 다중 스키마로 대체
- collect_all_stocks.py, test_stock_info_collector.py 지원
"""
import mysql.connector
from mysql.connector import Error as MySQLError
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class MySQLMultiSchemaService:
    """MySQL 다중 스키마 데이터베이스 서비스"""

    def __init__(self):
        # MySQL 연결 기본 설정
        self.mysql_base_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'stock_user',
            'password': 'StockPass2025!',
            'charset': 'utf8mb4',
            'autocommit': False
        }

        # 스키마별 연결 설정
        self.schemas = {
            'main': 'stock_trading_db',  # stocks, collection_progress 등
            'daily': 'daily_prices_db',  # daily_prices_* 테이블들
            'supply': 'supply_demand_db',  # 향후 수급 데이터
            'minute': 'minute_data_db'  # 향후 분봉 데이터
        }

    def add_or_update_stock_info(self, stock_code: str, stock_data: Dict[str, Any]) -> bool:
        """종목 기본정보 추가 또는 업데이트"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor()

            # stock_data에서 필요한 필드 추출 및 기본값 설정
            data = {
                'code': stock_code,
                'name': stock_data.get('name', ''),
                'market': stock_data.get('market', ''),
                'current_price': stock_data.get('current_price', 0),
                'prev_day_diff': stock_data.get('prev_day_diff', 0),
                'change_rate': stock_data.get('change_rate', 0),
                'volume': stock_data.get('volume', 0),
                'open_price': stock_data.get('open_price', 0),
                'high_price': stock_data.get('high_price', 0),
                'low_price': stock_data.get('low_price', 0),
                'upper_limit': stock_data.get('upper_limit', 0),
                'lower_limit': stock_data.get('lower_limit', 0),
                'market_cap': stock_data.get('market_cap', 0),
                'market_cap_size': stock_data.get('market_cap_size', ''),
                'listed_shares': stock_data.get('listed_shares', 0),
                'per_ratio': stock_data.get('per_ratio', 0),
                'pbr_ratio': stock_data.get('pbr_ratio', 0),
                'data_source': stock_data.get('data_source', 'OPT10001'),
                'last_updated': datetime.now(),
                'is_active': stock_data.get('is_active', 1)
            }

            query = """
                REPLACE INTO stocks (
                    code, name, market, current_price, prev_day_diff, change_rate,
                    volume, open_price, high_price, low_price, upper_limit, lower_limit,
                    market_cap, market_cap_size, listed_shares, per_ratio, pbr_ratio,
                    data_source, last_updated, is_active
                ) VALUES (
                    %(code)s, %(name)s, %(market)s, %(current_price)s, %(prev_day_diff)s, %(change_rate)s,
                    %(volume)s, %(open_price)s, %(high_price)s, %(low_price)s, %(upper_limit)s, %(lower_limit)s,
                    %(market_cap)s, %(market_cap_size)s, %(listed_shares)s, %(per_ratio)s, %(pbr_ratio)s,
                    %(data_source)s, %(last_updated)s, %(is_active)s
                )
            """

            cursor.execute(query, data)
            conn.commit()
            conn.close()

            logger.info(f"종목 {stock_code} 정보 저장 성공")
            return True

        except Exception as e:
            logger.error(f"종목 {stock_code} 정보 저장 실패: {e}")
            return False

    def add_daily_price(self, stock_code: str, date: str, current_price: int,
                        volume: int = 0, trading_value: int = 0, start_price: int = 0,
                        high_price: int = 0, low_price: int = 0, prev_day_diff: int = 0,
                        change_rate: int = 0) -> bool:
        """일봉 데이터 추가 (종목별 테이블에 저장)"""
        try:
            # 종목별 테이블 생성 (필요시)
            table_name = f"daily_prices_{stock_code}"
            if not self._ensure_daily_table_exists(stock_code):
                return False

            conn = self._get_connection('daily')
            cursor = conn.cursor()

            query = f"""
                REPLACE INTO {table_name} (
                    stock_code, date, open_price, high_price, low_price, close_price,
                    volume, trading_value, prev_day_diff, change_rate
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            cursor.execute(query, (
                stock_code, date, start_price, high_price, low_price, current_price,
                volume, trading_value, prev_day_diff, change_rate
            ))

            conn.commit()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"일봉 데이터 저장 실패 ({stock_code}, {date}): {e}")
            return False

    def _ensure_daily_table_exists(self, stock_code: str) -> bool:
        """종목별 일봉 테이블 존재 확인 및 생성"""
        try:
            table_name = f"daily_prices_{stock_code}"

            conn = self._get_connection('daily')
            cursor = conn.cursor()

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10) NOT NULL,
                    date VARCHAR(8) NOT NULL,
                    open_price INT DEFAULT 0,
                    high_price INT DEFAULT 0,
                    low_price INT DEFAULT 0,
                    close_price INT DEFAULT 0,
                    volume BIGINT DEFAULT 0,
                    trading_value BIGINT DEFAULT 0,
                    prev_day_diff INT DEFAULT 0,
                    change_rate INT DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

                    UNIQUE KEY uk_stock_date (stock_code, date),
                    INDEX idx_date (date),
                    INDEX idx_close_price (close_price)
                ) ENGINE=InnoDB COMMENT='종목별 일봉 데이터'
            """)

            conn.commit()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"종목별 테이블 생성 실패 {stock_code}: {e}")
            return False

    def get_latest_daily_date(self, stock_code: str) -> str:
        """종목의 최신 일봉 데이터 날짜 조회"""
        try:
            table_name = f"daily_prices_{stock_code}"

            conn = self._get_connection('daily')
            cursor = conn.cursor()

            cursor.execute(f"SELECT MAX(date) FROM {table_name} WHERE stock_code = %s", (stock_code,))
            result = cursor.fetchone()

            conn.close()

            return result[0] if result and result[0] else ""

        except Exception as e:
            logger.error(f"최신 날짜 조회 실패 {stock_code}: {e}")
            return ""

    def get_daily_data_count(self, stock_code: str) -> int:
        """종목의 일봉 데이터 개수 조회"""
        try:
            table_name = f"daily_prices_{stock_code}"

            conn = self._get_connection('daily')
            cursor = conn.cursor()

            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE stock_code = %s", (stock_code,))
            result = cursor.fetchone()

            conn.close()

            return result[0] if result else 0

        except Exception as e:
            logger.error(f"데이터 개수 조회 실패 {stock_code}: {e}")
            return 0

    def _get_connection(self, schema_key: str = 'main'):
        """스키마별 MySQL 연결 반환"""
        config = self.mysql_base_config.copy()
        config['database'] = self.schemas[schema_key]
        return mysql.connector.connect(**config)

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            for schema_key, schema_name in self.schemas.items():
                conn = self._get_connection(schema_key)
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                conn.close()
            return True
        except Exception as e:
            logger.error(f"MySQL 연결 테스트 실패: {e}")
            return False

    def create_tables(self):
        """필요한 테이블들 생성"""
        try:
            # main 스키마에 기본 테이블들 생성
            main_conn = self._get_connection('main')
            main_cursor = main_conn.cursor()

            # stocks 테이블
            main_cursor.execute("""
                CREATE TABLE IF NOT EXISTS stocks (
                    code VARCHAR(10) PRIMARY KEY COMMENT '종목코드',
                    name VARCHAR(100) COMMENT '종목명',
                    market VARCHAR(10) COMMENT '시장구분',
                    current_price INT COMMENT '현재가',
                    prev_day_diff INT DEFAULT 0 COMMENT '전일대비',
                    change_rate INT DEFAULT 0 COMMENT '등락율',
                    volume BIGINT COMMENT '거래량',
                    open_price INT COMMENT '시가',
                    high_price INT COMMENT '고가',
                    low_price INT COMMENT '저가',
                    upper_limit INT COMMENT '상한가',
                    lower_limit INT COMMENT '하한가',
                    market_cap BIGINT COMMENT '시가총액',
                    market_cap_size VARCHAR(10) COMMENT '시가총액 규모',
                    listed_shares BIGINT COMMENT '상장주식수',
                    per_ratio DECIMAL(10,2) COMMENT 'PER',
                    pbr_ratio DECIMAL(10,2) COMMENT 'PBR',
                    data_source VARCHAR(20) DEFAULT 'OPT10001' COMMENT '데이터 출처',
                    last_updated DATETIME COMMENT '최종 업데이트',
                    is_active BOOLEAN DEFAULT TRUE COMMENT '활성 여부',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',

                    INDEX idx_market (market),
                    INDEX idx_active (is_active),
                    INDEX idx_updated (last_updated)
                ) ENGINE=InnoDB COMMENT='종목 기본정보'
            """)

            # collection_progress 테이블
            main_cursor.execute("""
                CREATE TABLE IF NOT EXISTS collection_progress (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10) NOT NULL COMMENT '종목코드',
                    stock_name VARCHAR(100) COMMENT '종목명',
                    status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending' COMMENT '수집상태',
                    attempt_count INT DEFAULT 0 COMMENT '시도횟수',
                    last_attempt_time DATETIME COMMENT '마지막 시도시간',
                    success_time DATETIME COMMENT '성공시간',
                    error_message TEXT COMMENT '오류 메시지',
                    data_count INT DEFAULT 0 COMMENT '수집된 데이터 수',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                    UNIQUE KEY uk_stock_code (stock_code),
                    INDEX idx_status (status),
                    INDEX idx_attempt_time (last_attempt_time)
                ) ENGINE=InnoDB COMMENT='수집 진행상황'
            """)

            main_conn.commit()
            main_conn.close()

            logger.info("MySQL 테이블 생성 완료")

        except Exception as e:
            logger.error(f"테이블 생성 실패: {e}")
            raise

    def get_table_info(self) -> Dict[str, int]:
        """테이블별 레코드 수 반환"""
        info = {}

        try:
            # main 스키마 테이블들
            main_conn = self._get_connection('main')
            main_cursor = main_conn.cursor()

            main_cursor.execute("SELECT COUNT(*) FROM stocks")
            info['stocks'] = main_cursor.fetchone()[0]

            main_cursor.execute("SELECT COUNT(*) FROM collection_progress")
            info['collection_progress'] = main_cursor.fetchone()[0]

            main_conn.close()

            # daily 스키마 테이블 수
            daily_conn = self._get_connection('daily')
            daily_cursor = daily_conn.cursor()

            daily_cursor.execute("SHOW TABLES LIKE 'daily_prices_%'")
            daily_tables = daily_cursor.fetchall()
            info['daily_tables'] = len(daily_tables)

            daily_conn.close()

        except Exception as e:
            logger.error(f"테이블 정보 조회 실패: {e}")

        return info

    # stocks 테이블 관련 메서드들
    def save_stock_info(self, stock_data: Dict[str, Any]) -> bool:
        """종목 기본정보 저장"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor()

            query = """
                REPLACE INTO stocks (
                    code, name, market, current_price, prev_day_diff, change_rate,
                    volume, open_price, high_price, low_price, upper_limit, lower_limit,
                    market_cap, market_cap_size, listed_shares, per_ratio, pbr_ratio,
                    data_source, last_updated, is_active
                ) VALUES (
                    %(code)s, %(name)s, %(market)s, %(current_price)s, %(prev_day_diff)s, %(change_rate)s,
                    %(volume)s, %(open_price)s, %(high_price)s, %(low_price)s, %(upper_limit)s, %(lower_limit)s,
                    %(market_cap)s, %(market_cap_size)s, %(listed_shares)s, %(per_ratio)s, %(pbr_ratio)s,
                    %(data_source)s, %(last_updated)s, %(is_active)s
                )
            """

            cursor.execute(query, stock_data)
            conn.commit()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"종목정보 저장 실패: {e}")
            return False

    def get_stock_info(self, stock_code: str) -> List[Dict[str, Any]]:
        """종목 기본정보 조회"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            cursor.execute("SELECT * FROM stocks WHERE code = %s", (stock_code,))
            result = cursor.fetchall()

            conn.close()
            return result

        except Exception as e:
            logger.error(f"종목정보 조회 실패: {e}")
            return []

    # daily_prices 관련 메서드들
    def save_daily_price_data(self, stock_code: str, daily_data: List[Dict[str, Any]]) -> bool:
        """일봉 데이터 저장 (daily_prices_db 스키마)"""
        if not daily_data:
            return True

        try:
            conn = self._get_connection('daily')
            cursor = conn.cursor()

            # 테이블 생성 (존재하지 않는 경우)
            table_name = f"daily_prices_{stock_code}"
            self._create_daily_price_table(cursor, table_name)

            # 데이터 삽입
            query = f"""
                INSERT IGNORE INTO {table_name} (
                    date, open_price, high_price, low_price, close_price,
                    volume, trading_value, prev_day_diff, change_rate,
                    data_source, created_at
                ) VALUES (
                    %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
                    %(volume)s, %(trading_value)s, %(prev_day_diff)s, %(change_rate)s,
                    %(data_source)s, %(created_at)s
                )
            """

            cursor.executemany(query, daily_data)
            conn.commit()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"일봉 데이터 저장 실패 ({stock_code}): {e}")
            return False

    def _create_daily_price_table(self, cursor, table_name: str):
        """일봉 데이터 테이블 생성"""
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                date VARCHAR(8) NOT NULL COMMENT '일자(YYYYMMDD)',
                open_price INT COMMENT '시가',
                high_price INT COMMENT '고가',
                low_price INT COMMENT '저가',
                close_price INT COMMENT '종가/현재가',
                volume BIGINT COMMENT '거래량',
                trading_value BIGINT COMMENT '거래대금',
                prev_day_diff INT DEFAULT 0 COMMENT '전일대비',
                change_rate INT DEFAULT 0 COMMENT '등락율',
                data_source VARCHAR(20) DEFAULT 'OPT10081' COMMENT '데이터 출처',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',

                UNIQUE KEY uk_date (date),
                INDEX idx_date (date),
                INDEX idx_close_price (close_price),
                INDEX idx_volume (volume)
            ) ENGINE=InnoDB COMMENT='종목 {table_name.replace("daily_prices_", "")} 일봉 데이터'
        """)

    def get_daily_price_data(self, stock_code: str, start_date: str = None, end_date: str = None) -> List[
        Dict[str, Any]]:
        """일봉 데이터 조회"""
        try:
            conn = self._get_connection('daily')
            cursor = conn.cursor(dictionary=True)

            table_name = f"daily_prices_{stock_code}"

            # 테이블 존재 확인
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if not cursor.fetchone():
                conn.close()
                return []

            query = f"SELECT * FROM {table_name}"
            params = []

            if start_date and end_date:
                query += " WHERE date BETWEEN %s AND %s"
                params = [start_date, end_date]
            elif start_date:
                query += " WHERE date >= %s"
                params = [start_date]
            elif end_date:
                query += " WHERE date <= %s"
                params = [end_date]

            query += " ORDER BY date DESC"

            cursor.execute(query, params)
            result = cursor.fetchall()

            conn.close()
            return result

        except Exception as e:
            logger.error(f"일봉 데이터 조회 실패 ({stock_code}): {e}")
            return []

    # collection_progress 관련 메서드들
    def initialize_collection_progress(self, stock_codes_with_names: List[Tuple[str, str]]) -> bool:
        """수집 진행상황 초기화"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor()

            # 기존 데이터 삭제
            cursor.execute("DELETE FROM collection_progress")

            # 새 데이터 삽입
            query = """
                INSERT INTO collection_progress (stock_code, stock_name, status)
                VALUES (%s, %s, 'pending')
            """

            cursor.executemany(query, stock_codes_with_names)
            conn.commit()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"수집 진행상황 초기화 실패: {e}")
            return False

    def update_collection_progress(self, stock_code: str, status: str,
                                   error_message: str = None, data_count: int = None) -> bool:
        """수집 진행상황 업데이트"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor()

            update_fields = ["status = %s", "attempt_count = attempt_count + 1", "last_attempt_time = NOW()"]
            params = [status]

            if status == 'completed':
                update_fields.append("success_time = NOW()")
                if data_count is not None:
                    update_fields.append("data_count = %s")
                    params.append(data_count)

            if error_message:
                update_fields.append("error_message = %s")
                params.append(error_message)

            params.append(stock_code)

            query = f"""
                UPDATE collection_progress 
                SET {', '.join(update_fields)}
                WHERE stock_code = %s
            """

            cursor.execute(query, params)
            conn.commit()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"수집 진행상황 업데이트 실패: {e}")
            return False

    def get_collection_status_summary(self) -> Dict[str, Any]:
        """수집 상태 요약"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            # 전체 통계
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_stocks,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing
                FROM collection_progress
            """)

            summary = cursor.fetchone()

            # 성공률 계산
            if summary['total_stocks'] > 0:
                summary['success_rate'] = (summary['completed'] / summary['total_stocks']) * 100
            else:
                summary['success_rate'] = 0

            # 상태별 세부 정보
            cursor.execute("""
                SELECT status, COUNT(*) as count
                FROM collection_progress
                GROUP BY status
            """)

            status_breakdown = {row['status']: row['count'] for row in cursor.fetchall()}
            summary['status_breakdown'] = status_breakdown

            conn.close()
            return summary

        except Exception as e:
            logger.error(f"수집 상태 요약 조회 실패: {e}")
            return {}

    def get_pending_stocks(self) -> List[str]:
        """미완료 종목 목록 조회"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor()

            cursor.execute("""
                SELECT stock_code 
                FROM collection_progress 
                WHERE status IN ('pending', 'failed')
                ORDER BY stock_code
            """)

            result = [row[0] for row in cursor.fetchall()]
            conn.close()

            return result

        except Exception as e:
            logger.error(f"미완료 종목 조회 실패: {e}")
            return []

    def get_failed_stocks(self, max_attempts: int = 3) -> List[Dict[str, Any]]:
        """실패한 종목 목록 조회"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            cursor.execute("""
                SELECT stock_code, stock_name, attempt_count, error_message
                FROM collection_progress 
                WHERE status = 'failed' AND attempt_count >= %s
                ORDER BY stock_code
            """, (max_attempts,))

            result = cursor.fetchall()
            conn.close()

            return result

        except Exception as e:
            logger.error(f"실패 종목 조회 실패: {e}")
            return []

    # 🔧 src/core/database.py 확장 코드 (기존 코드 끝에 추가)

    # ================================
    # 🆕 비동기 stock_info 업데이트를 위한 확장 메서드들
    # ================================

    def get_active_stock_codes(self) -> List[Dict[str, Any]]:
        """stock_codes 테이블에서 활성 종목 조회"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            query = """
                SELECT code, name, market 
                FROM stock_codes 
                WHERE is_active = TRUE 
                  AND LENGTH(code) = 6 
                  AND code REGEXP '^[0-9]{6}$'
                ORDER BY market, code
            """

            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            conn.close()

            logger.info(f"✅ 활성 종목 조회 완료: {len(result):,}개")
            return result

        except Exception as e:
            logger.error(f"❌ 활성 종목 조회 실패: {e}")
            return []

    def get_active_stock_codes_by_market(self, market: str) -> List[Dict[str, Any]]:
        """시장별 활성 종목 조회"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            query = """
                SELECT code, name, market 
                FROM stock_codes 
                WHERE is_active = TRUE 
                  AND market = %s
                  AND LENGTH(code) = 6 
                  AND code REGEXP '^[0-9]{6}$'
                ORDER BY code
            """

            cursor.execute(query, (market,))
            result = cursor.fetchall()
            cursor.close()
            conn.close()

            logger.info(f"✅ {market} 활성 종목 조회 완료: {len(result):,}개")
            return result

        except Exception as e:
            logger.error(f"❌ {market} 활성 종목 조회 실패: {e}")
            return []

    def upsert_stock_info(self, stock_code: str, stock_data: Dict[str, Any]) -> bool:
        """stocks 테이블에 INSERT OR UPDATE (UPSERT) 처리"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor()

            current_time = datetime.now()

            # INSERT ON DUPLICATE KEY UPDATE 사용
            query = """
                INSERT INTO stocks (
                    code, name, market, current_price, prev_day_diff, 
                    change_rate, volume, open_price, high_price, low_price,
                    upper_limit, lower_limit, market_cap, market_cap_size,
                    listed_shares, per_ratio, pbr_ratio, data_source,
                    last_updated, is_active, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    current_price = VALUES(current_price),
                    prev_day_diff = VALUES(prev_day_diff),
                    change_rate = VALUES(change_rate),
                    volume = VALUES(volume),
                    open_price = VALUES(open_price),
                    high_price = VALUES(high_price),
                    low_price = VALUES(low_price),
                    upper_limit = VALUES(upper_limit),
                    lower_limit = VALUES(lower_limit),
                    market_cap = VALUES(market_cap),
                    market_cap_size = VALUES(market_cap_size),
                    listed_shares = VALUES(listed_shares),
                    per_ratio = VALUES(per_ratio),
                    pbr_ratio = VALUES(pbr_ratio),
                    data_source = VALUES(data_source),
                    last_updated = VALUES(last_updated),
                    is_active = TRUE,
                    updated_at = VALUES(updated_at)
            """

            # 시장 정보 추론 (stock_codes에서 조회)
            market = self._get_stock_market(stock_code)

            values = (
                stock_code,
                stock_data.get('name', ''),
                market,
                stock_data.get('current_price', 0),
                stock_data.get('prev_day_diff', 0),
                stock_data.get('change_rate', 0),
                stock_data.get('volume', 0),
                stock_data.get('open_price', 0),
                stock_data.get('high_price', 0),
                stock_data.get('low_price', 0),
                stock_data.get('upper_limit', 0),
                stock_data.get('lower_limit', 0),
                stock_data.get('market_cap', 0),
                stock_data.get('market_cap_size', ''),
                stock_data.get('listed_shares', 0),
                stock_data.get('per_ratio', 0.0),
                stock_data.get('pbr_ratio', 0.0),
                stock_data.get('data_source', 'OPT10001'),
                current_time,
                True,  # is_active
                current_time,  # created_at
                current_time  # updated_at
            )

            cursor.execute(query, values)
            conn.commit()

            # INSERT인지 UPDATE인지 확인
            is_new = cursor.rowcount == 1

            cursor.close()
            conn.close()

            action = "추가" if is_new else "업데이트"
            logger.debug(f"✅ {stock_code} stocks 테이블 {action} 완료")
            return True

        except Exception as e:
            logger.error(f"❌ {stock_code} stocks 테이블 UPSERT 실패: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False

    def _get_stock_market(self, stock_code: str) -> str:
        """stock_codes 테이블에서 종목의 시장 정보 조회"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor()

            cursor.execute("SELECT market FROM stock_codes WHERE code = %s", (stock_code,))
            result = cursor.fetchone()

            cursor.close()
            conn.close()

            return result[0] if result else 'UNKNOWN'

        except Exception as e:
            logger.debug(f"시장 정보 조회 실패 {stock_code}: {e}")
            return 'UNKNOWN'

    def batch_upsert_stock_info(self, stock_data_list: List[Tuple[str, Dict[str, Any]]]) -> Dict[str, int]:
        """stocks 테이블에 배치 UPSERT 처리"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor()

            current_time = datetime.now()
            stats = {'success': 0, 'failed': 0, 'new': 0, 'updated': 0}

            for stock_code, stock_data in stock_data_list:
                try:
                    # 개별 UPSERT 실행
                    success = self._single_upsert_stock_info(cursor, stock_code, stock_data, current_time)

                    if success:
                        stats['success'] += 1
                        # 새 레코드인지 확인 (rowcount가 1이면 INSERT, 2이면 UPDATE)
                        if cursor.rowcount == 1:
                            stats['new'] += 1
                        else:
                            stats['updated'] += 1
                    else:
                        stats['failed'] += 1

                except Exception as e:
                    logger.error(f"❌ {stock_code} 개별 UPSERT 실패: {e}")
                    stats['failed'] += 1

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"✅ 배치 UPSERT 완료 - 성공: {stats['success']}, 실패: {stats['failed']}")
            logger.info(f"   📥 신규: {stats['new']}, 🔄 업데이트: {stats['updated']}")

            return stats

        except Exception as e:
            logger.error(f"❌ 배치 UPSERT 실패: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return {'success': 0, 'failed': len(stock_data_list), 'new': 0, 'updated': 0}

    def _single_upsert_stock_info(self, cursor, stock_code: str, stock_data: Dict[str, Any], current_time) -> bool:
        """단일 레코드 UPSERT (내부 메서드)"""
        try:
            query = """
                INSERT INTO stocks (
                    code, name, market, current_price, prev_day_diff, 
                    change_rate, volume, open_price, high_price, low_price,
                    upper_limit, lower_limit, market_cap, market_cap_size,
                    listed_shares, per_ratio, pbr_ratio, data_source,
                    last_updated, is_active, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    current_price = VALUES(current_price),
                    prev_day_diff = VALUES(prev_day_diff),
                    change_rate = VALUES(change_rate),
                    volume = VALUES(volume),
                    open_price = VALUES(open_price),
                    high_price = VALUES(high_price),
                    low_price = VALUES(low_price),
                    upper_limit = VALUES(upper_limit),
                    lower_limit = VALUES(lower_limit),
                    market_cap = VALUES(market_cap),
                    market_cap_size = VALUES(market_cap_size),
                    listed_shares = VALUES(listed_shares),
                    per_ratio = VALUES(per_ratio),
                    pbr_ratio = VALUES(pbr_ratio),
                    data_source = VALUES(data_source),
                    last_updated = VALUES(last_updated),
                    is_active = TRUE,
                    updated_at = VALUES(updated_at)
            """

            market = self._get_stock_market(stock_code)

            values = (
                stock_code,
                stock_data.get('name', ''),
                market,
                stock_data.get('current_price', 0),
                stock_data.get('prev_day_diff', 0),
                stock_data.get('change_rate', 0),
                stock_data.get('volume', 0),
                stock_data.get('open_price', 0),
                stock_data.get('high_price', 0),
                stock_data.get('low_price', 0),
                stock_data.get('upper_limit', 0),
                stock_data.get('lower_limit', 0),
                stock_data.get('market_cap', 0),
                stock_data.get('market_cap_size', ''),
                stock_data.get('listed_shares', 0),
                stock_data.get('per_ratio', 0.0),
                stock_data.get('pbr_ratio', 0.0),
                stock_data.get('data_source', 'OPT10001'),
                current_time,
                True,
                current_time,
                current_time
            )

            cursor.execute(query, values)
            return True

        except Exception as e:
            logger.error(f"❌ {stock_code} 단일 UPSERT 실패: {e}")
            return False

    def get_stocks_update_stats(self) -> Dict[str, Any]:
        """stocks 테이블 업데이트 통계 조회"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            # 전체 통계
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_stocks,
                    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) as active_stocks,
                    SUM(CASE WHEN last_updated >= CURDATE() THEN 1 ELSE 0 END) as today_updated,
                    MAX(last_updated) as last_update_time
                FROM stocks
            """)

            overall_stats = cursor.fetchone()

            # 시장별 통계
            cursor.execute("""
                SELECT 
                    market,
                    COUNT(*) as count,
                    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) as active_count,
                    SUM(CASE WHEN last_updated >= CURDATE() THEN 1 ELSE 0 END) as today_count
                FROM stocks
                GROUP BY market
                ORDER BY market
            """)

            market_stats = cursor.fetchall()

            cursor.close()
            conn.close()

            return {
                'overall': overall_stats,
                'by_market': market_stats,
                'retrieved_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"❌ stocks 테이블 통계 조회 실패: {e}")
            return {}

    def check_stock_exists(self, stock_code: str) -> bool:
        """특정 종목이 stocks 테이블에 존재하는지 확인"""
        try:
            conn = self._get_connection('main')
            cursor = conn.cursor()

            cursor.execute("SELECT 1 FROM stocks WHERE code = %s", (stock_code,))
            exists = cursor.fetchone() is not None

            cursor.close()
            conn.close()

            return exists

        except Exception as e:
            logger.error(f"❌ {stock_code} 존재 여부 확인 실패: {e}")
            return False

    def get_stocks_last_updated(self, stock_codes: List[str]) -> Dict[str, Optional[datetime]]:
        """여러 종목의 마지막 업데이트 시간 조회"""
        try:
            if not stock_codes:
                return {}

            conn = self._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            # IN 절을 위한 플레이스홀더
            placeholders = ','.join(['%s'] * len(stock_codes))
            query = f"""
                SELECT code, last_updated 
                FROM stocks 
                WHERE code IN ({placeholders})
            """

            cursor.execute(query, stock_codes)
            results = cursor.fetchall()

            cursor.close()
            conn.close()

            # 딕셔너리로 변환
            update_times = {}
            for row in results:
                update_times[row['code']] = row['last_updated']

            # 요청한 모든 종목에 대해 결과 보장 (없는 종목은 None)
            for code in stock_codes:
                if code not in update_times:
                    update_times[code] = None

            return update_times

        except Exception as e:
            logger.error(f"❌ 마지막 업데이트 시간 조회 실패: {e}")
            return {}

    # ================================
    # 🆕 비동기 지원을 위한 헬퍼 메서드들
    # ================================

    async def get_active_stock_codes_async(self) -> List[Dict[str, Any]]:
        """비동기 활성 종목 조회 (스레드 풀 사용)"""
        import asyncio

        # CPU 바운드 작업을 스레드 풀에서 실행
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.get_active_stock_codes)

    async def upsert_stock_info_async(self, stock_code: str, stock_data: Dict[str, Any]) -> bool:
        """비동기 stock 정보 UPSERT"""
        import asyncio

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.upsert_stock_info, stock_code, stock_data)

    async def batch_upsert_stock_info_async(self, stock_data_list: List[Tuple[str, Dict[str, Any]]]) -> Dict[
        str, int]:
        """비동기 배치 UPSERT"""
        import asyncio

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.batch_upsert_stock_info, stock_data_list)

    # ================================
    # 🔧 기존 save_stock_info 메서드와의 호환성 유지
    # ================================

    def save_stock_info(self, stock_code: str, stock_data: Dict[str, Any]) -> bool:
        """기존 save_stock_info 메서드 (하위 호환성 유지)"""
        # 기존 코드와 호환성을 위해 upsert_stock_info를 호출
        return self.upsert_stock_info(stock_code, stock_data)

    # ================================
    # 🆕 성능 최적화를 위한 커넥션 풀링 준비
    # ================================

    def get_connection_pool_status(self) -> Dict[str, Any]:
        """커넥션 풀 상태 확인 (향후 확장용)"""
        try:
            # 현재는 단순 연결 테스트만 수행
            test_results = {}

            for schema_key in self.schemas.keys():
                try:
                    conn = self._get_connection(schema_key)
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    conn.close()
                    test_results[schema_key] = 'connected'
                except Exception as e:
                    test_results[schema_key] = f'error: {e}'

            return {
                'status': 'ok',
                'schemas': test_results,
                'checked_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"❌ 커넥션 풀 상태 확인 실패: {e}")
            return {'status': 'error', 'message': str(e)}


# 기존 함수들과의 호환성을 위한 함수들
def get_database_manager():
    """데이터베이스 매니저 반환 (호환성)"""
    return MySQLMultiSchemaService()


def get_database_service():
    """데이터베이스 서비스 반환 (호환성)"""
    return MySQLMultiSchemaService()


# 호환성을 위한 별칭들
DatabaseService = MySQLMultiSchemaService
DatabaseManager = MySQLMultiSchemaService


# CollectionProgress 클래스 (호환성)
class CollectionProgress:
    def __init__(self):
        self.db_service = MySQLMultiSchemaService()

    def update_progress(self, stock_code: str, status: str, **kwargs):
        return self.db_service.update_collection_progress(stock_code, status, **kwargs)