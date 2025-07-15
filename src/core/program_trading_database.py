#!/usr/bin/env python3
"""
파일 경로: src/core/program_trading_database.py

프로그램매매 데이터 전용 데이터베이스 서비스
- program_trading_db 스키마 관리
- 종목별 테이블 생성 (program_trading_XXXXXX)
- 1년치 데이터 완성도 체크
- 연속 요청 지원을 위한 데이터 추적
"""
import mysql.connector
from mysql.connector import Error as MySQLError
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime, timedelta
import calendar

logger = logging.getLogger(__name__)


class ProgramTradingDatabaseService:
    """프로그램매매 데이터 전용 데이터베이스 서비스"""

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

        # 프로그램매매 데이터 스키마
        self.program_schema = 'program_trading_db'

        # 1년치 데이터 기준 (평일 기준 약 250일)
        self.one_year_days = 250

        # 프로그램매매 데이터 필드 정의 (OPT90013 기반)
        self.program_fields = [
            '일자', '현재가', '대비기호', '전일대비', '등락율', '거래량',
            '프로그램매도금액', '프로그램매수금액', '프로그램순매수금액', '프로그램순매수금액증감',
            '프로그램매도수량', '프로그램매수수량', '프로그램순매수수량', '프로그램순매수수량증감',
            '기준가시간', '대차거래상환주수합', '잔고수주합', '거래소구분'
        ]

    def _get_connection(self) -> mysql.connector.MySQLConnection:
        """program_trading_db 스키마 연결 반환"""
        config = self.mysql_base_config.copy()
        config['database'] = self.program_schema
        return mysql.connector.connect(**config)

    def _get_main_connection(self) -> mysql.connector.MySQLConnection:
        """main 스키마 연결 반환 (stocks 테이블 조회용)"""
        config = self.mysql_base_config.copy()
        config['database'] = 'stock_trading_db'
        return mysql.connector.connect(**config)

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"프로그램매매 DB 연결 테스트 실패: {e}")
            return False

    def create_schema_if_not_exists(self) -> bool:
        """program_trading_db 스키마 생성"""
        try:
            # 스키마 없는 연결로 시작
            config = self.mysql_base_config.copy()
            config.pop('database', None)  # database 키 제거

            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()

            # 스키마 생성
            cursor.execute(f"""
                CREATE DATABASE IF NOT EXISTS {self.program_schema}
                CHARACTER SET utf8mb4
                COLLATE utf8mb4_unicode_ci
            """)

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"프로그램매매 스키마 '{self.program_schema}' 준비 완료")
            return True

        except Exception as e:
            logger.error(f"프로그램매매 스키마 생성 실패: {e}")
            return False

    def get_all_stock_codes(self) -> List[Dict[str, Any]]:
        """stock_codes 테이블에서 모든 활성 종목 조회 (수급데이터와 동일한 방식)"""
        try:
            conn = self._get_main_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute("""
                SELECT code, name, market 
                FROM stock_codes 
                WHERE is_active = TRUE 
                ORDER BY code
            """)

            results = cursor.fetchall()
            cursor.close()
            conn.close()

            logger.info(f"활성 종목 조회 완료: {len(results)}개")
            return results

        except Exception as e:
            logger.error(f"종목 조회 실패: {e}")
            return []

    def table_exists(self, stock_code: str) -> bool:
        """종목별 프로그램매매 테이블 존재 여부 확인"""
        try:
            table_name = f"program_trading_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """, (self.program_schema, table_name))

            result = cursor.fetchone()
            exists = result[0] > 0 if result else False

            cursor.close()
            conn.close()

            return exists

        except Exception as e:
            logger.error(f"테이블 존재 확인 실패 {stock_code}: {e}")
            return False

    def create_program_trading_table(self, stock_code: str) -> bool:
        """종목별 프로그램매매 데이터 테이블 생성"""
        try:
            table_name = f"program_trading_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                date DATE NOT NULL COMMENT '거래일자',

                -- 주가 정보 (tr_codes.py 기준)
                current_price INT DEFAULT 0 COMMENT '현재가',
                price_change_sign VARCHAR(5) DEFAULT '' COMMENT '대비기호',
                price_change INT DEFAULT 0 COMMENT '전일대비',
                change_rate DECIMAL(6,3) DEFAULT 0 COMMENT '등락율',
                volume BIGINT DEFAULT 0 COMMENT '거래량',

                -- 프로그램매매 금액 (단위: 천원)
                program_sell_amount BIGINT DEFAULT 0 COMMENT '프로그램매도금액',
                program_buy_amount BIGINT DEFAULT 0 COMMENT '프로그램매수금액',
                program_net_amount BIGINT DEFAULT 0 COMMENT '프로그램순매수금액',
                program_net_amount_change BIGINT DEFAULT 0 COMMENT '프로그램순매수금액증감',

                -- 프로그램매매 수량 (단위: 주)
                program_sell_quantity BIGINT DEFAULT 0 COMMENT '프로그램매도수량',
                program_buy_quantity BIGINT DEFAULT 0 COMMENT '프로그램매수수량',
                program_net_quantity BIGINT DEFAULT 0 COMMENT '프로그램순매수수량',
                program_net_quantity_change BIGINT DEFAULT 0 COMMENT '프로그램순매수수량증감',

                -- 기타 필드 (tr_codes.py 기준)
                base_price_time VARCHAR(20) DEFAULT '' COMMENT '기준가시간',
                short_sell_return_stock VARCHAR(50) DEFAULT '' COMMENT '대차거래상환주수합',
                balance_stock VARCHAR(50) DEFAULT '' COMMENT '잔고수주합',
                exchange_type VARCHAR(10) DEFAULT '' COMMENT '거래소구분',

                -- 메타데이터
                data_source VARCHAR(20) DEFAULT 'OPT90013' COMMENT '데이터 소스',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',

                -- 인덱스
                UNIQUE KEY idx_date (date),
                KEY idx_created_at (created_at),
                KEY idx_program_net_amount (program_net_amount),
                KEY idx_program_net_quantity (program_net_quantity)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            COMMENT='프로그램매매 데이터 - {stock_code}'
            """

            cursor.execute(create_sql)
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"프로그램매매 테이블 생성 완료: {table_name}")
            return True

        except Exception as e:
            logger.error(f"프로그램매매 테이블 생성 실패 {stock_code}: {e}")
            return False

    def get_data_completeness_info(self, stock_code: str) -> Dict[str, Any]:
        """종목별 프로그램매매 데이터 완성도 정보 조회"""
        try:
            table_name = f"program_trading_{stock_code}"

            # 테이블이 없으면 빈 상태 반환
            if not self.table_exists(stock_code):
                return self._create_empty_completeness_info()

            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)

            # 데이터 완성도 쿼리
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    MIN(date) as oldest_date,
                    MAX(date) as newest_date,
                    COUNT(DISTINCT date) as unique_dates,
                    SUM(CASE WHEN date = CURDATE() THEN 1 ELSE 0 END) as today_records,
                    AVG(program_net_amount) as avg_net_amount,
                    AVG(program_net_quantity) as avg_net_quantity
                FROM {table_name}
                WHERE date >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)
            """)

            stats = cursor.fetchone()
            cursor.close()
            conn.close()

            if not stats or stats['total_records'] == 0:
                return self._create_empty_completeness_info()

            # 완성도 계산
            completion_rate = (stats['unique_dates'] / self.one_year_days) * 100
            is_complete = completion_rate >= 90.0  # 90% 이상이면 완성으로 간주

            return {
                'total_records': stats['total_records'],
                'unique_dates': stats['unique_dates'],
                'newest_date': stats['newest_date'].strftime('%Y%m%d') if stats['newest_date'] else '',
                'oldest_date': stats['oldest_date'].strftime('%Y%m%d') if stats['oldest_date'] else '',
                'is_complete': is_complete,
                'completion_rate': round(completion_rate, 1),
                'missing_days': max(0, self.one_year_days - stats['unique_dates']),
                'needs_update': not is_complete or stats['today_records'] == 0,
                'collection_mode': 'update' if is_complete else 'full',
                'avg_net_amount': int(stats['avg_net_amount'] or 0),
                'avg_net_quantity': int(stats['avg_net_quantity'] or 0)
            }

        except Exception as e:
            logger.error(f"프로그램매매 완성도 정보 조회 실패 {stock_code}: {e}")
            return self._create_empty_completeness_info()

    def _create_empty_completeness_info(self) -> Dict[str, Any]:
        """빈 완성도 정보 생성"""
        return {
            'total_records': 0,
            'unique_dates': 0,
            'newest_date': '',
            'oldest_date': '',
            'is_complete': False,
            'completion_rate': 0.0,
            'missing_days': self.one_year_days,
            'needs_update': True,
            'collection_mode': 'full',
            'avg_net_amount': 0,
            'avg_net_quantity': 0
        }

    def save_program_trading_data(self, stock_code: str, data_list: List[Dict[str, Any]]) -> int:
        """프로그램매매 데이터 저장 (중복 방지) - 디버그 강화"""
        try:
            if not data_list:
                print(f"   ⚠️ [{stock_code}] 저장할 데이터가 없음")
                return 0

            table_name = f"program_trading_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            print(f"   💾 [{stock_code}] 저장 시작: {len(data_list)}건")

            # INSERT ... ON DUPLICATE KEY UPDATE 사용 (모든 필드 포함)
            insert_sql = f"""
            INSERT INTO {table_name} (
                date, current_price, price_change_sign, price_change, change_rate, volume,
                program_sell_amount, program_buy_amount, program_net_amount, program_net_amount_change,
                program_sell_quantity, program_buy_quantity, program_net_quantity, program_net_quantity_change,
                base_price_time, short_sell_return_stock, balance_stock, exchange_type,
                data_source, created_at
            ) VALUES (
                %(date)s, %(current_price)s, %(price_change_sign)s, %(price_change)s, %(change_rate)s, %(volume)s,
                %(program_sell_amount)s, %(program_buy_amount)s, %(program_net_amount)s, %(program_net_amount_change)s,
                %(program_sell_quantity)s, %(program_buy_quantity)s, %(program_net_quantity)s, %(program_net_quantity_change)s,
                %(base_price_time)s, %(short_sell_return_stock)s, %(balance_stock)s, %(exchange_type)s,
                %(data_source)s, %(created_at)s
            ) ON DUPLICATE KEY UPDATE
                current_price = VALUES(current_price),
                price_change_sign = VALUES(price_change_sign),
                price_change = VALUES(price_change),
                change_rate = VALUES(change_rate),
                volume = VALUES(volume),
                program_sell_amount = VALUES(program_sell_amount),
                program_buy_amount = VALUES(program_buy_amount),
                program_net_amount = VALUES(program_net_amount),
                program_net_amount_change = VALUES(program_net_amount_change),
                program_sell_quantity = VALUES(program_sell_quantity),
                program_buy_quantity = VALUES(program_buy_quantity),
                program_net_quantity = VALUES(program_net_quantity),
                program_net_quantity_change = VALUES(program_net_quantity_change),
                base_price_time = VALUES(base_price_time),
                short_sell_return_stock = VALUES(short_sell_return_stock),
                balance_stock = VALUES(balance_stock),
                exchange_type = VALUES(exchange_type),
                data_source = VALUES(data_source),
                updated_at = CURRENT_TIMESTAMP
            """

            # 데이터 변환 및 저장
            saved_count = 0
            error_count = 0

            for i, data in enumerate(data_list):
                try:
                    # 날짜 형식 변환 (YYYYMMDD -> YYYY-MM-DD)
                    date_str = data.get('일자', '')
                    if not date_str:
                        print(f"   ⚠️ [{stock_code}] 데이터 {i}: 일자 필드 없음")
                        error_count += 1
                        continue

                    if len(date_str) == 8 and date_str.isdigit():
                        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    else:
                        print(f"   ⚠️ [{stock_code}] 데이터 {i}: 잘못된 날짜 형식 ({date_str})")
                        error_count += 1
                        continue

                    # 데이터 준비 (tr_codes.py 필드 기준)
                    insert_data = {
                        'date': formatted_date,
                        'current_price': data.get('current_price', 0),
                        'price_change_sign': data.get('price_change_sign', ''),
                        'price_change': data.get('price_change', 0),
                        'change_rate': data.get('change_rate', 0.0),
                        'volume': data.get('volume', 0),
                        'program_sell_amount': data.get('program_sell_amount', 0),
                        'program_buy_amount': data.get('program_buy_amount', 0),
                        'program_net_amount': data.get('program_net_amount', 0),
                        'program_net_amount_change': data.get('program_net_amount_change', 0),
                        'program_sell_quantity': data.get('program_sell_quantity', 0),
                        'program_buy_quantity': data.get('program_buy_quantity', 0),
                        'program_net_quantity': data.get('program_net_quantity', 0),
                        'program_net_quantity_change': data.get('program_net_quantity_change', 0),
                        'base_price_time': data.get('base_price_time', ''),
                        'short_sell_return_stock': data.get('short_sell_return_stock', ''),
                        'balance_stock': data.get('balance_stock', ''),
                        'exchange_type': data.get('exchange_type', ''),
                        'data_source': 'OPT90013',
                        'created_at': datetime.now()
                    }

                    # 첫 번째 데이터 샘플 로깅
                    if i == 0:
                        print(f"   📊 [{stock_code}] 첫 번째 저장 샘플:")
                        print(f"       날짜: {formatted_date}")
                        print(f"       현재가: {insert_data['current_price']}")
                        print(f"       프로그램순매수금액: {insert_data['program_net_amount']}")

                    cursor.execute(insert_sql, insert_data)

                    # affected_rows 확인 (INSERT=1, UPDATE=2, 변화없음=0)
                    affected_rows = cursor.rowcount
                    if affected_rows > 0:
                        saved_count += 1

                    # 디버그: 처음 몇 개 결과 출력
                    if i < 3:
                        action = "신규삽입" if affected_rows == 1 else "업데이트" if affected_rows == 2 else "변화없음"
                        print(f"   📝 [{stock_code}] 데이터 {i}: {formatted_date} - {action} (affected: {affected_rows})")

                except Exception as e:
                    print(f"   ❌ [{stock_code}] 데이터 {i} 저장 오류: {e}")
                    logger.warning(f"프로그램매매 데이터 저장 오류 {stock_code}: {e}")
                    error_count += 1
                    continue

            conn.commit()
            cursor.close()
            conn.close()

            print(f"   ✅ [{stock_code}] 저장 완료: {saved_count}건 성공, {error_count}건 오류")
            logger.info(f"프로그램매매 데이터 저장 완료 {stock_code}: {saved_count}건")
            return saved_count

        except Exception as e:
            print(f"   ❌ [{stock_code}] 저장 실패: {e}")
            logger.error(f"프로그램매매 데이터 저장 실패 {stock_code}: {e}")
            import traceback
            traceback.print_exc()
            return 0

    def get_latest_date(self, stock_code: str) -> Optional[str]:
        """종목별 최신 데이터 날짜 조회"""
        try:
            table_name = f"program_trading_{stock_code}"

            if not self.table_exists(stock_code):
                return None

            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                SELECT MAX(date) as latest_date 
                FROM {table_name}
            """)

            result = cursor.fetchone()
            cursor.close()
            conn.close()

            if result and result[0]:
                return result[0].strftime('%Y%m%d')
            return None

        except Exception as e:
            logger.error(f"최신 날짜 조회 실패 {stock_code}: {e}")
            return None

    def get_program_trading_summary(self) -> Dict[str, Any]:
        """프로그램매매 전체 요약 정보"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)

            # 테이블 목록 조회
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                AND table_name LIKE 'program_trading_%'
            """, (self.program_schema,))

            tables = cursor.fetchall()
            table_count = len(tables)

            if table_count == 0:
                return {
                    'total_tables': 0,
                    'total_records': 0,
                    'avg_completion_rate': 0.0,
                    'latest_update': None
                }

            # 전체 레코드 수 및 최신 업데이트 조회
            total_records = 0
            latest_update = None

            for table in tables:
                table_name = table['table_name']
                cursor.execute(f"""
                    SELECT COUNT(*) as count, MAX(updated_at) as latest 
                    FROM {table_name}
                """)

                result = cursor.fetchone()
                if result:
                    total_records += result['count']
                    if result['latest'] and (not latest_update or result['latest'] > latest_update):
                        latest_update = result['latest']

            cursor.close()
            conn.close()

            return {
                'total_tables': table_count,
                'total_records': total_records,
                'avg_completion_rate': 0.0,  # 개별 계산 필요
                'latest_update': latest_update
            }

        except Exception as e:
            logger.error(f"프로그램매매 요약 정보 조회 실패: {e}")
            return {
                'total_tables': 0,
                'total_records': 0,
                'avg_completion_rate': 0.0,
                'latest_update': None
            }

    def cleanup_old_data(self, stock_code: str, keep_days: int = 400) -> int:
        """오래된 프로그램매매 데이터 정리"""
        try:
            table_name = f"program_trading_{stock_code}"

            if not self.table_exists(stock_code):
                return 0

            conn = self._get_connection()
            cursor = conn.cursor()

            # 지정한 일수보다 오래된 데이터 삭제
            cursor.execute(f"""
                DELETE FROM {table_name}
                WHERE date < DATE_SUB(CURDATE(), INTERVAL %s DAY)
            """, (keep_days,))

            deleted_count = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"프로그램매매 오래된 데이터 정리 {stock_code}: {deleted_count}건 삭제")
            return deleted_count

        except Exception as e:
            logger.error(f"프로그램매매 데이터 정리 실패 {stock_code}: {e}")
            return 0


# 전역 서비스 인스턴스 (싱글톤 패턴)
_program_trading_db_service = None


def get_program_trading_database_service() -> ProgramTradingDatabaseService:
    """프로그램매매 데이터베이스 서비스 인스턴스 반환"""
    global _program_trading_db_service
    if _program_trading_db_service is None:
        _program_trading_db_service = ProgramTradingDatabaseService()
    return _program_trading_db_service


# 테스트 함수
if __name__ == "__main__":
    print("🚀 프로그램매매 데이터베이스 서비스 테스트")
    print("=" * 60)

    # 서비스 초기화
    service = get_program_trading_database_service()

    # 연결 테스트
    print("1️⃣ 연결 테스트...")
    if service.test_connection():
        print("✅ 연결 성공")
    else:
        print("❌ 연결 실패")

    # 스키마 생성 테스트
    print("\n2️⃣ 스키마 생성 테스트...")
    if service.create_schema_if_not_exists():
        print("✅ 스키마 생성 성공")
    else:
        print("❌ 스키마 생성 실패")

    # 테이블 생성 테스트
    print("\n3️⃣ 테이블 생성 테스트...")
    test_code = "005930"
    if service.create_program_trading_table(test_code):
        print(f"✅ 테이블 생성 성공: program_trading_{test_code}")
    else:
        print(f"❌ 테이블 생성 실패: program_trading_{test_code}")

    # 완성도 정보 테스트
    print("\n4️⃣ 완성도 정보 테스트...")
    info = service.get_data_completeness_info(test_code)
    print(f"📊 완성도 정보: {info}")

    # 요약 정보 테스트
    print("\n5️⃣ 요약 정보 테스트...")
    summary = service.get_program_trading_summary()
    print(f"📋 전체 요약: {summary}")

    print("\n✅ 프로그램매매 데이터베이스 서비스 테스트 완료!")