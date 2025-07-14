#!/usr/bin/env python3
"""
파일 경로: src/core/nxt_database.py

NXT 전용 데이터베이스 서비스
- stock_codes 테이블 기반 NXT 종목 관리
- daily_prices_db 스키마의 종목별 테이블 관리
- 기존 MySQLMultiSchemaService 활용하여 간결하게 구현
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from src.core.database import MySQLMultiSchemaService

logger = logging.getLogger(__name__)


class NXTDatabaseService:
    """NXT 전용 데이터베이스 서비스"""

    def __init__(self):
        """NXT 전용 DB 서비스 초기화"""
        self.db_service = MySQLMultiSchemaService()

    # ================================
    # NXT 종목 관리
    # ================================

    def get_nxt_stock_codes(self) -> List[str]:
        """NXT 종목 코드 리스트 조회"""
        try:
            conn = self.db_service._get_connection('main')
            cursor = conn.cursor()

            query = """
                SELECT code 
                FROM stock_codes 
                WHERE is_active = TRUE 
                ORDER BY code
            """

            cursor.execute(query)
            result = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()

            logger.info(f"✅ NXT 종목 조회 완료: {len(result)}개")
            return result

        except Exception as e:
            logger.error(f"❌ NXT 종목 조회 실패: {e}")
            return []

    def get_nxt_stock_info(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """NXT 특정 종목 정보 조회"""
        try:
            conn = self.db_service._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            query = """
                SELECT code, name, market, is_active, created_at, updated_at
                FROM stock_codes 
                WHERE code = %s
            """

            cursor.execute(query, (stock_code,))
            result = cursor.fetchone()
            cursor.close()
            conn.close()

            return result

        except Exception as e:
            logger.error(f"❌ {stock_code} 정보 조회 실패: {e}")
            return None

    def get_nxt_statistics(self) -> Dict[str, Any]:
        """NXT 종목 통계 정보"""
        try:
            conn = self.db_service._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            # 기본 통계
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_stocks,
                    COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_stocks,
                    COUNT(CASE WHEN market = 'KOSPI' THEN 1 END) as kospi_stocks,
                    COUNT(CASE WHEN market = 'KOSDAQ' THEN 1 END) as kosdaq_stocks
                FROM stock_codes
            """)

            stats = cursor.fetchone()
            cursor.close()
            conn.close()

            return stats or {}

        except Exception as e:
            logger.error(f"❌ NXT 통계 조회 실패: {e}")
            return {}

    # ================================
    # NXT 일봉 데이터 관리
    # ================================

    def daily_table_exists(self, stock_code: str) -> bool:
        """종목별 일봉 테이블 존재 여부 확인"""
        try:
            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            exists = cursor.fetchone() is not None

            cursor.close()
            conn.close()

            return exists

        except Exception as e:
            logger.error(f"❌ {stock_code} 테이블 존재 확인 실패: {e}")
            return False

    def get_daily_data_count(self, stock_code: str) -> int:
        """종목별 일봉 데이터 개수 조회"""
        try:
            if not self.daily_table_exists(stock_code):
                return 0

            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            return count

        except Exception as e:
            logger.error(f"❌ {stock_code} 데이터 개수 조회 실패: {e}")
            return 0

    def get_latest_date(self, stock_code: str) -> Optional[str]:
        """종목별 최신 데이터 날짜 조회"""
        try:
            if not self.daily_table_exists(stock_code):
                return None

            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"
            cursor.execute(f"SELECT MAX(date) FROM {table_name}")
            latest_date = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            return latest_date

        except Exception as e:
            logger.error(f"❌ {stock_code} 최신 날짜 조회 실패: {e}")
            return None

    def create_daily_table(self, stock_code: str) -> bool:
        """종목별 일봉 테이블 생성"""
        try:
            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"

            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
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
                    data_source VARCHAR(20) DEFAULT 'OPT10081' COMMENT '데이터 출처',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',

                    UNIQUE KEY idx_date (date),
                    INDEX idx_close_price (close_price),
                    INDEX idx_volume (volume)
                ) ENGINE=InnoDB 
                CHARACTER SET utf8mb4 
                COLLATE utf8mb4_unicode_ci
                COMMENT='{stock_code} 종목 일봉 데이터'
            """

            cursor.execute(create_sql)
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"✅ {stock_code} 테이블 생성 완료")
            return True

        except Exception as e:
            logger.error(f"❌ {stock_code} 테이블 생성 실패: {e}")
            return False

    def save_daily_data_batch(self, stock_code: str, daily_data: List[Dict[str, Any]],
                              replace_mode: bool = False, update_recent_only: bool = False) -> int:
        """일봉 데이터 배치 저장 (교체 모드 및 최근 데이터 업데이트 모드 지원)"""
        try:
            if not daily_data:
                return 0

            # 테이블 존재 확인 및 생성
            if not self.daily_table_exists(stock_code):
                if not self.create_daily_table(stock_code):
                    return 0

            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"

            # 모드별 처리
            if replace_mode:
                # 전체 교체 모드: 기존 데이터 삭제 후 전체 삽입
                cursor.execute(f"DELETE FROM {table_name}")
                logger.info(f"🔄 {stock_code} 전체 교체 모드: 기존 데이터 삭제")
                insert_mode = "INSERT"

            elif update_recent_only:
                # 최근 데이터 업데이트 모드: 중복 날짜는 교체, 새 날짜는 추가
                logger.info(f"🔄 {stock_code} 최근 데이터 업데이트 모드")
                insert_mode = "REPLACE"  # MySQL REPLACE INTO 사용

            else:
                # 일반 모드: 중복 시 무시
                insert_mode = "INSERT IGNORE"

            # 삽입 쿼리 결정
            if insert_mode == "INSERT":
                insert_sql = f"""
                    INSERT INTO {table_name} (
                        date, open_price, high_price, low_price, close_price,
                        volume, trading_value, prev_day_diff, change_rate,
                        data_source, created_at
                    ) VALUES (
                        %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
                        %(volume)s, %(trading_value)s, %(prev_day_diff)s, %(change_rate)s,
                        %(data_source)s, %(created_at)s
                    )
                """
            elif insert_mode == "REPLACE":
                insert_sql = f"""
                    REPLACE INTO {table_name} (
                        date, open_price, high_price, low_price, close_price,
                        volume, trading_value, prev_day_diff, change_rate,
                        data_source, created_at
                    ) VALUES (
                        %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
                        %(volume)s, %(trading_value)s, %(prev_day_diff)s, %(change_rate)s,
                        %(data_source)s, %(created_at)s
                    )
                """
            else:  # INSERT IGNORE
                insert_sql = f"""
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

            # 데이터 전처리
            processed_data = []
            for data in daily_data:
                processed_data.append({
                    'date': data.get('date', ''),
                    'open_price': data.get('open_price', 0),
                    'high_price': data.get('high_price', 0),
                    'low_price': data.get('low_price', 0),
                    'close_price': data.get('close_price', 0),
                    'volume': data.get('volume', 0),
                    'trading_value': data.get('trading_value', 0),
                    'prev_day_diff': data.get('prev_day_diff', 0),
                    'change_rate': data.get('change_rate', 0),
                    'data_source': data.get('data_source', 'OPT10081'),
                    'created_at': datetime.now()
                })

            # 배치 실행
            cursor.executemany(insert_sql, processed_data)
            conn.commit()

            saved_count = cursor.rowcount
            cursor.close()
            conn.close()

            mode_desc = "전체교체" if replace_mode else ("최근업데이트" if update_recent_only else "일반삽입")
            logger.info(f"✅ {stock_code} 일봉 데이터 저장 완료 ({mode_desc}): {saved_count}개")
            return saved_count

        except Exception as e:
            logger.error(f"❌ {stock_code} 일봉 데이터 저장 실패: {e}")
            return 0

    # ================================
    # NXT 수집 상태 관리
    # ================================

    def get_nxt_stocks_need_update(self) -> List[str]:
        """업데이트가 필요한 NXT 종목 리스트"""
        try:
            # 1. NXT 종목 조회
            conn_main = self.db_service._get_connection('main')
            cursor_main = conn_main.cursor()
            cursor_main.execute("SELECT code FROM stock_codes WHERE is_active = TRUE")
            nxt_codes = [row[0] for row in cursor_main.fetchall()]
            cursor_main.close()
            conn_main.close()

            need_update = []

            # 2. daily_prices_db에서 확인
            conn_daily = self.db_service._get_connection('daily')
            cursor_daily = conn_daily.cursor()

            for code in nxt_codes:
                try:
                    table_name = f"daily_prices_{code}"

                    # 테이블 존재 확인
                    cursor_daily.execute(f"SHOW TABLES LIKE '{table_name}'")
                    if not cursor_daily.fetchone():
                        need_update.append(code)
                        continue

                    # 데이터 개수 확인
                    cursor_daily.execute(f"SELECT COUNT(*) FROM {table_name}")
                    data_count = cursor_daily.fetchone()[0]
                    if data_count < 1000:  # 5년치 미만
                        need_update.append(code)
                        continue

                    # 최신 날짜 확인
                    cursor_daily.execute(f"SELECT MAX(date) FROM {table_name}")
                    latest_date = cursor_daily.fetchone()[0]
                    if latest_date:
                        try:
                            latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                            days_old = (datetime.now() - latest_dt).days
                            if days_old >= 3:  # 3일 이상 오래됨
                                need_update.append(code)
                        except:
                            need_update.append(code)
                    else:
                        need_update.append(code)

                except Exception:
                    need_update.append(code)

            cursor_daily.close()
            conn_daily.close()

            logger.info(f"✅ 업데이트 필요 종목: {len(need_update)}개 / 전체 {len(nxt_codes)}개")
            return need_update

        except Exception as e:
            logger.error(f"❌ 업데이트 필요 종목 조회 실패: {e}")
            return []

    def get_nxt_collection_status(self) -> Dict[str, Any]:
        """NXT 수집 현황 요약"""
        try:
            # 1. NXT 종목 조회
            conn_main = self.db_service._get_connection('main')
            cursor_main = conn_main.cursor()
            cursor_main.execute("SELECT code FROM stock_codes WHERE is_active = TRUE")
            nxt_codes = [row[0] for row in cursor_main.fetchall()]
            cursor_main.close()
            conn_main.close()

            total_count = len(nxt_codes)

            # 2. 완료 종목 확인 (daily_prices_db에서)
            conn_daily = self.db_service._get_connection('daily')
            cursor_daily = conn_daily.cursor()

            completed_count = 0
            total_records = 0

            for code in nxt_codes:
                try:
                    table_name = f"daily_prices_{code}"
                    # 테이블 존재 확인
                    cursor_daily.execute(f"SHOW TABLES LIKE '{table_name}'")
                    if cursor_daily.fetchone():
                        # 데이터 개수 확인
                        cursor_daily.execute(f"SELECT COUNT(*) FROM {table_name}")
                        data_count = cursor_daily.fetchone()[0]
                        total_records += data_count

                        if data_count >= 1000:  # 5년치 기준
                            completed_count += 1
                except Exception:
                    continue

            cursor_daily.close()
            conn_daily.close()

            return {
                'total_nxt_stocks': total_count,
                'completed_stocks': completed_count,
                'completion_rate': round(completed_count / total_count * 100, 1) if total_count > 0 else 0,
                'total_records': total_records,
                'need_update': total_count - completed_count,
                'checked_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"❌ NXT 수집 현황 조회 실패: {e}")
            return {}

    # ================================
    # 유틸리티 메서드
    # ================================

    def test_connection(self) -> bool:
        """NXT DB 연결 테스트"""
        try:
            # 1. stock_codes 접근 테스트
            conn_main = self.db_service._get_connection('main')
            cursor_main = conn_main.cursor()
            cursor_main.execute("SELECT COUNT(*) FROM stock_codes WHERE is_active = TRUE")
            nxt_count = cursor_main.fetchone()[0]
            cursor_main.close()
            conn_main.close()

            # 2. daily_prices_db 접근 테스트
            conn_daily = self.db_service._get_connection('daily')
            cursor_daily = conn_daily.cursor()
            cursor_daily.execute("SELECT 1")
            cursor_daily.fetchone()  # 결과 읽기
            cursor_daily.close()
            conn_daily.close()

            logger.info(f"✅ NXT DB 연결 테스트 성공 (NXT 종목: {nxt_count}개)")
            return True

        except Exception as e:
            logger.error(f"❌ NXT DB 연결 테스트 실패: {e}")
            return False


# 편의 함수들
def get_nxt_database_service():
    """NXT 데이터베이스 서비스 인스턴스 반환"""
    return NXTDatabaseService()


def test_nxt_database():
    """NXT 데이터베이스 서비스 테스트"""
    print("🧪 NXT 데이터베이스 서비스 테스트")
    print("=" * 50)

    nxt_db = NXTDatabaseService()

    # 연결 테스트
    if not nxt_db.test_connection():
        print("❌ 연결 테스트 실패")
        return False

    # NXT 종목 통계
    stats = nxt_db.get_nxt_statistics()
    print(f"📊 NXT 종목 통계:")
    print(f"   전체: {stats.get('total_stocks', 0)}개")
    print(f"   활성: {stats.get('active_stocks', 0)}개")
    print(f"   KOSPI: {stats.get('kospi_stocks', 0)}개")
    print(f"   KOSDAQ: {stats.get('kosdaq_stocks', 0)}개")

    # 수집 현황
    status = nxt_db.get_nxt_collection_status()
    print(f"\n📈 수집 현황:")
    print(f"   완료율: {status.get('completion_rate', 0)}%")
    print(f"   완료 종목: {status.get('completed_stocks', 0)}개")
    print(f"   업데이트 필요: {status.get('need_update', 0)}개")

    print("\n✅ 테스트 완료!")
    return True


if __name__ == "__main__":
    test_nxt_database()