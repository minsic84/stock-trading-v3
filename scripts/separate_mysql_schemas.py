#!/usr/bin/env python3
"""
파일 경로: scripts/separate_mysql_schemas.py

MySQL 스키마 분리 스크립트
- daily_prices_* 테이블들을 별도 스키마로 이동
- 깔끔한 데이터베이스 구조 생성
- 향후 확장을 위한 스키마 준비
"""
import sys
import mysql.connector
from mysql.connector import Error as MySQLError
from pathlib import Path
from datetime import datetime
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('schema_separation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MySQLSchemaSeparator:
    """MySQL 스키마 분리 관리자"""

    def __init__(self):
        # MySQL 연결 정보
        self.mysql_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'stock_user',
            'password': 'StockPass2025!',
            'charset': 'utf8mb4',
            'autocommit': True
        }

        # 새 스키마 정보
        self.schemas = {
            'main': 'stock_trading_db',
            'daily': 'daily_prices_db',
            'supply': 'supply_demand_db',
            'minute': 'minute_data_db'
        }

        # 통계
        self.stats = {
            'schemas_created': 0,
            'tables_moved': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }

    def separate_schemas(self) -> bool:
        """스키마 분리 실행"""
        print("🚀 MySQL 스키마 분리 시작")
        print("=" * 50)

        self.stats['start_time'] = datetime.now()

        try:
            # 1. 연결 테스트
            if not self._test_connection():
                return False

            # 2. 현재 상태 분석
            table_info = self._analyze_current_state()
            if not table_info:
                return False

            # 3. 새 스키마 생성
            print(f"\n📁 1단계: 새 스키마 생성")
            if not self._create_new_schemas():
                return False

            # 4. daily_prices 테이블들 이동
            print(f"\n📊 2단계: daily_prices 테이블 이동")
            if not self._move_daily_tables(table_info['daily_tables']):
                return False

            # 5. 향후 확장용 템플릿 테이블 생성
            print(f"\n🔧 3단계: 향후 확장용 템플릿 생성")
            if not self._create_template_tables():
                return False

            # 6. 권한 설정
            print(f"\n🔐 4단계: 스키마별 권한 설정")
            if not self._setup_permissions():
                return False

            # 7. 검증
            print(f"\n✅ 5단계: 분리 결과 검증")
            if not self._verify_separation():
                return False

            # 8. 최종 리포트
            self._print_final_report()

            return True

        except Exception as e:
            logger.error(f"스키마 분리 실패: {e}")
            print(f"❌ 스키마 분리 실패: {e}")
            return False
        finally:
            self.stats['end_time'] = datetime.now()

    def _test_connection(self) -> bool:
        """MySQL 연결 테스트"""
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            print(f"✅ MySQL 연결 성공 - Version: {version}")
            conn.close()
            return True
        except MySQLError as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def _analyze_current_state(self) -> dict:
        """현재 상태 분석"""
        try:
            print("🔍 현재 테이블 상태 분석 중...")

            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # stock_trading_db의 테이블 목록 조회
            cursor.execute("USE stock_trading_db")
            cursor.execute("SHOW TABLES")
            all_tables = [table[0] for table in cursor.fetchall()]

            # daily_prices 테이블들 분류
            daily_tables = [table for table in all_tables if table.startswith('daily_prices_')]
            other_tables = [table for table in all_tables if not table.startswith('daily_prices_')]

            conn.close()

            print(f"📊 분석 결과:")
            print(f"   📋 전체 테이블: {len(all_tables)}개")
            print(f"   📈 daily_prices 테이블: {len(daily_tables)}개")
            print(f"   🗂️ 기타 테이블: {len(other_tables)}개 ({', '.join(other_tables)})")

            return {
                'daily_tables': daily_tables,
                'other_tables': other_tables,
                'total_tables': len(all_tables)
            }

        except Exception as e:
            logger.error(f"상태 분석 실패: {e}")
            print(f"❌ 상태 분석 실패: {e}")
            return None

    def _create_new_schemas(self) -> bool:
        """새 스키마 생성"""
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            schemas_to_create = ['daily', 'supply', 'minute']

            for schema_key in schemas_to_create:
                schema_name = self.schemas[schema_key]

                try:
                    # 스키마 존재 확인
                    cursor.execute(f"SHOW DATABASES LIKE '{schema_name}'")
                    if cursor.fetchone():
                        print(f"   ⚠️ {schema_name}: 이미 존재함")
                        continue

                    # 스키마 생성
                    cursor.execute(f"""
                        CREATE DATABASE {schema_name} 
                        DEFAULT CHARACTER SET utf8mb4 
                        DEFAULT COLLATE utf8mb4_unicode_ci
                    """)

                    print(f"   ✅ {schema_name}: 생성 완료")
                    self.stats['schemas_created'] += 1

                except Exception as e:
                    logger.error(f"스키마 {schema_name} 생성 실패: {e}")
                    print(f"   ❌ {schema_name}: 생성 실패 - {e}")
                    self.stats['errors'] += 1

            conn.close()
            print(f"✅ 스키마 생성 완료: {self.stats['schemas_created']}개")
            return True

        except Exception as e:
            logger.error(f"스키마 생성 실패: {e}")
            print(f"❌ 스키마 생성 실패: {e}")
            return False

    def _move_daily_tables(self, daily_tables: list) -> bool:
        """daily_prices 테이블들 이동"""
        try:
            if not daily_tables:
                print("ℹ️ 이동할 daily_prices 테이블이 없습니다.")
                return True

            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            print(f"📊 {len(daily_tables)}개 daily_prices 테이블 이동 중...")

            moved_count = 0
            for i, table_name in enumerate(daily_tables):
                try:
                    print(f"   📈 {i + 1}/{len(daily_tables)} 이동 중: {table_name}")

                    # 테이블 이동 (RENAME TABLE 사용)
                    cursor.execute(f"""
                        RENAME TABLE stock_trading_db.{table_name} 
                        TO daily_prices_db.{table_name}
                    """)

                    moved_count += 1
                    self.stats['tables_moved'] += 1

                    # 진행률 표시
                    if (i + 1) % 100 == 0:
                        progress = (i + 1) / len(daily_tables) * 100
                        print(f"      📊 진행률: {progress:.1f}% ({i + 1}/{len(daily_tables)})")

                except Exception as e:
                    logger.error(f"테이블 {table_name} 이동 실패: {e}")
                    print(f"      ❌ {table_name}: 이동 실패")
                    self.stats['errors'] += 1
                    continue

            conn.close()

            print(f"✅ daily_prices 테이블 이동 완료: {moved_count}/{len(daily_tables)}개")
            return moved_count > 0

        except Exception as e:
            logger.error(f"테이블 이동 실패: {e}")
            print(f"❌ 테이블 이동 실패: {e}")
            return False

    def _create_template_tables(self) -> bool:
        """향후 확장용 템플릿 테이블 생성"""
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 1. supply_demand_db에 템플릿 생성
            cursor.execute("USE supply_demand_db")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS supply_demand_template (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10) NOT NULL COMMENT '종목코드',
                    date VARCHAR(8) NOT NULL COMMENT '일자(YYYYMMDD)',
                    time VARCHAR(6) COMMENT '시간(HHMMSS)',

                    -- 기관/외국인/개인 매매 정보
                    institution_buy BIGINT DEFAULT 0 COMMENT '기관 매수',
                    institution_sell BIGINT DEFAULT 0 COMMENT '기관 매도',
                    foreign_buy BIGINT DEFAULT 0 COMMENT '외국인 매수',
                    foreign_sell BIGINT DEFAULT 0 COMMENT '외국인 매도',
                    individual_buy BIGINT DEFAULT 0 COMMENT '개인 매수',
                    individual_sell BIGINT DEFAULT 0 COMMENT '개인 매도',

                    -- 공매도 정보
                    short_sell_volume BIGINT DEFAULT 0 COMMENT '공매도 거래량',
                    short_sell_value BIGINT DEFAULT 0 COMMENT '공매도 거래대금',

                    -- 대차거래 정보
                    loan_balance BIGINT DEFAULT 0 COMMENT '대차잔고',

                    data_source VARCHAR(20) DEFAULT 'KIWOOM' COMMENT '데이터 출처',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

                    UNIQUE KEY uk_stock_date_time (stock_code, date, time),
                    INDEX idx_date (date),
                    INDEX idx_stock_code (stock_code)
                ) ENGINE=InnoDB COMMENT='수급 데이터 템플릿'
            """)

            # 2. minute_data_db에 템플릿 생성
            cursor.execute("USE minute_data_db")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS minute_data_template (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10) NOT NULL COMMENT '종목코드',
                    date VARCHAR(8) NOT NULL COMMENT '일자(YYYYMMDD)',
                    time VARCHAR(6) NOT NULL COMMENT '시간(HHMMSS)',

                    open_price INT COMMENT '시가',
                    high_price INT COMMENT '고가',
                    low_price INT COMMENT '저가',
                    close_price INT COMMENT '종가',
                    volume BIGINT COMMENT '거래량',
                    trading_value BIGINT COMMENT '거래대금',

                    data_source VARCHAR(20) DEFAULT 'KIWOOM' COMMENT '데이터 출처',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

                    UNIQUE KEY uk_stock_datetime (stock_code, date, time),
                    INDEX idx_date (date),
                    INDEX idx_stock_code (stock_code),
                    INDEX idx_datetime (date, time)
                ) ENGINE=InnoDB COMMENT='분봉 데이터 템플릿'
            """)

            # 3. stock_trading_db에 관리 테이블 생성
            cursor.execute("USE stock_trading_db")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_management (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    stock_code VARCHAR(10) NOT NULL COMMENT '종목코드',
                    stock_name VARCHAR(100) COMMENT '종목명',

                    -- 수집 설정
                    collect_daily BOOLEAN DEFAULT TRUE COMMENT '일봉 수집 여부',
                    collect_supply BOOLEAN DEFAULT FALSE COMMENT '수급 수집 여부',
                    collect_minute BOOLEAN DEFAULT FALSE COMMENT '분봉 수집 여부',
                    minute_interval INT DEFAULT 3 COMMENT '분봉 간격(분)',

                    -- 수집 상태
                    last_daily_date VARCHAR(8) COMMENT '마지막 일봉 날짜',
                    last_supply_date VARCHAR(8) COMMENT '마지막 수급 날짜',
                    last_minute_datetime VARCHAR(14) COMMENT '마지막 분봉 일시',

                    -- 관리 정보
                    is_active BOOLEAN DEFAULT TRUE COMMENT '활성 여부',
                    priority_level INT DEFAULT 1 COMMENT '우선순위(1-10)',
                    notes TEXT COMMENT '메모',

                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                    UNIQUE KEY uk_stock_code (stock_code),
                    INDEX idx_active (is_active),
                    INDEX idx_priority (priority_level)
                ) ENGINE=InnoDB COMMENT='종목별 수집 관리'
            """)

            conn.close()
            print("✅ 템플릿 테이블 생성 완료")
            return True

        except Exception as e:
            logger.error(f"템플릿 생성 실패: {e}")
            print(f"❌ 템플릿 생성 실패: {e}")
            return False

    def _setup_permissions(self) -> bool:
        """스키마별 권한 설정"""
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # stock_user에게 모든 스키마 권한 부여
            schemas = ['stock_trading_db', 'daily_prices_db', 'supply_demand_db', 'minute_data_db']

            for schema in schemas:
                cursor.execute(f"GRANT ALL PRIVILEGES ON {schema}.* TO 'stock_user'@'localhost'")

            cursor.execute("FLUSH PRIVILEGES")
            conn.close()

            print("✅ 권한 설정 완료")
            return True

        except Exception as e:
            logger.error(f"권한 설정 실패: {e}")
            print(f"❌ 권한 설정 실패: {e}")
            return False

    def _verify_separation(self) -> bool:
        """분리 결과 검증"""
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            verification_results = {}

            # 각 스키마별 테이블 수 확인
            for schema_key, schema_name in self.schemas.items():
                cursor.execute(f"USE {schema_name}")
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                verification_results[schema_name] = len(tables)

            conn.close()

            print("📊 검증 결과:")
            for schema_name, table_count in verification_results.items():
                print(f"   📁 {schema_name}: {table_count}개 테이블")

            # 기본 검증: daily_prices_db에 테이블이 있는지 확인
            if verification_results.get('daily_prices_db', 0) > 0:
                print("✅ 스키마 분리 검증 성공")
                return True
            else:
                print("❌ 스키마 분리 검증 실패: daily_prices_db에 테이블이 없음")
                return False

        except Exception as e:
            logger.error(f"검증 실패: {e}")
            print(f"❌ 검증 실패: {e}")
            return False

    def _print_final_report(self):
        """최종 리포트"""
        elapsed_time = None
        if self.stats['end_time'] and self.stats['start_time']:
            elapsed_time = self.stats['end_time'] - self.stats['start_time']

        print(f"\n🎉 MySQL 스키마 분리 완료 리포트")
        print("=" * 50)
        print(f"📊 분리 결과:")
        print(f"   📁 생성된 스키마: {self.stats['schemas_created']}개")
        print(f"   📈 이동된 테이블: {self.stats['tables_moved']}개")
        print(f"   ❌ 오류 발생: {self.stats['errors']}개")
        if elapsed_time:
            print(f"   ⏱️ 소요시간: {elapsed_time}")

        print(f"\n🏗️ 새로운 구조:")
        print(f"   📋 stock_trading_db: 메인 관리")
        print(f"   📊 daily_prices_db: 일봉 데이터")
        print(f"   💰 supply_demand_db: 수급 데이터 (준비됨)")
        print(f"   ⚡ minute_data_db: 분봉 데이터 (준비됨)")

        print(f"\n🎯 다음 단계:")
        print(f"   1. sync_sqlite_to_mysql_incremental.py 수정")
        print(f"   2. 일일 업데이트 시스템 다중 스키마 지원")
        print(f"   3. 수급 데이터 수집 시스템 개발")
        print(f"   4. 분봉 데이터 수집 시스템 개발")


def main():
    """메인 실행 함수"""
    print("🚀 MySQL 스키마 분리 도구")
    print("=" * 50)

    try:
        print("📋 분리 계획:")
        print("   📁 stock_trading_db → 메인 관리 (stocks, collection_progress 등)")
        print("   📊 daily_prices_db → 모든 daily_prices_* 테이블")
        print("   💰 supply_demand_db → 향후 수급 데이터")
        print("   ⚡ minute_data_db → 향후 분봉 데이터")

        response = input("\n계속 진행하시겠습니까? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("❌ 스키마 분리가 취소되었습니다.")
            return False

        # 스키마 분리 실행
        separator = MySQLSchemaSeparator()
        success = separator.separate_schemas()

        if success:
            print(f"\n🎉 스키마 분리 성공!")
            print(f"💡 이제 깔끔한 다중 스키마 구조를 사용할 수 있습니다.")
            return True
        else:
            print(f"\n❌ 스키마 분리 실패!")
            return False

    except KeyboardInterrupt:
        print(f"\n👋 사용자가 중단했습니다.")
        return False
    except Exception as e:
        print(f"❌ 실행 중 오류: {e}")
        logger.error(f"메인 함수 실행 중 오류: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)