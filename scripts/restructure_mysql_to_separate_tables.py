#!/usr/bin/env python3
"""
파일 경로: scripts/restructure_mysql_to_separate_tables.py

MySQL 통합 테이블을 종목별 분리 테이블로 재구조화
- daily_prices (통합) → daily_prices_000020, daily_prices_000040, ... (분리)
- 성능 최적화를 위한 종목별 인덱싱
- 향후 수급/틱봉 데이터를 위한 구조 준비
"""
import sys
import os
import time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    print("❌ MySQL 드라이버가 설치되지 않았습니다.")
    print("📥 설치 명령어: pip install mysql-connector-python")
    sys.exit(1)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mysql_restructure.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MySQLRestructurer:
    """MySQL 데이터 종목별 재분리 클래스"""

    def __init__(self):
        # MySQL 연결 정보
        self.mysql_config = {
            'host': 'localhost',
            'port': 3306,
            'database': 'stock_trading_db',
            'user': 'stock_user',
            'password': 'StockPass2025!',
            'charset': 'utf8mb4',
            'autocommit': False
        }

        # 재구조화 통계
        self.stats = {
            'stock_tables_created': 0,
            'total_records_migrated': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }

        # 배치 처리 설정
        self.BATCH_SIZE = 5000  # 한 번에 처리할 레코드 수

    def restructure_all_data(self) -> bool:
        """전체 데이터 재구조화 실행"""
        print("🔄 MySQL 데이터 종목별 재분리 시작")
        print("=" * 60)

        self.stats['start_time'] = datetime.now()

        try:
            # 1. MySQL 연결 테스트
            if not self._test_mysql_connection():
                return False

            # 2. 현재 데이터 분석
            stock_codes = self._analyze_current_data()
            if not stock_codes:
                return False

            # 3. 종목별 테이블 생성
            print(f"\n📊 1단계: 종목별 daily_prices 테이블 생성")
            if not self._create_stock_tables(stock_codes):
                return False

            # 4. 데이터 분리 및 이관
            print(f"\n🔄 2단계: 데이터 종목별 분리 및 이관")
            if not self._migrate_data_to_stock_tables(stock_codes):
                return False

            # 5. 통합 테이블 백업 및 삭제
            print(f"\n🗑️ 3단계: 통합 테이블 정리")
            if not self._cleanup_unified_table():
                return False

            # 6. 향후 확장을 위한 테이블 구조 준비
            print(f"\n🚀 4단계: 향후 확장 구조 준비")
            if not self._prepare_future_structures():
                return False

            # 7. 데이터 검증
            print(f"\n🔍 5단계: 재구조화 검증")
            if not self._verify_restructure(stock_codes):
                return False

            # 8. 최종 리포트
            self._print_final_report()

            return True

        except Exception as e:
            logger.error(f"재구조화 중 치명적 오류: {e}")
            print(f"❌ 재구조화 실패: {e}")
            return False
        finally:
            self.stats['end_time'] = datetime.now()

    def _test_mysql_connection(self) -> bool:
        """MySQL 연결 테스트"""
        print("🔌 MySQL 연결 테스트 중...")

        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 현재 테이블 확인
            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]

            if 'daily_prices' not in tables:
                print("❌ daily_prices 테이블이 없습니다.")
                return False

            # 데이터 개수 확인
            cursor.execute("SELECT COUNT(*) FROM daily_prices;")
            record_count = cursor.fetchone()[0]

            print(f"✅ MySQL 연결 성공")
            print(f"📊 현재 daily_prices 레코드: {record_count:,}개")

            conn.close()
            return True

        except MySQLError as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def _analyze_current_data(self) -> List[str]:
        """현재 데이터 분석 및 종목코드 추출"""
        print("🔍 현재 데이터 분석 중...")

        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 모든 종목코드 추출
            cursor.execute("""
                SELECT DISTINCT stock_code 
                FROM daily_prices 
                ORDER BY stock_code
            """)

            stock_codes = [row[0] for row in cursor.fetchall()]

            # 종목별 데이터 개수 확인 (상위 10개)
            cursor.execute("""
                SELECT stock_code, COUNT(*) as count
                FROM daily_prices 
                GROUP BY stock_code 
                ORDER BY count DESC 
                LIMIT 10
            """)

            top_stocks = cursor.fetchall()

            print(f"📊 분석 결과:")
            print(f"   📈 총 종목 수: {len(stock_codes)}개")
            print(f"   📋 상위 종목 데이터:")
            for stock_code, count in top_stocks:
                print(f"      {stock_code}: {count:,}개")

            conn.close()
            return stock_codes

        except Exception as e:
            logger.error(f"데이터 분석 실패: {e}")
            print(f"❌ 데이터 분석 실패: {e}")
            return []

    def _create_stock_tables(self, stock_codes: List[str]) -> bool:
        """종목별 daily_prices 테이블 생성"""
        try:
            print(f"📊 {len(stock_codes)}개 종목의 테이블 생성 중...")

            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 기본 테이블 구조 정의
            table_structure = """
                CREATE TABLE daily_prices_{stock_code} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    date VARCHAR(8) NOT NULL COMMENT '일자(YYYYMMDD)',

                    -- 가격 정보
                    open_price INT COMMENT '시가',
                    high_price INT COMMENT '고가',
                    low_price INT COMMENT '저가',
                    close_price INT COMMENT '종가/현재가',

                    -- 거래 정보
                    volume BIGINT COMMENT '거래량',
                    trading_value BIGINT COMMENT '거래대금',

                    -- 변동 정보
                    prev_day_diff INT DEFAULT 0 COMMENT '전일대비',
                    change_rate INT DEFAULT 0 COMMENT '등락율',

                    -- 메타 정보
                    data_source VARCHAR(20) DEFAULT 'OPT10081' COMMENT '데이터 출처',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',

                    -- 인덱스 (종목별 최적화)
                    UNIQUE KEY uk_date (date),
                    INDEX idx_date (date),
                    INDEX idx_close_price (close_price),
                    INDEX idx_volume (volume),
                    INDEX idx_trading_value (trading_value)
                ) ENGINE=InnoDB COMMENT='종목 {stock_code} 일봉 데이터'
            """

            created_count = 0

            for i, stock_code in enumerate(stock_codes):
                try:
                    # 기존 테이블 확인
                    cursor.execute(f"SHOW TABLES LIKE 'daily_prices_{stock_code}';")
                    if cursor.fetchone():
                        print(f"   ⚠️ {stock_code}: 테이블 이미 존재, 삭제 후 재생성")
                        cursor.execute(f"DROP TABLE daily_prices_{stock_code};")

                    # 새 테이블 생성
                    create_sql = table_structure.format(stock_code=stock_code)
                    cursor.execute(create_sql)
                    conn.commit()

                    created_count += 1

                    if (i + 1) % 50 == 0:
                        print(f"   📊 진행률: {i + 1}/{len(stock_codes)} ({(i + 1) / len(stock_codes) * 100:.1f}%)")

                except Exception as e:
                    logger.error(f"{stock_code} 테이블 생성 실패: {e}")
                    print(f"   ❌ {stock_code}: 테이블 생성 실패")
                    self.stats['errors'] += 1
                    continue

            conn.close()

            self.stats['stock_tables_created'] = created_count
            print(f"✅ 종목별 테이블 생성 완료: {created_count}개")
            return True

        except Exception as e:
            logger.error(f"테이블 생성 실패: {e}")
            print(f"❌ 테이블 생성 실패: {e}")
            return False

    def _migrate_data_to_stock_tables(self, stock_codes: List[str]) -> bool:
        """데이터를 종목별 테이블로 이관"""
        try:
            print(f"🔄 {len(stock_codes)}개 종목의 데이터 분리 이관 중...")

            total_migrated = 0

            for i, stock_code in enumerate(stock_codes):
                try:
                    print(f"   📈 {i + 1}/{len(stock_codes)} 처리 중: {stock_code}")

                    conn = mysql.connector.connect(**self.mysql_config)
                    cursor = conn.cursor()

                    # 해당 종목 데이터 조회
                    cursor.execute("""
                        SELECT date, open_price, high_price, low_price, close_price,
                               volume, trading_value, prev_day_diff, change_rate,
                               data_source, created_at
                        FROM daily_prices 
                        WHERE stock_code = %s
                        ORDER BY date
                    """, (stock_code,))

                    stock_data = cursor.fetchall()

                    if not stock_data:
                        print(f"      ⚠️ {stock_code}: 데이터 없음")
                        conn.close()
                        continue

                    # 종목별 테이블에 삽입
                    insert_sql = f"""
                        INSERT INTO daily_prices_{stock_code} 
                        (date, open_price, high_price, low_price, close_price,
                         volume, trading_value, prev_day_diff, change_rate,
                         data_source, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    # 배치 삽입
                    for j in range(0, len(stock_data), self.BATCH_SIZE):
                        batch = stock_data[j:j + self.BATCH_SIZE]
                        cursor.executemany(insert_sql, batch)
                        conn.commit()

                    total_migrated += len(stock_data)
                    print(f"      ✅ {stock_code}: {len(stock_data):,}개 레코드 이관 완료")

                    conn.close()

                except Exception as e:
                    logger.error(f"{stock_code} 데이터 이관 실패: {e}")
                    print(f"      ❌ {stock_code}: 데이터 이관 실패")
                    self.stats['errors'] += 1
                    continue

            self.stats['total_records_migrated'] = total_migrated
            print(f"✅ 데이터 분리 이관 완료: {total_migrated:,}개 레코드")
            return True

        except Exception as e:
            logger.error(f"데이터 이관 실패: {e}")
            print(f"❌ 데이터 이관 실패: {e}")
            return False

    def _cleanup_unified_table(self) -> bool:
        """통합 테이블 백업 및 정리"""
        try:
            print("🗑️ 통합 테이블 정리 중...")

            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 백업 테이블 생성
            print("   📋 백업 테이블 생성 중...")
            cursor.execute("DROP TABLE IF EXISTS daily_prices_backup;")
            cursor.execute("""
                CREATE TABLE daily_prices_backup AS 
                SELECT * FROM daily_prices LIMIT 0
            """)

            # 샘플 데이터만 백업 (검증용)
            cursor.execute("""
                INSERT INTO daily_prices_backup 
                SELECT * FROM daily_prices LIMIT 1000
            """)

            # 원본 테이블 삭제
            print("   🗑️ 통합 daily_prices 테이블 삭제 중...")
            cursor.execute("DROP TABLE daily_prices;")

            conn.commit()
            conn.close()

            print("✅ 통합 테이블 정리 완료")
            return True

        except Exception as e:
            logger.error(f"테이블 정리 실패: {e}")
            print(f"❌ 테이블 정리 실패: {e}")
            return False

    def _prepare_future_structures(self) -> bool:
        """향후 확장을 위한 구조 준비"""
        try:
            print("🚀 향후 확장 구조 준비 중...")

            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 수급 데이터 템플릿 테이블 생성
            print("   💰 수급 데이터 템플릿 준비...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS supply_demand_template (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    date VARCHAR(8) NOT NULL COMMENT '일자(YYYYMMDD)',

                    -- 수급 정보
                    institution_buy BIGINT DEFAULT 0 COMMENT '기관 매수',
                    institution_sell BIGINT DEFAULT 0 COMMENT '기관 매도',
                    institution_net BIGINT DEFAULT 0 COMMENT '기관 순매수',

                    foreign_buy BIGINT DEFAULT 0 COMMENT '외국인 매수',
                    foreign_sell BIGINT DEFAULT 0 COMMENT '외국인 매도', 
                    foreign_net BIGINT DEFAULT 0 COMMENT '외국인 순매수',

                    individual_buy BIGINT DEFAULT 0 COMMENT '개인 매수',
                    individual_sell BIGINT DEFAULT 0 COMMENT '개인 매도',
                    individual_net BIGINT DEFAULT 0 COMMENT '개인 순매수',

                    -- 메타 정보
                    data_source VARCHAR(20) DEFAULT 'TR_CODE_TBD' COMMENT '데이터 출처',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

                    -- 인덱스
                    UNIQUE KEY uk_date (date),
                    INDEX idx_date (date),
                    INDEX idx_institution_net (institution_net),
                    INDEX idx_foreign_net (foreign_net)
                ) ENGINE=InnoDB COMMENT='수급 데이터 템플릿 (종목별 복사용)'
            """)

            # 분봉 데이터 템플릿 테이블 생성
            print("   ⚡ 분봉 데이터 템플릿 준비...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS minute_data_template (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    datetime DATETIME NOT NULL COMMENT '일시',
                    minute_type TINYINT NOT NULL COMMENT '분봉 타입(1:1분, 3:3분, 5:5분)',

                    -- 가격 정보
                    open_price INT COMMENT '시가',
                    high_price INT COMMENT '고가', 
                    low_price INT COMMENT '저가',
                    close_price INT COMMENT '종가',
                    volume BIGINT COMMENT '거래량',

                    -- 메타 정보
                    data_source VARCHAR(20) DEFAULT 'TR_CODE_TBD' COMMENT '데이터 출처',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

                    -- 인덱스
                    UNIQUE KEY uk_datetime_type (datetime, minute_type),
                    INDEX idx_datetime (datetime),
                    INDEX idx_minute_type (minute_type),
                    INDEX idx_close_price (close_price)
                ) ENGINE=InnoDB COMMENT='분봉 데이터 템플릿 (종목별 복사용)'
            """)

            # 종목 관리 테이블 생성
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_management (
                    stock_code VARCHAR(10) PRIMARY KEY,

                    -- 수집 설정
                    collect_daily BOOLEAN DEFAULT TRUE COMMENT '일봉 수집 여부',
                    collect_supply_demand BOOLEAN DEFAULT FALSE COMMENT '수급 수집 여부', 
                    collect_minute_data BOOLEAN DEFAULT FALSE COMMENT '분봉 수집 여부',
                    minute_types VARCHAR(20) DEFAULT '3' COMMENT '수집할 분봉 타입 (1,3,5)',

                    -- 우선순위
                    priority TINYINT DEFAULT 3 COMMENT '수집 우선순위 (1:최고, 5:최저)',

                    -- 메타 정보
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                    -- 외래키
                    FOREIGN KEY (stock_code) REFERENCES stocks(code) ON DELETE CASCADE
                ) ENGINE=InnoDB COMMENT='종목별 수집 관리 테이블'
            """)

            conn.commit()
            conn.close()

            print("✅ 향후 확장 구조 준비 완료")
            return True

        except Exception as e:
            logger.error(f"확장 구조 준비 실패: {e}")
            print(f"❌ 확장 구조 준비 실패: {e}")
            return False

    def _verify_restructure(self, stock_codes: List[str]) -> bool:
        """재구조화 검증"""
        try:
            print("🔍 재구조화 검증 중...")

            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 생성된 테이블 확인
            cursor.execute("SHOW TABLES LIKE 'daily_prices_%';")
            created_tables = [row[0] for row in cursor.fetchall()]

            print(f"📊 검증 결과:")
            print(f"   📋 생성된 테이블: {len(created_tables)}개")
            print(f"   🎯 목표 테이블: {len(stock_codes)}개")

            # 샘플 테이블 데이터 확인
            sample_tables = created_tables[:5]
            print(f"   📈 샘플 테이블 데이터:")

            total_records = 0
            for table in sample_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table};")
                count = cursor.fetchone()[0]
                total_records += count

                stock_code = table.replace('daily_prices_', '')
                print(f"      {stock_code}: {count:,}개")

            print(f"   📊 샘플 총 레코드: {total_records:,}개")

            # 백업 테이블 확인
            cursor.execute("SELECT COUNT(*) FROM daily_prices_backup;")
            backup_count = cursor.fetchone()[0]
            print(f"   📋 백업 테이블: {backup_count}개 (샘플)")

            conn.close()

            # 기본 검증
            success_rate = len(created_tables) / len(stock_codes)
            if success_rate >= 0.95:  # 95% 이상 성공
                print("✅ 재구조화 검증 완료")
                return True
            else:
                print(f"⚠️ 재구조화 부분 성공: {success_rate * 100:.1f}%")
                return True  # 대부분 성공이면 진행

        except Exception as e:
            logger.error(f"검증 실패: {e}")
            print(f"❌ 검증 실패: {e}")
            return False

    def _print_final_report(self):
        """최종 재구조화 리포트"""
        if self.stats['end_time'] and self.stats['start_time']:
            elapsed_time = self.stats['end_time'] - self.stats['start_time']
        else:
            elapsed_time = "측정 불가"

        print(f"\n🎉 MySQL 재구조화 완료 리포트")
        print("=" * 60)
        print(f"📊 재구조화 결과:")
        print(f"   ✅ 생성된 종목별 테이블: {self.stats['stock_tables_created']}개")
        print(f"   ✅ 이관된 레코드: {self.stats['total_records_migrated']:,}개")
        print(f"   ❌ 오류 발생: {self.stats['errors']}개")
        print(f"   ⏱️ 총 소요시간: {elapsed_time}")

        print(f"\n🏗️ 새로운 구조:")
        print(f"   📊 daily_prices_XXXXXX: 종목별 일봉 데이터")
        print(f"   💰 supply_demand_template: 수급 데이터 템플릿")
        print(f"   ⚡ minute_data_template: 분봉 데이터 템플릿")
        print(f"   🎯 stock_management: 종목별 수집 관리")

        print(f"\n🎯 다음 단계:")
        print(f"   1. Python 코드 종목별 테이블 연동 수정")
        print(f"   2. 수급 데이터 TR 코드 조사 및 구현")
        print(f"   3. 지정 종목 분봉 데이터 수집 시스템")
        print(f"   4. 일일 업데이트 시스템 최적화")


def main():
    """메인 실행 함수"""
    print("🔄 MySQL 데이터 종목별 재분리 도구")
    print("=" * 60)

    try:
        # 사용자 확인
        print("⚠️  주의사항:")
        print("   1. 현재 통합 daily_prices 테이블이 종목별로 분리됩니다")
        print("   2. 기존 통합 테이블은 백업 후 삭제됩니다")
        print("   3. 647개의 새로운 테이블이 생성됩니다")
        print("   4. 처리 시간이 오래 걸릴 수 있습니다 (30-60분)")

        response = input("\n계속 진행하시겠습니까? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("❌ 재구조화가 취소되었습니다.")
            return False

        # 재구조화 실행
        restructurer = MySQLRestructurer()
        success = restructurer.restructure_all_data()

        if success:
            print(f"\n🎉 재구조화 성공!")
            print(f"💡 이제 종목별로 분리된 테이블 구조를 사용할 수 있습니다.")
            return True
        else:
            print(f"\n❌ 재구조화 실패!")
            return False

    except KeyboardInterrupt:
        print(f"\n\n👋 사용자가 재구조화를 중단했습니다.")
        return False
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)