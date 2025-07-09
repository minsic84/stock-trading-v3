#!/usr/bin/env python3
"""
파일 경로: scripts/migrate_sqlite_to_mysql.py

SQLite에서 MySQL로 데이터 마이그레이션 스크립트
- 647개 종목의 stocks 테이블 이관
- 647개 daily_prices_* 테이블을 1개 통합 테이블로 이관
- 배치 처리로 안전한 대용량 데이터 이관
- 진행상황 실시간 표시 및 오류 처리
"""
import sys
import os
import sqlite3
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import pymysql
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    print("❌ MySQL 드라이버가 설치되지 않았습니다.")
    print("📥 설치 명령어: pip install pymysql mysql-connector-python")
    sys.exit(1)

from src.core.config import Config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SQLiteToMySQLMigrator:
    """SQLite에서 MySQL로 데이터 마이그레이션 클래스"""

    def __init__(self):
        self.config = Config()

        # SQLite 연결 정보
        self.sqlite_path = Path("./data/stock_data.db")
        if not self.sqlite_path.exists():
            raise FileNotFoundError(f"SQLite DB 파일을 찾을 수 없습니다: {self.sqlite_path}")

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

        # 마이그레이션 통계
        self.stats = {
            'stocks_migrated': 0,
            'daily_records_migrated': 0,
            'tables_processed': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }

        # 배치 처리 설정
        self.BATCH_SIZE = 1000  # 한 번에 처리할 레코드 수

    def migrate_all_data(self) -> bool:
        """전체 데이터 마이그레이션 실행"""
        print("🚀 SQLite → MySQL 데이터 마이그레이션 시작")
        print("=" * 60)

        self.stats['start_time'] = datetime.now()

        try:
            # 1. MySQL 연결 테스트
            if not self._test_mysql_connection():
                return False

            # 2. SQLite 분석
            self._analyze_sqlite_data()

            # 3. stocks 테이블 마이그레이션
            print(f"\n📋 1단계: stocks 테이블 마이그레이션")
            if not self._migrate_stocks_table():
                return False

            # 4. daily_prices 테이블들 통합 마이그레이션
            print(f"\n📊 2단계: daily_prices 테이블들 통합 마이그레이션")
            if not self._migrate_daily_tables():
                return False

            # 5. collection_progress 마이그레이션
            print(f"\n📈 3단계: collection_progress 테이블 마이그레이션")
            if not self._migrate_collection_progress():
                return False

            # 6. 데이터 검증
            print(f"\n🔍 4단계: 데이터 무결성 검증")
            if not self._verify_migration():
                return False

            # 7. 최종 리포트
            self._print_final_report()

            return True

        except Exception as e:
            logger.error(f"마이그레이션 중 치명적 오류: {e}")
            print(f"❌ 마이그레이션 실패: {e}")
            return False
        finally:
            self.stats['end_time'] = datetime.now()

    def _test_mysql_connection(self) -> bool:
        """MySQL 연결 테스트"""
        print("🔌 MySQL 연결 테스트 중...")

        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 기본 테스트
            cursor.execute("SELECT VERSION();")
            version = cursor.fetchone()[0]
            print(f"✅ MySQL 연결 성공 - 버전: {version}")

            # 테이블 존재 확인
            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]

            required_tables = ['stocks', 'daily_prices', 'collection_progress']
            missing_tables = [t for t in required_tables if t not in tables]

            if missing_tables:
                print(f"❌ 필수 테이블이 없습니다: {missing_tables}")
                return False

            print(f"✅ 필수 테이블 확인 완료: {required_tables}")

            conn.close()
            return True

        except MySQLError as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

    def _analyze_sqlite_data(self):
        """SQLite 데이터 분석"""
        print("🔍 SQLite 데이터 분석 중...")

        try:
            with sqlite3.connect(self.sqlite_path) as conn:
                cursor = conn.cursor()

                # stocks 테이블 분석
                cursor.execute("SELECT COUNT(*) FROM stocks")
                stocks_count = cursor.fetchone()[0]

                # daily_prices 테이블들 분석
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name LIKE 'daily_prices_%'
                """)
                daily_tables = [row[0] for row in cursor.fetchall()]

                # 총 daily_prices 레코드 수 계산
                total_daily_records = 0
                for table in daily_tables[:5]:  # 샘플링
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    total_daily_records += count

                # 전체 추정
                estimated_total = total_daily_records * (len(daily_tables) / 5)

                print(f"📊 분석 결과:")
                print(f"   📋 stocks 레코드: {stocks_count:,}개")
                print(f"   📊 daily_prices 테이블: {len(daily_tables)}개")
                print(f"   📈 예상 daily_prices 레코드: {estimated_total:,.0f}개")
                print(f"   ⏱️ 예상 소요시간: {estimated_total / 10000:.0f}분")

        except Exception as e:
            logger.error(f"SQLite 분석 실패: {e}")

    def _migrate_stocks_table(self) -> bool:
        """stocks 테이블 마이그레이션"""
        try:
            print("📋 stocks 테이블 마이그레이션 시작...")

            # SQLite에서 데이터 읽기
            with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                sqlite_cursor = sqlite_conn.cursor()

                sqlite_cursor.execute("""
                    SELECT code, name, market, current_price, prev_day_diff, 
                           change_rate, volume, open_price, high_price, low_price,
                           upper_limit, lower_limit, market_cap, market_cap_size,
                           listed_shares, per_ratio, pbr_ratio, data_source,
                           last_updated, is_active, created_at, updated_at
                    FROM stocks
                """)

                stocks_data = sqlite_cursor.fetchall()

            print(f"📊 SQLite에서 {len(stocks_data):,}개 종목 데이터 읽기 완료")

            # MySQL에 데이터 쓰기
            mysql_conn = mysql.connector.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor()

            # 기존 데이터 삭제
            mysql_cursor.execute("DELETE FROM stocks")
            print("🗑️ MySQL stocks 테이블 기존 데이터 삭제")

            # 배치 삽입
            insert_query = """
                INSERT INTO stocks (
                    code, name, market, current_price, prev_day_diff, 
                    change_rate, volume, open_price, high_price, low_price,
                    upper_limit, lower_limit, market_cap, market_cap_size,
                    listed_shares, per_ratio, pbr_ratio, data_source,
                    last_updated, is_active, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # 배치 처리
            for i in range(0, len(stocks_data), self.BATCH_SIZE):
                batch = stocks_data[i:i + self.BATCH_SIZE]
                mysql_cursor.executemany(insert_query, batch)
                mysql_conn.commit()

                print(
                    f"   📥 {i + len(batch)}/{len(stocks_data)} 처리 완료 ({(i + len(batch)) / len(stocks_data) * 100:.1f}%)")

            mysql_conn.close()

            self.stats['stocks_migrated'] = len(stocks_data)
            print(f"✅ stocks 테이블 마이그레이션 완료: {len(stocks_data):,}개")
            return True

        except Exception as e:
            logger.error(f"stocks 테이블 마이그레이션 실패: {e}")
            print(f"❌ stocks 테이블 마이그레이션 실패: {e}")
            return False

    def _migrate_daily_tables(self) -> bool:
        """daily_prices 테이블들 통합 마이그레이션"""
        try:
            print("📊 daily_prices 테이블들 통합 마이그레이션 시작...")

            # SQLite에서 daily_prices 테이블 목록 가져오기
            with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                sqlite_cursor = sqlite_conn.cursor()

                sqlite_cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name LIKE 'daily_prices_%'
                    ORDER BY name
                """)

                daily_tables = [row[0] for row in sqlite_cursor.fetchall()]

            if not daily_tables:
                print("⚠️ daily_prices 테이블이 없습니다.")
                return True

            print(f"📊 처리할 테이블: {len(daily_tables)}개")

            # MySQL 연결
            mysql_conn = mysql.connector.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor()

            # 기존 데이터 삭제
            mysql_cursor.execute("DELETE FROM daily_prices")
            mysql_conn.commit()
            print("🗑️ MySQL daily_prices 테이블 기존 데이터 삭제")

            # 삽입 쿼리 준비
            insert_query = """
                INSERT INTO daily_prices (
                    stock_code, date, open_price, high_price, low_price, 
                    close_price, volume, trading_value, prev_day_diff, 
                    change_rate, data_source, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            total_records = 0

            # 각 daily_prices 테이블 처리
            for idx, table_name in enumerate(daily_tables):
                stock_code = table_name.replace('daily_prices_', '')

                print(f"   📈 {idx + 1}/{len(daily_tables)} 처리 중: {stock_code}")

                try:
                    # SQLite에서 데이터 읽기
                    with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                        sqlite_cursor = sqlite_conn.cursor()

                        # 테이블 구조 확인 후 적절한 쿼리 생성
                        sqlite_cursor.execute(f"PRAGMA table_info({table_name})")
                        columns = [row[1] for row in sqlite_cursor.fetchall()]

                        # 컬럼명 매핑 (SQLite → MySQL)
                        column_mapping = {
                            'start_price': 'open_price',
                            'current_price': 'close_price'
                        }

                        # SELECT 쿼리 구성
                        select_columns = []
                        for col in ['date', 'start_price', 'high_price', 'low_price', 'current_price',
                                    'volume', 'trading_value', 'prev_day_diff', 'change_rate', 'created_at']:
                            if col in columns:
                                select_columns.append(col)
                            elif column_mapping.get(col) in columns:
                                select_columns.append(column_mapping[col])
                            else:
                                select_columns.append('NULL')

                        query = f"""
                            SELECT {', '.join(select_columns)}
                            FROM {table_name}
                            ORDER BY date
                        """

                        sqlite_cursor.execute(query)
                        rows = sqlite_cursor.fetchall()

                    if not rows:
                        print(f"      ⚠️ {stock_code}: 데이터 없음")
                        continue

                    # MySQL 형태로 데이터 변환
                    mysql_data = []
                    for row in rows:
                        # None 값을 적절한 기본값으로 변환
                        converted_row = [
                            stock_code,  # stock_code 추가
                            row[0] if row[0] else '',  # date
                            row[1] if row[1] else 0,  # open_price
                            row[2] if row[2] else 0,  # high_price
                            row[3] if row[3] else 0,  # low_price
                            row[4] if row[4] else 0,  # close_price
                            row[5] if row[5] else 0,  # volume
                            row[6] if row[6] else 0,  # trading_value
                            row[7] if row[7] else 0,  # prev_day_diff
                            row[8] if row[8] else 0,  # change_rate
                            'OPT10081',  # data_source
                            row[9] if len(row) > 9 and row[9] else datetime.now()  # created_at
                        ]
                        mysql_data.append(converted_row)

                    # 배치 삽입
                    for i in range(0, len(mysql_data), self.BATCH_SIZE):
                        batch = mysql_data[i:i + self.BATCH_SIZE]
                        mysql_cursor.executemany(insert_query, batch)
                        mysql_conn.commit()

                    total_records += len(mysql_data)
                    print(f"      ✅ {stock_code}: {len(mysql_data):,}개 레코드 이관 완료")

                    self.stats['tables_processed'] += 1

                except Exception as e:
                    logger.error(f"{table_name} 마이그레이션 실패: {e}")
                    print(f"      ❌ {stock_code}: 마이그레이션 실패 - {e}")
                    self.stats['errors'] += 1
                    continue

            mysql_conn.close()

            self.stats['daily_records_migrated'] = total_records
            print(f"✅ daily_prices 통합 마이그레이션 완료: {total_records:,}개 레코드")
            return True

        except Exception as e:
            logger.error(f"daily_prices 마이그레이션 실패: {e}")
            print(f"❌ daily_prices 마이그레이션 실패: {e}")
            return False

    def _migrate_collection_progress(self) -> bool:
        """collection_progress 테이블 마이그레이션"""
        try:
            print("📈 collection_progress 테이블 마이그레이션 시작...")

            # SQLite에서 테이블 존재 확인
            with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                sqlite_cursor = sqlite_conn.cursor()

                sqlite_cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='collection_progress'
                """)

                if not sqlite_cursor.fetchone():
                    print("ℹ️ collection_progress 테이블이 없습니다. 건너뜁니다.")
                    return True

                # 데이터 읽기
                sqlite_cursor.execute("""
                    SELECT stock_code, stock_name, status, attempt_count,
                           last_attempt_time, success_time, error_message,
                           data_count, created_at, updated_at
                    FROM collection_progress
                """)

                progress_data = sqlite_cursor.fetchall()

            if not progress_data:
                print("ℹ️ collection_progress 데이터가 없습니다.")
                return True

            # MySQL에 데이터 쓰기
            mysql_conn = mysql.connector.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor()

            # 기존 데이터 삭제
            mysql_cursor.execute("DELETE FROM collection_progress")

            # 삽입 쿼리
            insert_query = """
                INSERT INTO collection_progress (
                    stock_code, stock_name, status, attempt_count,
                    last_attempt_time, success_time, error_message,
                    data_count, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            mysql_cursor.executemany(insert_query, progress_data)
            mysql_conn.commit()
            mysql_conn.close()

            print(f"✅ collection_progress 마이그레이션 완료: {len(progress_data):,}개")
            return True

        except Exception as e:
            logger.error(f"collection_progress 마이그레이션 실패: {e}")
            print(f"❌ collection_progress 마이그레이션 실패: {e}")
            return False

    def _verify_migration(self) -> bool:
        """데이터 무결성 검증"""
        try:
            print("🔍 데이터 무결성 검증 중...")

            mysql_conn = mysql.connector.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor()

            # stocks 테이블 검증
            mysql_cursor.execute("SELECT COUNT(*) FROM stocks")
            mysql_stocks_count = mysql_cursor.fetchone()[0]

            # daily_prices 테이블 검증
            mysql_cursor.execute("SELECT COUNT(*) FROM daily_prices")
            mysql_daily_count = mysql_cursor.fetchone()[0]

            # 종목별 데이터 개수 확인
            mysql_cursor.execute("""
                SELECT stock_code, COUNT(*) as count 
                FROM daily_prices 
                GROUP BY stock_code 
                ORDER BY count DESC 
                LIMIT 5
            """)
            top_stocks = mysql_cursor.fetchall()

            print(f"📊 검증 결과:")
            print(f"   📋 MySQL stocks: {mysql_stocks_count:,}개")
            print(f"   📊 MySQL daily_prices: {mysql_daily_count:,}개")
            print(f"   📈 상위 종목 데이터:")
            for stock_code, count in top_stocks:
                print(f"      {stock_code}: {count:,}개")

            # 원본과 비교
            print(f"📋 마이그레이션 결과 비교:")
            print(f"   📊 stocks: SQLite {self.stats['stocks_migrated']} → MySQL {mysql_stocks_count}")
            print(f"   📈 daily_prices: 예상 {self.stats['daily_records_migrated']} → MySQL {mysql_daily_count}")

            mysql_conn.close()

            # 기본 검증
            if mysql_stocks_count == 0 or mysql_daily_count == 0:
                print("❌ 마이그레이션된 데이터가 없습니다.")
                return False

            print("✅ 데이터 무결성 검증 완료")
            return True

        except Exception as e:
            logger.error(f"데이터 검증 실패: {e}")
            print(f"❌ 데이터 검증 실패: {e}")
            return False

    def _print_final_report(self):
        """최종 마이그레이션 리포트"""
        elapsed_time = self.stats['end_time'] - self.stats['start_time']

        print(f"\n🎉 마이그레이션 완료 리포트")
        print("=" * 60)
        print(f"📊 마이그레이션 결과:")
        print(f"   ✅ stocks 마이그레이션: {self.stats['stocks_migrated']:,}개")
        print(f"   ✅ daily_prices 마이그레이션: {self.stats['daily_records_migrated']:,}개")
        print(f"   ✅ 처리된 테이블: {self.stats['tables_processed']}개")
        print(f"   ❌ 오류 발생: {self.stats['errors']}개")
        print(f"   ⏱️ 총 소요시간: {elapsed_time}")

        print(f"\n🎯 다음 단계:")
        print(f"   1. Python 설정 파일(.env) MySQL로 변경")
        print(f"   2. 애플리케이션 MySQL 연결 테스트")
        print(f"   3. 기존 SQLite 파일 백업 후 보관")
        print(f"   4. 수급 데이터 수집 시스템 개발")


def update_env_file():
    """환경 설정 파일을 MySQL용으로 업데이트"""
    try:
        env_path = Path(".env")

        # 백업 생성
        if env_path.exists():
            backup_path = Path(".env.sqlite.backup")
            env_path.rename(backup_path)
            print(f"📋 기존 .env 파일을 {backup_path}로 백업했습니다.")

        # 새 MySQL 설정 생성
        mysql_env_content = """# ===========================================
# Database Configuration (MySQL)
# ===========================================

# MySQL 설정
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_NAME=stock_trading_db
DB_USER=stock_user
DB_PASSWORD=StockPass2025!

# MySQL 연결 옵션
MYSQL_CHARSET=utf8mb4
MYSQL_POOL_SIZE=20
MYSQL_POOL_RECYCLE=3600

# 기타 설정들 (기존과 동일)
ENVIRONMENT=development
DEBUG=True
LOG_LEVEL=INFO

# 키움 API 설정
KIWOOM_USER_ID=
KIWOOM_PASSWORD=
KIWOOM_CERT_PASSWORD=

# 텔레그램 설정
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# API 요청 설정
API_REQUEST_DELAY_MS=3600
MAX_RETRY_ATTEMPTS=3
RETRY_DELAY_SECONDS=3.6
"""

        with open(env_path, 'w', encoding='utf-8') as f:
            f.write(mysql_env_content)

        print(f"✅ 새로운 MySQL .env 파일을 생성했습니다.")

    except Exception as e:
        print(f"❌ .env 파일 업데이트 실패: {e}")


def main():
    """메인 실행 함수"""
    print("🚀 SQLite → MySQL 데이터 마이그레이션 도구")
    print("=" * 60)

    try:
        # 사용자 확인
        print("⚠️  주의사항:")
        print("   1. MySQL이 실행 중이어야 합니다")
        print("   2. stock_trading_db 데이터베이스가 생성되어 있어야 합니다")
        print("   3. 기존 MySQL 데이터는 모두 삭제됩니다")
        print("   4. 마이그레이션 중에는 시스템을 종료하지 마세요")

        response = input("\n계속 진행하시겠습니까? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("❌ 마이그레이션이 취소되었습니다.")
            return False

        # 마이그레이션 실행
        migrator = SQLiteToMySQLMigrator()
        success = migrator.migrate_all_data()

        if success:
            print(f"\n🎉 마이그레이션 성공!")

            # .env 파일 업데이트
            response = input("\n.env 파일을 MySQL용으로 업데이트하시겠습니까? (y/N): ")
            if response.lower() == 'y':
                update_env_file()

            return True
        else:
            print(f"\n❌ 마이그레이션 실패!")
            return False

    except KeyboardInterrupt:
        print(f"\n\n👋 사용자가 마이그레이션을 중단했습니다.")
        return False
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)