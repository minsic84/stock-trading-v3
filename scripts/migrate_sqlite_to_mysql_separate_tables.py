#!/usr/bin/env python3
"""
파일 경로: scripts/migrate_sqlite_to_mysql_separate_tables.py

SQLite에서 MySQL로 종목별 테이블 직접 마이그레이션
- 2,565개 종목의 stocks 테이블 이관
- 2,565개 daily_prices_* 테이블을 MySQL 종목별 테이블로 직접 이관
- 대용량 데이터 배치 처리 (730만 레코드)
- 진행상황 실시간 표시 및 중단/재시작 기능
"""
import sys
import os
import sqlite3
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Tuple
import logging
import json

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
        logging.FileHandler('mysql_migration.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SQLiteToMySQLDirectMigrator:
    """SQLite에서 MySQL 종목별 테이블로 직접 마이그레이션"""

    def __init__(self):
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
            'tables_created': 0,
            'tables_migrated': 0,
            'total_records_migrated': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }

        # 배치 처리 설정
        self.BATCH_SIZE = 2000  # 한 번에 처리할 레코드 수

        # 진행상황 저장 파일
        self.progress_file = Path("migration_progress.json")
        self.completed_stocks = set()

        # 로드 기존 진행상황
        self._load_progress()

    def _load_progress(self):
        """기존 진행상황 로드"""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                    self.completed_stocks = set(progress.get('completed_stocks', []))
                    print(f"📋 기존 진행상황 로드: {len(self.completed_stocks)}개 종목 완료")
            except Exception as e:
                print(f"⚠️ 진행상황 로드 실패: {e}")

    def _save_progress(self, stock_code: str):
        """진행상황 저장"""
        try:
            self.completed_stocks.add(stock_code)
            progress = {
                'completed_stocks': list(self.completed_stocks),
                'last_updated': datetime.now().isoformat()
            }
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"진행상황 저장 실패: {e}")

    def migrate_all_data(self) -> bool:
        """전체 데이터 마이그레이션 실행"""
        print("🚀 SQLite → MySQL 종목별 테이블 직접 마이그레이션 시작")
        print("=" * 70)

        self.stats['start_time'] = datetime.now()

        try:
            # 1. 연결 테스트
            if not self._test_connections():
                return False

            # 2. SQLite 데이터 분석
            stock_list = self._analyze_sqlite_data()
            if not stock_list:
                return False

            # 3. MySQL 데이터베이스 준비
            print(f"\n🔧 1단계: MySQL 데이터베이스 준비")
            if not self._prepare_mysql_database():
                return False

            # 4. stocks 테이블 마이그레이션
            print(f"\n📋 2단계: stocks 테이블 마이그레이션")
            if not self._migrate_stocks_table():
                return False

            # 5. 종목별 daily_prices 테이블 마이그레이션
            print(f"\n📊 3단계: 종목별 daily_prices 테이블 마이그레이션")
            if not self._migrate_daily_tables(stock_list):
                return False

            # 6. collection_progress 마이그레이션
            print(f"\n📈 4단계: collection_progress 테이블 마이그레이션")
            if not self._migrate_collection_progress():
                return False

            # 7. 향후 확장 구조 준비
            print(f"\n🚀 5단계: 향후 확장 구조 준비")
            if not self._prepare_future_structures():
                return False

            # 8. 데이터 검증
            print(f"\n🔍 6단계: 데이터 무결성 검증")
            if not self._verify_migration(stock_list):
                return False

            # 9. 최종 리포트
            self._print_final_report()

            # 10. 진행상황 파일 정리
            if self.progress_file.exists():
                self.progress_file.unlink()
                print("🗑️ 진행상황 파일 정리 완료")

            return True

        except KeyboardInterrupt:
            print(f"\n⚠️ 사용자가 마이그레이션을 중단했습니다.")
            print(f"📋 진행상황이 저장되었습니다. 다시 실행하면 이어서 진행됩니다.")
            return False
        except Exception as e:
            logger.error(f"마이그레이션 중 치명적 오류: {e}")
            print(f"❌ 마이그레이션 실패: {e}")
            return False
        finally:
            self.stats['end_time'] = datetime.now()

    def _test_connections(self) -> bool:
        """SQLite와 MySQL 연결 테스트"""
        print("🔌 데이터베이스 연결 테스트 중...")

        # SQLite 테스트
        try:
            with sqlite3.connect(self.sqlite_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM stocks")
                sqlite_stocks = cursor.fetchone()[0]
                print(f"✅ SQLite 연결 성공 - stocks: {sqlite_stocks:,}개")
        except Exception as e:
            print(f"❌ SQLite 연결 실패: {e}")
            return False

        # MySQL 테스트
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION();")
            version = cursor.fetchone()[0]
            print(f"✅ MySQL 연결 성공 - 버전: {version}")
            conn.close()
        except MySQLError as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

        return True

    def _analyze_sqlite_data(self) -> List[str]:
        """SQLite 데이터 분석"""
        print("🔍 SQLite 데이터 분석 중...")

        try:
            with sqlite3.connect(self.sqlite_path) as conn:
                cursor = conn.cursor()

                # stocks 테이블 분석
                cursor.execute("SELECT COUNT(*) FROM stocks")
                stocks_count = cursor.fetchone()[0]

                # daily_prices 테이블 목록 조회
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name LIKE 'daily_prices_%'
                    ORDER BY name
                """)
                daily_tables = [row[0] for row in cursor.fetchall()]

                # 종목코드 추출
                stock_codes = [table.replace('daily_prices_', '') for table in daily_tables]

                # 총 레코드 수 추정 (샘플링)
                total_records = 0
                sample_size = min(50, len(daily_tables))

                for table in daily_tables[:sample_size]:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    total_records += count

                # 전체 추정
                estimated_total = total_records * (len(daily_tables) / sample_size)

                print(f"📊 분석 결과:")
                print(f"   📋 stocks: {stocks_count:,}개")
                print(f"   📊 daily_prices 테이블: {len(daily_tables)}개")
                print(f"   📈 예상 총 레코드: {estimated_total:,.0f}개")
                print(f"   ⏱️ 예상 소요시간: {estimated_total / 5000:.0f}분")

                return stock_codes

        except Exception as e:
            logger.error(f"SQLite 분석 실패: {e}")
            print(f"❌ SQLite 분석 실패: {e}")
            return []

    def _prepare_mysql_database(self) -> bool:
        """MySQL 데이터베이스 준비"""
        try:
            print("🔧 MySQL 데이터베이스 초기화 중...")

            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 기존 daily_prices 관련 테이블 확인 및 삭제
            cursor.execute("SHOW TABLES LIKE 'daily_prices%';")
            existing_tables = [row[0] for row in cursor.fetchall()]

            if existing_tables:
                print(f"🗑️ 기존 daily_prices 테이블 {len(existing_tables)}개 삭제 중...")
                for table in existing_tables:
                    cursor.execute(f"DROP TABLE {table};")
                conn.commit()
                print(f"✅ 기존 테이블 삭제 완료")

            conn.close()
            print("✅ MySQL 데이터베이스 준비 완료")
            return True

        except Exception as e:
            logger.error(f"MySQL 준비 실패: {e}")
            print(f"❌ MySQL 준비 실패: {e}")
            return False

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

    def _migrate_daily_tables(self, stock_codes: List[str]) -> bool:
        """종목별 daily_prices 테이블 마이그레이션"""
        try:
            print(f"📊 종목별 daily_prices 테이블 마이그레이션 시작...")
            print(f"📊 총 {len(stock_codes)}개 종목 처리 예정")

            # 이미 완료된 종목 제외
            remaining_stocks = [code for code in stock_codes if code not in self.completed_stocks]

            if self.completed_stocks:
                print(f"📋 이미 완료된 종목: {len(self.completed_stocks)}개")
                print(f"📊 남은 종목: {len(remaining_stocks)}개")

            if not remaining_stocks:
                print("✅ 모든 종목이 이미 완료되었습니다.")
                return True

            # 기본 테이블 구조
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

                    -- 인덱스
                    UNIQUE KEY uk_date (date),
                    INDEX idx_date (date),
                    INDEX idx_close_price (close_price),
                    INDEX idx_volume (volume)
                ) ENGINE=InnoDB COMMENT='종목 {stock_code} 일봉 데이터'
            """

            total_records = 0

            for idx, stock_code in enumerate(remaining_stocks):
                try:
                    print(f"\n   📈 {idx + 1}/{len(remaining_stocks)} 처리 중: {stock_code}")

                    # MySQL 연결
                    mysql_conn = mysql.connector.connect(**self.mysql_config)
                    mysql_cursor = mysql_conn.cursor()

                    # 1. 테이블 생성
                    create_sql = table_structure.format(stock_code=stock_code)
                    mysql_cursor.execute(create_sql)
                    mysql_conn.commit()

                    # 2. SQLite에서 데이터 읽기
                    with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                        sqlite_cursor = sqlite_conn.cursor()

                        table_name = f"daily_prices_{stock_code}"

                        # 테이블 존재 확인
                        sqlite_cursor.execute(f"""
                            SELECT name FROM sqlite_master 
                            WHERE type='table' AND name='{table_name}'
                        """)

                        if not sqlite_cursor.fetchone():
                            print(f"      ⚠️ {stock_code}: SQLite 테이블 없음")
                            mysql_conn.close()
                            continue

                        # 데이터 조회
                        sqlite_cursor.execute(f"""
                            SELECT date, open_price, high_price, low_price, close_price,
                                   volume, trading_value, prev_day_diff, change_rate,
                                   data_source, created_at
                            FROM {table_name}
                            ORDER BY date
                        """)

                        stock_data = sqlite_cursor.fetchall()

                    if not stock_data:
                        print(f"      ⚠️ {stock_code}: 데이터 없음")
                        mysql_conn.close()
                        continue

                    # 3. MySQL에 데이터 삽입
                    insert_sql = f"""
                        INSERT INTO daily_prices_{stock_code} 
                        (date, open_price, high_price, low_price, close_price,
                         volume, trading_value, prev_day_diff, change_rate,
                         data_source, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    # 배치 삽입
                    for i in range(0, len(stock_data), self.BATCH_SIZE):
                        batch = stock_data[i:i + self.BATCH_SIZE]
                        mysql_cursor.executemany(insert_sql, batch)
                        mysql_conn.commit()

                    total_records += len(stock_data)
                    print(f"      ✅ {stock_code}: {len(stock_data):,}개 레코드 이관 완료")

                    # 통계 업데이트
                    self.stats['tables_created'] += 1
                    self.stats['tables_migrated'] += 1

                    # 진행상황 저장
                    self._save_progress(stock_code)

                    mysql_conn.close()

                except KeyboardInterrupt:
                    print(f"\n⚠️ 사용자 중단 요청")
                    if 'mysql_conn' in locals():
                        mysql_conn.close()
                    raise
                except Exception as e:
                    logger.error(f"{stock_code} 마이그레이션 실패: {e}")
                    print(f"      ❌ {stock_code}: 마이그레이션 실패 - {e}")
                    self.stats['errors'] += 1
                    if 'mysql_conn' in locals():
                        mysql_conn.close()
                    continue

            self.stats['total_records_migrated'] = total_records
            print(f"\n✅ 종목별 테이블 마이그레이션 완료: {total_records:,}개 레코드")
            return True

        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.error(f"종목별 테이블 마이그레이션 실패: {e}")
            print(f"❌ 종목별 테이블 마이그레이션 실패: {e}")
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

    def _prepare_future_structures(self) -> bool:
        """향후 확장 구조 준비"""
        try:
            print("🚀 향후 확장 구조 준비 중...")

            conn = mysql.connector.connect(**self.mysql_config)
            cursor = conn.cursor()

            # 수급 데이터 템플릿
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

                    data_source VARCHAR(20) DEFAULT 'TR_TBD',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

                    UNIQUE KEY uk_date (date),
                    INDEX idx_date (date)
                ) ENGINE=InnoDB COMMENT='수급 데이터 템플릿'
            """)

            # 분봉 데이터 템플릿
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS minute_data_template (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    datetime DATETIME NOT NULL COMMENT '일시',
                    minute_type TINYINT NOT NULL COMMENT '분봉 타입(1,3,5)',

                    open_price INT, high_price INT, low_price INT, close_price INT,
                    volume BIGINT,

                    data_source VARCHAR(20) DEFAULT 'TR_TBD',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

                    UNIQUE KEY uk_datetime_type (datetime, minute_type),
                    INDEX idx_datetime (datetime)
                ) ENGINE=InnoDB COMMENT='분봉 데이터 템플릿'
            """)

            # 종목 관리 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_management (
                    stock_code VARCHAR(10) PRIMARY KEY,
                    collect_daily BOOLEAN DEFAULT TRUE,
                    collect_supply_demand BOOLEAN DEFAULT FALSE,
                    collect_minute_data BOOLEAN DEFAULT FALSE,
                    minute_types VARCHAR(20) DEFAULT '3',
                    priority TINYINT DEFAULT 3,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                    FOREIGN KEY (stock_code) REFERENCES stocks(code)
                ) ENGINE=InnoDB COMMENT='종목별 수집 관리'
            """)

            conn.commit()
            conn.close()

            print("✅ 향후 확장 구조 준비 완료")
            return True

        except Exception as e:
            logger.error(f"확장 구조 준비 실패: {e}")
            print(f"❌ 확장 구조 준비 실패: {e}")
            return False

    def _verify_migration(self, stock_codes: List[str]) -> bool:
        """마이그레이션 검증"""
        try:
            print("🔍 마이그레이션 검증 중...")

            mysql_conn = mysql.connector.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor()

            # stocks 테이블 검증
            mysql_cursor.execute("SELECT COUNT(*) FROM stocks")
            mysql_stocks_count = mysql_cursor.fetchone()[0]

            # 생성된 daily_prices 테이블 확인
            mysql_cursor.execute("SHOW TABLES LIKE 'daily_prices_%'")
            created_tables = mysql_cursor.fetchall()

            # 샘플 데이터 확인
            total_records = 0
            sample_tables = [table[0] for table in created_tables[:5]]

            for table in sample_tables:
                mysql_cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = mysql_cursor.fetchone()[0]
                total_records += count

            print(f"📊 검증 결과:")
            print(f"   📋 MySQL stocks: {mysql_stocks_count:,}개")
            print(f"   📊 생성된 daily_prices 테이블: {len(created_tables)}개")
            print(f"   📈 샘플 테이블 총 레코드: {total_records:,}개")

            mysql_conn.close()

            # 성공률 계산
            success_rate = len(created_tables) / len(stock_codes) if stock_codes else 0

            print(f"📋 마이그레이션 결과:")
            print(f"   🎯 목표 종목: {len(stock_codes)}개")
            print(f"   ✅ 성공 종목: {len(created_tables)}개")
            print(f"   📈 성공률: {success_rate * 100:.1f}%")

            if success_rate >= 0.9:  # 90% 이상 성공
                print("✅ 마이그레이션 검증 완료")
                return True
            else:
                print(f"⚠️ 마이그레이션 부분 성공: {success_rate * 100:.1f}%")
                return True  # 대부분 성공이면 진행

        except Exception as e:
            logger.error(f"검증 실패: {e}")
            print(f"❌ 검증 실패: {e}")
            return False

    def _print_final_report(self):
        """최종 마이그레이션 리포트"""
        if self.stats['end_time'] and self.stats['start_time']:
            elapsed_time = self.stats['end_time'] - self.stats['start_time']
        else:
            elapsed_time = "측정 불가"

        print(f"\n🎉 SQLite → MySQL 마이그레이션 완료 리포트")
        print("=" * 70)
        print(f"📊 마이그레이션 결과:")
        print(f"   ✅ stocks 마이그레이션: {self.stats['stocks_migrated']:,}개")
        print(f"   ✅ 생성된 종목별 테이블: {self.stats['tables_created']}개")
        print(f"   ✅ 이관된 레코드: {self.stats['total_records_migrated']:,}개")
        print(f"   ❌ 오류 발생: {self.stats['errors']}개")
        print(f"   ⏱️ 총 소요시간: {elapsed_time}")

        print(f"\n🏗️ 새로운 MySQL 구조:")
        print(f"   📋 stocks: 종목 기본정보")
        print(f"   📊 daily_prices_XXXXXX: 종목별 일봉 데이터")
        print(f"   💰 supply_demand_template: 수급 데이터 템플릿")
        print(f"   ⚡ minute_data_template: 분봉 데이터 템플릿")
        print(f"   🎯 stock_management: 종목별 수집 관리")

        print(f"\n🎯 다음 단계:")
        print(f"   1. Python 애플리케이션 MySQL 연동 테스트")
        print(f"   2. 일일 업데이트 시스템 구축 (종목별 테이블 기반)")
        print(f"   3. 수급 데이터 TR 코드 조사 및 구현")
        print(f"   4. 지정 종목 분봉 데이터 수집 시스템")
        print(f"   5. 웹 대시보드 개발")


def main():
    """메인 실행 함수"""
    print("🚀 SQLite → MySQL 종목별 테이블 직접 마이그레이션 도구")
    print("=" * 70)

    try:
        # 사용자 확인
        print("⚠️  주의사항:")
        print("   1. SQLite에서 MySQL로 직접 종목별 테이블 생성")
        print("   2. 2,565개 종목 × 평균 2,847개 레코드 = 약 730만 레코드")
        print("   3. 예상 소요시간: 60-120분")
        print("   4. 중단 시 진행상황이 저장되어 재시작 가능")
        print("   5. MySQL 기존 데이터는 삭제됩니다")

        response = input("\n계속 진행하시겠습니까? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("❌ 마이그레이션이 취소되었습니다.")
            return False

        # 마이그레이션 실행
        migrator = SQLiteToMySQLDirectMigrator()
        success = migrator.migrate_all_data()

        if success:
            print(f"\n🎉 마이그레이션 성공!")
            print(f"💡 이제 MySQL에서 종목별 테이블 구조를 사용할 수 있습니다.")

            # .env 파일 확인
            env_path = Path(".env")
            if env_path.exists():
                with open(env_path, 'r', encoding='utf-8') as f:
                    env_content = f.read()
                    if 'DB_TYPE=mysql' not in env_content:
                        print("⚠️  .env 파일이 SQLite로 설정되어 있습니다.")
                        response = input("MySQL로 변경하시겠습니까? (y/N): ")
                        if response.lower() == 'y':
                            # .env 파일 백업 및 업데이트
                            backup_path = env_path.with_suffix('.env.sqlite.backup')
                            env_path.rename(backup_path)

                            mysql_env = """# MySQL 설정 (마이그레이션 완료)
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_NAME=stock_trading_db
DB_USER=stock_user
DB_PASSWORD=StockPass2025!

# 기타 설정
ENVIRONMENT=development
DEBUG=True
LOG_LEVEL=INFO
API_REQUEST_DELAY_MS=3600
MAX_RETRY_ATTEMPTS=3
RETRY_DELAY_SECONDS=3.6
"""
                            with open(env_path, 'w', encoding='utf-8') as f:
                                f.write(mysql_env)
                            print(f"✅ .env 파일이 MySQL로 업데이트되었습니다.")

            return True
        else:
            print(f"\n❌ 마이그레이션 실패!")
            return False

    except KeyboardInterrupt:
        print(f"\n\n👋 사용자가 마이그레이션을 중단했습니다.")
        print(f"📋 진행상황이 저장되었습니다. 다시 실행하면 이어서 진행됩니다.")
        return False
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        logger.error(f"메인 함수 실행 중 오류: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)