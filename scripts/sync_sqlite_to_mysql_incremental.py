#!/usr/bin/env python3
"""
파일 경로: scripts/sync_sqlite_to_mysql_incremental.py

SQLite → MySQL 증분 동기화 시스템
- SQLite에서 새로 추가된 데이터만 MySQL로 동기화
- 기존 MySQL 데이터 유지하면서 신규 데이터만 추가
- 실시간 동기화 및 스케줄링 기능
- 충돌 감지 및 해결
"""
import sys
import os
import sqlite3
import time
import schedule
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Set
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
    print("📥 설치 명령어: pip install mysql-connector-python schedule")
    sys.exit(1)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('incremental_sync.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class IncrementalSyncManager:
    """SQLite → MySQL 증분 동기화 관리자"""

    def __init__(self):
        # 데이터베이스 연결 정보
        self.sqlite_path = Path("./data/stock_data.db")
        self.mysql_config = {
            'host': 'localhost',
            'port': 3306,
            'database': 'stock_trading_db',
            'user': 'stock_user',
            'password': 'StockPass2025!',
            'charset': 'utf8mb4',
            'autocommit': False
        }

        # 동기화 상태 파일
        self.sync_state_file = Path("sync_state.json")
        self.sync_state = self._load_sync_state()

        # 배치 처리 설정
        self.BATCH_SIZE = 1000

        # 통계
        self.stats = {
            'sync_start_time': None,
            'sync_end_time': None,
            'stocks_synced': 0,
            'records_synced': 0,
            'tables_created': 0,
            'errors': 0
        }

    def _load_sync_state(self) -> Dict[str, Any]:
        """동기화 상태 로드"""
        if self.sync_state_file.exists():
            try:
                with open(self.sync_state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    logger.info(f"동기화 상태 로드 완료: 마지막 동기화 {state.get('last_sync_time', 'N/A')}")
                    return state
            except Exception as e:
                logger.error(f"동기화 상태 로드 실패: {e}")

        # 기본 상태
        return {
            'last_sync_time': None,
            'last_synced_dates': {},  # 종목별 마지막 동기화 날짜
            'mysql_table_status': {},  # MySQL 테이블 존재 여부
            'sync_history': []
        }

    def _save_sync_state(self):
        """동기화 상태 저장"""
        try:
            self.sync_state['last_sync_time'] = datetime.now().isoformat()
            with open(self.sync_state_file, 'w', encoding='utf-8') as f:
                json.dump(self.sync_state, f, indent=2, ensure_ascii=False, default=str)
        except Exception as e:
            logger.error(f"동기화 상태 저장 실패: {e}")

    def sync_incremental(self, force_resync: bool = False) -> bool:
        """증분 동기화 실행"""
        print("🔄 SQLite → MySQL 증분 동기화 시작")
        print("=" * 50)

        self.stats['sync_start_time'] = datetime.now()

        try:
            # 1. 연결 테스트
            if not self._test_connections():
                return False

            # 2. 변경된 데이터 감지
            changes = self._detect_changes(force_resync)
            if not changes['has_changes'] and not force_resync:
                print("ℹ️ 동기화할 새로운 데이터가 없습니다.")
                return True

            # 3. stocks 테이블 동기화
            if changes['stocks_changed'] or force_resync:
                print("\n📋 1단계: stocks 테이블 증분 동기화")
                if not self._sync_stocks_table():
                    return False

            # 4. 신규 종목 테이블 생성
            if changes['new_stocks']:
                print(f"\n🆕 2단계: 신규 종목 테이블 생성 ({len(changes['new_stocks'])}개)")
                if not self._create_new_stock_tables(changes['new_stocks']):
                    return False

            # 5. 기존 종목 데이터 동기화
            if changes['updated_stocks']:
                print(f"\n🔄 3단계: 기존 종목 데이터 증분 동기화 ({len(changes['updated_stocks'])}개)")
                if not self._sync_existing_stocks(changes['updated_stocks']):
                    return False

            # 6. 동기화 상태 업데이트
            self._update_sync_state(changes)

            # 7. 최종 리포트
            self._print_sync_report()

            return True

        except Exception as e:
            logger.error(f"증분 동기화 실패: {e}")
            print(f"❌ 동기화 실패: {e}")
            return False
        finally:
            self.stats['sync_end_time'] = datetime.now()

    def _test_connections(self) -> bool:
        """데이터베이스 연결 테스트"""
        # SQLite 테스트
        try:
            if not self.sqlite_path.exists():
                print(f"❌ SQLite 파일을 찾을 수 없습니다: {self.sqlite_path}")
                return False

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
            cursor.execute("SELECT COUNT(*) FROM stocks")
            mysql_stocks = cursor.fetchone()[0]
            print(f"✅ MySQL 연결 성공 - stocks: {mysql_stocks:,}개")
            conn.close()
        except MySQLError as e:
            print(f"❌ MySQL 연결 실패: {e}")
            return False

        return True

    def _detect_changes(self, force_resync: bool = False) -> Dict[str, Any]:
        """변경된 데이터 감지"""
        print("🔍 변경 데이터 감지 중...")

        changes = {
            'has_changes': False,
            'stocks_changed': False,
            'new_stocks': [],
            'updated_stocks': {},  # {stock_code: {'last_mysql_date': 'YYYYMMDD', 'new_records': count}}
            'detection_time': datetime.now().isoformat()
        }

        try:
            # SQLite에서 현재 종목 목록 조회
            with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                cursor = sqlite_conn.cursor()

                # 전체 종목 조회
                cursor.execute("SELECT code FROM stocks ORDER BY code")
                sqlite_stocks = {row[0] for row in cursor.fetchall()}

                # daily_prices 테이블 목록 조회
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name LIKE 'daily_prices_%'
                """)
                sqlite_daily_tables = {
                    table[0].replace('daily_prices_', '')
                    for table in cursor.fetchall()
                }

            # MySQL에서 현재 상태 조회
            mysql_conn = mysql.connector.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor()

            # MySQL 종목 목록
            mysql_cursor.execute("SELECT code FROM stocks")
            mysql_stocks = {row[0] for row in mysql_cursor.fetchall()}

            # MySQL daily_prices 테이블 목록
            mysql_cursor.execute("SHOW TABLES LIKE 'daily_prices_%'")
            mysql_daily_tables = {
                table[0].replace('daily_prices_', '')
                for table in mysql_cursor.fetchall()
            }

            # 1. stocks 테이블 변경 감지
            if sqlite_stocks != mysql_stocks or force_resync:
                changes['stocks_changed'] = True
                changes['has_changes'] = True
                print(f"   📋 stocks 테이블 변경 감지: SQLite {len(sqlite_stocks)}개 vs MySQL {len(mysql_stocks)}개")

            # 2. 신규 종목 감지
            new_stocks = sqlite_daily_tables - mysql_daily_tables
            if new_stocks:
                changes['new_stocks'] = list(new_stocks)
                changes['has_changes'] = True
                print(f"   🆕 신규 종목 감지: {len(new_stocks)}개")
                for stock in sorted(new_stocks)[:5]:
                    print(f"      - {stock}")
                if len(new_stocks) > 5:
                    print(f"      ... 외 {len(new_stocks) - 5}개")

            # 3. 기존 종목의 새 데이터 감지
            common_stocks = sqlite_daily_tables & mysql_daily_tables
            for stock_code in common_stocks:
                # SQLite 최신 날짜
                with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                    cursor = sqlite_conn.cursor()
                    cursor.execute(f"SELECT MAX(date) FROM daily_prices_{stock_code}")
                    sqlite_max_date = cursor.fetchone()[0]

                # MySQL 최신 날짜
                mysql_cursor.execute(f"SELECT MAX(date) FROM daily_prices_{stock_code}")
                mysql_result = mysql_cursor.fetchone()
                mysql_max_date = mysql_result[0] if mysql_result and mysql_result[0] else '00000000'

                # 비교
                if sqlite_max_date and (not mysql_max_date or sqlite_max_date > mysql_max_date):
                    # 새로운 레코드 개수 계산
                    with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                        cursor = sqlite_conn.cursor()
                        cursor.execute(
                            f"SELECT COUNT(*) FROM daily_prices_{stock_code} WHERE date > ?",
                            (mysql_max_date,)
                        )
                        new_records = cursor.fetchone()[0]

                    if new_records > 0:
                        changes['updated_stocks'][stock_code] = {
                            'last_mysql_date': mysql_max_date,
                            'last_sqlite_date': sqlite_max_date,
                            'new_records': new_records
                        }
                        changes['has_changes'] = True

            mysql_conn.close()

            # 결과 출력
            if changes['updated_stocks']:
                print(f"   🔄 업데이트된 종목: {len(changes['updated_stocks'])}개")
                total_new_records = sum(info['new_records'] for info in changes['updated_stocks'].values())
                print(f"   📊 새로운 레코드: {total_new_records:,}개")

                # 상위 5개 종목 표시
                sorted_stocks = sorted(
                    changes['updated_stocks'].items(),
                    key=lambda x: x[1]['new_records'],
                    reverse=True
                )
                for stock_code, info in sorted_stocks[:5]:
                    print(
                        f"      - {stock_code}: {info['new_records']}개 ({info['last_mysql_date']} → {info['last_sqlite_date']})")

            print(f"📊 변경 감지 완료: {'변경사항 있음' if changes['has_changes'] else '변경사항 없음'}")
            return changes

        except Exception as e:
            logger.error(f"변경 감지 실패: {e}")
            print(f"❌ 변경 감지 실패: {e}")
            return changes

    def _sync_stocks_table(self) -> bool:
        """stocks 테이블 동기화"""
        try:
            print("📋 stocks 테이블 동기화 중...")

            # SQLite에서 모든 stocks 데이터 읽기
            with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                cursor = sqlite_conn.cursor()
                cursor.execute("""
                    SELECT code, name, market, current_price, prev_day_diff, 
                           change_rate, volume, open_price, high_price, low_price,
                           upper_limit, lower_limit, market_cap, market_cap_size,
                           listed_shares, per_ratio, pbr_ratio, data_source,
                           last_updated, is_active, created_at, updated_at
                    FROM stocks
                """)
                sqlite_stocks = cursor.fetchall()

            # MySQL 기존 데이터와 비교하여 INSERT OR UPDATE
            mysql_conn = mysql.connector.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor()

            # REPLACE INTO 사용 (MySQL의 INSERT OR UPDATE)
            replace_query = """
                REPLACE INTO stocks (
                    code, name, market, current_price, prev_day_diff, 
                    change_rate, volume, open_price, high_price, low_price,
                    upper_limit, lower_limit, market_cap, market_cap_size,
                    listed_shares, per_ratio, pbr_ratio, data_source,
                    last_updated, is_active, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # 배치 처리
            synced_count = 0
            for i in range(0, len(sqlite_stocks), self.BATCH_SIZE):
                batch = sqlite_stocks[i:i + self.BATCH_SIZE]
                mysql_cursor.executemany(replace_query, batch)
                mysql_conn.commit()
                synced_count += len(batch)

                print(
                    f"   📥 {synced_count}/{len(sqlite_stocks)} 동기화 완료 ({synced_count / len(sqlite_stocks) * 100:.1f}%)")

            mysql_conn.close()

            self.stats['stocks_synced'] = synced_count
            print(f"✅ stocks 테이블 동기화 완료: {synced_count:,}개")
            return True

        except Exception as e:
            logger.error(f"stocks 테이블 동기화 실패: {e}")
            print(f"❌ stocks 테이블 동기화 실패: {e}")
            return False

    def _create_new_stock_tables(self, new_stocks: List[str]) -> bool:
        """신규 종목 테이블 생성 및 데이터 이관"""
        try:
            print(f"🆕 신규 종목 테이블 생성 중: {len(new_stocks)}개")

            # 테이블 구조 템플릿
            table_structure = """
                CREATE TABLE daily_prices_{stock_code} (
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
                ) ENGINE=InnoDB COMMENT='종목 {stock_code} 일봉 데이터'
            """

            mysql_conn = mysql.connector.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor()

            for i, stock_code in enumerate(new_stocks):
                try:
                    print(f"   🆕 {i + 1}/{len(new_stocks)} 생성 중: {stock_code}")

                    # 1. 테이블 생성
                    create_sql = table_structure.format(stock_code=stock_code)
                    mysql_cursor.execute(create_sql)

                    # 2. SQLite에서 데이터 읽기
                    with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                        cursor = sqlite_conn.cursor()
                        cursor.execute(f"""
                            SELECT date, open_price, high_price, low_price, close_price,
                                   volume, trading_value, prev_day_diff, change_rate,
                                   data_source, created_at
                            FROM daily_prices_{stock_code}
                            ORDER BY date
                        """)
                        stock_data = cursor.fetchall()

                    if stock_data:
                        # 3. MySQL에 데이터 삽입
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
                            mysql_cursor.executemany(insert_sql, batch)
                            mysql_conn.commit()

                        print(f"      ✅ {stock_code}: {len(stock_data):,}개 레코드 이관 완료")
                        self.stats['records_synced'] += len(stock_data)
                    else:
                        print(f"      ⚠️ {stock_code}: 데이터 없음")

                    self.stats['tables_created'] += 1

                except Exception as e:
                    logger.error(f"신규 종목 {stock_code} 생성 실패: {e}")
                    print(f"      ❌ {stock_code}: 생성 실패")
                    self.stats['errors'] += 1
                    continue

            mysql_conn.close()
            print(f"✅ 신규 종목 테이블 생성 완료: {self.stats['tables_created']}개")
            return True

        except Exception as e:
            logger.error(f"신규 종목 테이블 생성 실패: {e}")
            print(f"❌ 신규 종목 테이블 생성 실패: {e}")
            return False

    def _sync_existing_stocks(self, updated_stocks: Dict[str, Dict]) -> bool:
        """기존 종목의 새 데이터 동기화"""
        try:
            print(f"🔄 기존 종목 증분 동기화 중: {len(updated_stocks)}개")

            mysql_conn = mysql.connector.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor()

            for i, (stock_code, info) in enumerate(updated_stocks.items()):
                try:
                    last_mysql_date = info['last_mysql_date']
                    new_records_count = info['new_records']

                    print(f"   🔄 {i + 1}/{len(updated_stocks)} 동기화 중: {stock_code} (+{new_records_count}개)")

                    # SQLite에서 새 데이터만 읽기
                    with sqlite3.connect(self.sqlite_path) as sqlite_conn:
                        cursor = sqlite_conn.cursor()
                        cursor.execute(f"""
                            SELECT date, open_price, high_price, low_price, close_price,
                                   volume, trading_value, prev_day_diff, change_rate,
                                   data_source, created_at
                            FROM daily_prices_{stock_code}
                            WHERE date > ?
                            ORDER BY date
                        """, (last_mysql_date,))
                        new_data = cursor.fetchall()

                    if new_data:
                        # MySQL에 새 데이터 삽입
                        insert_sql = f"""
                            INSERT IGNORE INTO daily_prices_{stock_code} 
                            (date, open_price, high_price, low_price, close_price,
                             volume, trading_value, prev_day_diff, change_rate,
                             data_source, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

                        # 배치 삽입
                        for j in range(0, len(new_data), self.BATCH_SIZE):
                            batch = new_data[j:j + self.BATCH_SIZE]
                            mysql_cursor.executemany(insert_sql, batch)
                            mysql_conn.commit()

                        print(f"      ✅ {stock_code}: {len(new_data):,}개 새 레코드 추가")
                        self.stats['records_synced'] += len(new_data)
                    else:
                        print(f"      ⚠️ {stock_code}: 새 데이터 없음")

                except Exception as e:
                    logger.error(f"종목 {stock_code} 동기화 실패: {e}")
                    print(f"      ❌ {stock_code}: 동기화 실패")
                    self.stats['errors'] += 1
                    continue

            mysql_conn.close()
            print(f"✅ 기존 종목 증분 동기화 완료")
            return True

        except Exception as e:
            logger.error(f"기존 종목 동기화 실패: {e}")
            print(f"❌ 기존 종목 동기화 실패: {e}")
            return False

    def _update_sync_state(self, changes: Dict[str, Any]):
        """동기화 상태 업데이트"""
        try:
            # 마지막 동기화 시간 업데이트
            self.sync_state['last_sync_time'] = datetime.now().isoformat()

            # 종목별 마지막 동기화 날짜 업데이트
            mysql_conn = mysql.connector.connect(**self.mysql_config)
            mysql_cursor = mysql_conn.cursor()

            # 모든 종목의 최신 날짜 조회
            mysql_cursor.execute("SHOW TABLES LIKE 'daily_prices_%'")
            tables = [table[0] for table in mysql_cursor.fetchall()]

            for table in tables:
                stock_code = table.replace('daily_prices_', '')
                mysql_cursor.execute(f"SELECT MAX(date) FROM {table}")
                result = mysql_cursor.fetchone()
                if result and result[0]:
                    self.sync_state['last_synced_dates'][stock_code] = result[0]

            mysql_conn.close()

            # 동기화 히스토리 추가
            sync_record = {
                'sync_time': datetime.now().isoformat(),
                'new_stocks': len(changes.get('new_stocks', [])),
                'updated_stocks': len(changes.get('updated_stocks', {})),
                'records_synced': self.stats['records_synced'],
                'tables_created': self.stats['tables_created'],
                'errors': self.stats['errors']
            }

            if 'sync_history' not in self.sync_state:
                self.sync_state['sync_history'] = []

            self.sync_state['sync_history'].append(sync_record)

            # 히스토리는 최근 10개만 유지
            if len(self.sync_state['sync_history']) > 10:
                self.sync_state['sync_history'] = self.sync_state['sync_history'][-10:]

            # 상태 저장
            self._save_sync_state()

        except Exception as e:
            logger.error(f"동기화 상태 업데이트 실패: {e}")

    def _print_sync_report(self):
        """동기화 결과 리포트"""
        elapsed_time = None
        if self.stats['sync_end_time'] and self.stats['sync_start_time']:
            elapsed_time = self.stats['sync_end_time'] - self.stats['sync_start_time']

        print(f"\n🎉 증분 동기화 완료 리포트")
        print("=" * 50)
        print(f"📊 동기화 결과:")
        print(f"   ✅ stocks 동기화: {self.stats['stocks_synced']:,}개")
        print(f"   🆕 신규 테이블 생성: {self.stats['tables_created']}개")
        print(f"   📈 새 레코드 동기화: {self.stats['records_synced']:,}개")
        print(f"   ❌ 오류 발생: {self.stats['errors']}개")
        if elapsed_time:
            print(f"   ⏱️ 소요시간: {elapsed_time}")

        print(f"\n🔄 다음 동기화:")
        print(f"   📅 마지막 동기화: {self.sync_state['last_sync_time']}")
        print(f"   🎯 모니터링 종목: {len(self.sync_state.get('last_synced_dates', {}))}")

    def start_scheduler(self, interval_minutes: int = 30):
        """스케줄러 시작 (주기적 동기화)"""
        print(f"🕐 스케줄러 시작: {interval_minutes}분마다 자동 동기화")

        # 스케줄 등록
        schedule.every(interval_minutes).minutes.do(self.sync_incremental)

        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # 1분마다 스케줄 확인
        except KeyboardInterrupt:
            print(f"\n👋 스케줄러 중단됨")

    def sync_status(self):
        """현재 동기화 상태 표시"""
        print("📊 현재 동기화 상태")
        print("=" * 30)

        if self.sync_state.get('last_sync_time'):
            print(f"🕐 마지막 동기화: {self.sync_state['last_sync_time']}")
            print(f"🎯 모니터링 종목: {len(self.sync_state.get('last_synced_dates', {}))}")

            # 최근 동기화 히스토리
            if self.sync_state.get('sync_history'):
                print(f"\n📈 최근 동기화 히스토리:")
                for record in self.sync_state['sync_history'][-3:]:
                    sync_time = record['sync_time'][:19]  # YYYY-MM-DD HH:MM:SS
                    print(
                        f"   {sync_time}: 신규 {record['new_stocks']}개, 업데이트 {record['updated_stocks']}개, 레코드 {record['records_synced']}개")
        else:
            print("ℹ️ 아직 동기화된 적이 없습니다.")


def main():
    """메인 실행 함수"""
    import argparse

    parser = argparse.ArgumentParser(description='SQLite → MySQL 증분 동기화 시스템')
    parser.add_argument('--sync', action='store_true', help='즉시 동기화 실행')
    parser.add_argument('--force', action='store_true', help='강제 전체 재동기화')
    parser.add_argument('--schedule', type=int, metavar='MINUTES', help='스케줄러 시작 (분 단위)')
    parser.add_argument('--status', action='store_true', help='현재 동기화 상태 확인')

    args = parser.parse_args()

    try:
        sync_manager = IncrementalSyncManager()

        if args.status:
            # 상태 확인
            sync_manager.sync_status()

        elif args.sync:
            # 즉시 동기화
            print("🚀 즉시 증분 동기화 실행")
            success = sync_manager.sync_incremental(force_resync=args.force)
            if success:
                print("✅ 동기화 완료!")
            else:
                print("❌ 동기화 실패!")
                return False

        elif args.schedule:
            # 스케줄러 시작
            sync_manager.start_scheduler(args.schedule)

        else:
            # 기본: 메뉴 표시
            print("🔄 SQLite → MySQL 증분 동기화 시스템")
            print("=" * 50)
            print("사용법:")
            print("  python scripts/sync_sqlite_to_mysql_incremental.py --sync          # 즉시 동기화")
            print("  python scripts/sync_sqlite_to_mysql_incremental.py --sync --force  # 강제 전체 재동기화")
            print("  python scripts/sync_sqlite_to_mysql_incremental.py --schedule 30   # 30분마다 자동 동기화")
            print("  python scripts/sync_sqlite_to_mysql_incremental.py --status        # 상태 확인")
            print()

            # 인터랙티브 모드
            while True:
                print("\n선택하세요:")
                print("1. 즉시 동기화")
                print("2. 강제 전체 재동기화")
                print("3. 현재 상태 확인")
                print("4. 스케줄러 시작 (30분 간격)")
                print("5. 종료")

                choice = input("\n선택 (1-5): ").strip()

                if choice == '1':
                    print("\n🔄 즉시 동기화 시작...")
                    sync_manager.sync_incremental()

                elif choice == '2':
                    print("\n🔄 강제 전체 재동기화 시작...")
                    confirm = input("⚠️ 모든 데이터를 다시 동기화합니다. 계속하시겠습니까? (y/N): ")
                    if confirm.lower() == 'y':
                        sync_manager.sync_incremental(force_resync=True)

                elif choice == '3':
                    sync_manager.sync_status()

                elif choice == '4':
                    interval = input("동기화 간격 (분, 기본값 30): ").strip()
                    interval = int(interval) if interval.isdigit() else 30
                    print(f"\n🕐 {interval}분 간격으로 스케줄러 시작...")
                    sync_manager.start_scheduler(interval)

                elif choice == '5':
                    print("👋 종료합니다.")
                    break

                else:
                    print("❌ 잘못된 선택입니다.")

        return True

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