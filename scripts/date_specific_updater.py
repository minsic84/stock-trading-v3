#!/usr/bin/env python3
"""
파일 경로: scripts/date_specific_updater.py

날짜 지정 데이터 업데이트 시스템 (개선 완료)
- 사용자가 원하는 날짜 지정 가능
- 기존 데이터가 있으면 수정 (UPDATE)
- 기존 데이터가 없으면 추가 (INSERT)
- 디폴트: 오늘 날짜
- 종목 코드 표준화: DB(숫자6자리) / API(_AL형식)
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import logging
from dataclasses import dataclass, field
import time
import argparse
import mysql.connector
from contextlib import contextmanager

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config
from src.core.database import get_database_service
from src.api.base_session import create_kiwoom_session
from src.collectors.integrated_collector import create_integrated_collector
from rich.console import Console
from rich.progress import Progress, TaskID
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from src.collectors.daily_price import DailyPriceCollector

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/date_specific_update.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class UpdateTask:
    """업데이트 작업 정보 (완성된 클래스)"""
    stock_code: str  # DB용 일반 형식 (005930)
    stock_name: str  # 종목명
    target_date: str  # 대상 날짜 (YYYYMMDD)
    action: str  # 'INSERT', 'UPDATE', 'SKIP'
    market: str = ""  # 시장 구분 (KOSPI/KOSDAQ)
    existing_data: Optional[Dict[str, Any]] = None  # 기존 데이터
    last_date: Optional[str] = None  # 마지막 수집 날짜
    api_stock_code: str = field(init=False)  # API용 _AL 형식 (자동 생성)

    def __post_init__(self):
        """초기화 후 API 코드 자동 생성"""
        self.api_stock_code = f"{self.stock_code}_AL"


class MySQLConnectionManager:
    """MySQL 연결 관리자 (중복 코드 제거)"""

    def __init__(self):
        self.stock_trading_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'stock_user',
            'password': 'StockPass2025!',
            'database': 'stock_trading_db',
            'charset': 'utf8mb4'
        }

        self.daily_prices_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'stock_user',
            'password': 'StockPass2025!',
            'database': 'daily_prices_db',
            'charset': 'utf8mb4'
        }

    @contextmanager
    def get_stock_trading_connection(self):
        """stock_trading_db 연결 컨텍스트 매니저"""
        conn = None
        try:
            conn = mysql.connector.connect(**self.stock_trading_config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()

    @contextmanager
    def get_daily_prices_connection(self):
        """daily_prices_db 연결 컨텍스트 매니저"""
        conn = None
        try:
            conn = mysql.connector.connect(**self.daily_prices_config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            raise e
        finally:
            if conn:
                conn.close()


class StockCodeManager:
    """종목 코드 처리 표준화 (일관성 확보)"""

    @staticmethod
    def normalize_to_db_format(stock_code: str) -> str:
        """DB 저장용 형식으로 정규화 (005930)"""
        # _AL 제거
        if stock_code.endswith('_AL'):
            base_code = stock_code[:-3]
        else:
            base_code = stock_code

        # 숫자 6자리 검증
        if len(base_code) == 6 and base_code.isdigit():
            return base_code
        else:
            raise ValueError(f"잘못된 종목 코드 형식: {stock_code} (6자리 숫자 필요)")

    @staticmethod
    def normalize_to_api_format(stock_code: str) -> str:
        """API 요청용 형식으로 변환 (005930_AL)"""
        db_code = StockCodeManager.normalize_to_db_format(stock_code)
        return f"{db_code}_AL"

    @staticmethod
    def get_table_name(stock_code: str) -> str:
        """종목별 테이블명 생성 (daily_prices_005930)"""
        db_code = StockCodeManager.normalize_to_db_format(stock_code)
        return f"daily_prices_{db_code}"


class DateSpecificUpdater:
    """날짜 지정 업데이트 시스템 (완전 개선)"""

    def __init__(self, target_date: str = None):
        self.config = Config()
        self.db_manager = MySQLConnectionManager()
        self.session = None
        self.collector = None
        self.console = Console()

        # 대상 날짜 설정 (디폴트: 오늘)
        if target_date:
            self.target_date = self._validate_date(target_date)
        else:
            self.target_date = datetime.now().strftime('%Y%m%d')

        # 업데이트 통계
        self.stats = {
            'total_stocks': 0,
            'inserted': 0,  # 신규 추가
            'updated': 0,  # 기존 데이터 수정
            'skipped': 0,  # 건너뛴 항목
            'failed': 0,  # 실패
            'start_time': None,
            'end_time': None
        }

    def _validate_date(self, date_str: str) -> str:
        """날짜 형식 검증 및 변환"""
        try:
            # 다양한 형식 지원
            formats = ['%Y%m%d', '%Y-%m-%d', '%Y.%m.%d', '%Y/%m/%d']

            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y%m%d')
                except ValueError:
                    continue

            raise ValueError(f"지원하지 않는 날짜 형식: {date_str}")

        except Exception as e:
            logger.error(f"날짜 검증 실패: {e}")
            raise

    def initialize_services(self) -> bool:
        """서비스 초기화"""
        try:
            # 키움 API 세션 초기화
            self.session = create_kiwoom_session()

            # 이미 연결되어 있는지 확인
            if hasattr(self.session, 'is_connected') and self.session.is_connected():
                logger.info("키움 API 이미 연결됨")
            else:
                logger.info("키움 API 연결 대기 중...")

            # 일봉 수집기 초기화 (날짜 지정용)
            self.daily_collector = DailyPriceCollector(self.config)

            logger.info("서비스 초기화 완료")
            return True

        except Exception as e:
            logger.error(f"서비스 초기화 실패: {e}")
            return False

    def analyze_update_tasks(self, stock_codes: List[str] = None) -> List[UpdateTask]:
        """업데이트 작업 분석 (개선된 버전)"""
        logger.info(f"날짜 {self.target_date} 업데이트 작업 분석 중...")

        try:
            with self.db_manager.get_stock_trading_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # 대상 종목 조회
                if stock_codes:
                    # 특정 종목들만 (종목 코드 정규화)
                    normalized_codes = []
                    for code in stock_codes:
                        try:
                            normalized_code = StockCodeManager.normalize_to_db_format(code)
                            normalized_codes.append(normalized_code)
                        except ValueError as e:
                            logger.warning(f"종목 코드 정규화 실패: {e}")
                            continue

                    if not normalized_codes:
                        logger.error("유효한 종목 코드가 없습니다")
                        return []

                    placeholders = ','.join(['%s'] * len(normalized_codes))
                    query = f"""
                    SELECT code, name, market 
                    FROM stocks 
                    WHERE code IN ({placeholders}) AND is_active = 1
                    ORDER BY market, code
                    """
                    cursor.execute(query, normalized_codes)
                    stocks = cursor.fetchall()
                else:
                    # 전체 활성 종목 (숫자 6자리만)
                    query = """
                    SELECT code, name, market 
                    FROM stocks 
                    WHERE is_active = 1 AND code REGEXP '^[0-9]{6}$'
                    ORDER BY market, code
                    """
                    cursor.execute(query)
                    stocks = cursor.fetchall()

            tasks = []

            for stock in stocks:
                stock_code = stock['code']  # DB 형식 (005930)
                stock_name = stock['name']
                market = stock['market']

                # 해당 날짜 데이터 존재 여부 확인
                existing_data = self._check_existing_data(stock_code, self.target_date)

                # 마지막 수집 날짜 확인
                last_date = self._get_last_collection_date(stock_code)

                if existing_data:
                    action = 'UPDATE'
                else:
                    action = 'INSERT'

                tasks.append(UpdateTask(
                    stock_code=stock_code,
                    stock_name=stock_name,
                    target_date=self.target_date,
                    action=action,
                    market=market,
                    existing_data=existing_data,
                    last_date=last_date
                ))

            logger.info(f"총 {len(tasks)}개 작업 분석 완료")
            return tasks

        except Exception as e:
            logger.error(f"업데이트 작업 분석 실패: {e}")
            return []

    def _check_existing_data(self, stock_code: str, date: str) -> Optional[Dict[str, Any]]:
        """특정 날짜의 기존 데이터 확인 (통합된 단일 메서드)"""
        try:
            table_name = StockCodeManager.get_table_name(stock_code)

            with self.db_manager.get_daily_prices_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # 테이블 존재 확인
                check_query = """
                SELECT COUNT(*) as cnt 
                FROM information_schema.tables 
                WHERE table_schema = 'daily_prices_db' 
                AND table_name = %s
                """

                cursor.execute(check_query, (table_name,))
                result = cursor.fetchone()

                if not result or result['cnt'] == 0:
                    return None

                # 해당 날짜 데이터 조회
                query = f"""
                SELECT * FROM {table_name} 
                WHERE date = %s
                """

                cursor.execute(query, (date,))
                result = cursor.fetchone()

                return result if result else None

        except Exception as e:
            logger.debug(f"[{stock_code}] 기존 데이터 확인 중 오류: {e}")
            return None

    def _get_last_collection_date(self, stock_code: str) -> Optional[str]:
        """종목의 마지막 수집 날짜 확인"""
        try:
            table_name = StockCodeManager.get_table_name(stock_code)

            with self.db_manager.get_daily_prices_connection() as conn:
                cursor = conn.cursor()

                # 테이블 존재 확인
                check_query = """
                SELECT COUNT(*) as cnt 
                FROM information_schema.tables 
                WHERE table_schema = 'daily_prices_db' 
                AND table_name = %s
                """

                cursor.execute(check_query, (table_name,))
                result = cursor.fetchone()

                if not result or result[0] == 0:
                    return None

                # 최신 날짜 조회
                query = f"""
                SELECT MAX(date) as last_date 
                FROM {table_name}
                """

                cursor.execute(query)
                result = cursor.fetchone()

                return result[0] if result and result[0] else None

        except Exception as e:
            logger.debug(f"[{stock_code}] 마지막 수집 날짜 확인 중 오류: {e}")
            return None

    def update_single_stock(self, task: UpdateTask) -> bool:
        """개별 종목 데이터 업데이트 (표준화된 버전)"""
        try:
            stock_code = task.stock_code  # DB용 일반 형식 (005930)
            api_code = task.api_stock_code  # API용 _AL 형식 (005930_AL)
            stock_name = task.stock_name
            target_date = task.target_date

            logger.info(f"[{stock_code}] {stock_name} 업데이트: {target_date} (API: {api_code})")

            # 데이터 수집 (API는 _AL 형식으로 요청)
            result = self.daily_collector.collect_single_stock(
                stock_code=api_code,
                start_date=target_date,
                end_date=target_date
            )

            if not result or not result.get('success'):
                logger.warning(f"[{stock_code}] API 데이터 수집 실패")
                return False

            api_data = result.get('data')
            if not api_data:
                logger.warning(f"[{stock_code}] {target_date} 데이터 없음 (휴장일 가능성)")
                return False

            # 데이터베이스 업데이트 (DB는 일반 형식으로 저장)
            success = self._save_or_update_data(task, api_data)

            if success:
                logger.info(f"[{stock_code}] {task.action} 완료")
                return True
            else:
                logger.error(f"[{stock_code}] 데이터 저장 실패")
                return False

        except Exception as e:
            logger.error(f"[{task.stock_code}] 업데이트 중 오류: {e}")
            return False

    def _save_or_update_data(self, task: UpdateTask, api_data: Dict[str, Any]) -> bool:
        """데이터 저장 또는 업데이트 (개선된 버전)"""
        try:
            stock_code = task.stock_code  # 일반 형식 (005930)
            table_name = StockCodeManager.get_table_name(stock_code)

            # 테이블이 없으면 생성
            self._ensure_table_exists(stock_code)

            with self.db_manager.get_daily_prices_connection() as conn:
                cursor = conn.cursor()

                if task.action == 'INSERT':
                    # 신규 데이터 추가
                    query = f"""
                    INSERT INTO {table_name} 
                    (date, open_price, high_price, low_price, close_price, volume, trading_value, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    """

                    params = (
                        api_data['date'],
                        api_data.get('open_price', 0),
                        api_data.get('high_price', 0),
                        api_data.get('low_price', 0),
                        api_data.get('close_price', 0),
                        api_data.get('volume', 0),
                        api_data.get('trading_value', 0)
                    )

                    cursor.execute(query, params)
                    conn.commit()
                    self.stats['inserted'] += 1

                elif task.action == 'UPDATE':
                    # 기존 데이터 수정
                    query = f"""
                    UPDATE {table_name} 
                    SET open_price = %s, high_price = %s, low_price = %s, 
                        close_price = %s, volume = %s, trading_value = %s,
                        updated_at = NOW()
                    WHERE date = %s
                    """

                    params = (
                        api_data.get('open_price', 0),
                        api_data.get('high_price', 0),
                        api_data.get('low_price', 0),
                        api_data.get('close_price', 0),
                        api_data.get('volume', 0),
                        api_data.get('trading_value', 0),
                        api_data['date']
                    )

                    cursor.execute(query, params)
                    conn.commit()
                    self.stats['updated'] += 1

            return True

        except Exception as e:
            logger.error(f"[{task.stock_code}] 데이터 저장 실패: {e}")
            return False

    def _ensure_table_exists(self, stock_code: str):
        """종목별 테이블 존재 확인 및 생성 (표준화된 버전)"""
        try:
            table_name = StockCodeManager.get_table_name(stock_code)

            with self.db_manager.get_daily_prices_connection() as conn:
                cursor = conn.cursor()

                create_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    date VARCHAR(8) UNIQUE NOT NULL,
                    open_price INT DEFAULT 0,
                    high_price INT DEFAULT 0,
                    low_price INT DEFAULT 0,
                    close_price INT DEFAULT 0,
                    volume BIGINT DEFAULT 0,
                    trading_value BIGINT DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_date (date)
                ) ENGINE=InnoDB COMMENT='종목 {stock_code} 일봉 데이터'
                """

                cursor.execute(create_query)
                conn.commit()

        except Exception as e:
            logger.error(f"[{stock_code}] 테이블 생성 실패: {e}")
            raise

    def manual_data_edit(self, stock_code: str) -> bool:
        """수동 데이터 편집 모드 (개선된 버전)"""
        try:
            # 종목 코드 정규화
            normalized_code = StockCodeManager.normalize_to_db_format(stock_code)

            self.console.print(Panel.fit(
                f"🛠️ 수동 데이터 편집 모드\n종목: [{normalized_code}] - 날짜: {self.target_date}",
                style="bold yellow"
            ))

            # 기존 데이터 확인
            existing_data = self._check_existing_data(normalized_code, self.target_date)

            if existing_data:
                self.console.print("📊 기존 데이터:")
                data_table = Table()
                data_table.add_column("항목", style="cyan")
                data_table.add_column("현재 값", style="green")

                data_table.add_row("시가", f"{existing_data.get('open_price', 0):,}원")
                data_table.add_row("고가", f"{existing_data.get('high_price', 0):,}원")
                data_table.add_row("저가", f"{existing_data.get('low_price', 0):,}원")
                data_table.add_row("종가", f"{existing_data.get('close_price', 0):,}원")
                data_table.add_row("거래량", f"{existing_data.get('volume', 0):,}주")
                data_table.add_row("거래대금", f"{existing_data.get('trading_value', 0):,}백만원")

                self.console.print(data_table)
            else:
                self.console.print("⚠️ 기존 데이터가 없습니다. 새로 입력하세요.")

            # 사용자 입력 받기
            self.console.print("\n✏️ 새로운 값을 입력하세요 (엔터키: 기존값 유지)")

            def get_price_input(field_name: str, current_value: int = 0) -> int:
                while True:
                    user_input = Prompt.ask(f"{field_name} ({current_value:,}원)", default=str(current_value))
                    try:
                        value = int(user_input.replace(',', ''))
                        if value < 0:
                            self.console.print("❌ 음수는 입력할 수 없습니다.")
                            continue
                        return value
                    except ValueError:
                        self.console.print("❌ 올바른 숫자를 입력하세요.")

            def get_volume_input(field_name: str, current_value: int = 0) -> int:
                while True:
                    user_input = Prompt.ask(f"{field_name} ({current_value:,}주)", default=str(current_value))
                    try:
                        value = int(user_input.replace(',', ''))
                        if value < 0:
                            self.console.print("❌ 음수는 입력할 수 없습니다.")
                            continue
                        return value
                    except ValueError:
                        self.console.print("❌ 올바른 숫자를 입력하세요.")

            # 입력 받기
            new_data = {
                'date': self.target_date,
                'open_price': get_price_input("시가", existing_data.get('open_price', 0) if existing_data else 0),
                'high_price': get_price_input("고가", existing_data.get('high_price', 0) if existing_data else 0),
                'low_price': get_price_input("저가", existing_data.get('low_price', 0) if existing_data else 0),
                'close_price': get_price_input("종가", existing_data.get('close_price', 0) if existing_data else 0),
                'volume': get_volume_input("거래량", existing_data.get('volume', 0) if existing_data else 0),
                'trading_value': get_volume_input("거래대금(백만원)",
                                                  existing_data.get('trading_value', 0) if existing_data else 0)
            }

            # 입력 확인
            self.console.print("\n📋 입력된 데이터:")
            confirm_table = Table()
            confirm_table.add_column("항목", style="cyan")
            confirm_table.add_column("새 값", style="green")

            confirm_table.add_row("시가", f"{new_data['open_price']:,}원")
            confirm_table.add_row("고가", f"{new_data['high_price']:,}원")
            confirm_table.add_row("저가", f"{new_data['low_price']:,}원")
            confirm_table.add_row("종가", f"{new_data['close_price']:,}원")
            confirm_table.add_row("거래량", f"{new_data['volume']:,}주")
            confirm_table.add_row("거래대금", f"{new_data['trading_value']:,}백만원")

            self.console.print(confirm_table)

            if not Confirm.ask("\n💾 이 데이터로 저장하시겠습니까?"):
                self.console.print("❌ 수동 편집이 취소되었습니다.")
                return False

            # 데이터 저장
            task = UpdateTask(
                stock_code=normalized_code,
                stock_name="수동편집",
                target_date=self.target_date,
                action='UPDATE' if existing_data else 'INSERT',
                existing_data=existing_data
            )

            success = self._save_or_update_data(task, new_data)

            if success:
                self.console.print("✅ 데이터가 성공적으로 저장되었습니다!")
                return True
            else:
                self.console.print("❌ 데이터 저장에 실패했습니다.")
                return False

        except Exception as e:
            logger.error(f"수동 편집 중 오류: {e}")
            self.console.print(f"❌ 오류 발생: {e}")
            return False

    def run_date_specific_update(self, stock_codes: List[str] = None,
                                 manual_edit: bool = False, confirm: bool = True) -> bool:
        """날짜 지정 업데이트 실행 (완전 개선)"""
        self.stats['start_time'] = datetime.now()

        # 수동 편집 모드
        if manual_edit:
            if not stock_codes or len(stock_codes) != 1:
                self.console.print("❌ 수동 편집 모드는 정확히 하나의 종목 코드가 필요합니다.")
                self.console.print(
                    "예: python scripts/date_specific_updater.py --codes 005930 --date 2025-07-01 --manual-edit")
                return False

            if not self.initialize_services():
                return False

            return self.manual_data_edit(stock_codes[0])

        # 일반 업데이트 모드
        today = datetime.now().strftime('%Y%m%d')
        date_display = f"{self.target_date}"
        if self.target_date == today:
            date_display += " (오늘)"

        self.console.print(Panel.fit(
            f"📅 날짜 지정 데이터 업데이트\n대상 날짜: {date_display}\n대상 시장: 코스피 + 코스닥 (숫자 6자리)\nAPI 요청: XXXXXX_AL 형식 (통합 데이터)",
            style="bold blue"
        ))

        try:
            # 1. 서비스 초기화
            if not self.initialize_services():
                return False

            # 2. 업데이트 작업 분석
            tasks = self.analyze_update_tasks(stock_codes)
            if not tasks:
                self.console.print("❌ 업데이트 대상이 없습니다!")
                return False

            self.stats['total_stocks'] = len(tasks)

            # 3. 작업 요약 출력
            self._display_task_summary(tasks)

            # 4. 사용자 확인 (옵션)
            if confirm:
                if not Confirm.ask(f"\n📅 {date_display} 데이터를 업데이트하시겠습니까?"):
                    self.console.print("❌ 업데이트가 취소되었습니다.")
                    return False

            # 5. 개별 업데이트 실행
            with Progress() as progress:
                task_id = progress.add_task("📊 업데이트 진행", total=len(tasks))

                for i, task in enumerate(tasks):
                    progress.update(
                        task_id,
                        completed=i,
                        description=f"📊 [{task.stock_code}] {task.stock_name} {task.action}..."
                    )

                    # 개별 업데이트 실행
                    success = self.update_single_stock(task)

                    if not success:
                        self.stats['failed'] += 1

                    # API 제한 준수 (3.6초 대기)
                    if i < len(tasks) - 1:  # 마지막이 아니면
                        time.sleep(3.6)

                progress.update(task_id, completed=len(tasks))

            # 6. 최종 결과 출력
            self._display_final_results()
            return True

        except Exception as e:
            logger.error(f"날짜 지정 업데이트 실행 실패: {e}")
            self.console.print(f"❌ 실행 실패: {e}")
            return False

        finally:
            self.stats['end_time'] = datetime.now()
            if self.session:
                self.session.disconnect()

    def _display_task_summary(self, tasks: List[UpdateTask]):
        """작업 요약 정보 출력 (개선된 버전)"""
        # 작업별 통계
        insert_count = sum(1 for t in tasks if t.action == 'INSERT')
        update_count = sum(1 for t in tasks if t.action == 'UPDATE')

        # 시장별 통계
        kospi_count = sum(1 for t in tasks if t.market == 'KOSPI')
        kosdaq_count = sum(1 for t in tasks if t.market == 'KOSDAQ')

        market_text = f"{self.target_date} (코스피 {kospi_count}개 + 코스닥 {kosdaq_count}개)"

        table = Table(title=f"📋 업데이트 작업 요약 - {market_text}")
        table.add_column("작업 유형", style="cyan")
        table.add_column("종목 수", style="green")
        table.add_column("설명", style="dim")

        table.add_row("🆕 INSERT", f"{insert_count}개", "신규 데이터 추가")
        table.add_row("🔄 UPDATE", f"{update_count}개", "기존 데이터 수정")
        table.add_row("📊 총계", f"{len(tasks)}개", "전체 업데이트 대상 (DB: 숫자6자리, API: _AL형식)")

        self.console.print(table)

        # 상위 종목 미리보기
        if len(tasks) > 0:
            preview_table = Table(title="🔍 상위 종목 미리보기")
            preview_table.add_column("종목코드", style="cyan")
            preview_table.add_column("종목명", width=20)
            preview_table.add_column("작업", style="yellow")
            preview_table.add_column("기존 데이터", style="dim")
            preview_table.add_column("마지막 수집", style="dim")

            for task in tasks[:10]:  # 상위 10개만
                existing = "있음" if task.existing_data else "없음"
                last_date = task.last_date if task.last_date else "없음"
                preview_table.add_row(
                    task.stock_code,
                    task.stock_name,
                    task.action,
                    existing,
                    last_date
                )

            if len(tasks) > 10:
                preview_table.add_row("...", f"외 {len(tasks) - 10}개", "...", "...", "...")

            self.console.print(preview_table)

    def _display_final_results(self):
        """최종 결과 출력 (개선된 버전)"""
        self.stats['end_time'] = datetime.now()
        duration = self.stats['end_time'] - self.stats['start_time']

        result_table = Table(title=f"🎉 날짜 지정 업데이트 완료 - {self.target_date}")
        result_table.add_column("항목", style="cyan")
        result_table.add_column("결과", style="green")

        result_table.add_row("📊 대상 종목", f"{self.stats['total_stocks']}개")
        result_table.add_row("🆕 신규 추가", f"{self.stats['inserted']}개")
        result_table.add_row("🔄 기존 수정", f"{self.stats['updated']}개")
        result_table.add_row("❌ 실패", f"{self.stats['failed']}개")
        result_table.add_row("⏱️ 소요 시간", str(duration).split('.')[0])

        if self.stats['total_stocks'] > 0:
            result_table.add_row("🚀 처리 속도", f"{self.stats['total_stocks'] / duration.total_seconds() * 60:.1f}개/분")

        self.console.print(result_table)

        # 성공률 계산
        success_count = self.stats['inserted'] + self.stats['updated']
        if self.stats['total_stocks'] > 0:
            success_rate = (success_count / self.stats['total_stocks']) * 100
            if success_rate >= 95:
                status = "🎉 우수"
                style = "bold green"
            elif success_rate >= 80:
                status = "✅ 양호"
                style = "green"
            else:
                status = "⚠️ 점검필요"
                style = "yellow"

            self.console.print(f"\n{status} 성공률: {success_rate:.1f}%", style=style)


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description="날짜 지정 데이터 업데이트 시스템 (완전 개선)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  # 오늘 날짜 전체 종목 업데이트
  python scripts/date_specific_updater.py

  # 특정 날짜 전체 종목 업데이트
  python scripts/date_specific_updater.py --date 2025-07-10

  # 특정 종목만 업데이트 (자동으로 일반 형식으로 정규화)
  python scripts/date_specific_updater.py --codes 005930 000660  # 삼성전자, SK하이닉스
  python scripts/date_specific_updater.py --codes 005930_AL      # _AL 입력해도 자동 변환

  # 수동 편집 모드 (종목코드 + 날짜 필요)
  python scripts/date_specific_updater.py --codes 005930 --date 2025-07-01 --manual-edit

  # 확인 없이 자동 실행 (스케줄러용)
  python scripts/date_specific_updater.py --no-confirm
        """
    )

    parser.add_argument("--date", help="대상 날짜 (YYYYMMDD, YYYY-MM-DD 등)")
    parser.add_argument("--codes", nargs="+", help="특정 종목 코드들 (공백으로 구분, 일반 형식 사용)")
    parser.add_argument("--manual-edit", action="store_true", help="수동 데이터 편집 모드 (종목코드 + 날짜 필요)")
    parser.add_argument("--no-confirm", action="store_true", help="확인 없이 자동 실행")

    args = parser.parse_args()

    # 실행 정보 출력
    print("=" * 60)
    print("📅 날짜 지정 데이터 업데이트 시스템 (완전 개선)")
    print("=" * 60)

    target_date = args.date if args.date else datetime.now().strftime('%Y%m%d')
    stock_codes = args.codes
    manual_edit = args.manual_edit
    no_confirm = args.no_confirm

    if stock_codes:
        print(f"🎯 대상 종목: {', '.join(stock_codes)} (DB: 일반형식, API: _AL형식)")
    else:
        print("🎯 대상 종목: 전체 활성 종목 (코스피 + 코스닥, 숫자 6자리)")

    print(f"📅 대상 날짜: {target_date}")

    if manual_edit:
        print("🛠️ 모드: 수동 데이터 편집")
        if not stock_codes or len(stock_codes) != 1:
            print("❌ 수동 편집 모드는 정확히 하나의 종목 코드가 필요합니다.")
            return False
    else:
        print("📋 기능: 지정 날짜의 데이터를 추가/수정")
        print("🔄 로직: 기존 데이터 있으면 UPDATE, 없으면 INSERT")
        print("🎯 대상: 숫자 6자리 종목 (DB저장용)")
        print("📡 API: _AL 형식으로 요청 (통합 데이터)")
    print("=" * 60)

    try:
        # 업데이트 시스템 실행
        updater = DateSpecificUpdater(target_date)
        success = updater.run_date_specific_update(
            stock_codes=stock_codes,
            manual_edit=manual_edit,
            confirm=not no_confirm
        )

        if success:
            print("\n✅ 날짜 지정 업데이트 완료!")
            return True
        else:
            print("\n❌ 날짜 지정 업데이트 실패!")
            return False

    except Exception as e:
        print(f"\n❌ 실행 중 오류 발생: {e}")
        logger.error(f"메인 실행 오류: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)