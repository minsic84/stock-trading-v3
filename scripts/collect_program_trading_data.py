#!/usr/bin/env python3
"""
파일 경로: scripts/collect_program_trading_data.py

프로그램매매 데이터 수집 실행 스크립트
- supply_demand_data.py 구조 참고
- 시가총액 2000억 이상 종목 대상
- Rich 진행 표시줄 지원
"""
import sys
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config
from src.api.base_session import create_kiwoom_session
from src.collectors.program_trading_collector import ProgramTradingCollector
from src.core.program_trading_database import get_program_trading_database_service
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, SpinnerColumn, TextColumn
from rich.table import Table
from rich.prompt import Confirm

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/program_trading_collection.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
console = Console()


class ProgramTradingCollectionRunner:
    """프로그램매매 수집 실행기"""

    def __init__(self, use_rich: bool = True):
        self.config = Config()
        self.db_service = get_program_trading_database_service()
        self.session = None
        self.collector = None
        self.console = console if use_rich else None

        # 수집 통계
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_stocks': 0,
            'completed_stocks': 0,
            'failed_stocks': 0,
            'skipped_stocks': 0,
            'new_data_stocks': 0,
            'total_records': 0
        }

    def display_welcome(self):
        """환영 메시지 출력"""
        welcome_text = """
🚀 프로그램매매 데이터 수집 시스템

📊 수집 데이터:
   • OPT90013: 프로그램매매 추이 데이터
   • 매도/매수/순매수 금액 및 수량
   • 증감 정보 포함

🎯 대상 종목: stock_codes 테이블의 모든 활성 종목
📅 수집 기간: 최근 1년
💾 저장 위치: program_trading_db.program_trading_XXXXXX
"""

        if self.console:
            self.console.print(Panel(welcome_text, title="프로그램매매 수집 시스템", border_style="blue"))
        else:
            print(welcome_text)

    def check_target_stocks(self) -> int:
        """대상 종목 수 확인"""
        try:
            # 시가총액 2000억 이상 종목 조회
            target_stocks = self.get_target_stocks()

            if self.console:
                table = Table(title="📊 수집 대상 종목 현황")
                table.add_column("구분", style="cyan")
                table.add_column("종목 수", style="magenta")

                kospi_count = sum(1 for stock in target_stocks if stock.get('market') == 'KOSPI')
                kosdaq_count = sum(1 for stock in target_stocks if stock.get('market') == 'KOSDAQ')

                table.add_row("전체", f"{len(target_stocks):,}개")
                table.add_row("코스피", f"{kospi_count:,}개")
                table.add_row("코스닥", f"{kosdaq_count:,}개")

                self.console.print(table)
            else:
                print(f"📊 수집 대상 종목: {len(target_stocks):,}개")

            return len(target_stocks)

        except Exception as e:
            error_msg = f"❌ 대상 종목 확인 실패: {e}"
            if self.console:
                self.console.print(error_msg, style="red")
            else:
                print(error_msg)
            return 0

    def get_target_stocks(self) -> List[Dict[str, Any]]:
        """stock_trading_db.stock_codes 테이블의 활성 종목 조회 (수급데이터 방식과 동일)"""
        try:
            # stock_codes 테이블에서 활성 종목 조회
            target_stocks = self.db_service.get_all_stock_codes()

            logger.info(f"활성 종목 조회 완료: {len(target_stocks)}개")
            return target_stocks

        except Exception as e:
            logger.error(f"활성 종목 조회 실패: {e}")
            return []

    def confirm_collection(self, target_count: int) -> bool:
        """수집 시작 확인"""
        if target_count == 0:
            error_msg = "❌ 수집 대상 종목이 없습니다!"
            if self.console:
                self.console.print(error_msg, style="red")
            else:
                print(error_msg)
            return False

        if self.console:
            return Confirm.ask(f"📊 {target_count:,}개 종목의 프로그램매매 데이터를 수집하시겠습니까?")
        else:
            response = input(f"📊 {target_count:,}개 종목의 프로그램매매 데이터를 수집하시겠습니까? (y/N): ")
            return response.lower() in ['y', 'yes']

    def initialize_services(self) -> bool:
        """서비스 초기화"""
        try:
            # 1. 데이터베이스 연결 테스트
            if not self.db_service.test_connection():
                error_msg = "❌ 프로그램매매 데이터베이스 연결 실패"
                if self.console:
                    self.console.print(error_msg, style="red")
                else:
                    print(error_msg)
                return False

            # 2. 스키마 생성
            if not self.db_service.create_schema_if_not_exists():
                error_msg = "❌ 프로그램매매 스키마 생성 실패"
                if self.console:
                    self.console.print(error_msg, style="red")
                else:
                    print(error_msg)
                return False

            # 3. 키움 세션 생성
            if self.console:
                self.console.print("🔌 키움 API 연결 중...", style="yellow")
            else:
                print("🔌 키움 API 연결 중...")

            self.session = create_kiwoom_session(auto_login=True, show_progress=True)
            if not self.session or not self.session.is_ready():
                error_msg = "❌ 키움 API 연결 실패"
                if self.console:
                    self.console.print(error_msg, style="red")
                else:
                    print(error_msg)
                return False

            # 4. 수집기 초기화
            self.collector = ProgramTradingCollector(self.session)

            success_msg = "✅ 모든 서비스 초기화 완료"
            if self.console:
                self.console.print(success_msg, style="green")
            else:
                print(success_msg)

            return True

        except Exception as e:
            error_msg = f"❌ 서비스 초기화 실패: {e}"
            if self.console:
                self.console.print(error_msg, style="red")
            else:
                print(error_msg)
            logger.error(f"서비스 초기화 실패: {e}")
            return False

    def execute_collection(self, target_stocks: List[Dict[str, Any]], force_full: bool = False) -> bool:
        """수집 실행"""
        try:
            self.stats['start_time'] = datetime.now()
            self.stats['total_stocks'] = len(target_stocks)

            # 이미 완료된 종목 필터링
            if not force_full:
                target_stocks = self._filter_incomplete_stocks(target_stocks)

                if not target_stocks:
                    success_msg = "🎉 모든 종목의 수집이 이미 완료되었습니다!"
                    if self.console:
                        self.console.print(success_msg, style="green")
                    else:
                        print(success_msg)
                    return True

            start_msg = f"🚀 수집 시작: {len(target_stocks):,}개 종목"
            if self.console:
                self.console.print(f"\n{start_msg}", style="bold")
                self.console.print("=" * 80)
            else:
                print(f"\n{start_msg}")
                print("=" * 80)

            # Rich 진행 표시줄 사용
            if self.console:
                return self._execute_with_rich_progress(target_stocks, force_full)
            else:
                return self._execute_with_basic_progress(target_stocks, force_full)

        except Exception as e:
            error_msg = f"❌ 수집 실행 오류: {e}"
            if self.console:
                self.console.print(error_msg, style="red")
            else:
                print(error_msg)
            return False

    def _filter_incomplete_stocks(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """완료되지 않은 종목만 필터링"""
        incomplete_stocks = []

        for stock in stocks:
            stock_code = stock['code']
            completeness = self.db_service.get_data_completeness_info(stock_code)

            if completeness['needs_update']:
                incomplete_stocks.append(stock)

        filtered_count = len(stocks) - len(incomplete_stocks)
        if filtered_count > 0:
            filter_msg = f"📊 이미 완료된 종목 {filtered_count}개 제외"
            if self.console:
                self.console.print(filter_msg, style="yellow")
            else:
                print(filter_msg)

        return incomplete_stocks

    def _execute_with_rich_progress(self, target_stocks: List[Dict[str, Any]], force_full: bool) -> bool:
        """Rich 진행 표시줄과 함께 수집 실행"""
        with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=self.console
        ) as progress:

            task = progress.add_task(
                f"프로그램매매 데이터 수집 중...",
                total=len(target_stocks)
            )

            for i, stock_info in enumerate(target_stocks):
                stock_code = stock_info['code']
                stock_name = stock_info.get('name', stock_code)

                # 진행 상황 업데이트
                progress.update(
                    task,
                    description=f"수집 중: {stock_name} ({stock_code})",
                    advance=0
                )

                # 수집 실행
                success, is_new = self.collector.collect_single_stock_program_trading(stock_code, force_full)

                # 통계 업데이트
                if success:
                    self.stats['completed_stocks'] += 1
                    if is_new:
                        self.stats['new_data_stocks'] += 1
                else:
                    self.stats['failed_stocks'] += 1

                # 진행바 업데이트
                progress.update(task, advance=1)

                # 중간 통계 표시 (100개마다)
                if (i + 1) % 100 == 0:
                    self._display_interim_stats(i + 1, len(target_stocks))

            # 최종 통계 표시
            self.stats['end_time'] = datetime.now()
            self._display_final_stats()

            return True

    def _execute_with_basic_progress(self, target_stocks: List[Dict[str, Any]], force_full: bool) -> bool:
        """기본 진행 표시와 함께 수집 실행"""
        for i, stock_info in enumerate(target_stocks):
            stock_code = stock_info['code']
            stock_name = stock_info.get('name', stock_code)

            print(f"\n[{i + 1}/{len(target_stocks)}] {stock_name} ({stock_code}) 수집 중...")

            # 수집 실행
            success, is_new = self.collector.collect_single_stock_program_trading(stock_code, force_full)

            # 통계 업데이트
            if success:
                self.stats['completed_stocks'] += 1
                if is_new:
                    self.stats['new_data_stocks'] += 1
                print(f"✅ {stock_code}: 수집 완료")
            else:
                self.stats['failed_stocks'] += 1
                print(f"❌ {stock_code}: 수집 실패")

            # 진행률 표시
            progress_pct = ((i + 1) / len(target_stocks)) * 100
            print(f"📊 진행률: {progress_pct:.1f}% ({i + 1}/{len(target_stocks)})")

            # 중간 통계 표시 (100개마다)
            if (i + 1) % 100 == 0:
                self._display_interim_stats(i + 1, len(target_stocks))

        # 최종 통계 표시
        self.stats['end_time'] = datetime.now()
        self._display_final_stats()

        return True

    def _display_interim_stats(self, current: int, total: int):
        """중간 통계 표시"""
        stats_text = f"""
📊 중간 통계 ({current}/{total})
   ✅ 완료: {self.stats['completed_stocks']}개
   ❌ 실패: {self.stats['failed_stocks']}개
   🆕 신규: {self.stats['new_data_stocks']}개
"""

        if self.console:
            self.console.print(Panel(stats_text, title="중간 통계", border_style="yellow"))
        else:
            print(stats_text)

    def _display_final_stats(self):
        """최종 통계 표시"""
        duration = self.stats['end_time'] - self.stats['start_time']

        stats_text = f"""
🎉 프로그램매매 수집 완료!

📊 수집 통계:
   📈 전체 종목: {self.stats['total_stocks']:,}개
   ✅ 완료: {self.stats['completed_stocks']:,}개
   ❌ 실패: {self.stats['failed_stocks']:,}개
   🆕 신규 데이터: {self.stats['new_data_stocks']:,}개

⏱️ 수집 시간: {duration}
📅 완료 시간: {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}
"""

        if self.console:
            self.console.print(Panel(stats_text, title="최종 결과", border_style="green"))
        else:
            print(stats_text)

    def run(self, force_full: bool = False) -> bool:
        """메인 실행 함수"""
        try:
            # 1. 환영 메시지
            self.display_welcome()

            # 2. 대상 종목 확인
            target_count = self.check_target_stocks()
            if target_count == 0:
                return False

            # 3. 수집 확인
            if not self.confirm_collection(target_count):
                if self.console:
                    self.console.print("❌ 수집이 취소되었습니다.", style="yellow")
                else:
                    print("❌ 수집이 취소되었습니다.")
                return False

            # 4. 서비스 초기화
            if not self.initialize_services():
                return False

            # 5. 대상 종목 조회
            target_stocks = self.get_target_stocks()
            if not target_stocks:
                error_msg = "❌ 대상 종목 조회 실패"
                if self.console:
                    self.console.print(error_msg, style="red")
                else:
                    print(error_msg)
                return False

            # 6. 수집 실행
            return self.execute_collection(target_stocks, force_full)

        except KeyboardInterrupt:
            interrupt_msg = "\n⚠️ 사용자에 의해 수집이 중단되었습니다."
            if self.console:
                self.console.print(interrupt_msg, style="yellow")
            else:
                print(interrupt_msg)
            return False
        except Exception as e:
            error_msg = f"❌ 수집 실행 중 오류: {e}"
            if self.console:
                self.console.print(error_msg, style="red")
            else:
                print(error_msg)
            logger.error(f"수집 실행 실패: {e}")
            return False


def main():
    """메인 실행 함수"""
    import argparse

    parser = argparse.ArgumentParser(description='프로그램매매 데이터 수집 스크립트')
    parser.add_argument('--force-full', action='store_true', help='강제 전체 수집')
    parser.add_argument('--no-rich', action='store_true', help='Rich UI 비활성화')

    args = parser.parse_args()

    # 로그 디렉토리 생성
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)

    # 수집기 실행
    runner = ProgramTradingCollectionRunner(use_rich=not args.no_rich)

    try:
        success = runner.run(force_full=args.force_full)

        if success:
            print("\n🎉 프로그램매매 데이터 수집 완료!")
            return 0
        else:
            print("\n❌ 프로그램매매 데이터 수집 실패!")
            return 1

    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        logger.error(f"예상치 못한 오류: {e}")
        return 1


if __name__ == "__main__":
    exit(main())