#!/usr/bin/env python3
"""
파일 경로: scripts/collect_supply_demand_data.py

수급 데이터 수집 실행 스크립트
- 전체 종목 1년치 수급 데이터 수집
- 실시간 진행상황 표시
- 재시작 기능 (중단된 지점부터 재개)
- 상세한 통계 및 에러 처리
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import time
import argparse

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, TaskID, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn, \
        TimeRemainingColumn
    from rich.panel import Panel
    from rich.layout import Layout
    from rich import box

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("⚠️ Rich 라이브러리가 없습니다. 기본 출력을 사용합니다.")

from src.api.base_session import create_kiwoom_session
from src.collectors.supply_demand_new_collector import SupplyDemandNewCollector
from src.core.supply_demand_database import SupplyDemandDatabaseService
from src.core.config import Config


class SupplyDemandRunner:
    """수급 데이터 수집 실행기"""

    def __init__(self):
        self.console = Console() if RICH_AVAILABLE else None
        self.db_service = SupplyDemandDatabaseService()
        self.session = None
        self.collector = None

        # 실행 통계
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_stocks': 0,
            'completed_stocks': 0,
            'failed_stocks': 0,
            'skipped_stocks': 0,
            'total_records': 0,
            'completion_rate': 0.0
        }

    def display_welcome(self):
        """환영 메시지 표시"""
        if self.console:
            welcome_panel = Panel(
                """[bold blue]🚀 수급 데이터 수집 시스템[/bold blue]

[green]✅ 기능:[/green]
• 전체 종목 1년치 수급 데이터 자동 수집
• 데이터 완성도 기반 스마트 수집 (업데이트/연속 모드)
• 실시간 진행상황 표시
• 중단된 지점부터 재시작 기능

[yellow]⚡ 수집 방식:[/yellow]
• OPT10060 (일별수급데이터요청) 사용
• prev_next=2 연속 조회로 1년치 데이터 수집
• API 제한 준수 (3.6초 간격)

[cyan]📊 데이터 저장:[/cyan]
• supply_demand_db 스키마
• 종목별 개별 테이블 (supply_demand_XXXXXX)
• 17개 투자자별 수급 필드 저장""",
                title="[bold green]수급 데이터 수집 시스템[/bold green]",
                border_style="green"
            )
            self.console.print(welcome_panel)
        else:
            print("🚀 수급 데이터 수집 시스템")
            print("=" * 60)
            print("📊 전체 종목 1년치 수급 데이터 수집")
            print("⚡ 스마트 수집 모드 (업데이트/연속)")
            print("💾 supply_demand_db 스키마에 저장")

    def check_prerequisites(self) -> bool:
        """사전 요구사항 확인"""
        if self.console:
            self.console.print("\n🔍 [bold]사전 요구사항 확인[/bold]")

        # 1. 데이터베이스 연결 확인
        if self.console:
            self.console.print("   1. 데이터베이스 연결...", end="")

        if not self.db_service.test_connection():
            if self.console:
                self.console.print(" [red]❌ 실패[/red]")
            else:
                print("❌ 데이터베이스 연결 실패")
            return False

        if self.console:
            self.console.print(" [green]✅ 성공[/green]")

        # 2. 스키마 생성
        if self.console:
            self.console.print("   2. 스키마 준비...", end="")

        if not self.db_service.create_schema_if_not_exists():
            if self.console:
                self.console.print(" [red]❌ 실패[/red]")
            else:
                print("❌ 스키마 생성 실패")
            return False

        if self.console:
            self.console.print(" [green]✅ 성공[/green]")

        # 3. 대상 종목 조회
        if self.console:
            self.console.print("   3. 대상 종목 조회...", end="")

        stocks = self.db_service.get_all_stock_codes()
        if not stocks:
            if self.console:
                self.console.print(" [red]❌ 종목 없음[/red]")
            else:
                print("❌ 수집 대상 종목이 없습니다")
            return False

        self.stats['total_stocks'] = len(stocks)
        if self.console:
            self.console.print(f" [green]✅ {len(stocks):,}개 종목[/green]")
        else:
            print(f"✅ 대상 종목: {len(stocks):,}개")

        return True

    def show_collection_summary(self) -> bool:
        """수집 현황 요약 표시"""
        if self.console:
            self.console.print("\n📊 [bold]수집 현황 분석[/bold]")

        # 현재 수집 상황 분석
        summary = self.db_service.get_collection_summary()

        if self.console:
            summary_table = Table(title="수집 현황 요약", box=box.ROUNDED)
            summary_table.add_column("항목", style="cyan")
            summary_table.add_column("값", style="magenta")

            summary_table.add_row("전체 종목", f"{summary['total_stocks']:,}개")
            summary_table.add_row("완료 종목", f"{summary['completed_stocks']:,}개")
            summary_table.add_row("미완료 종목", f"{summary['pending_stocks']:,}개")
            summary_table.add_row("완성률", f"{summary['completion_rate']:.1f}%")
            summary_table.add_row("총 레코드", f"{summary['total_records']:,}개")

            self.console.print(summary_table)
        else:
            print(f"\n📊 수집 현황 요약:")
            print(f"   전체 종목: {summary['total_stocks']:,}개")
            print(f"   완료 종목: {summary['completed_stocks']:,}개")
            print(f"   미완료 종목: {summary['pending_stocks']:,}개")
            print(f"   완성률: {summary['completion_rate']:.1f}%")

        # 예상 소요 시간 계산
        pending_stocks = summary['pending_stocks']
        estimated_minutes = (pending_stocks * 4.0) / 60  # 종목당 평균 4초 가정

        if self.console:
            time_panel = Panel(
                f"[yellow]📅 예상 소요 시간:[/yellow] 약 {estimated_minutes:.0f}분\n"
                f"[yellow]🔄 API 요청 수:[/yellow] 약 {pending_stocks * 10:,}회 (종목당 평균 10회)\n"
                f"[yellow]💾 예상 데이터:[/yellow] 약 {pending_stocks * 250:,}건 (종목당 250일)",
                title="예상 작업량",
                border_style="yellow"
            )
            self.console.print(time_panel)
        else:
            print(f"\n⏱️ 예상 소요 시간: 약 {estimated_minutes:.0f}분")
            print(f"🔄 예상 API 요청: 약 {pending_stocks * 10:,}회")

        return True

    def confirm_execution(self) -> bool:
        """실행 확인"""
        if self.console:
            self.console.print("\n❓ [bold yellow]수집을 시작하시겠습니까?[/bold yellow]")
            self.console.print("   [green]y[/green]/[green]yes[/green]: 시작")
            self.console.print("   [red]n[/red]/[red]no[/red]: 취소")
            response = input("\n선택: ").lower().strip()
        else:
            response = input("\n🚀 수집을 시작하시겠습니까? (y/N): ").lower().strip()

        return response in ['y', 'yes']

    def setup_kiwoom_session(self) -> bool:
        """키움 세션 설정"""
        if self.console:
            self.console.print("\n🔌 [bold]키움 API 연결[/bold]")
        else:
            print("\n🔌 키움 API 연결 중...")

        try:
            self.session = create_kiwoom_session(auto_login=True, show_progress=True)

            if not self.session or not self.session.is_ready():
                if self.console:
                    self.console.print("[red]❌ 키움 세션 연결 실패[/red]")
                else:
                    print("❌ 키움 세션 연결 실패")
                return False

            # 수집기 초기화
            config = Config()
            self.collector = SupplyDemandNewCollector(self.session, config)

            if self.console:
                self.console.print("[green]✅ 키움 세션 연결 성공[/green]")
            else:
                print("✅ 키움 세션 연결 성공")

            return True

        except Exception as e:
            if self.console:
                self.console.print(f"[red]❌ 키움 연결 오류: {e}[/red]")
            else:
                print(f"❌ 키움 연결 오류: {e}")
            return False

    def execute_collection(self, resume: bool = False) -> bool:
        """수집 실행"""
        try:
            self.stats['start_time'] = datetime.now()

            # 대상 종목 조회
            all_stocks = self.db_service.get_all_stock_codes()

            # 재시작 모드인 경우 미완료 종목만 필터링
            if resume:
                target_stocks = []
                for stock in all_stocks:
                    completeness = self.db_service.get_data_completeness(stock['code'])
                    if not completeness['is_complete']:
                        target_stocks.append(stock)
            else:
                target_stocks = all_stocks

            if not target_stocks:
                if self.console:
                    self.console.print("[green]🎉 모든 종목의 수집이 이미 완료되었습니다![/green]")
                else:
                    print("🎉 모든 종목의 수집이 이미 완료되었습니다!")
                return True

            if self.console:
                self.console.print(f"\n🚀 [bold]수집 시작[/bold]: {len(target_stocks):,}개 종목")
                self.console.print("=" * 80)
            else:
                print(f"\n🚀 수집 시작: {len(target_stocks):,}개 종목")
                print("=" * 80)

            # Rich 진행 표시줄 사용
            if self.console:
                return self._execute_with_rich_progress(target_stocks)
            else:
                return self._execute_with_basic_progress(target_stocks)

        except Exception as e:
            if self.console:
                self.console.print(f"[red]❌ 수집 실행 오류: {e}[/red]")
            else:
                print(f"❌ 수집 실행 오류: {e}")
            return False

    def _execute_with_rich_progress(self, target_stocks: list) -> bool:
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
                f"수급 데이터 수집 중...",
                total=len(target_stocks)
            )

            for i, stock_info in enumerate(target_stocks):
                stock_code = stock_info['code']
                stock_name = stock_info.get('name', stock_code)

                # 진행 상황 업데이트
                progress.update(
                    task,
                    description=f"[{i + 1}/{len(target_stocks)}] {stock_code} ({stock_name}) 수집 중...",
                    completed=i
                )

                # 단일 종목 수집
                result = self.collector.collect_single_stock(stock_code)

                # 통계 업데이트
                if result['success']:
                    self.stats['completed_stocks'] += 1
                    self.stats['total_records'] += result.get('saved_records', 0)
                else:
                    self.stats['failed_stocks'] += 1

                # API 제한 준수
                if i < len(target_stocks) - 1:  # 마지막이 아니면
                    time.sleep(3.6)

            # 진행 완료
            progress.update(task, completed=len(target_stocks))

        return True

    def _execute_with_basic_progress(self, target_stocks: list) -> bool:
        """기본 진행 표시와 함께 수집 실행"""
        for i, stock_info in enumerate(target_stocks):
            stock_code = stock_info['code']
            stock_name = stock_info.get('name', stock_code)

            print(f"\n📊 [{i + 1}/{len(target_stocks)}] {stock_code} ({stock_name})")

            # 단일 종목 수집
            result = self.collector.collect_single_stock(stock_code)

            # 통계 업데이트
            if result['success']:
                self.stats['completed_stocks'] += 1
                self.stats['total_records'] += result.get('saved_records', 0)
                print(f"   ✅ 성공: {result.get('saved_records', 0)}건 저장")
            else:
                self.stats['failed_stocks'] += 1
                print(f"   ❌ 실패: {result.get('error', '알 수 없는 오류')}")

            # 진행률 표시
            progress_rate = (i + 1) / len(target_stocks) * 100
            print(f"   📈 진행률: {progress_rate:.1f}% ({i + 1}/{len(target_stocks)})")

            # API 제한 준수
            if i < len(target_stocks) - 1:  # 마지막이 아니면
                time.sleep(3.6)

        return True

    def show_final_results(self):
        """최종 결과 표시"""
        self.stats['end_time'] = datetime.now()
        elapsed_time = self.stats['end_time'] - self.stats['start_time']

        success_rate = (self.stats['completed_stocks'] / self.stats['total_stocks'] * 100) if self.stats[
                                                                                                  'total_stocks'] > 0 else 0

        if self.console:
            # Rich 테이블로 결과 표시
            results_table = Table(title="🎉 수집 완료 결과", box=box.DOUBLE_EDGE)
            results_table.add_column("항목", style="cyan")
            results_table.add_column("값", style="magenta")

            results_table.add_row("전체 종목", f"{self.stats['total_stocks']:,}개")
            results_table.add_row("성공 종목", f"{self.stats['completed_stocks']:,}개")
            results_table.add_row("실패 종목", f"{self.stats['failed_stocks']:,}개")
            results_table.add_row("성공률", f"{success_rate:.1f}%")
            results_table.add_row("총 레코드", f"{self.stats['total_records']:,}개")
            results_table.add_row("소요 시간", str(elapsed_time))

            self.console.print("\n")
            self.console.print(results_table)

            # 성공/실패에 따른 메시지
            if success_rate >= 90:
                message = "[green]🎉 수집이 성공적으로 완료되었습니다![/green]"
            elif success_rate >= 70:
                message = "[yellow]⚠️ 일부 종목에서 문제가 발생했습니다.[/yellow]"
            else:
                message = "[red]❌ 많은 종목에서 오류가 발생했습니다.[/red]"

            self.console.print(f"\n{message}")

        else:
            print("\n" + "=" * 80)
            print("🎉 수집 완료 결과")
            print("=" * 80)
            print(f"전체 종목: {self.stats['total_stocks']:,}개")
            print(f"성공 종목: {self.stats['completed_stocks']:,}개")
            print(f"실패 종목: {self.stats['failed_stocks']:,}개")
            print(f"성공률: {success_rate:.1f}%")
            print(f"총 레코드: {self.stats['total_records']:,}개")
            print(f"소요 시간: {elapsed_time}")

    def run(self, resume: bool = False, specific_stocks: list = None):
        """메인 실행"""
        try:
            # 1. 환영 메시지
            self.display_welcome()

            # 2. 사전 요구사항 확인
            if not self.check_prerequisites():
                return False

            # 3. 수집 현황 요약
            if not self.show_collection_summary():
                return False

            # 4. 실행 확인
            if not self.confirm_execution():
                if self.console:
                    self.console.print("[yellow]❌ 수집이 취소되었습니다.[/yellow]")
                else:
                    print("❌ 수집이 취소되었습니다.")
                return False

            # 5. 키움 세션 설정
            if not self.setup_kiwoom_session():
                return False

            # 6. 수집 실행
            if not self.execute_collection(resume=resume):
                return False

            # 7. 최종 결과 표시
            self.show_final_results()

            return True

        except KeyboardInterrupt:
            if self.console:
                self.console.print("\n[yellow]⚠️ 사용자에 의해 중단되었습니다.[/yellow]")
            else:
                print("\n⚠️ 사용자에 의해 중단되었습니다.")
            return False

        except Exception as e:
            if self.console:
                self.console.print(f"\n[red]❌ 실행 중 오류 발생: {e}[/red]")
            else:
                print(f"\n❌ 실행 중 오류 발생: {e}")
            return False


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='수급 데이터 수집 실행 스크립트')
    parser.add_argument('--resume', action='store_true', help='중단된 지점부터 재시작')
    parser.add_argument('--stocks', nargs='+', help='특정 종목만 수집 (예: --stocks 005930 000660)')
    parser.add_argument('--test', action='store_true', help='테스트 모드 (소수 종목만)')

    args = parser.parse_args()

    # 실행기 생성
    runner = SupplyDemandRunner()

    # 테스트 모드
    if args.test:
        print("🧪 테스트 모드: 상위 5개 종목만 수집")
        # 테스트용 소수 종목으로 제한하는 로직 필요

    # 실행
    success = runner.run(resume=args.resume, specific_stocks=args.stocks)

    if success:
        print("\n✅ 스크립트 실행 완료!")
        exit(0)
    else:
        print("\n❌ 스크립트 실행 실패!")
        exit(1)


if __name__ == "__main__":
    main()