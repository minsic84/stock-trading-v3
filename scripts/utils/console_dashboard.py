#!/usr/bin/env python3
"""
콘솔 대시보드 모듈
전체 종목 수집 진행 상황을 실시간으로 표시
"""
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, TaskID
from rich.live import Live


class CollectionDashboard:
    """전체 종목 수집 대시보드"""

    def __init__(self, total_stocks: int):
        self.total_stocks = total_stocks
        self.completed_stocks = 0
        self.failed_stocks = 0
        self.current_stock = ""
        self.current_stock_name = ""

        # 통계
        self.start_time = datetime.now()
        self.last_update = datetime.now()

        # Rich 컴포넌트
        self.console = Console()
        self.live = None
        self.is_running = False

    def start(self):
        """대시보드 시작"""
        self.is_running = True
        self.start_time = datetime.now()

        # 별도 스레드에서 대시보드 실행
        dashboard_thread = threading.Thread(target=self._run_dashboard, daemon=True)
        dashboard_thread.start()

    def stop(self):
        """대시보드 중지"""
        self.is_running = False
        if self.live:
            self.live.stop()

    def update_completed(self, completed: int):
        """완료 수 업데이트"""
        self.completed_stocks = completed
        self.last_update = datetime.now()

    def increment_completed(self):
        """완료 수 증가"""
        self.completed_stocks += 1
        self.last_update = datetime.now()

    def increment_failed(self):
        """실패 수 증가"""
        self.failed_stocks += 1
        self.last_update = datetime.now()

    def update_current_stock(self, stock_code: str, stock_name: str = ""):
        """현재 처리 중인 종목 업데이트"""
        self.current_stock = stock_code
        self.current_stock_name = stock_name
        self.last_update = datetime.now()

    def _run_dashboard(self):
        """대시보드 실행 (별도 스레드)"""
        try:
            with Live(self._generate_layout(), refresh_per_second=1) as live:
                self.live = live
                while self.is_running:
                    live.update(self._generate_layout())
                    time.sleep(1)
        except Exception as e:
            print(f"대시보드 실행 오류: {e}")

    def _generate_layout(self):
        """대시보드 레이아웃 생성"""
        # 진행률 계산
        progress_percent = (self.completed_stocks / self.total_stocks * 100) if self.total_stocks > 0 else 0
        remaining = self.total_stocks - self.completed_stocks - self.failed_stocks

        # 시간 계산
        elapsed = datetime.now() - self.start_time
        elapsed_str = str(elapsed).split('.')[0]  # 초 단위 제거

        # 예상 완료 시간
        if self.completed_stocks > 0:
            avg_time_per_stock = elapsed.total_seconds() / self.completed_stocks
            remaining_time = timedelta(seconds=int(avg_time_per_stock * remaining))
            eta_str = str(remaining_time).split('.')[0]
        else:
            eta_str = "계산 중..."

        # 메인 테이블
        table = Table(title="📊 전체 종목 데이터 수집 현황")
        table.add_column("항목", style="cyan", no_wrap=True)
        table.add_column("값", style="magenta")
        table.add_column("비율", style="green")

        table.add_row("📈 전체 종목", f"{self.total_stocks:,}개", "100.0%")
        table.add_row("✅ 완료", f"{self.completed_stocks:,}개", f"{progress_percent:.1f}%")
        table.add_row("❌ 실패", f"{self.failed_stocks:,}개", f"{(self.failed_stocks / self.total_stocks * 100):.1f}%")
        table.add_row("⏳ 남은 종목", f"{remaining:,}개", f"{(remaining / self.total_stocks * 100):.1f}%")
        table.add_row("", "", "")
        table.add_row("⏱️ 경과 시간", elapsed_str, "")
        table.add_row("🎯 예상 완료", eta_str, "")
        table.add_row("📊 현재 종목", f"{self.current_stock}", "")
        table.add_row("📋 종목명", f"{self.current_stock_name}", "")

        # 진행률 바
        progress_bar = "█" * int(progress_percent / 2) + "░" * (50 - int(progress_percent / 2))
        progress_text = f"[{progress_bar}] {progress_percent:.1f}%"

        return Panel(
            f"{table}\n\n{progress_text}",
            title="🚀 주식 데이터 수집 시스템",
            border_style="blue"
        )

    def show_retry_info(self, failed_stocks: List[Dict], retry_round: int):
        """재시도 정보 표시"""
        retry_table = Table(title=f"🔄 {retry_round}차 재시도")
        retry_table.add_column("종목코드", style="yellow")
        retry_table.add_column("시도 횟수", style="red")
        retry_table.add_column("오류 메시지", style="white")

        for stock in failed_stocks[:10]:  # 최대 10개만 표시
            retry_table.add_row(
                stock.get('stock_code', ''),
                str(stock.get('attempt_count', 0)),
                stock.get('error_message', '')[:50] + "..." if len(stock.get('error_message', '')) > 50 else stock.get(
                    'error_message', '')
            )

        if len(failed_stocks) > 10:
            retry_table.add_row("...", f"외 {len(failed_stocks) - 10}개", "")

        self.console.print(retry_table)

    def show_final_report(self, summary: Dict[str, Any]):
        """최종 리포트 표시"""
        report_table = Table(title="🎉 수집 완료 리포트")
        report_table.add_column("항목", style="cyan", no_wrap=True)
        report_table.add_column("값", style="magenta")
        report_table.add_column("비율", style="green")

        total = summary.get('total_stocks', 0)
        completed = summary.get('completed', 0)
        failed = summary.get('status_breakdown', {}).get('failed', 0)
        success_rate = summary.get('success_rate', 0)

        report_table.add_row("📊 총 종목", f"{total:,}개", "100.0%")
        report_table.add_row("✅ 성공", f"{completed:,}개", f"{success_rate:.1f}%")
        report_table.add_row("❌ 실패", f"{failed:,}개", f"{(100 - success_rate):.1f}%")

        # 경과시간 계산
        elapsed = datetime.now() - self.start_time
        elapsed_str = str(elapsed).split('.')[0]
        report_table.add_row("⏱️ 총 소요시간", elapsed_str, "")

        # 시간당 처리량
        if elapsed.total_seconds() > 0:
            stocks_per_hour = completed / (elapsed.total_seconds() / 3600)
            report_table.add_row("⚡ 시간당 처리", f"{stocks_per_hour:.1f}개/시간", "")

        self.console.print(Panel(
            report_table,
            title="🎉 수집 완료!",
            border_style="green"
        ))