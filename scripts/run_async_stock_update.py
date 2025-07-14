#!/usr/bin/env python3
"""
파일 경로: scripts/run_async_stock_update.py

비동기 주식정보 업데이트 실행 스크립트
- stock_codes DB에서 활성 종목 조회
- 5개 동시 비동기 처리로 stocks DB 업데이트
- 실시간 진행상황 모니터링
- 상세한 성능 리포트 제공
"""

import sys
import asyncio
import argparse
from pathlib import Path
from datetime import datetime
import logging
from typing import Optional, List
import signal

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config
from src.core.database import get_database_service
from src.api.base_session import create_kiwoom_session
from src.collectors.stock_info import StockInfoCollector
from src.utils.async_helpers import AsyncTimer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Confirm, Prompt

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/async_stock_update.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Rich 콘솔
console = Console()


class AsyncStockUpdateRunner:
    """비동기 주식정보 업데이트 실행기"""

    def __init__(self):
        self.config = Config()
        self.session = None
        self.collector = None
        self.db_service = None
        self.interrupted = False

        # 기본 설정
        self.default_concurrency = 5
        self.default_batch_size = 10
        self.default_max_retries = 3

    def setup_signal_handlers(self):
        """신호 처리기 설정 (Ctrl+C 등)"""

        def signal_handler(signum, frame):
            console.print("\n⚠️ 중단 신호 감지! 안전하게 종료 중...", style="bold yellow")
            self.interrupted = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def initialize_services(self) -> bool:
        """서비스 초기화"""
        try:
            console.print("🔧 서비스 초기화 중...", style="bold blue")

            # 1. 데이터베이스 연결
            console.print("   📊 데이터베이스 연결 중...")
            self.db_service = get_database_service()

            if not self.db_service.test_connection():
                console.print("   ❌ 데이터베이스 연결 실패", style="bold red")
                return False

            console.print("   ✅ 데이터베이스 연결 성공", style="green")

            # 2. 키움 세션 준비
            console.print("   🔌 키움 API 세션 준비 중...")
            self.session = create_kiwoom_session(auto_login=True, show_progress=True)

            if not self.session or not self.session.is_ready():
                console.print("   ❌ 키움 세션 준비 실패", style="bold red")
                return False

            console.print("   ✅ 키움 세션 준비 완료", style="green")

            # 3. 수집기 초기화
            console.print("   🏗️ StockInfoCollector 초기화 중...")
            self.collector = StockInfoCollector(self.session, self.config)

            console.print("   ✅ StockInfoCollector 초기화 완료", style="green")

            console.print("🎉 모든 서비스 초기화 완료!", style="bold green")
            return True

        except Exception as e:
            console.print(f"❌ 서비스 초기화 실패: {e}", style="bold red")
            logger.error(f"서비스 초기화 실패: {e}")
            return False

    async def show_pre_execution_info(self, args) -> bool:
        """실행 전 정보 표시 및 확인"""
        try:
            # 활성 종목 수 조회
            if args.market:
                stock_codes_data = await self.db_service.get_active_stock_codes_by_market_async(args.market)
                market_info = f" ({args.market})"
            else:
                stock_codes_data = await self.db_service.get_active_stock_codes_async()
                market_info = " (전체)"

            if not stock_codes_data:
                console.print("❌ 활성 종목이 없습니다!", style="bold red")
                return False

            total_stocks = len(stock_codes_data)

            # 예상 시간 계산
            estimated_seconds = (total_stocks * 3.6) / args.concurrency
            estimated_minutes = estimated_seconds / 60

            # 시장별 통계
            market_stats = {}
            for item in stock_codes_data:
                market = item['market']
                if market not in market_stats:
                    market_stats[market] = 0
                market_stats[market] += 1

            # 정보 테이블 생성
            info_table = Table(title=f"📊 비동기 주식정보 업데이트 계획{market_info}")
            info_table.add_column("항목", style="cyan", no_wrap=True)
            info_table.add_column("값", style="white")

            info_table.add_row("🎯 대상 종목 수", f"{total_stocks:,}개")
            info_table.add_row("⚡ 동시 처리 수", f"{args.concurrency}개")
            info_table.add_row("📦 배치 크기", f"{args.batch_size}개")
            info_table.add_row("🔄 최대 재시도", f"{args.max_retries}회")
            info_table.add_row("⏱️ 예상 소요시간", f"{estimated_minutes:.1f}분")
            info_table.add_row("🔗 API 간격", "3.6초 (키움 제한 준수)")

            # 시장별 현황
            for market, count in market_stats.items():
                info_table.add_row(f"📈 {market}", f"{count:,}개")

            console.print(info_table)

            # 성능 비교 정보
            sync_time = total_stocks * 3.6 / 60  # 동기 처리 시간 (분)
            performance_gain = sync_time / estimated_minutes

            perf_panel = Panel(
                f"🚀 [bold green]성능 향상 예상[/bold green]\n"
                f"   동기 처리: {sync_time:.1f}분\n"
                f"   비동기 처리: {estimated_minutes:.1f}분\n"
                f"   [bold yellow]약 {performance_gain:.1f}배 빠름![/bold yellow]",
                title="⚡ 성능 비교"
            )
            console.print(perf_panel)

            # 실행 확인
            if not args.yes:
                if not Confirm.ask("계속 진행하시겠습니까?"):
                    console.print("❌ 사용자가 실행을 취소했습니다.", style="yellow")
                    return False

            return True

        except Exception as e:
            console.print(f"❌ 실행 전 정보 조회 실패: {e}", style="bold red")
            logger.error(f"실행 전 정보 조회 실패: {e}")
            return False

    async def run_async_update(self, args) -> bool:
        """비동기 업데이트 실행"""
        try:
            console.print("\n🚀 비동기 주식정보 업데이트 시작!", style="bold green")

            async with AsyncTimer("전체 비동기 업데이트"):
                # 특정 종목 리스트가 제공된 경우
                if args.codes:
                    console.print(f"🎯 지정된 {len(args.codes)}개 종목 수집")
                    result = await self.collector.collect_stocks_by_codes_async(
                        stock_codes=args.codes,
                        concurrency=args.concurrency,
                        max_retries=args.max_retries
                    )
                else:
                    # 전체 또는 시장별 수집
                    result = await self.collector.collect_and_update_stocks_async(
                        concurrency=args.concurrency,
                        batch_size=args.batch_size,
                        market_filter=args.market,
                        max_retries=args.max_retries
                    )

                # 중단 확인
                if self.interrupted:
                    console.print("⚠️ 사용자 요청으로 중단되었습니다.", style="yellow")
                    return False

                # 결과 처리
                if result.get('success', False):
                    await self._show_success_report(result)
                    return True
                else:
                    await self._show_error_report(result)
                    return False

        except Exception as e:
            console.print(f"❌ 비동기 업데이트 실행 실패: {e}", style="bold red")
            logger.error(f"비동기 업데이트 실행 실패: {e}")
            return False

    async def _show_success_report(self, result: dict):
        """성공 리포트 표시"""
        # 메인 결과 테이블
        result_table = Table(title="🎉 비동기 업데이트 완료 결과")
        result_table.add_column("메트릭", style="cyan")
        result_table.add_column("값", style="white")
        result_table.add_column("비고", style="dim")

        result_table.add_row(
            "📊 전체 종목",
            f"{result['total_stocks']:,}개",
            "stock_codes DB 기준"
        )
        result_table.add_row(
            "✅ 성공",
            f"{result['successful']:,}개",
            f"{result['success_rate']:.1f}% 성공률"
        )
        result_table.add_row(
            "❌ 실패",
            f"{result['failed']:,}개",
            "재시도 후에도 실패"
        )
        result_table.add_row(
            "⏱️ 총 시간",
            f"{result['elapsed_seconds']:.1f}초",
            f"{result['elapsed_seconds'] / 60:.1f}분"
        )
        result_table.add_row(
            "🚀 처리량",
            f"{result['items_per_second']:.1f} 종목/초",
            "평균 처리 속도"
        )

        console.print(result_table)

        # 성능 상세 정보
        if 'performance' in result:
            perf = result['performance']
            perf_table = Table(title="⚡ 성능 상세 분석")
            perf_table.add_column("지표", style="cyan")
            perf_table.add_column("시간", style="white")

            perf_table.add_row("평균 처리 시간", f"{perf['avg_time_per_stock']:.2f}초/종목")
            perf_table.add_row("최소 처리 시간", f"{perf['min_time']:.2f}초")
            perf_table.add_row("최대 처리 시간", f"{perf['max_time']:.2f}초")

            console.print(perf_table)

        # 시장별 결과
        if 'market_breakdown' in result and result['market_breakdown']:
            market_table = Table(title="📈 시장별 결과")
            market_table.add_column("시장", style="cyan")
            market_table.add_column("성공/전체", style="white")
            market_table.add_column("성공률", style="green")

            for market, stats in result['market_breakdown'].items():
                success_rate = (stats['successful'] / stats['total'] * 100) if stats['total'] > 0 else 0
                market_table.add_row(
                    market,
                    f"{stats['successful']}/{stats['total']}",
                    f"{success_rate:.1f}%"
                )

            console.print(market_table)

        # 성공 패널
        success_panel = Panel(
            f"[bold green]✅ 비동기 업데이트 성공![/bold green]\n"
            f"   📊 stocks 테이블이 최신 정보로 업데이트되었습니다.\n"
            f"   ⏱️ 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"   🎯 다음 실행 권장: 장 마감 후 (오후 4시경)",
            title="🎉 업데이트 완료"
        )
        console.print(success_panel)

    async def _show_error_report(self, result: dict):
        """에러 리포트 표시"""
        error_panel = Panel(
            f"[bold red]❌ 비동기 업데이트 실패![/bold red]\n"
            f"   오류: {result.get('error', '알 수 없는 오류')}\n"
            f"   로그를 확인하여 자세한 정보를 확인하세요.\n"
            f"   파일: logs/async_stock_update.log",
            title="💥 실행 실패"
        )
        console.print(error_panel)

        # 실패한 종목 정보 (있는 경우)
        if 'failed_stocks' in result and result['failed_stocks']:
            failed_table = Table(title="❌ 실패한 종목들 (상위 10개)")
            failed_table.add_column("종목코드", style="red")
            failed_table.add_column("오류 메시지", style="dim")

            for failed in result['failed_stocks'][:10]:
                failed_table.add_row(
                    failed.get('stock_code', 'unknown'),
                    failed.get('error', '알 수 없는 오류')
                )

            console.print(failed_table)

    def cleanup(self):
        """정리 작업"""
        try:
            if self.session:
                # 키움 세션 정리
                pass

            console.print("🧹 정리 작업 완료", style="dim")

        except Exception as e:
            logger.error(f"정리 작업 중 오류: {e}")

    async def run(self, args) -> bool:
        """메인 실행"""
        try:
            # 신호 처리기 설정
            self.setup_signal_handlers()

            # 서비스 초기화
            if not await self.initialize_services():
                return False

            # 실행 전 정보 표시
            if not await self.show_pre_execution_info(args):
                return False

            # 비동기 업데이트 실행
            success = await self.run_async_update(args)

            return success

        except Exception as e:
            console.print(f"❌ 실행 중 치명적 오류: {e}", style="bold red")
            logger.error(f"실행 중 치명적 오류: {e}")
            return False
        finally:
            self.cleanup()


def parse_arguments():
    """명령행 인수 파싱"""
    parser = argparse.ArgumentParser(
        description="비동기 주식정보 업데이트 스크립트",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예제:
  # 전체 활성 종목 수집 (기본)
  python scripts/run_async_stock_update.py

  # KOSPI만 수집
  python scripts/run_async_stock_update.py --market KOSPI

  # 동시 처리 수 조정
  python scripts/run_async_stock_update.py --concurrency 3

  # 특정 종목들만 수집
  python scripts/run_async_stock_update.py --codes 005930 000660 035420

  # 자동 실행 (확인 없이)
  python scripts/run_async_stock_update.py --yes
        """
    )

    # 기본 옵션들
    parser.add_argument(
        "--concurrency", "-c",
        type=int,
        default=5,
        help="동시 처리 수 (기본: 5개, 권장: 3-5개)"
    )

    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=10,
        help="배치 크기 (기본: 10개)"
    )

    parser.add_argument(
        "--max-retries", "-r",
        type=int,
        default=3,
        help="최대 재시도 횟수 (기본: 3회)"
    )

    parser.add_argument(
        "--market", "-m",
        choices=["KOSPI", "KOSDAQ"],
        help="시장 필터 (기본: 전체 시장)"
    )

    parser.add_argument(
        "--codes",
        nargs="+",
        help="특정 종목코드들 (예: 005930 000660)"
    )

    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="실행 확인 건너뛰기"
    )

    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="상세 로그 출력"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="실제 실행 없이 계획만 표시"
    )

    return parser.parse_args()


def validate_arguments(args):
    """인수 유효성 검증"""
    # 동시 처리 수 검증
    if args.concurrency < 1 or args.concurrency > 10:
        console.print("❌ 동시 처리 수는 1-10 사이여야 합니다.", style="bold red")
        return False

    # 배치 크기 검증
    if args.batch_size < 1 or args.batch_size > 50:
        console.print("❌ 배치 크기는 1-50 사이여야 합니다.", style="bold red")
        return False

    # 종목코드 형식 검증
    if args.codes:
        for code in args.codes:
            if not (len(code) == 6 and code.isdigit()):
                console.print(f"❌ 잘못된 종목코드 형식: {code} (6자리 숫자 필요)", style="bold red")
                return False

    return True


async def main():
    """메인 함수"""
    console.print("🚀 비동기 주식정보 업데이트 시스템", style="bold blue")
    console.print("=" * 60)

    try:
        # 인수 파싱
        args = parse_arguments()

        # 인수 유효성 검증
        if not validate_arguments(args):
            sys.exit(1)

        # 상세 로그 설정
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)

        # Dry run 모드
        if args.dry_run:
            console.print("🧪 Dry Run 모드: 실제 실행 없이 계획만 표시", style="yellow")
            # Dry run 로직 (실제 구현 시 추가)
            return

        # 실행기 생성 및 실행
        runner = AsyncStockUpdateRunner()
        success = await runner.run(args)

        if success:
            console.print("\n🎉 비동기 업데이트 성공적으로 완료!", style="bold green")
            sys.exit(0)
        else:
            console.print("\n❌ 비동기 업데이트 실패!", style="bold red")
            sys.exit(1)

    except KeyboardInterrupt:
        console.print("\n⚠️ 사용자가 중단했습니다.", style="yellow")
        sys.exit(130)
    except Exception as e:
        console.print(f"\n💥 치명적 오류: {e}", style="bold red")
        logger.exception("치명적 오류 발생")
        sys.exit(1)


if __name__ == "__main__":
    # 비동기 메인 실행
    asyncio.run(main())