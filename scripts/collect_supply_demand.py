#!/usr/bin/env python3
"""
수급 데이터 수집 실행 스크립트
시가총액 3000억 이상 종목의 OPT10060(상세수급) + OPT10014(프로그램매매) 데이터 수집
"""
import sys
import os
import logging
from datetime import datetime
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config
from src.api.base_session import create_kiwoom_session
from src.collectors.supply_demand import SupplyDemandCollector
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('supply_demand_collection.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
console = Console()


def display_welcome():
    """환영 메시지 출력"""
    welcome_text = """
🚀 수급 데이터 수집 시스템

📊 수집 데이터:
   • OPT10060: 상세 투자자별 매매동향 (7개 주체)
   • OPT10014: 프로그램매매 동향

🎯 대상 종목: 시가총액 2000억원 이상  # ← 3000억 → 2000억 수정
📅 수집 기간: 최근 1년
💾 저장 위치: supply_demand_db.supply_demand_XXXXXX
"""

    console.print(Panel(welcome_text, title="수급 데이터 수집 시스템", border_style="blue"))


def check_target_stocks():
    """대상 종목 수 확인"""
    try:
        # 임시 수집기로 대상 종목 확인
        from src.core.database import get_database_service

        db_service = get_database_service()
        connection = db_service._get_connection('main')
        cursor = connection.cursor(dictionary=True)

        # 시가총액 2000억 이상 종목 조회
        query = """
        SELECT 
            COUNT(*) as total_count,
            SUM(CASE WHEN market = 'KOSPI' THEN 1 ELSE 0 END) as kospi_count,
            SUM(CASE WHEN market = 'KOSDAQ' THEN 1 ELSE 0 END) as kosdaq_count
        FROM stocks 
        WHERE market_cap >= 2000
          AND LENGTH(TRIM(code)) = 6
          AND code REGEXP '^[0-9]{6}$'
        """

        cursor.execute(query)
        result = cursor.fetchone()

        cursor.close()
        connection.close()

        # 결과 출력
        table = Table(title="📊 수집 대상 종목 현황")
        table.add_column("구분", style="cyan")
        table.add_column("종목 수", style="magenta")

        table.add_row("전체", f"{result['total_count']:,}개")
        table.add_row("코스피", f"{result['kospi_count']:,}개")
        table.add_row("코스닥", f"{result['kosdaq_count']:,}개")

        console.print(table)

        return result['total_count']

    except Exception as e:
        console.print(f"❌ 대상 종목 확인 실패: {e}", style="red")
        return 0


def confirm_collection(target_count: int) -> bool:
    """수집 시작 확인"""
    if target_count == 0:
        console.print("❌ 수집 대상 종목이 없습니다!", style="red")
        return False

    # 예상 소요 시간 계산 (종목당 약 7.2초 = 3.6초 * 2TR)
    estimated_minutes = (target_count * 7.2) / 60

    console.print(f"\n⏱️ 예상 소요 시간: 약 {estimated_minutes:.0f}분")
    console.print(f"🔄 API 요청 수: {target_count * 2:,}회 (OPT10060 + OPT10014)")
    console.print(f"💾 생성될 테이블: {target_count}개")

    response = console.input("\n🚀 수집을 시작하시겠습니까? (y/N): ")
    return response.lower() in ['y', 'yes']


def main():
    """메인 실행 함수"""
    try:
        display_welcome()

        # 1. 대상 종목 확인
        console.print("\n🔍 1단계: 대상 종목 확인 중...")
        target_count = check_target_stocks()

        if not confirm_collection(target_count):
            console.print("❌ 수집이 취소되었습니다.", style="yellow")
            return False

        # 2. 키움 API 연결
        console.print("\n🔌 2단계: 키움 API 연결 중...")
        session = create_kiwoom_session(auto_login=True)

        if not session:
            console.print("❌ 키움 API 연결 실패", style="red")
            return False

        console.print("✅ 키움 API 연결 성공", style="green")

        # 3. 수급 데이터 수집기 초기화
        console.print("\n📊 3단계: 수급 데이터 수집기 초기화 중...")
        config = Config()
        collector = SupplyDemandCollector(session, config)

        console.print("✅ 수집기 초기화 완료", style="green")

        # 4. 수집 실행
        console.print(f"\n🚀 4단계: 수급 데이터 수집 시작!")
        console.print("=" * 80)

        start_time = datetime.now()

        # 시가총액 2000억 이상 종목 수집  # ← 3000억 → 2000억 주석 수정
        result = collector.collect_multiple_stocks(min_market_cap=2000)  # ← 300000 → 2000 수정

        end_time = datetime.now()
        elapsed_time = end_time - start_time

        # 5. 결과 출력
        console.print("\n" + "=" * 80)
        console.print("🎉 수급 데이터 수집 완료!")

        if result.get('success'):
            # 성공 통계
            stats_table = Table(title="📈 수집 결과 통계")
            stats_table.add_column("항목", style="cyan")
            stats_table.add_column("값", style="magenta")

            stats_table.add_row("전체 종목", f"{result['total_stocks']:,}개")
            stats_table.add_row("성공 종목", f"{result['completed_stocks']:,}개")
            stats_table.add_row("실패 종목", f"{result['failed_stocks']:,}개")
            stats_table.add_row("수집 레코드", f"{result['total_records']:,}개")
            stats_table.add_row("성공률", f"{result['success_rate']:.1f}%")
            stats_table.add_row("소요 시간", f"{elapsed_time}")

            console.print(stats_table)

            console.print(f"\n✅ 수집 성공!", style="green")
            console.print(f"💾 데이터 저장 위치: supply_demand_db.supply_demand_XXXXXX")

        else:
            console.print(f"❌ 수집 실패: {result.get('message')}", style="red")
            return False

        # 6. 데이터 검증 (샘플)
        console.print(f"\n🔍 5단계: 데이터 검증 중...")
        verify_sample_data(collector)

        return True

    except KeyboardInterrupt:
        console.print("\n⚠️ 사용자에 의해 중단되었습니다", style="yellow")
        return False
    except Exception as e:
        console.print(f"\n❌ 실행 중 오류 발생: {e}", style="red")
        logger.error(f"메인 실행 오류: {e}")
        return False


def verify_sample_data(collector: SupplyDemandCollector):
    """샘플 데이터 검증"""
    try:
        # 3. 라인 192: 데이터 검증 함수 수정
        target_stocks = collector.get_target_stocks(2000)  # ← 300000 → 2000 수정

        if not target_stocks:
            console.print("❌ 검증할 데이터가 없습니다", style="red")
            return

        sample_stock = target_stocks[0]
        stock_code = sample_stock['code']
        stock_name = sample_stock['name']

        # 샘플 데이터 조회
        connection = collector.db_service.get_connection('supply_demand')
        cursor = connection.cursor(dictionary=True)

        table_name = f"supply_demand_{stock_code}"
        query = f"""
        SELECT 
            date,
            foreign_net,
            program_net,
            securities_net + bank_net + insurance_net + trust_net + etc_corp_net as institutional_net
        FROM {table_name}
        ORDER BY date DESC
        LIMIT 5
        """

        cursor.execute(query)
        sample_data = cursor.fetchall()

        cursor.close()
        connection.close()

        if sample_data:
            # 샘플 데이터 출력
            sample_table = Table(title=f"📊 샘플 데이터: {stock_code} {stock_name}")
            sample_table.add_column("날짜", style="cyan")
            sample_table.add_column("외국인순매수", style="magenta")
            sample_table.add_column("프로그램순매수", style="green")
            sample_table.add_column("기관순매수", style="yellow")

            for row in sample_data:
                sample_table.add_row(
                    row['date'],
                    f"{row['foreign_net']:,}",
                    f"{row['program_net']:,}",
                    f"{row['institutional_net']:,}"
                )

            console.print(sample_table)
            console.print("✅ 데이터 검증 완료", style="green")
        else:
            console.print("❌ 샘플 데이터가 없습니다", style="red")

    except Exception as e:
        console.print(f"❌ 데이터 검증 실패: {e}", style="red")


if __name__ == "__main__":
    main()