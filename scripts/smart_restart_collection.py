#!/usr/bin/env python3
"""
스마트 수집 재시작 스크립트
기존 MySQL 데이터를 기반으로 collection_progress 테이블을 재구성하여
이미 완료된 종목은 건너뛰고 미완료 종목부터 시작
"""
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.database import get_database_service
from src.api.base_session import create_kiwoom_session
from src.market.code_collector import StockCodeCollector


def rebuild_progress_from_existing_data():
    """기존 MySQL 데이터를 기반으로 진행상황 재구성"""
    print("🔄 기존 데이터 기반 진행상황 재구성")
    print("=" * 50)

    try:
        db_service = get_database_service()

        # 1. 전체 종목코드 수집
        print("📊 전체 종목코드 수집 중...")
        session = create_kiwoom_session(auto_login=True, show_progress=True)
        if not session or not session.is_ready():
            print("❌ 키움 세션 준비 실패")
            return False

        connector = session.get_connector()
        code_collector = StockCodeCollector(connector)
        codes_result = code_collector.get_all_stock_codes()

        if codes_result.get('error'):
            print(f"❌ 종목코드 수집 실패: {codes_result['error']}")
            return False

        all_codes = codes_result['all']
        print(f"✅ 전체 종목코드: {len(all_codes):,}개")

        # 2. 기존 stocks 테이블에서 완료된 종목 확인
        print("🔍 기존 완료 종목 확인 중...")
        conn_main = db_service._get_connection('main')
        cursor_main = conn_main.cursor()

        cursor_main.execute("SELECT code FROM stocks")
        completed_stocks = [row[0] for row in cursor_main.fetchall()]
        print(f"✅ 기본정보 완료 종목: {len(completed_stocks):,}개")

        # 3. 일봉 데이터 완료된 종목 확인
        conn_daily = db_service._get_connection('daily')
        cursor_daily = conn_daily.cursor()

        cursor_daily.execute("SHOW TABLES LIKE 'daily_prices_%'")
        daily_tables = [row[0] for row in cursor_daily.fetchall()]
        daily_completed = [table.replace('daily_prices_', '') for table in daily_tables]
        print(f"✅ 일봉데이터 완료 종목: {len(daily_completed):,}개")

        # 4. 완전 완료된 종목 (기본정보 + 일봉 모두 있음)
        fully_completed = list(set(completed_stocks) & set(daily_completed))
        print(f"🎉 완전 완료된 종목: {len(fully_completed):,}개")

        # 5. collection_progress 테이블 재구성
        print("🗂️ collection_progress 테이블 재구성 중...")

        # 기존 진행상황 삭제
        cursor_main.execute("DELETE FROM collection_progress")

        # 새로운 진행상황 생성
        for stock_code in all_codes:
            if stock_code in fully_completed:
                # 완료로 표시
                cursor_main.execute("""
                    INSERT INTO collection_progress 
                    (stock_code, stock_name, status, attempt_count)
                    VALUES (%s, '', 'completed', 1)
                """, (stock_code,))
            else:
                # 대기로 표시
                cursor_main.execute("""
                    INSERT INTO collection_progress 
                    (stock_code, stock_name, status, attempt_count)
                    VALUES (%s, '', 'pending', 0)
                """, (stock_code,))

        conn_main.commit()
        cursor_main.close()
        cursor_daily.close()
        conn_main.close()
        conn_daily.close()

        # 6. 결과 확인
        summary = db_service.get_collection_status_summary()
        print(f"\n📊 재구성 완료:")
        print(f"   총 종목: {summary.get('total_stocks', 0):,}개")
        print(f"   완료: {summary.get('completed', 0):,}개")
        print(f"   대기: {len(all_codes) - len(fully_completed):,}개")
        print(f"   성공률: {summary.get('success_rate', 0):.1f}%")

        # 7. 다음 수집할 종목 확인
        pending_stocks = db_service.get_pending_stocks()
        if len(pending_stocks) > 0:
            print(f"\n🔄 다음 수집 대상 (처음 10개):")
            for i, stock_code in enumerate(pending_stocks[:10]):
                print(f"   {i + 1:2d}. {stock_code}")

        return True

    except Exception as e:
        print(f"❌ 재구성 실패: {e}")
        return False


def main():
    print("🚀 스마트 수집 재시작 도구")
    print("=" * 50)
    print("이 스크립트는 다음 작업을 수행합니다:")
    print("1. 기존 MySQL stocks 테이블에서 완료된 종목 확인")
    print("2. 기존 daily_prices_* 테이블에서 일봉 완료 종목 확인")
    print("3. collection_progress 테이블을 재구성")
    print("4. 이미 완료된 종목은 건너뛰고 미완료 종목부터 수집 재개")

    response = input("\n계속 진행하시겠습니까? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("❌ 작업이 취소되었습니다.")
        return

    success = rebuild_progress_from_existing_data()

    if success:
        print("\n🎉 진행상황 재구성 완료!")
        print("💡 이제 collect_all_stocks.py를 실행하면 미완료 종목부터 시작됩니다.")
    else:
        print("\n❌ 재구성 실패")


if __name__ == "__main__":
    main()