#!/usr/bin/env python3
"""
수집 진행상황 확인 스크립트
collection_progress 테이블 상태 확인 및 MySQL 데이터와 동기화
"""
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.database import get_database_service


def check_progress_status():
    """진행상황 테이블 상태 확인"""
    print("🔍 collection_progress 테이블 상태 확인")
    print("=" * 50)

    try:
        db_service = get_database_service()

        # 1. 전체 통계
        summary = db_service.get_collection_status_summary()
        print("📊 전체 통계:")
        print(f"   총 종목: {summary.get('total_stocks', 0):,}개")
        print(f"   완료된 종목: {summary.get('completed', 0):,}개")
        print(f"   성공률: {summary.get('success_rate', 0):.1f}%")

        # 2. 상태별 분포
        status_breakdown = summary.get('status_breakdown', {})
        print(f"\n📋 상태별 분포:")
        for status, count in status_breakdown.items():
            print(f"   {status}: {count:,}개")

        # 3. 미완료 종목 수
        pending_stocks = db_service.get_pending_stocks()
        print(f"\n🔄 미완료 종목: {len(pending_stocks):,}개")

        if len(pending_stocks) > 0:
            print("처음 10개 미완료 종목:")
            for i, stock_code in enumerate(pending_stocks[:10]):
                print(f"   {i + 1:2d}. {stock_code}")

        # 4. 완료된 종목 중 일부 확인
        if summary.get('completed', 0) > 0:
            print(f"\n✅ 완료된 종목 (최근 10개):")
            # 완료된 종목 조회 로직 필요

    except Exception as e:
        print(f"❌ 진행상황 확인 실패: {e}")


def check_mysql_stocks_data():
    """MySQL stocks 테이블 확인"""
    print("\n🗄️ MySQL stocks 테이블 확인")
    print("=" * 30)

    try:
        db_service = get_database_service()
        conn = db_service._get_connection('main')
        cursor = conn.cursor()

        # stocks 테이블 총 개수
        cursor.execute("SELECT COUNT(*) FROM stocks")
        total_stocks = cursor.fetchone()[0]
        print(f"stocks 테이블 총 종목: {total_stocks:,}개")

        # 처음 10개 종목
        cursor.execute("SELECT code, name FROM stocks ORDER BY code LIMIT 10")
        stocks = cursor.fetchall()

        print("처음 10개 종목:")
        for code, name in stocks:
            print(f"   {code}: {name}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ MySQL 확인 실패: {e}")


def check_daily_tables():
    """MySQL 일봉 테이블들 확인"""
    print("\n📊 MySQL 일봉 테이블 확인")
    print("=" * 30)

    try:
        db_service = get_database_service()
        conn = db_service._get_connection('daily')
        cursor = conn.cursor()

        # daily_prices_* 테이블 개수
        cursor.execute("SHOW TABLES LIKE 'daily_prices_%'")
        tables = cursor.fetchall()

        print(f"일봉 테이블 개수: {len(tables):,}개")

        if len(tables) > 0:
            print("처음 10개 일봉 테이블:")
            for i, (table_name,) in enumerate(tables[:10]):
                stock_code = table_name.replace('daily_prices_', '')

                # 각 테이블의 레코드 수 확인
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"   {i + 1:2d}. {stock_code}: {count:,}개 레코드")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ 일봉 테이블 확인 실패: {e}")


if __name__ == "__main__":
    check_progress_status()
    check_mysql_stocks_data()
    check_daily_tables()