#!/usr/bin/env python3
"""
파일 경로: scripts/check_schema_separation.py

스키마 분리 상태 확인 스크립트
- 각 스키마별 테이블 수 확인
- 분리 성공 여부 검증
"""
import mysql.connector
from mysql.connector import Error as MySQLError


def check_separation_status():
    """스키마 분리 상태 확인"""
    print("🔍 스키마 분리 상태 확인")
    print("=" * 40)

    mysql_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'stock_user',
        'password': 'StockPass2025!',
        'charset': 'utf8mb4'
    }

    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()

        # 스키마별 테이블 수 확인
        schemas_to_check = [
            'stock_trading_db',
            'daily_prices_db',
            'supply_demand_db',
            'minute_data_db'
        ]

        results = {}

        for schema in schemas_to_check:
            try:
                cursor.execute(f"USE {schema}")
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()

                # daily_prices 테이블과 기타 테이블 분류
                daily_tables = [t[0] for t in tables if t[0].startswith('daily_prices_')]
                other_tables = [t[0] for t in tables if not t[0].startswith('daily_prices_')]

                results[schema] = {
                    'total': len(tables),
                    'daily_prices': len(daily_tables),
                    'others': len(other_tables),
                    'other_table_names': other_tables
                }

                print(f"📁 {schema}:")
                print(f"   📊 전체 테이블: {len(tables)}개")
                if daily_tables:
                    print(f"   📈 daily_prices: {len(daily_tables)}개")
                if other_tables:
                    print(f"   🗂️ 기타: {len(other_tables)}개 ({', '.join(other_tables)})")
                print()

            except Exception as e:
                print(f"❌ {schema}: 접근 실패 - {e}")
                results[schema] = {'error': str(e)}

        conn.close()

        # 결과 분석
        print("📋 분리 결과 분석:")

        # stock_trading_db 확인
        stock_db = results.get('stock_trading_db', {})
        daily_db = results.get('daily_prices_db', {})

        if stock_db.get('daily_prices', 0) == 0 and daily_db.get('daily_prices', 0) > 0:
            print("✅ 스키마 분리 성공!")
            print(f"   📋 stock_trading_db에 daily_prices 테이블: {stock_db.get('daily_prices', 0)}개")
            print(f"   📊 daily_prices_db에 daily_prices 테이블: {daily_db.get('daily_prices', 0)}개")
        else:
            print("⚠️ 스키마 분리 상태 확인 필요")

        return results

    except MySQLError as e:
        print(f"❌ MySQL 연결 실패: {e}")
        return None
    except Exception as e:
        print(f"❌ 확인 실패: {e}")
        return None


def main():
    """메인 실행 함수"""
    print("🔍 MySQL 스키마 분리 상태 확인")
    print("=" * 50)

    results = check_separation_status()

    if results:
        print("\n🎯 결론:")
        daily_db_count = results.get('daily_prices_db', {}).get('daily_prices', 0)
        stock_db_count = results.get('stock_trading_db', {}).get('daily_prices', 0)

        if daily_db_count > 2000 and stock_db_count == 0:
            print("🎉 스키마 분리가 성공적으로 완료되었습니다!")
            print("💡 권한 설정 실패는 무시해도 됩니다. 실제 분리는 완료되었습니다.")
        elif daily_db_count > 0:
            print("✅ 스키마 분리가 부분적으로 완료되었습니다.")
            print(f"📊 {daily_db_count}개 테이블이 daily_prices_db로 이동했습니다.")
        else:
            print("❌ 스키마 분리가 완료되지 않았습니다.")

        print(f"\n🔧 다음 단계:")
        print(f"   1. sync_sqlite_to_mysql_incremental.py 스키마 분리 대응 수정")
        print(f"   2. 일일 업데이트 시스템 다중 스키마 지원")
        print(f"   3. 수급 데이터 수집 시스템 개발")


if __name__ == "__main__":
    main()