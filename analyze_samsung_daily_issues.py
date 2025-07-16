#!/usr/bin/env python3
"""
삼성전자 일봉 데이터 상세 문제 분석
"""
import mysql.connector


def analyze_daily_issues():
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'stock_user',
        'password': 'StockPass2025!',
        'charset': 'utf8mb4',
        'autocommit': True
    }

    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        cursor.execute("USE daily_prices_db")

        print("🔍 삼성전자 일봉 데이터 상세 분석")
        print("=" * 50)

        # 1. 기본 통계
        cursor.execute("SELECT COUNT(*), MIN(id), MAX(id) FROM daily_prices_005930")
        count, min_id, max_id = cursor.fetchone()
        print(f"📊 레코드 수: {count:,}개")
        print(f"📊 ID 범위: {min_id} ~ {max_id} (차이: {max_id - min_id + 1})")

        # 2. 중복 날짜 확인
        cursor.execute("""
            SELECT date, COUNT(*) as cnt 
            FROM daily_prices_005930 
            GROUP BY date 
            HAVING COUNT(*) > 1 
            ORDER BY cnt DESC 
            LIMIT 10
        """)
        duplicates = cursor.fetchall()
        if duplicates:
            print(f"\n❌ 중복 날짜 발견:")
            for date, cnt in duplicates:
                print(f"   {date}: {cnt}개")
        else:
            print(f"\n✅ 중복 날짜 없음")

        # 3. ID 누락 확인 (연속성)
        cursor.execute("""
            SELECT COUNT(*) as missing_count
            FROM (
                SELECT id + 1 as next_id
                FROM daily_prices_005930 
                WHERE id < (SELECT MAX(id) FROM daily_prices_005930)
                AND id + 1 NOT IN (SELECT id FROM daily_prices_005930)
            ) as missing
        """)
        missing_ids = cursor.fetchone()[0]
        print(f"📊 누락된 ID: {missing_ids}개")

        # 4. 날짜별 분포 확인
        cursor.execute("""
            SELECT 
                SUBSTR(date, 1, 4) as year,
                COUNT(*) as count
            FROM daily_prices_005930 
            GROUP BY SUBSTR(date, 1, 4)
            ORDER BY year
        """)
        year_dist = cursor.fetchall()
        print(f"\n📅 연도별 분포:")
        for year, count in year_dist:
            print(f"   {year}: {count:,}개")

        # 5. 최신 10개 레코드의 ID와 날짜
        print(f"\n📈 최신 10개 레코드:")
        cursor.execute("""
            SELECT id, date, close_price 
            FROM daily_prices_005930 
            ORDER BY date DESC 
            LIMIT 10
        """)
        for id, date, price in cursor.fetchall():
            print(f"   ID: {id:>4}, 날짜: {date}, 종가: {price:>6}")

        conn.close()

    except Exception as e:
        print(f"❌ 오류: {e}")


if __name__ == "__main__":
    analyze_daily_issues()