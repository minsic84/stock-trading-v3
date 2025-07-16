#!/usr/bin/env python3
"""
업종 지수 데이터 순서 확인 (kospi, kosdaq)
"""
import mysql.connector


def check_sector_order():
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
        cursor.execute("USE sector_data_db")

        tables = ['kospi', 'kosdaq']

        for table in tables:
            print(f"\n{'=' * 50}")
            print(f"📊 {table} 데이터 순서 확인")
            print(f"{'=' * 50}")

            # 레코드 수
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"📈 총 레코드: {count:,}개")

            # 상위 5개 (ID 순서)
            cursor.execute(f"SELECT id, date, close_index FROM {table} ORDER BY id ASC LIMIT 5")
            print("🔝 상위 5개 (ID 순서):")
            for row in cursor.fetchall():
                print(f"   ID: {row[0]:>4}, 날짜: {row[1]}, 종가: {row[2]:>8}")

            # 하위 5개 (ID 역순)
            cursor.execute(f"SELECT id, date, close_index FROM {table} ORDER BY id DESC LIMIT 5")
            print("🔻 하위 5개 (ID 역순):")
            for row in cursor.fetchall():
                print(f"   ID: {row[0]:>4}, 날짜: {row[1]}, 종가: {row[2]:>8}")

            # 날짜 범위
            cursor.execute(f"SELECT MIN(date), MAX(date) FROM {table}")
            min_date, max_date = cursor.fetchone()
            print(f"📅 날짜 범위: {min_date} ~ {max_date}")

            # 순서 진단
            cursor.execute(f"SELECT date FROM {table} ORDER BY id ASC LIMIT 1")
            first_date = cursor.fetchone()[0]
            cursor.execute(f"SELECT date FROM {table} ORDER BY id DESC LIMIT 1")
            last_date = cursor.fetchone()[0]

            if str(first_date) <= str(last_date):
                print(f"✅ 정상: 과거({first_date}) → 최신({last_date})")
            else:
                print(f"❌ 문제: 최신({first_date}) → 과거({last_date})")

        conn.close()

    except Exception as e:
        print(f"❌ 오류: {e}")


if __name__ == "__main__":
    check_sector_order()