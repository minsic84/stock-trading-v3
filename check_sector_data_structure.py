#!/usr/bin/env python3
"""
sector_data_db 구조 및 날짜 형식 확인
"""
import mysql.connector


def check_sector_data_structure():
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

        print("🔍 sector_data_db 구조 분석")
        print("=" * 50)

        # 1. 스키마 사용
        cursor.execute("USE sector_data_db")

        # 2. 테이블 목록 확인
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        print(f"📊 총 테이블 수: {len(tables)}개")
        print(f"📋 테이블 목록: {tables[:10]}..." if len(tables) > 10 else f"📋 테이블 목록: {tables}")

        # 3. 샘플 테이블들의 구조 확인
        sample_tables = tables[:3]  # 처음 3개 테이블만 샘플로

        for table in sample_tables:
            print(f"\n{'=' * 40}")
            print(f"📊 {table} 구조")
            print(f"{'=' * 40}")

            # 테이블 구조 확인
            cursor.execute(f"DESCRIBE {table}")
            print("컬럼명        | 타입           | Null | Key")
            print("-" * 45)
            date_columns = []
            for row in cursor.fetchall():
                field, type_info, null, key = row[0], row[1], row[2], row[3]
                print(f"{field:<12} | {type_info:<13} | {null:<4} | {key}")

                # date 관련 컬럼 찾기
                if 'date' in field.lower() or 'time' in field.lower():
                    date_columns.append((field, type_info))

            # 레코드 수 확인
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"\n📈 레코드 수: {count:,}개")

            # 날짜 컬럼이 있다면 샘플 확인
            if date_columns:
                print(f"\n📅 날짜 관련 컬럼: {date_columns}")

                for date_col, date_type in date_columns:
                    cursor.execute(f"SELECT {date_col} FROM {table} WHERE {date_col} IS NOT NULL LIMIT 5")
                    samples = [str(row[0]) for row in cursor.fetchall()]
                    print(f"   {date_col} ({date_type}) 샘플: {samples}")
            else:
                print(f"📅 날짜 컬럼 없음")

        # 4. 전체 테이블의 날짜 컬럼 통계
        print(f"\n{'=' * 50}")
        print(f"📊 전체 테이블 날짜 컬럼 통계")
        print(f"{'=' * 50}")

        date_formats = {'DATE': 0, 'VARCHAR': 0, 'DATETIME': 0, 'TIMESTAMP': 0, 'NONE': 0}

        for table in tables:
            try:
                cursor.execute(f"DESCRIBE {table}")
                has_date = False
                for row in cursor.fetchall():
                    field, type_info = row[0], row[1]
                    if 'date' in field.lower():
                        has_date = True
                        if 'varchar' in type_info.lower():
                            date_formats['VARCHAR'] += 1
                        elif 'date' in type_info.lower():
                            date_formats['DATE'] += 1
                        elif 'datetime' in type_info.lower():
                            date_formats['DATETIME'] += 1
                        elif 'timestamp' in type_info.lower():
                            date_formats['TIMESTAMP'] += 1
                        break

                if not has_date:
                    date_formats['NONE'] += 1

            except Exception as e:
                print(f"❌ {table} 분석 실패: {e}")

        print(f"📊 날짜 형식 분포:")
        for format_type, count in date_formats.items():
            if count > 0:
                print(f"   {format_type}: {count}개 테이블")

        conn.close()

    except Exception as e:
        print(f"❌ 오류: {e}")


if __name__ == "__main__":
    check_sector_data_structure()