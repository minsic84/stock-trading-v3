#!/usr/bin/env python3
"""
더 오래된 백업에서 데이터 복구
"""
import mysql.connector
from datetime import datetime


def get_connection():
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'stock_user',
        'password': 'StockPass2025!',
        'charset': 'utf8mb4',
        'autocommit': False
    }
    return mysql.connector.connect(**config)


def recover_from_specific_backup(schema, original_table, backup_table):
    """특정 백업에서 복구"""
    print(f"\n🔄 {schema}.{original_table} 복구 중...")
    print(f"   백업 소스: {backup_table}")

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"USE {schema}")

        # 백업 테이블 존재 확인
        cursor.execute(f"SHOW TABLES LIKE '{backup_table}'")
        if not cursor.fetchone():
            print(f"   ❌ 백업 테이블이 존재하지 않습니다.")
            conn.close()
            return False

        # 백업 데이터 확인
        cursor.execute(f"SELECT COUNT(*) FROM {backup_table}")
        backup_count = cursor.fetchone()[0]
        print(f"   📊 백업 데이터: {backup_count:,}개")

        if backup_count == 0:
            print(f"   ❌ 백업에 데이터가 없습니다.")
            conn.close()
            return False

        # 백업 테이블 구조 확인
        cursor.execute(f"DESCRIBE {backup_table}")
        backup_columns = cursor.fetchall()
        print(f"   📋 백업 컬럼 수: {len(backup_columns)}개")

        # date 컬럼 타입 확인
        date_column_type = None
        for col in backup_columns:
            if col[0] == 'date':
                date_column_type = col[1]
                break

        print(f"   📅 백업 date 타입: {date_column_type}")

        # 백업 데이터 샘플 확인
        cursor.execute(f"SELECT date FROM {backup_table} LIMIT 5")
        samples = [str(row[0]) for row in cursor.fetchall()]
        print(f"   📋 백업 샘플: {samples}")

        # 메인 테이블 구조 확인
        cursor.execute(f"DESCRIBE {original_table}")
        main_columns = cursor.fetchall()

        # 컬럼 수 비교
        if len(backup_columns) != len(main_columns):
            print(f"   ⚠️ 컬럼 수 다름: 백업 {len(backup_columns)}, 메인 {len(main_columns)}")

        # 메인 테이블 비우기
        print(f"   🗑️ 메인 테이블 초기화...")
        cursor.execute(f"TRUNCATE TABLE {original_table}")

        # 백업에서 복구
        print(f"   📥 데이터 복구 중...")
        cursor.execute(f"INSERT INTO {original_table} SELECT * FROM {backup_table}")

        # 복구 확인
        cursor.execute(f"SELECT COUNT(*) FROM {original_table}")
        recovered_count = cursor.fetchone()[0]
        print(f"   ✅ 복구 완료: {recovered_count:,}개")

        # 복구된 데이터 샘플
        cursor.execute(f"SELECT date FROM {original_table} LIMIT 5")
        recovered_samples = [str(row[0]) for row in cursor.fetchall()]
        print(f"   📋 복구된 샘플: {recovered_samples}")

        # 현재 date 컬럼 타입 확인
        cursor.execute(f"DESCRIBE {original_table}")
        for col in cursor.fetchall():
            if col[0] == 'date':
                current_date_type = col[1]
                print(f"   📅 현재 date 타입: {current_date_type}")
                break

        conn.commit()
        conn.close()
        return True

    except Exception as e:
        print(f"   ❌ 복구 오류: {e}")
        try:
            conn.rollback()
        except:
            pass
        return False


def main():
    """메인 실행"""
    print(f"🚨 오래된 백업에서 데이터 복구")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 복구 계획
    recovery_tasks = [
        ('supply_demand_db', 'supply_demand_005930', 'supply_demand_005930_backup_1752651898'),
        ('program_trading_db', 'program_trading_005930', 'program_trading_005930_backup_1752651898')
    ]

    print(f"\n📋 복구 계획 (더 오래된 백업):")
    for schema, table, backup in recovery_tasks:
        print(f"   {schema}.{table} ← {backup}")

    response = input(f"\n📍 더 오래된 백업에서 복구하시겠습니까? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print(f"❌ 복구 취소")
        return

    # 복구 실행
    success_count = 0

    for schema, table, backup in recovery_tasks:
        if recover_from_specific_backup(schema, table, backup):
            success_count += 1

    # 결과
    print(f"\n{'=' * 60}")
    print(f"📊 복구 작업 완료")
    print(f"✅ 성공: {success_count}/{len(recovery_tasks)}개")
    print(f"완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if success_count == len(recovery_tasks):
        print(f"\n🎉 데이터 복구 성공!")
        print(f"\n📋 다음 단계:")
        print(f"   1. 복구된 데이터 확인")
        print(f"   2. 날짜 형식 확인 (VARCHAR vs DATE)")
        print(f"   3. 필요시 형식 변환")
        print(f"   4. 순서 재정렬")

        print(f"\n📋 확인 명령어:")
        print(f"   python scripts/check_samsung_data_order.py")
    else:
        print(f"\n⚠️ 일부 복구 실패")

    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()