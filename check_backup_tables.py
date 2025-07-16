#!/usr/bin/env python3
"""
백업 테이블 확인 및 데이터 복구
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


def find_backup_tables():
    """백업 테이블 찾기"""
    print("🔍 백업 테이블 검색 중...")

    try:
        schemas = [
            'supply_demand_db',
            'program_trading_db'
        ]

        backup_tables = {}

        for schema in schemas:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(f"USE {schema}")

            # 백업 테이블 찾기 (이름에 backup이 포함된 테이블)
            cursor.execute("SHOW TABLES")
            all_tables = [table[0] for table in cursor.fetchall()]

            backup_list = [table for table in all_tables if 'backup' in table.lower() or 'convert' in table.lower()]

            if backup_list:
                backup_tables[schema] = backup_list
                print(f"\n📊 {schema}:")
                for backup_table in backup_list:
                    cursor.execute(f"SELECT COUNT(*) FROM {backup_table}")
                    count = cursor.fetchone()[0]
                    print(f"   {backup_table}: {count:,}개")

            conn.close()

        return backup_tables

    except Exception as e:
        print(f"❌ 백업 테이블 검색 오류: {e}")
        return {}


def check_main_tables():
    """메인 테이블 상태 확인"""
    print(f"\n📊 메인 테이블 상태:")

    try:
        tables = [
            ('supply_demand_db', 'supply_demand_005930'),
            ('program_trading_db', 'program_trading_005930')
        ]

        for schema, table in tables:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute(f"USE {schema}")

            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]

                # 컬럼 구조 확인
                cursor.execute(f"DESCRIBE {table}")
                columns = cursor.fetchall()
                date_column = None
                for col in columns:
                    if col[0] == 'date':
                        date_column = col[1]
                        break

                print(f"   {schema}.{table}: {count:,}개, date 타입: {date_column}")

            except Exception as e:
                print(f"   {schema}.{table}: 테이블 오류 - {e}")

            conn.close()

    except Exception as e:
        print(f"❌ 메인 테이블 확인 오류: {e}")


def recover_from_backup(schema, original_table, backup_table):
    """백업에서 데이터 복구"""
    print(f"\n🔄 {schema}.{original_table} 복구 중...")

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"USE {schema}")

        # 백업 테이블 확인
        cursor.execute(f"SELECT COUNT(*) FROM {backup_table}")
        backup_count = cursor.fetchone()[0]
        print(f"   백업 데이터: {backup_count:,}개")

        if backup_count == 0:
            print(f"   ❌ 백업에 데이터가 없습니다.")
            conn.close()
            return False

        # 백업 데이터 샘플 확인
        cursor.execute(f"DESCRIBE {backup_table}")
        columns = [col[0] for col in cursor.fetchall()]
        print(f"   백업 테이블 컬럼: {columns}")

        # 메인 테이블 비우기
        cursor.execute(f"TRUNCATE TABLE {original_table}")

        # 백업에서 복구
        cursor.execute(f"INSERT INTO {original_table} SELECT * FROM {backup_table}")

        # 복구 확인
        cursor.execute(f"SELECT COUNT(*) FROM {original_table}")
        recovered_count = cursor.fetchone()[0]
        print(f"   ✅ 복구 완료: {recovered_count:,}개")

        # 샘플 데이터 확인
        if 'date' in columns:
            cursor.execute(f"SELECT date FROM {original_table} LIMIT 3")
            samples = [str(row[0]) for row in cursor.fetchall()]
            print(f"   📅 복구된 데이터 샘플: {samples}")

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
    print(f"🚨 데이터 복구 작업")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. 현재 상태 확인
    check_main_tables()

    # 2. 백업 테이블 찾기
    backup_tables = find_backup_tables()

    if not backup_tables:
        print(f"\n❌ 백업 테이블을 찾을 수 없습니다.")
        print(f"💡 수동으로 확인해보세요:")
        print(f"   SHOW TABLES LIKE '%backup%';")
        print(f"   SHOW TABLES LIKE '%convert%';")
        return

    # 3. 복구 가능한 백업 선택
    print(f"\n📋 복구 계획:")
    recovery_plan = []

    for schema, backups in backup_tables.items():
        if schema == 'supply_demand_db':
            target_table = 'supply_demand_005930'
        elif schema == 'program_trading_db':
            target_table = 'program_trading_005930'
        else:
            continue

        # 가장 최근 백업 선택 (타임스탬프 기준)
        latest_backup = max(backups, key=lambda x: x.split('_')[-1] if '_' in x else '0')
        recovery_plan.append((schema, target_table, latest_backup))
        print(f"   {schema}.{target_table} ← {latest_backup}")

    if not recovery_plan:
        print(f"❌ 복구 가능한 백업이 없습니다.")
        return

    response = input(f"\n📍 위 계획대로 데이터를 복구하시겠습니까? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print(f"❌ 복구 취소")
        return

    # 4. 복구 실행
    success_count = 0

    for schema, table, backup in recovery_plan:
        if recover_from_backup(schema, table, backup):
            success_count += 1

    # 5. 결과 확인
    print(f"\n{'=' * 50}")
    print(f"📊 복구 작업 완료")
    print(f"✅ 성공: {success_count}/{len(recovery_plan)}개")
    print(f"완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if success_count == len(recovery_plan):
        print(f"\n🎉 데이터 복구 성공!")
        print(f"📋 다음 단계:")
        print(f"   1. 복구된 데이터 확인")
        print(f"   2. 필요시 날짜 형식 재변환")
        print(f"   3. 순서 재정렬")
    else:
        print(f"\n⚠️ 일부 복구 실패")

    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()