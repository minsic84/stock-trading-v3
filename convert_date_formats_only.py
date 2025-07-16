#!/usr/bin/env python3
"""
삼성전자 2개 테이블 최종 재정렬
- supply_demand_005930 (역순 → 정순)
- program_trading_005930 (역순 → 정순)
- 이제 모든 테이블이 VARCHAR(8) 형식으로 통일됨
"""
import mysql.connector
from datetime import datetime
import time


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


def check_current_order(schema, table):
    """현재 순서 확인"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"USE {schema}")

        # 상위/하위 날짜 확인
        cursor.execute(f"SELECT date FROM {table} ORDER BY id ASC LIMIT 1")
        first_date = cursor.fetchone()[0]

        cursor.execute(f"SELECT date FROM {table} ORDER BY id DESC LIMIT 1")
        last_date = cursor.fetchone()[0]

        # 레코드 수
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]

        conn.close()

        is_correct = first_date <= last_date
        status = "✅ 정상" if is_correct else "❌ 역순"

        print(f"📊 {schema}.{table}:")
        print(f"   레코드: {count:,}개")
        print(f"   순서: {first_date} → {last_date} {status}")

        return is_correct, count

    except Exception as e:
        print(f"❌ 순서 확인 오류: {e}")
        return False, 0


def reorder_single_table(schema, table):
    """단일 테이블 재정렬"""
    print(f"\n{'=' * 50}")
    print(f"🔄 {schema}.{table} 재정렬")
    print(f"{'=' * 50}")

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(f"USE {schema}")

        # 1. 현재 상태 확인
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total_count = cursor.fetchone()[0]
        print(f"📊 총 레코드: {total_count:,}개")

        # 2. 현재 순서 확인
        cursor.execute(f"SELECT date FROM {table} ORDER BY id ASC LIMIT 1")
        current_first = cursor.fetchone()[0]
        cursor.execute(f"SELECT date FROM {table} ORDER BY id DESC LIMIT 1")
        current_last = cursor.fetchone()[0]
        print(f"📅 현재 순서: {current_first} → {current_last}")

        # 3. 백업 생성
        backup_table = f"{table}_final_reorder_{int(time.time())}"
        print(f"💾 백업: {backup_table}")
        cursor.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {table}")

        # 4. 백업 확인
        cursor.execute(f"SELECT COUNT(*) FROM {backup_table}")
        backup_count = cursor.fetchone()[0]
        if backup_count != total_count:
            print(f"❌ 백업 실패")
            conn.rollback()
            return False

        # 5. 테이블 초기화
        print(f"🗑️ 테이블 초기화...")
        cursor.execute(f"TRUNCATE TABLE {table}")

        # 6. 날짜 오름차순 재삽입 (VARCHAR(8) 문자열 정렬)
        print(f"📥 날짜 오름차순 재삽입...")
        cursor.execute(f"""
            INSERT INTO {table} 
            SELECT * FROM {backup_table} 
            ORDER BY date ASC
        """)

        # 7. 재삽입 확인
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        new_count = cursor.fetchone()[0]
        print(f"✅ 재삽입: {new_count:,}개")

        if new_count != total_count:
            print(f"❌ 레코드 수 불일치")
            conn.rollback()
            return False

        # 8. 결과 확인
        cursor.execute(f"SELECT date FROM {table} ORDER BY id ASC LIMIT 1")
        new_first = cursor.fetchone()[0]
        cursor.execute(f"SELECT date FROM {table} ORDER BY id DESC LIMIT 1")
        new_last = cursor.fetchone()[0]
        print(f"📅 재정렬 후: {new_first} → {new_last}")

        # 9. 순서 검증
        if new_first <= new_last:
            print(f"✅ 재정렬 성공: 과거 → 최신")

            # 커밋 및 백업 삭제
            conn.commit()
            cursor.execute(f"DROP TABLE {backup_table}")
            conn.commit()
            print(f"🗑️ 백업 삭제 완료")

            conn.close()
            return True
        else:
            print(f"❌ 재정렬 실패: 순서 확인")
            conn.rollback()
            conn.close()
            return False

    except Exception as e:
        print(f"❌ 재정렬 오류: {e}")
        try:
            conn.rollback()
        except:
            pass
        return False


def final_verification():
    """최종 검증"""
    print(f"\n{'=' * 60}")
    print(f"🏆 삼성전자 테이블 최종 검증")
    print(f"{'=' * 60}")

    tables = [
        ('daily_prices_db', 'daily_prices_005930'),
        ('supply_demand_db', 'supply_demand_005930'),
        ('program_trading_db', 'program_trading_005930')
    ]

    all_correct = True

    for schema, table in tables:
        is_correct, count = check_current_order(schema, table)
        if not is_correct:
            all_correct = False

    if all_correct:
        print(f"\n🎉 모든 삼성전자 테이블이 올바른 순서로 정렬되었습니다!")
    else:
        print(f"\n⚠️ 일부 테이블에 문제가 있습니다.")

    return all_correct


def main():
    """메인 실행"""
    print(f"🎯 삼성전자 2개 테이블 최종 재정렬")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    print(f"\n📋 작업 대상:")
    print(f"   1. supply_demand_005930 (❌ 역순 → ✅ 정순)")
    print(f"   2. program_trading_005930 (❌ 역순 → ✅ 정순)")
    print(f"   3. daily_prices_005930 (✅ 이미 정상)")

    # 현재 상태 확인
    print(f"\n📊 현재 상태:")
    check_current_order('supply_demand_db', 'supply_demand_005930')
    check_current_order('program_trading_db', 'program_trading_005930')
    check_current_order('daily_prices_db', 'daily_prices_005930')

    response = input(f"\n📍 역순인 2개 테이블을 재정렬하시겠습니까? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print(f"❌ 취소")
        return

    # 재정렬 실행
    success_count = 0

    # 1. 수급 데이터
    if reorder_single_table('supply_demand_db', 'supply_demand_005930'):
        success_count += 1
        print(f"✅ 수급 데이터 재정렬 성공")
    else:
        print(f"❌ 수급 데이터 재정렬 실패")

    # 2. 프로그램매매 데이터
    if reorder_single_table('program_trading_db', 'program_trading_005930'):
        success_count += 1
        print(f"✅ 프로그램매매 데이터 재정렬 성공")
    else:
        print(f"❌ 프로그램매매 데이터 재정렬 실패")

    # 최종 결과
    print(f"\n{'=' * 60}")
    print(f"📊 재정렬 작업 완료")
    print(f"✅ 성공: {success_count}/2개")
    print(f"완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if success_count == 2:
        # 최종 검증
        if final_verification():
            print(f"\n🎉🎉🎉 삼성전자 데이터 완전 정리 성공! 🎉🎉🎉")
            print(f"\n📋 현재 상태:")
            print(f"   ✅ 모든 테이블: VARCHAR(8) 'YYYYMMDD' 형식 통일")
            print(f"   ✅ 모든 테이블: 과거 → 최신 순서 정렬")
            print(f"   ✅ 매일 업데이트 준비 완료")

            print(f"\n📋 확인 명령어:")
            print(f"   python scripts/check_samsung_data_order.py")
    else:
        print(f"\n⚠️ 일부 실패. 로그를 확인해주세요.")

    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()