#!/usr/bin/env python3
"""
파일명: scripts/final_reorder_samsung_only.py
삼성전자(005930) 테이블 재정렬 스크립트 (샘플 테스트)
- 수급 데이터와 프로그램매매 데이터만 재정렬 (일봉은 정상)
- 안전한 백업 → 재정렬 → 검증 방식
"""
import mysql.connector
from datetime import datetime
import time


def get_connection():
    """MySQL 연결 반환"""
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'stock_user',
        'password': 'StockPass2025!',
        'charset': 'utf8mb4',
        'autocommit': False  # 트랜잭션 관리를 위해 False
    }
    return mysql.connector.connect(**config)


def reorder_table(schema, table_name, sort_column):
    """단일 테이블 재정렬"""
    print(f"\n{'=' * 60}")
    print(f"🔄 {schema}.{table_name} 재정렬 시작")
    print(f"{'=' * 60}")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 1. 스키마 변경
        cursor.execute(f"USE {schema}")

        # 2. 현재 레코드 수 확인
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        original_count = cursor.fetchone()[0]
        print(f"📊 원본 레코드 수: {original_count:,}개")

        # 3. 백업 테이블 이름 생성
        backup_table = f"{table_name}_backup_{int(time.time())}"
        print(f"💾 백업 테이블: {backup_table}")

        # 4. 백업 테이블 생성
        print(f"🔄 백업 생성 중...")
        cursor.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {table_name}")

        # 백업 확인
        cursor.execute(f"SELECT COUNT(*) FROM {backup_table}")
        backup_count = cursor.fetchone()[0]
        print(f"✅ 백업 완료: {backup_count:,}개")

        if backup_count != original_count:
            print(f"❌ 백업 실패: 레코드 수 불일치")
            conn.rollback()
            return False

        # 5. 기존 테이블 비우기 (AUTO_INCREMENT 리셋)
        print(f"🗑️ 기존 테이블 초기화...")
        cursor.execute(f"TRUNCATE TABLE {table_name}")

        # 6. 날짜 오름차순으로 재삽입
        print(f"📥 날짜 오름차순으로 재삽입 중...")
        cursor.execute(f"""
            INSERT INTO {table_name} 
            SELECT * FROM {backup_table} 
            ORDER BY {sort_column} ASC
        """)

        # 7. 재삽입 확인
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        reordered_count = cursor.fetchone()[0]
        print(f"✅ 재삽입 완료: {reordered_count:,}개")

        if reordered_count != original_count:
            print(f"❌ 재삽입 실패: 레코드 수 불일치")
            conn.rollback()
            return False

        # 8. 결과 검증
        print(f"🔍 재정렬 결과 검증...")

        # 첫 번째와 마지막 레코드 확인
        cursor.execute(f"SELECT {sort_column} FROM {table_name} ORDER BY id ASC LIMIT 1")
        first_date = cursor.fetchone()[0]

        cursor.execute(f"SELECT {sort_column} FROM {table_name} ORDER BY id DESC LIMIT 1")
        last_date = cursor.fetchone()[0]

        print(f"📅 재정렬 후 - 첫 날짜: {first_date}, 마지막 날짜: {last_date}")

        # 순서 검증
        if str(first_date) <= str(last_date):
            print(f"✅ 재정렬 성공: 과거 → 최신 순서")

            # 커밋
            conn.commit()

            # 9. 백업 테이블 삭제 (성공 시)
            print(f"🗑️ 백업 테이블 삭제...")
            cursor.execute(f"DROP TABLE {backup_table}")
            conn.commit()
            print(f"✅ 백업 테이블 삭제 완료")

            return True
        else:
            print(f"❌ 재정렬 실패: 순서가 잘못됨")
            conn.rollback()
            return False

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        try:
            conn.rollback()
        except:
            pass
        return False
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


def verify_reordering():
    """재정렬 결과 검증"""
    print(f"\n{'=' * 60}")
    print(f"🔍 재정렬 결과 최종 검증")
    print(f"{'=' * 60}")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 일봉 데이터 검증
        print(f"\n📊 일봉 데이터 검증:")
        cursor.execute("USE daily_prices_db")
        cursor.execute("SELECT date FROM daily_prices_005930 ORDER BY id ASC LIMIT 3")
        first_dates = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT date FROM daily_prices_005930 ORDER BY id DESC LIMIT 3")
        last_dates = [row[0] for row in cursor.fetchall()]

        print(f"   상위 3개: {first_dates}")
        print(f"   하위 3개: {last_dates}")

        # 수급 데이터 검증
        print(f"\n📊 수급 데이터 검증:")
        cursor.execute("USE supply_demand_db")
        cursor.execute("SELECT date FROM supply_demand_005930 ORDER BY id ASC LIMIT 3")
        first_dates = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT date FROM supply_demand_005930 ORDER BY id DESC LIMIT 3")
        last_dates = [row[0] for row in cursor.fetchall()]

        print(f"   상위 3개: {first_dates}")
        print(f"   하위 3개: {last_dates}")

        # 프로그램매매 데이터 검증
        print(f"\n📊 프로그램매매 데이터 검증:")
        cursor.execute("USE program_trading_db")
        cursor.execute("SELECT date FROM program_trading_005930 ORDER BY id ASC LIMIT 3")
        first_dates = [str(row[0]) for row in cursor.fetchall()]
        cursor.execute("SELECT date FROM program_trading_005930 ORDER BY id DESC LIMIT 3")
        last_dates = [str(row[0]) for row in cursor.fetchall()]

        print(f"   상위 3개: {first_dates}")
        print(f"   하위 3개: {last_dates}")

        cursor.close()
        conn.close()

        print(f"\n✅ 검증 완료!")

    except Exception as e:
        print(f"❌ 검증 중 오류: {e}")


def main():
    """메인 실행 함수"""
    print(f"🚀 삼성전자(005930) 테이블 재정렬 시작")
    print(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\n📋 재정렬 대상:")
    print(f"   1. daily_prices_db.daily_prices_005930 (6,004개) ❌ 뒤죽박죽")
    print(f"   2. supply_demand_db.supply_demand_005930 (400개)")
    print(f"   3. program_trading_db.program_trading_005930 (241개)")
    print(f"   ⚠️ 일봉 데이터도 심각한 순서 문제 발견!")

    # 사용자 확인
    response = input(f"\n계속 진행하시겠습니까? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print(f"❌ 작업이 취소되었습니다.")
        return

    success_count = 0
    total_tables = 3

    # 1. 일봉 데이터 재정렬 (가장 중요!)
    print(f"\n🏆 1단계: 일봉 데이터 재정렬 (가장 중요)")
    if reorder_table('daily_prices_db', 'daily_prices_005930', 'date'):
        success_count += 1
        print(f"✅ 일봉 데이터 재정렬 성공")
    else:
        print(f"❌ 일봉 데이터 재정렬 실패")

    # 2. 수급 데이터 재정렬
    print(f"\n📊 2단계: 수급 데이터 재정렬")
    if reorder_table('supply_demand_db', 'supply_demand_005930', 'date'):
        success_count += 1
        print(f"✅ 수급 데이터 재정렬 성공")
    else:
        print(f"❌ 수급 데이터 재정렬 실패")

    # 3. 프로그램매매 데이터 재정렬
    print(f"\n💹 3단계: 프로그램매매 데이터 재정렬")
    if reorder_table('program_trading_db', 'program_trading_005930', 'date'):
        success_count += 1
        print(f"✅ 프로그램매매 데이터 재정렬 성공")
    else:
        print(f"❌ 프로그램매매 데이터 재정렬 실패")

    # 최종 결과
    print(f"\n{'=' * 60}")
    print(f"📊 재정렬 작업 완료")
    print(f"✅ 성공: {success_count}/{total_tables}개")
    print(f"❌ 실패: {total_tables - success_count}/{total_tables}개")
    print(f"완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    if success_count == total_tables:
        # 최종 검증
        verify_reordering()
        print(f"\n🎉 모든 재정렬 작업이 성공적으로 완료되었습니다!")
        print(f"💡 이제 원본 확인 스크립트로 결과를 다시 확인해보세요:")
        print(f"   python scripts/check_samsung_data_order.py")
    else:
        print(f"\n⚠️ 일부 작업이 실패했습니다. 로그를 확인해주세요.")


if __name__ == "__main__":
    main()