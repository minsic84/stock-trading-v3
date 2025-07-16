#!/usr/bin/env python3
"""
daily_prices_db 전체 정리 스크립트
- 모든 daily_prices_XXXXXX 테이블 삭제
- 백업 테이블들도 함께 정리
- 안전한 단계별 삭제
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


def get_all_tables_in_daily_prices_db():
    """daily_prices_db의 모든 테이블 목록 가져오기"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("USE daily_prices_db")

        cursor.execute("SHOW TABLES")
        all_tables = [table[0] for table in cursor.fetchall()]

        conn.close()
        return all_tables

    except Exception as e:
        print(f"❌ 테이블 목록 조회 오류: {e}")
        return []


def analyze_tables_to_delete():
    """삭제할 테이블들 분석"""
    all_tables = get_all_tables_in_daily_prices_db()

    if not all_tables:
        print("❌ daily_prices_db에서 테이블을 찾을 수 없습니다.")
        return None

    # 테이블 분류
    categories = {
        'daily_prices': [],  # daily_prices_XXXXXX
        'backup_tables': [],  # 백업 테이블들
        'other_tables': []  # 기타 테이블
    }

    for table in all_tables:
        if table.startswith('daily_prices_') and len(table.split('_')) >= 3:
            # daily_prices_005930 형태
            parts = table.split('_')
            if len(parts) == 3 and len(parts[2]) == 6 and parts[2].isdigit():
                categories['daily_prices'].append(table)
            else:
                # daily_prices_005930_backup_xxx 형태
                categories['backup_tables'].append(table)
        elif 'backup' in table.lower() or 'temp' in table.lower():
            categories['backup_tables'].append(table)
        else:
            categories['other_tables'].append(table)

    print(f"\n📊 daily_prices_db 테이블 분석:")
    print(f"   📈 일봉 테이블: {len(categories['daily_prices']):,}개")
    print(f"   💾 백업 테이블: {len(categories['backup_tables']):,}개")
    print(f"   🔧 기타 테이블: {len(categories['other_tables']):,}개")
    print(f"   📋 총 테이블: {len(all_tables):,}개")

    return categories


def delete_tables_batch(table_list, batch_size=50):
    """테이블들을 배치로 삭제"""
    if not table_list:
        return 0

    success_count = 0
    total_tables = len(table_list)

    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("USE daily_prices_db")

        print(f"🗑️ {total_tables}개 테이블 삭제 시작...")

        for i in range(0, total_tables, batch_size):
            batch = table_list[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_tables + batch_size - 1) // batch_size

            print(f"\n📦 배치 {batch_num}/{total_batches} ({len(batch)}개 테이블)")

            for j, table in enumerate(batch):
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                    success_count += 1

                    # 진행 상황 표시
                    progress = i + j + 1
                    percent = (progress / total_tables) * 100
                    print(f"\r   [{progress:>4}/{total_tables}] {percent:>5.1f}% | {table}", end="", flush=True)

                except Exception as e:
                    print(f"\n   ❌ {table} 삭제 실패: {e}")

            print()  # 배치 완료 후 개행

            # 배치별 커밋
            conn.commit()
            print(f"   ✅ 배치 {batch_num} 완료 ({len(batch)}개 삭제)")

            # CPU 부하 방지
            time.sleep(0.1)

        conn.close()

    except Exception as e:
        print(f"❌ 배치 삭제 오류: {e}")
        try:
            conn.rollback()
        except:
            pass

    return success_count


def verify_deletion():
    """삭제 결과 확인"""
    print(f"\n🔍 삭제 결과 확인...")

    remaining_tables = get_all_tables_in_daily_prices_db()

    if not remaining_tables:
        print("✅ daily_prices_db가 완전히 정리되었습니다!")
        return True
    else:
        print(f"⚠️ {len(remaining_tables)}개 테이블이 남아있습니다:")
        for table in remaining_tables[:10]:  # 처음 10개만 표시
            print(f"   - {table}")
        if len(remaining_tables) > 10:
            print(f"   ... 외 {len(remaining_tables) - 10}개")
        return False


def create_fresh_schema():
    """깔끔한 스키마 재생성 (필요시)"""
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 스키마 삭제 후 재생성
        print(f"\n🔄 daily_prices_db 스키마 재생성...")

        cursor.execute("DROP DATABASE IF EXISTS daily_prices_db")
        cursor.execute("""
            CREATE DATABASE daily_prices_db 
            CHARACTER SET utf8mb4 
            COLLATE utf8mb4_unicode_ci
        """)

        conn.commit()
        conn.close()

        print("✅ 깔끔한 daily_prices_db 스키마 생성 완료!")
        return True

    except Exception as e:
        print(f"❌ 스키마 재생성 오류: {e}")
        return False


def main():
    """메인 실행"""
    print(f"🗑️ daily_prices_db 전체 정리")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"=" * 60)

    # 1. 현재 상태 분석
    categories = analyze_tables_to_delete()
    if not categories:
        return

    total_to_delete = (len(categories['daily_prices']) +
                       len(categories['backup_tables']) +
                       len(categories['other_tables']))

    if total_to_delete == 0:
        print("삭제할 테이블이 없습니다.")
        return

    # 2. 사용자 확인
    print(f"\n⚠️ 주의: 이 작업은 되돌릴 수 없습니다!")
    print(f"📋 삭제 예정:")
    print(f"   📈 일봉 테이블: {len(categories['daily_prices']):,}개")
    print(f"   💾 백업 테이블: {len(categories['backup_tables']):,}개")
    print(f"   🔧 기타 테이블: {len(categories['other_tables']):,}개")
    print(f"   📊 총 삭제: {total_to_delete:,}개")

    response = input(f"\n📍 정말로 모든 테이블을 삭제하시겠습니까? (DELETE 입력): ")
    if response != "DELETE":
        print("❌ 삭제 작업이 취소되었습니다.")
        return

    # 3. 삭제 실행
    start_time = datetime.now()
    total_deleted = 0

    # 일봉 테이블 삭제
    if categories['daily_prices']:
        print(f"\n📈 일봉 테이블 삭제 중...")
        deleted = delete_tables_batch(categories['daily_prices'])
        total_deleted += deleted
        print(f"✅ 일봉 테이블 삭제 완료: {deleted}/{len(categories['daily_prices'])}개")

    # 백업 테이블 삭제
    if categories['backup_tables']:
        print(f"\n💾 백업 테이블 삭제 중...")
        deleted = delete_tables_batch(categories['backup_tables'])
        total_deleted += deleted
        print(f"✅ 백업 테이블 삭제 완료: {deleted}/{len(categories['backup_tables'])}개")

    # 기타 테이블 삭제
    if categories['other_tables']:
        print(f"\n🔧 기타 테이블 삭제 중...")
        deleted = delete_tables_batch(categories['other_tables'])
        total_deleted += deleted
        print(f"✅ 기타 테이블 삭제 완료: {deleted}/{len(categories['other_tables'])}개")

    # 4. 결과 확인
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    print(f"\n{'=' * 60}")
    print(f"📊 삭제 작업 완료")
    print(f"✅ 성공: {total_deleted:,}개")
    print(f"❌ 실패: {total_to_delete - total_deleted:,}개")
    print(f"⏱️ 소요 시간: {elapsed:.1f}초")
    print(f"완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 5. 최종 검증
    if verify_deletion():
        # 6. 선택사항: 스키마 재생성
        response = input(f"\n📍 깔끔한 스키마로 재생성하시겠습니까? (yes/no): ")
        if response.lower() in ['yes', 'y']:
            create_fresh_schema()

    print(f"\n🎉 daily_prices_db 정리 완료!")
    print(f"💡 이제 새로운 수집 로직으로 깔끔하게 시작할 수 있습니다.")
    print(f"=" * 60)


if __name__ == "__main__":
    main()