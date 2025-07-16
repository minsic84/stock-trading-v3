#!/usr/bin/env python3
"""
전체 종목 일봉 데이터 재정렬 시스템
- daily_prices_db의 모든 종목 테이블 처리
- 배치 처리로 효율성 극대화
- 실시간 진행 상황 표시
"""
import mysql.connector
from datetime import datetime
import time
import sys
from pathlib import Path


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


def get_all_daily_price_tables():
    """daily_prices_db의 모든 테이블 목록 가져오기"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("USE daily_prices_db")

        cursor.execute("SHOW TABLES")
        all_tables = [table[0] for table in cursor.fetchall()]

        # daily_prices_XXXXXX 패턴 필터링
        stock_tables = []
        for table in all_tables:
            if table.startswith('daily_prices_') and len(table.split('_')) == 3:
                stock_code = table.split('_')[2]
                if len(stock_code) == 6 and stock_code.isdigit():
                    stock_tables.append((stock_code, table))

        conn.close()

        # 종목코드 순으로 정렬
        stock_tables.sort(key=lambda x: x[0])

        return stock_tables

    except Exception as e:
        print(f"❌ 테이블 목록 조회 오류: {e}")
        return []


def check_table_order_status(table_name):
    """테이블의 현재 정렬 상태 확인"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("USE daily_prices_db")

        # 레코드 수 확인
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]

        if count == 0:
            conn.close()
            return 'empty', 0, None, None

        # 첫 번째와 마지막 날짜 (ID 순서)
        cursor.execute(f"SELECT date FROM {table_name} ORDER BY id ASC LIMIT 1")
        first_date = cursor.fetchone()[0]

        cursor.execute(f"SELECT date FROM {table_name} ORDER BY id DESC LIMIT 1")
        last_date = cursor.fetchone()[0]

        conn.close()

        # 순서 판정
        if first_date <= last_date:
            return 'correct', count, first_date, last_date
        else:
            return 'incorrect', count, first_date, last_date

    except Exception as e:
        return 'error', 0, None, None


def reorder_single_table(stock_code, table_name):
    """단일 테이블 재정렬"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("USE daily_prices_db")

        # 1. 레코드 수 확인
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cursor.fetchone()[0]

        if total_count == 0:
            conn.close()
            return True, "빈 테이블"

        # 2. 현재 순서 확인
        cursor.execute(f"SELECT date FROM {table_name} ORDER BY id ASC LIMIT 1")
        current_first = cursor.fetchone()[0]
        cursor.execute(f"SELECT date FROM {table_name} ORDER BY id DESC LIMIT 1")
        current_last = cursor.fetchone()[0]

        # 이미 정렬되어 있으면 스킵
        if current_first <= current_last:
            conn.close()
            return True, f"이미 정렬됨 ({current_first}→{current_last})"

        # 3. 백업 생성
        backup_table = f"{table_name}_temp_backup"

        # 기존 백업 삭제 (있다면)
        cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")

        cursor.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {table_name}")

        # 4. 재정렬
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        cursor.execute(f"""
            INSERT INTO {table_name} 
            SELECT * FROM {backup_table} 
            ORDER BY date ASC
        """)

        # 5. 검증
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        new_count = cursor.fetchone()[0]

        if new_count != total_count:
            conn.rollback()
            conn.close()
            return False, f"레코드 수 불일치: {new_count}/{total_count}"

        # 새 순서 확인
        cursor.execute(f"SELECT date FROM {table_name} ORDER BY id ASC LIMIT 1")
        new_first = cursor.fetchone()[0]
        cursor.execute(f"SELECT date FROM {table_name} ORDER BY id DESC LIMIT 1")
        new_last = cursor.fetchone()[0]

        if new_first <= new_last:
            # 성공 - 커밋 및 백업 삭제
            conn.commit()
            cursor.execute(f"DROP TABLE {backup_table}")
            conn.commit()
            conn.close()
            return True, f"재정렬 성공 ({new_first}→{new_last}, {total_count:,}개)"
        else:
            conn.rollback()
            conn.close()
            return False, f"재정렬 실패: {new_first}→{new_last}"

    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        return False, f"오류: {str(e)[:50]}"


def create_progress_display():
    """진행 상황 표시 클래스"""

    class ProgressDisplay:
        def __init__(self, total):
            self.total = total
            self.processed = 0
            self.success = 0
            self.skipped = 0
            self.failed = 0
            self.start_time = datetime.now()

        def update(self, stock_code, status, message=""):
            self.processed += 1

            if "성공" in message or "이미 정렬됨" in message:
                self.success += 1
                status_icon = "✅"
            elif "빈 테이블" in message:
                self.skipped += 1
                status_icon = "⚪"
            else:
                self.failed += 1
                status_icon = "❌"

            # 진행률 계산
            progress = (self.processed / self.total) * 100
            elapsed = (datetime.now() - self.start_time).total_seconds()

            if self.processed > 0:
                avg_time = elapsed / self.processed
                remaining = (self.total - self.processed) * avg_time
                eta = datetime.now().timestamp() + remaining
                eta_str = datetime.fromtimestamp(eta).strftime('%H:%M:%S')
            else:
                eta_str = "계산중"

            # 진행 상황 출력
            print(f"\r{status_icon} [{self.processed:>4}/{self.total}] {progress:>5.1f}% | "
                  f"성공:{self.success:>4} 스킵:{self.skipped:>3} 실패:{self.failed:>3} | "
                  f"ETA: {eta_str} | {stock_code}: {message[:30]}", end="", flush=True)

            # 주요 진행점에서 개행
            if self.processed % 100 == 0 or self.processed == self.total:
                print()

        def final_summary(self):
            elapsed = (datetime.now() - self.start_time).total_seconds()
            print(f"\n{'=' * 80}")
            print(f"🎉 전체 재정렬 작업 완료!")
            print(f"📊 처리 결과:")
            print(f"   ✅ 성공: {self.success:,}개")
            print(f"   ⚪ 스킵: {self.skipped:,}개 (빈 테이블 또는 이미 정렬됨)")
            print(f"   ❌ 실패: {self.failed:,}개")
            print(f"   📈 총 처리: {self.total:,}개")
            print(f"⏱️ 소요 시간: {elapsed / 60:.1f}분 ({elapsed:.1f}초)")
            print(f"⚡ 평균 속도: {elapsed / self.total:.2f}초/테이블")
            print(f"{'=' * 80}")

    return ProgressDisplay


def analyze_before_start():
    """시작 전 전체 상황 분석"""
    print("🔍 전체 종목 현황 분석 중...")

    stock_tables = get_all_daily_price_tables()
    total_tables = len(stock_tables)

    if total_tables == 0:
        print("❌ 처리할 테이블이 없습니다.")
        return None

    # 샘플 분석 (처음 100개만)
    sample_size = min(100, total_tables)

    correct_count = 0
    incorrect_count = 0
    empty_count = 0
    error_count = 0

    print(f"📊 샘플 분석 중... ({sample_size}개 테이블)")

    for i, (stock_code, table_name) in enumerate(stock_tables[:sample_size]):
        status, count, first_date, last_date = check_table_order_status(table_name)

        if status == 'correct':
            correct_count += 1
        elif status == 'incorrect':
            incorrect_count += 1
        elif status == 'empty':
            empty_count += 1
        else:
            error_count += 1

        if (i + 1) % 20 == 0:
            print(f"   분석 중... {i + 1}/{sample_size}")

    print(f"\n📊 샘플 분석 결과:")
    print(f"   ✅ 정상 정렬: {correct_count}개 ({correct_count / sample_size * 100:.1f}%)")
    print(f"   ❌ 역순 정렬: {incorrect_count}개 ({incorrect_count / sample_size * 100:.1f}%)")
    print(f"   ⚪ 빈 테이블: {empty_count}개 ({empty_count / sample_size * 100:.1f}%)")
    print(f"   🔧 오류: {error_count}개 ({error_count / sample_size * 100:.1f}%)")

    # 전체 예상
    estimated_need_reorder = int(incorrect_count / sample_size * total_tables)

    print(f"\n🎯 전체 예상:")
    print(f"   📋 총 테이블: {total_tables:,}개")
    print(f"   🔄 재정렬 필요 예상: {estimated_need_reorder:,}개")
    print(f"   ⏱️ 예상 소요시간: {estimated_need_reorder * 2 / 60:.1f}분")

    return stock_tables


def main():
    """메인 실행"""
    print(f"🚀 전체 종목 일봉 데이터 재정렬 시스템")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"=" * 80)

    # 1. 사전 분석
    stock_tables = analyze_before_start()
    if not stock_tables:
        return

    total_tables = len(stock_tables)

    response = input(f"\n📍 {total_tables:,}개 종목의 일봉 데이터를 재정렬하시겠습니까? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("❌ 재정렬 취소")
        return

    # 2. 배치 처리 시작
    print(f"\n🔄 배치 재정렬 시작...")
    print(f"📋 진행 상황 표시: ✅성공 ⚪스킵 ❌실패")
    print()

    progress = create_progress_display()(total_tables)

    # 3. 전체 테이블 처리
    for stock_code, table_name in stock_tables:
        success, message = reorder_single_table(stock_code, table_name)
        progress.update(stock_code, success, message)

        # CPU 부하 방지
        time.sleep(0.01)

    # 4. 최종 결과
    progress.final_summary()

    if progress.failed > 0:
        print(f"\n⚠️ 실패한 테이블이 있습니다. 로그를 확인해주세요.")
    else:
        print(f"\n🎉 모든 테이블이 성공적으로 재정렬되었습니다!")
        print(f"💡 이제 매일 업데이트 시 새 데이터가 맨 아래 순서대로 추가됩니다.")


if __name__ == "__main__":
    main()