#!/usr/bin/env python3
"""
실패한 테이블 분석 및 활성 종목 재처리
- 실패 원인 분석
- stock_codes 테이블과 비교
- 활성 종목만 재처리
"""
import mysql.connector
from datetime import datetime
import time


def get_connection(database=None):
    config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'stock_user',
        'password': 'StockPass2025!',
        'charset': 'utf8mb4',
        'autocommit': False
    }
    if database:
        config['database'] = database
    return mysql.connector.connect(**config)


def get_active_stock_codes():
    """stock_trading_db에서 활성 종목 코드 가져오기"""
    try:
        conn = get_connection('stock_trading_db')
        cursor = conn.cursor()

        # stock_codes 테이블에서 활성 종목 조회
        cursor.execute("""
            SELECT code, name, market 
            FROM stock_codes 
            WHERE is_active = 1 
            ORDER BY code
        """)

        active_stocks = {}
        for code, name, market in cursor.fetchall():
            active_stocks[code] = {'name': name, 'market': market}

        conn.close()
        return active_stocks

    except Exception as e:
        print(f"❌ 활성 종목 조회 오류: {e}")
        return {}


def get_all_daily_price_tables():
    """daily_prices_db의 모든 테이블과 상태 확인"""
    try:
        conn = get_connection('daily_prices_db')
        cursor = conn.cursor()

        cursor.execute("SHOW TABLES")
        all_tables = [table[0] for table in cursor.fetchall()]

        # daily_prices_XXXXXX 패턴 분석
        table_info = []

        for table in all_tables:
            if table.startswith('daily_prices_'):
                parts = table.split('_')
                if len(parts) >= 3:
                    stock_code = parts[2]
                    # 6자리 숫자인지 확인
                    if len(stock_code) == 6 and stock_code.isdigit():
                        table_info.append((stock_code, table))

        conn.close()
        return table_info

    except Exception as e:
        print(f"❌ 테이블 목록 조회 오류: {e}")
        return []


def analyze_table_status(table_name):
    """테이블 상세 상태 분석"""
    try:
        conn = get_connection('daily_prices_db')
        cursor = conn.cursor()

        # 1. 테이블 존재 확인
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if not cursor.fetchone():
            conn.close()
            return {'status': 'not_exist', 'count': 0, 'error': '테이블 없음'}

        # 2. 레코드 수 확인
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
        except Exception as e:
            conn.close()
            return {'status': 'count_error', 'count': 0, 'error': str(e)}

        if count == 0:
            conn.close()
            return {'status': 'empty', 'count': 0, 'error': '빈 테이블'}

        # 3. 날짜 순서 확인
        try:
            cursor.execute(f"SELECT date FROM {table_name} ORDER BY id ASC LIMIT 1")
            first_date = cursor.fetchone()[0]

            cursor.execute(f"SELECT date FROM {table_name} ORDER BY id DESC LIMIT 1")
            last_date = cursor.fetchone()[0]

            cursor.execute(f"SELECT MIN(date), MAX(date) FROM {table_name}")
            min_date, max_date = cursor.fetchone()

        except Exception as e:
            conn.close()
            return {'status': 'date_error', 'count': count, 'error': str(e)}

        # 4. 순서 판정
        is_correct_order = first_date <= last_date

        conn.close()

        return {
            'status': 'correct' if is_correct_order else 'incorrect',
            'count': count,
            'first_date': first_date,
            'last_date': last_date,
            'min_date': min_date,
            'max_date': max_date,
            'error': None
        }

    except Exception as e:
        return {'status': 'analysis_error', 'count': 0, 'error': str(e)}


def analyze_failed_tables():
    """실패한 테이블들 분석"""
    print("🔍 실패한 테이블 분석 시작...")

    # 1. 활성 종목 코드 가져오기
    active_stocks = get_active_stock_codes()
    print(f"📊 활성 종목 수: {len(active_stocks):,}개")

    # 2. 모든 일봉 테이블 가져오기
    all_tables = get_all_daily_price_tables()
    print(f"📊 총 일봉 테이블 수: {len(all_tables):,}개")

    # 3. 각 테이블 상태 분석
    print(f"\n🔍 테이블 상태 분석 중...")

    results = {
        'correct': [],  # 정상 정렬
        'incorrect': [],  # 역순 정렬 (재처리 필요)
        'empty': [],  # 빈 테이블
        'not_exist': [],  # 테이블 없음
        'error': [],  # 기타 오류
        'inactive': []  # 비활성 종목
    }

    for i, (stock_code, table_name) in enumerate(all_tables):
        if (i + 1) % 500 == 0:
            print(f"   분석 중... {i + 1:,}/{len(all_tables):,}")

        # 활성 종목 여부 확인
        is_active = stock_code in active_stocks

        # 테이블 상태 분석
        status_info = analyze_table_status(table_name)
        status_info['stock_code'] = stock_code
        status_info['table_name'] = table_name
        status_info['is_active'] = is_active

        if is_active:
            status_info['stock_name'] = active_stocks[stock_code]['name']
            status_info['market'] = active_stocks[stock_code]['market']

        # 분류
        if not is_active:
            results['inactive'].append(status_info)
        else:
            results[status_info['status']].append(status_info)

    return results, active_stocks


def display_analysis_results(results):
    """분석 결과 표시"""
    total_tables = sum(len(v) for v in results.values())

    print(f"\n{'=' * 80}")
    print(f"📊 테이블 분석 결과 (총 {total_tables:,}개)")
    print(f"{'=' * 80}")

    # 활성 종목 통계
    active_total = sum(len(v) for k, v in results.items() if k != 'inactive')

    print(f"🎯 활성 종목 ({active_total:,}개):")
    print(f"   ✅ 정상 정렬: {len(results['correct']):,}개")
    print(f"   ❌ 역순 정렬: {len(results['incorrect']):,}개 (재처리 필요)")
    print(f"   ⚪ 빈 테이블: {len(results['empty']):,}개")
    print(f"   🚫 테이블 없음: {len(results['not_exist']):,}개")
    print(f"   🔧 기타 오류: {len(results['error']):,}개")

    print(f"\n⚪ 비활성 종목: {len(results['inactive']):,}개 (무시)")

    # 재처리 필요한 활성 종목 상세 표시
    if results['incorrect']:
        print(f"\n❌ 재처리 필요한 활성 종목 ({len(results['incorrect'])}개):")
        print(f"{'종목코드':<10} {'종목명':<20} {'시장':<8} {'레코드수':<8} {'현재순서'}")
        print("-" * 65)

        for info in results['incorrect'][:20]:  # 처음 20개만 표시
            current_order = f"{info.get('first_date', 'N/A')}→{info.get('last_date', 'N/A')}"
            print(f"{info['stock_code']:<10} {info.get('stock_name', 'N/A'):<20} "
                  f"{info.get('market', 'N/A'):<8} {info['count']:<8,} {current_order}")

        if len(results['incorrect']) > 20:
            print(f"... 외 {len(results['incorrect']) - 20}개")

    # 오류 종목 표시
    if results['error']:
        print(f"\n🔧 오류 종목들 ({len(results['error'])}개):")
        for info in results['error'][:10]:
            print(f"   {info['stock_code']}: {info.get('error', 'Unknown error')}")
        if len(results['error']) > 10:
            print(f"   ... 외 {len(results['error']) - 10}개")


def reprocess_failed_active_stocks(failed_list):
    """실패한 활성 종목들 재처리"""
    if not failed_list:
        print("재처리할 종목이 없습니다.")
        return

    print(f"\n🔄 활성 종목 재처리 시작 ({len(failed_list)}개)")
    print(f"진행 상황:")

    success_count = 0
    failed_count = 0

    for i, info in enumerate(failed_list):
        stock_code = info['stock_code']
        table_name = info['table_name']

        print(f"\r[{i + 1:>3}/{len(failed_list)}] {stock_code} 처리 중...", end="", flush=True)

        try:
            conn = get_connection('daily_prices_db')
            cursor = conn.cursor()

            # 백업 생성
            backup_table = f"{table_name}_retry_backup"
            cursor.execute(f"DROP TABLE IF EXISTS {backup_table}")
            cursor.execute(f"CREATE TABLE {backup_table} AS SELECT * FROM {table_name}")

            # 재정렬
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            cursor.execute(f"""
                INSERT INTO {table_name} 
                SELECT * FROM {backup_table} 
                ORDER BY date ASC
            """)

            # 검증
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            new_count = cursor.fetchone()[0]

            cursor.execute(f"SELECT date FROM {table_name} ORDER BY id ASC LIMIT 1")
            new_first = cursor.fetchone()[0]
            cursor.execute(f"SELECT date FROM {table_name} ORDER BY id DESC LIMIT 1")
            new_last = cursor.fetchone()[0]

            if new_count > 0 and new_first <= new_last:
                conn.commit()
                cursor.execute(f"DROP TABLE {backup_table}")
                conn.commit()
                success_count += 1
            else:
                conn.rollback()
                failed_count += 1

            conn.close()

        except Exception as e:
            failed_count += 1
            try:
                conn.rollback()
            except:
                pass

        # 진행 상황 업데이트
        if (i + 1) % 10 == 0:
            print(f"\r[{i + 1:>3}/{len(failed_list)}] 성공: {success_count}, 실패: {failed_count}")

    print(f"\n\n📊 재처리 결과:")
    print(f"   ✅ 성공: {success_count}개")
    print(f"   ❌ 실패: {failed_count}개")


def main():
    """메인 실행"""
    print(f"🔍 실패한 테이블 분석 및 재처리")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. 분석 실행
    results, active_stocks = analyze_failed_tables()

    # 2. 결과 표시
    display_analysis_results(results)

    # 3. 재처리 제안
    need_reprocess = results['incorrect']

    if need_reprocess:
        response = input(f"\n📍 {len(need_reprocess)}개 활성 종목을 재처리하시겠습니까? (yes/no): ")
        if response.lower() in ['yes', 'y']:
            reprocess_failed_active_stocks(need_reprocess)
        else:
            print("재처리를 건너뛰었습니다.")

    # 4. 최종 요약
    print(f"\n{'=' * 80}")
    print(f"📋 최종 요약:")
    print(f"   📊 활성 종목: {len(active_stocks):,}개")
    print(f"   ✅ 정상 처리: {len(results['correct']):,}개")
    print(f"   ❌ 재처리 필요: {len(results['incorrect']):,}개")
    print(f"   ⚪ 기타 (빈테이블/오류): {len(results['empty']) + len(results['error']):,}개")
    print(f"   🗑️ 비활성 종목: {len(results['inactive']):,}개 (무시 가능)")
    print(f"완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()