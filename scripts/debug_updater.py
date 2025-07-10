#!/usr/bin/env python3
"""
디버깅용 간단 업데이터
문제 원인을 찾기 위한 단계별 실행
"""

import sys
import argparse
from pathlib import Path

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print("🔍 디버깅 시작...")
print("=" * 50)


def test_basic_functionality():
    """기본 기능 테스트"""
    print("1. 기본 import 테스트...")

    try:
        from src.core.config import Config
        print("   ✅ Config import 성공")

        from src.core.database import get_database_service
        print("   ✅ Database import 성공")

        from rich.console import Console
        print("   ✅ Rich import 성공")

        return True

    except Exception as e:
        print(f"   ❌ Import 실패: {e}")
        return False


def test_database_connection():
    """데이터베이스 연결 테스트"""
    print("\n2. 데이터베이스 연결 테스트...")

    try:
        from src.core.database import get_database_service

        db_service = get_database_service()
        print("   ✅ DB 서비스 생성 성공")

        if db_service.test_connection():
            print("   ✅ DB 연결 성공")
            return True
        else:
            print("   ❌ DB 연결 실패")
            return False

    except Exception as e:
        print(f"   ❌ DB 연결 오류: {e}")
        return False


def test_stock_code_query(stock_code):
    """종목 코드 조회 테스트"""
    print(f"\n3. 종목 코드 조회 테스트: {stock_code}")

    try:
        # MySQL 직접 연결
        import mysql.connector

        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'stock_user',
            'password': 'StockPass2025!',
            'database': 'stock_trading_db',
            'charset': 'utf8mb4'
        }

        conn = mysql.connector.connect(**config)
        cursor = conn.cursor(dictionary=True)

        # 원본 코드로 조회
        query1 = "SELECT * FROM stocks WHERE code = %s"
        cursor.execute(query1, (stock_code,))
        result1 = cursor.fetchall()
        print(f"   📊 원본 코드 '{stock_code}' 조회 결과: {len(result1)}개")

        # _AL 추가한 코드로 조회
        al_code = f"{stock_code}_AL" if not stock_code.endswith('_AL') else stock_code
        query2 = "SELECT * FROM stocks WHERE code = %s"
        cursor.execute(query2, (al_code,))
        result2 = cursor.fetchall()
        print(f"   📊 _AL 코드 '{al_code}' 조회 결과: {len(result2)}개")

        # LIKE 패턴으로 조회
        query3 = "SELECT * FROM stocks WHERE code LIKE %s"
        cursor.execute(query3, (f"{stock_code}%",))
        result3 = cursor.fetchall()
        print(f"   📊 LIKE 패턴 '{stock_code}%' 조회 결과: {len(result3)}개")

        # 전체 _AL 종목 수
        query4 = "SELECT COUNT(*) as cnt FROM stocks WHERE code LIKE '%_AL'"
        cursor.execute(query4)
        result4 = cursor.fetchone()
        total_al = result4['cnt'] if result4 else 0
        print(f"   📊 전체 _AL 종목 수: {total_al}개")

        # 샘플 종목들 확인
        query5 = "SELECT code, name FROM stocks WHERE code LIKE '005930%' OR code LIKE '%_AL' LIMIT 10"
        cursor.execute(query5)
        samples = cursor.fetchall()
        print(f"   📋 샘플 종목들:")
        for sample in samples:
            print(f"      {sample['code']} - {sample['name']}")

        conn.close()

        if result1 or result2 or result3:
            print("   ✅ 종목 조회 성공")
            if result2:
                stock_info = result2[0]
                print(f"   📋 종목 정보: {stock_info['name']} ({stock_info['market']})")
            elif result1:
                stock_info = result1[0]
                print(f"   📋 종목 정보: {stock_info['name']} ({stock_info['market']})")
            return True
        else:
            print("   ❌ 종목을 찾을 수 없습니다")
            print(f"   💡 힌트: DB에 '{stock_code}' 또는 '{al_code}' 형식의 종목이 없을 수 있습니다")
            return False

    except Exception as e:
        print(f"   ❌ 종목 조회 오류: {e}")
        return False


def test_argument_parsing():
    """인수 파싱 테스트"""
    print(f"\n4. 인수 파싱 테스트...")

    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--codes", nargs="+")
        parser.add_argument("--date")
        parser.add_argument("--manual-edit", action="store_true")

        # 테스트 인수
        test_args = ["--codes", "005930", "--date", "2025-07-08", "--manual-edit"]
        args = parser.parse_args(test_args)

        print(f"   📋 codes: {args.codes}")
        print(f"   📋 date: {args.date}")
        print(f"   📋 manual_edit: {args.manual_edit}")
        print("   ✅ 인수 파싱 성공")

        return True

    except Exception as e:
        print(f"   ❌ 인수 파싱 오류: {e}")
        return False


def test_kiwoom_connection():
    """키움 API 연결 테스트"""
    print(f"\n5. 키움 API 연결 테스트...")

    try:
        from src.api.base_session import create_kiwoom_session

        session = create_kiwoom_session()
        print("   ✅ 키움 세션 생성 성공")

        # 연결 시도 (실제로는 GUI 팝업이 나타남)
        print("   ⏳ 키움 API 연결 시도... (로그인 팝업 확인)")

        if session.connect():
            print("   ✅ 키움 API 연결 성공")
            session.disconnect()
            return True
        else:
            print("   ❌ 키움 API 연결 실패")
            return False

    except Exception as e:
        print(f"   ❌ 키움 API 오류: {e}")
        return False


def main():
    """메인 디버깅 함수"""
    parser = argparse.ArgumentParser(description="디버깅용 테스트")
    parser.add_argument("--codes", nargs="+", help="테스트할 종목 코드")
    parser.add_argument("--skip-kiwoom", action="store_true", help="키움 API 테스트 건너뛰기")

    args = parser.parse_args()

    print("🔍 날짜 지정 업데이터 디버깅")
    print("=" * 50)

    # 기본 종목 코드
    test_codes = args.codes if args.codes else ["005930"]

    # 단계별 테스트
    tests = [
        ("기본 기능", test_basic_functionality),
        ("데이터베이스 연결", test_database_connection),
        ("종목 코드 조회", lambda: test_stock_code_query(test_codes[0])),
        ("인수 파싱", test_argument_parsing),
    ]

    if not args.skip_kiwoom:
        tests.append(("키움 API 연결", test_kiwoom_connection))

    # 테스트 실행
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"   ❌ {test_name} 테스트 중 예외: {e}")
            results.append((test_name, False))

    # 최종 결과
    print("\n" + "=" * 50)
    print("📋 디버깅 결과 요약:")

    for test_name, result in results:
        status = "✅ 성공" if result else "❌ 실패"
        print(f"   {test_name}: {status}")

    failed_tests = [name for name, result in results if not result]

    if failed_tests:
        print(f"\n❌ 실패한 테스트: {', '.join(failed_tests)}")
        print("💡 이 부분들을 먼저 해결해야 합니다!")
    else:
        print("\n🎉 모든 테스트 통과!")
        print("💡 기본 기능은 정상입니다. 다른 문제를 찾아봅시다.")

    return len(failed_tests) == 0


if __name__ == "__main__":
    main()