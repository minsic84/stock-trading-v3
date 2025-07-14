#!/usr/bin/env python3
"""
간결한 분봉데이터 테스트 스크립트
scripts/test_minute_data.py
"""
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_minute_data():
    """분봉데이터 테스트 실행"""
    print("🚀 분봉데이터 수집기 테스트")
    print("=" * 50)

    try:
        # 키움 세션 생성
        from src.api.base_session import create_kiwoom_session

        print("🔌 키움 세션 준비 중...")
        session = create_kiwoom_session(auto_login=True, show_progress=True)

        if not session or not session.is_ready():
            print("❌ 키움 세션 준비 실패")
            return False

        print("✅ 키움 세션 준비 완료")

        # 분봉데이터 수집기 테스트
        from src.collectors.minute_data_collector import MinuteDataCollector

        print("🔧 분봉데이터 수집기 초기화...")
        collector = MinuteDataCollector(session)

        # 테스트 종목: 삼성전자
        test_stock = "005930"
        minute_type = "3"  # 3분봉
        print(f"📊 테스트 종목: {test_stock} (삼성전자) - {minute_type}분봉")

        # 분봉데이터 수집 테스트
        print("🔄 분봉데이터 수집 시작...")
        success, is_new = collector.collect_single_stock_minute_data(test_stock, minute_type)

        if success:
            print("✅ 분봉데이터 수집 성공!")

            # 통계 출력
            stats = collector.get_collection_stats()
            print(f"📈 수집 통계:")
            print(f"   성공: {stats['collected_count']}개")
            print(f"   오류: {stats['error_count']}개")
            print(f"   TR 코드: {stats['tr_code']}")
            print(f"   지원 분봉: {list(stats.get('supported_minute_types', {}).keys())}")

            return True
        else:
            print("❌ 분봉데이터 수집 실패")
            return False

    except Exception as e:
        print(f"❌ 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multiple_minute_types():
    """다중 분봉 타입 테스트"""
    print("\n🚀 다중 분봉 타입 테스트")
    print("=" * 50)

    try:
        from src.api.base_session import create_kiwoom_session
        from src.collectors.minute_data_collector import MinuteDataCollector

        session = create_kiwoom_session()
        if not session:
            return False

        collector = MinuteDataCollector(session)

        # 테스트할 분봉 타입들
        minute_types = ["1", "3", "5"]
        test_stock = "005930"

        print(f"📊 테스트: {minute_types} 분봉들")

        for minute_type in minute_types:
            print(f"\n⏰ {minute_type}분봉 테스트 중...")
            success, _ = collector.collect_single_stock_minute_data(test_stock, minute_type)

            if success:
                print(f"✅ {minute_type}분봉 성공")
            else:
                print(f"❌ {minute_type}분봉 실패")
                break

        # 최종 통계
        stats = collector.get_collection_stats()
        print(f"\n📊 전체 통계: {stats}")

        return True

    except Exception as e:
        print(f"❌ 다중 분봉 테스트 실패: {e}")
        return False


def main():
    """메인 실행 함수"""
    print("🧪 분봉데이터 테스트 스크립트 (OPT10080)")
    print("=" * 60)

    print("선택하세요:")
    print("1. 단일 분봉 테스트 (3분봉)")
    print("2. 다중 분봉 타입 테스트 (1분, 3분, 5분)")
    print("3. 종료")

    choice = input("\n선택 (1-3): ").strip()

    if choice == "1":
        success = test_minute_data()

        if success:
            print("\n🎉 단일 분봉 테스트 완료!")
            print("💡 실제 필드 구조를 확인할 수 있습니다.")
        else:
            print("\n⚠️ 테스트 실패. 로그를 확인하여 문제를 해결하세요.")

    elif choice == "2":
        success = test_multiple_minute_types()

        if success:
            print("\n🎉 다중 분봉 테스트 완료!")
            print("💡 다양한 분봉 타입의 데이터 구조를 확인했습니다.")
        else:
            print("\n⚠️ 다중 분봉 테스트 실패.")

    elif choice == "3":
        print("👋 테스트를 종료합니다.")
        return

    else:
        print("❌ 잘못된 선택입니다.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
    finally:
        print("\n👋 테스트를 종료합니다.")