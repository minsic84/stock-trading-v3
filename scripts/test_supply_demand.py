#!/usr/bin/env python3
"""
간결한 수급데이터 테스트 스크립트
scripts/test_supply_demand.py
"""
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_supply_demand():
    """수급데이터 테스트 실행"""
    print("🚀 수급데이터 수집기 테스트")
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

        # 수급데이터 수집기 테스트
        from src.collectors.supply_demand_collector import SupplyDemandNewCollector as SupplyDemandCollector

        print("🔧 수급데이터 수집기 초기화...")
        collector = SupplyDemandCollector(session)

        # 테스트 종목: 삼성전자
        test_stock = "005930"
        print(f"📊 테스트 종목: {test_stock} (삼성전자)")

        # 수급데이터 수집 테스트
        print("🔄 수급데이터 수집 시작...")
        success, is_new = collector.collect_single_stock_supply_demand(test_stock)

        if success:
            print("✅ 수급데이터 수집 성공!")

            # 통계 출력
            stats = collector.get_collection_stats()
            print(f"📈 수집 통계:")
            print(f"   성공: {stats['collected_count']}개")
            print(f"   오류: {stats['error_count']}개")
            print(f"   TR 코드: {stats['tr_code']}")

            return True
        else:
            print("❌ 수급데이터 수집 실패")
            return False

    except Exception as e:
        print(f"❌ 테스트 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """메인 실행 함수"""
    print("🧪 OPT10060 수급데이터 테스트 스크립트")
    print("=" * 60)

    # 사용자 확인
    response = input("수급데이터 테스트를 시작하시겠습니까? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("❌ 테스트가 취소되었습니다.")
        return

    # 테스트 실행
    success = test_supply_demand()

    if success:
        print("\n🎉 수급데이터 테스트 완료!")
        print("💡 이제 실제 필드 구조를 확인할 수 있습니다.")
    else:
        print("\n⚠️ 테스트 실패. 로그를 확인하여 문제를 해결하세요.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
    finally:
        print("\n👋 테스트를 종료합니다.")