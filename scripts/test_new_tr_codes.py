#!/usr/bin/env python3
"""
새로운 TR 코드 테스트 스크립트
OPT10060(수급), OPT10014(프로그램매매), OPT10080(분봉) 테스트
"""
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.api.base_session import create_kiwoom_session
from src.collectors.supply_demand_collector import SupplyDemandCollector
from src.collectors.program_trading_collector_test import ProgramTradingCollector
from src.collectors.minute_data_collector import MinuteDataCollector
from src.api.tr_codes import show_tr_info, get_all_tr_codes


def test_tr_codes_info():
    """TR 코드 정보 테스트"""
    print("🔍 확장된 TR 코드 정보 확인")
    print("=" * 60)

    # 모든 TR 코드 목록 출력
    all_codes = get_all_tr_codes()
    print(f"📊 지원하는 TR 코드: {len(all_codes)}개")

    for code in all_codes:
        print(f"\n{'=' * 50}")
        show_tr_info(code)

    print(f"\n✅ TR 코드 정보 확인 완료!")


def test_kiwoom_session():
    """키움 세션 준비 테스트"""
    print("\n🔌 키움 세션 준비 테스트")
    print("=" * 60)

    try:
        session = create_kiwoom_session(auto_login=True, show_progress=True)

        if session and session.is_ready():
            print("✅ 키움 세션 준비 완료")
            return session
        else:
            print("❌ 키움 세션 준비 실패")
            return None

    except Exception as e:
        print(f"❌ 키움 세션 준비 실패: {e}")
        return None


def test_supply_demand_collector(session):
    """수급데이터 수집기 테스트"""
    print("\n📊 수급데이터 수집기 테스트")
    print("=" * 60)

    try:
        # 수집기 초기화
        collector = SupplyDemandCollector(session)
        print("✅ 수급데이터 수집기 초기화 완료")

        # 테스트 종목: 삼성전자
        test_code = "005930"
        print(f"🔄 테스트 종목: {test_code} (삼성전자)")

        # 수급데이터 수집 시도
        success, is_new = collector.collect_single_stock_supply_demand(test_code)

        if success:
            print(f"✅ {test_code} 수급데이터 수집 성공!")

            # 통계 출력
            stats = collector.get_collection_stats()
            print(f"📈 수집 통계: {stats}")

            return True
        else:
            print(f"❌ {test_code} 수급데이터 수집 실패")
            return False

    except Exception as e:
        print(f"❌ 수급데이터 수집기 테스트 실패: {e}")
        return False


def test_program_trading_collector(session):
    """프로그램매매 수집기 테스트"""
    print("\n📈 프로그램매매 수집기 테스트")
    print("=" * 60)

    try:
        # 수집기 초기화
        collector = ProgramTradingCollector(session)
        print("✅ 프로그램매매 수집기 초기화 완료")

        # 테스트 종목: 삼성전자
        test_code = "005930"
        print(f"🔄 테스트 종목: {test_code} (삼성전자)")

        # 프로그램매매 데이터 수집 시도
        success, is_new = collector.collect_single_stock_program_trading(test_code)

        if success:
            print(f"✅ {test_code} 프로그램매매 수집 성공!")

            # 통계 출력
            stats = collector.get_collection_stats()
            print(f"📈 수집 통계: {stats}")

            return True
        else:
            print(f"❌ {test_code} 프로그램매매 수집 실패")
            return False

    except Exception as e:
        print(f"❌ 프로그램매매 수집기 테스트 실패: {e}")
        return False


def test_minute_data_collector(session):
    """분봉데이터 수집기 테스트"""
    print("\n⏰ 분봉데이터 수집기 테스트")
    print("=" * 60)

    try:
        # 수집기 초기화
        collector = MinuteDataCollector(session)
        print("✅ 분봉데이터 수집기 초기화 완료")

        # 테스트 종목: 삼성전자
        test_code = "005930"
        minute_type = "3"  # 3분봉
        print(f"🔄 테스트 종목: {test_code} (삼성전자) - {minute_type}분봉")

        # 3분봉 데이터 수집 시도
        success, is_new = collector.collect_single_stock_minute_data(test_code, minute_type)

        if success:
            print(f"✅ {test_code} 3분봉 수집 성공!")

            # 통계 출력
            stats = collector.get_collection_stats()
            print(f"📈 수집 통계: {stats}")

            return True
        else:
            print(f"❌ {test_code} 3분봉 수집 실패")
            return False

    except Exception as e:
        print(f"❌ 분봉데이터 수집기 테스트 실패: {e}")
        return False


def test_multiple_stocks():
    """다중 종목 테스트"""
    print("\n🚀 다중 종목 테스트")
    print("=" * 60)

    # 테스트 종목 리스트 (대형주 5개)
    test_stocks = [
        "005930",  # 삼성전자
        "000660",  # SK하이닉스
        "035420",  # NAVER
        "005380",  # 현대차
        "051910"  # LG화학
    ]

    print(f"📋 테스트 종목: {len(test_stocks)}개")
    for i, code in enumerate(test_stocks, 1):
        print(f"   {i}. {code}")

    # 키움 세션 준비
    session = test_kiwoom_session()
    if not session:
        print("❌ 키움 세션 준비 실패로 다중 종목 테스트 중단")
        return False

    print(f"\n🔄 다중 종목 테스트 시작...")

    try:
        # 1. 수급데이터 배치 테스트
        print(f"\n1️⃣ 수급데이터 배치 수집 테스트")
        supply_collector = SupplyDemandCollector(session)
        supply_result = supply_collector.collect_batch_supply_demand(test_stocks[:3])  # 3개만

        if supply_result:
            print(f"✅ 수급데이터 배치 수집: {supply_result.get('success_rate', 0):.1f}% 성공")

        # 2. 프로그램매매 배치 테스트
        print(f"\n2️⃣ 프로그램매매 배치 수집 테스트")
        program_collector = ProgramTradingCollector(session)
        program_result = program_collector.collect_batch_program_trading(test_stocks[:3])  # 3개만

        if program_result:
            print(f"✅ 프로그램매매 배치 수집: {program_result.get('success_rate', 0):.1f}% 성공")

        # 3. 지정 종목 3분봉 테스트
        print(f"\n3️⃣ 지정 종목 3분봉 수집 테스트")
        minute_collector = MinuteDataCollector(session)
        minute_result = minute_collector.collect_designated_stocks_minute_data(test_stocks[:3], "3")

        if minute_result:
            print(f"✅ 3분봉 지정 종목 수집: {minute_result.get('success_rate', 0):.1f}% 성공")

        print(f"\n🎉 다중 종목 테스트 완료!")
        return True

    except Exception as e:
        print(f"❌ 다중 종목 테스트 실패: {e}")
        return False


def main():
    """메인 테스트 실행"""
    print("🚀 새로운 TR 코드 확장 기능 테스트")
    print("=" * 80)
    print("📋 테스트 항목:")
    print("   1️⃣ TR 코드 정보 확인")
    print("   2️⃣ 키움 세션 준비")
    print("   3️⃣ 수급데이터 수집기 (OPT10060)")
    print("   4️⃣ 프로그램매매 수집기 (OPT10014)")
    print("   5️⃣ 분봉데이터 수집기 (OPT10080)")
    print("   6️⃣ 다중 종목 배치 테스트")
    print("=" * 80)

    # 사용자 확인
    response = input("\n테스트를 시작하시겠습니까? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("❌ 테스트가 취소되었습니다.")
        return False

    test_results = {}

    try:
        # 1. TR 코드 정보 확인
        print(f"\n" + "=" * 80)
        test_tr_codes_info()
        test_results['tr_codes_info'] = True

        # 2. 키움 세션 준비
        print(f"\n" + "=" * 80)
        session = test_kiwoom_session()
        if session:
            test_results['kiwoom_session'] = True
        else:
            test_results['kiwoom_session'] = False
            print("❌ 키움 세션 없이는 이후 테스트 불가능")
            return False

        # 3. 수급데이터 수집기 테스트
        print(f"\n" + "=" * 80)
        supply_result = test_supply_demand_collector(session)
        test_results['supply_demand'] = supply_result

        # 4. 프로그램매매 수집기 테스트
        print(f"\n" + "=" * 80)
        program_result = test_program_trading_collector(session)
        test_results['program_trading'] = program_result

        # 5. 분봉데이터 수집기 테스트
        print(f"\n" + "=" * 80)
        minute_result = test_minute_data_collector(session)
        test_results['minute_data'] = minute_result

        # 6. 다중 종목 테스트 (선택적)
        print(f"\n" + "=" * 80)
        response = input("다중 종목 배치 테스트를 진행하시겠습니까? (시간이 오래 걸립니다) (y/N): ")
        if response.lower() in ['y', 'yes']:
            multi_result = test_multiple_stocks()
            test_results['multiple_stocks'] = multi_result
        else:
            print("⏭️ 다중 종목 테스트 건너뛰기")
            test_results['multiple_stocks'] = None

        # 최종 결과 요약
        print_final_results(test_results)

        return True

    except Exception as e:
        print(f"❌ 메인 테스트 실행 중 오류: {e}")
        return False


def print_final_results(test_results: dict):
    """최종 테스트 결과 출력"""
    print(f"\n" + "=" * 80)
    print("🎉 새로운 TR 코드 확장 기능 테스트 결과")
    print("=" * 80)

    total_tests = 0
    passed_tests = 0

    test_items = {
        'tr_codes_info': 'TR 코드 정보 확인',
        'kiwoom_session': '키움 세션 준비',
        'supply_demand': '수급데이터 수집기 (OPT10060)',
        'program_trading': '프로그램매매 수집기 (OPT10014)',
        'minute_data': '분봉데이터 수집기 (OPT10080)',
        'multiple_stocks': '다중 종목 배치 테스트'
    }

    for key, description in test_items.items():
        if key in test_results:
            result = test_results[key]
            total_tests += 1

            if result is True:
                status = "✅ 성공"
                passed_tests += 1
            elif result is False:
                status = "❌ 실패"
            else:
                status = "⏭️ 건너뜀"
                total_tests -= 1  # 건너뛴 항목은 카운트에서 제외

            print(f"   {description}: {status}")

    # 성공률 계산
    if total_tests > 0:
        success_rate = (passed_tests / total_tests) * 100
        print(f"\n📊 테스트 결과 요약:")
        print(f"   전체 테스트: {total_tests}개")
        print(f"   성공: {passed_tests}개")
        print(f"   실패: {total_tests - passed_tests}개")
        print(f"   성공률: {success_rate:.1f}%")

        if success_rate >= 80:
            print(f"\n🎉 우수! TR 코드 확장이 성공적으로 완료되었습니다!")
        elif success_rate >= 60:
            print(f"\n👍 양호! 일부 개선이 필요하지만 기본 기능은 작동합니다.")
        else:
            print(f"\n⚠️ 주의! 많은 기능에서 문제가 발생했습니다. 코드 점검이 필요합니다.")

    print(f"\n💡 다음 단계:")

    if test_results.get('supply_demand', False):
        print(f"   ✅ 수급데이터 수집 시스템 운영 가능")

    if test_results.get('program_trading', False):
        print(f"   ✅ 프로그램매매 수집 시스템 운영 가능")

    if test_results.get('minute_data', False):
        print(f"   ✅ 지정 종목 분봉 수집 시스템 운영 가능")

    print(f"   🔄 기존 일봉 시스템과 통합하여 완전한 데이터 수집 시스템 구축")
    print(f"   🌐 웹 UI 또는 대시보드 개발로 사용자 편의성 향상")
    print(f"   📊 실시간 데이터 업데이트 스케줄링 시스템 구축")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n⚠️ 사용자에 의해 테스트가 중단되었습니다.")
    except Exception as e:
        print(f"\n\n❌ 예상치 못한 오류가 발생했습니다: {e}")
        import traceback

        traceback.print_exc()
    finally:
        print(f"\n👋 테스트를 종료합니다.")