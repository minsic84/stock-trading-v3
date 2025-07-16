#!/usr/bin/env python3
"""
수급 데이터 수집 스크립트 - 스마트 재시작 지원

스마트 재시작 기능:
- 중단된 지점부터 자동으로 이어서 수집
- 오늘 날짜 기준으로 미완료 종목만 필터링
- 전체 진행률 및 예상 시간 표시
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.supply_demand_database import SupplyDemandDatabaseService
from src.collectors.supply_demand_new_collector import SupplyDemandNewCollector
from src.api.base_session import create_kiwoom_session


def setup_logging():
    """로깅 설정"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"supply_demand_{datetime.now().strftime('%Y%m%d')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def print_header():
    """헤더 출력"""
    print("🚀 수급 데이터 수집 시스템 (스마트 재시작 지원)")
    print("=" * 60)
    print(f"⏰ 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 TR 코드: OPT10060 (일별수급데이터요청)")
    print("📊 대상: stock_codes 테이블의 모든 활성 종목")
    print("=" * 60)


def check_system_status() -> bool:
    """시스템 상태 확인"""
    try:
        print("\n🔍 시스템 상태 확인")
        print("-" * 40)

        db_service = SupplyDemandDatabaseService()

        # DB 연결 테스트
        if not db_service.test_connection():
            print("❌ 데이터베이스 연결 실패")
            return False

        # 스키마 생성
        if not db_service.create_schema_if_not_exists():
            print("❌ 스키마 생성 실패")
            return False

        # 종목 수 확인
        all_stocks = db_service.get_all_stock_codes()
        print(f"📋 활성 종목: {len(all_stocks)}개")

        if len(all_stocks) == 0:
            print("❌ 활성 종목이 없습니다")
            return False

        print("✅ 시스템 상태 정상")
        return True

    except Exception as e:
        print(f"❌ 시스템 상태 확인 실패: {e}")
        return False


def run_supply_demand_collection(force_update: bool = False, specific_codes: list = None) -> bool:
    """수급 데이터 수집 실행"""
    try:
        print(f"\n🚀 수급 데이터 수집 시작")
        print("-" * 40)

        # 데이터베이스 서비스 초기화
        db_service = SupplyDemandDatabaseService()

        # 특정 종목 수집인 경우
        if specific_codes:
            print(f"🎯 특정 종목 수집: {len(specific_codes)}개")
            target_stocks = []
            for code in specific_codes:
                target_stocks.append({'code': code, 'name': f'종목{code}', 'market': 'UNKNOWN'})

        else:
            # 🎯 스마트 재시작: 미완료 지점부터 시작
            print("🔍 수집 대상 분석 중...")
            target_stocks = db_service.get_stocks_smart_restart(
                force_update=force_update,
                target_date=datetime.now().strftime('%Y%m%d')
            )

            if not target_stocks:
                print("✅ 모든 종목이 이미 완료되었습니다!")
                return True

            # 전체 통계 정보 조회
            _, total_count, completed_count = db_service.find_supply_demand_restart_position()

            print("📊 수집 계획:")
            print(f"   📈 전체 활성 종목: {total_count}개")
            print(f"   ✅ 이미 완료: {completed_count}개 ({completed_count / total_count * 100:.1f}%)")
            print(f"   🔄 수집 대상: {len(target_stocks)}개")
            print(f"   📍 시작 종목: {target_stocks[0]['code'] if target_stocks else 'N/A'}")
            print(f"   ⏱️ 예상 소요시간: {len(target_stocks) * 3.6 / 60:.1f}분")

        if force_update:
            print("🔄 강제 업데이트 모드: 전체 종목 재수집")
        elif not specific_codes:
            today = datetime.now().strftime('%Y%m%d')
            print(f"🎯 스마트 재시작 모드: {today} 날짜 기준 미완료 종목만 수집")

        # 키움 API 연결
        print("\n🔌 키움 API 연결 중...")
        session = create_kiwoom_session(auto_login=True, show_progress=True)

        if not session or not session.is_ready():
            print("❌ 키움 API 연결 실패")
            return False

        print("✅ 키움 API 연결 완료")

        # 수집기 초기화
        collector = SupplyDemandNewCollector(session)

        # 개별 종목 수집
        print(f"\n📊 개별 종목 수집 시작")
        print("-" * 60)

        success_count = 0
        failed_count = 0

        for i, stock_info in enumerate(target_stocks, 1):
            stock_code = stock_info['code']

            try:
                # 현재 진행상황 표시
                if not specific_codes:
                    current_position = completed_count + i
                    overall_progress = current_position / total_count * 100
                    batch_progress = i / len(target_stocks) * 100

                    print(f"\n[전체: {current_position}/{total_count} ({overall_progress:.1f}%)] " +
                          f"[배치: {i}/{len(target_stocks)} ({batch_progress:.1f}%)] {stock_code}")
                else:
                    print(f"\n[{i}/{len(target_stocks)}] {stock_code}")

                # 종목 수집 실행
                result = collector.collect_single_stock(stock_code, force_full=force_update)

                if result.get('success', False):
                    saved_records = result.get('saved_records', 0)
                    print(f"✅ {stock_code} 완료: {saved_records}건 저장")
                    success_count += 1
                else:
                    error_msg = result.get('error', '알 수 없는 오류')
                    print(f"❌ {stock_code} 실패: {error_msg}")
                    failed_count += 1

                # 중간 통계 출력 (100개마다)
                if i % 100 == 0:
                    print(f"\n📊 중간 통계 ({i}/{len(target_stocks)}):")
                    print(f"   ✅ 성공: {success_count}개")
                    print(f"   ❌ 실패: {failed_count}개")
                    print(f"   📈 성공률: {success_count / (success_count + failed_count) * 100:.1f}%" if (
                                                                                                                  success_count + failed_count) > 0 else "   📈 성공률: 0%")

                # API 제한 준수
                if i < len(target_stocks):
                    import time
                    time.sleep(0.5)  # 수급 데이터는 0.5초 간격

            except KeyboardInterrupt:
                print(f"\n⚠️ 사용자 중단 요청 (Ctrl+C)")
                print(f"📊 중단 시점: {stock_code} ({i}/{len(target_stocks)})")
                print("💡 다시 실행하면 이 지점부터 이어서 수집됩니다.")
                break

            except Exception as e:
                print(f"❌ [{i}/{len(target_stocks)}] {stock_code} 오류: {e}")
                failed_count += 1

        # 최종 결과 출력
        print("\n" + "=" * 60)
        print("🎉 수급 데이터 수집 완료!")
        print("=" * 60)

        print("📊 이번 배치 결과:")
        print(f"   🎯 수집 대상: {len(target_stocks)}개")
        print(f"   ✅ 성공: {success_count}개")
        print(f"   ❌ 실패: {failed_count}개")
        print(f"   📈 성공률: {success_count / (success_count + failed_count) * 100:.1f}%" if (
                                                                                                      success_count + failed_count) > 0 else "   📈 성공률: 0%")

        if not specific_codes:
            final_completed = completed_count + success_count
            print(f"\n📊 전체 진행상황:")
            print(f"   📈 전체 활성 종목: {total_count}개")
            print(f"   ✅ 완료된 종목: {final_completed}개")
            print(f"   📊 전체 진행률: {final_completed / total_count * 100:.1f}%")

            remaining = total_count - final_completed
            if remaining > 0:
                print(f"   🔄 남은 종목: {remaining}개")
                print(f"   ⏱️ 예상 추가 시간: {remaining * 3.6 / 60:.1f}분")
                print("\n💡 다음에 실행하면 남은 종목부터 이어서 수집됩니다.")
            else:
                print("\n🎉 모든 종목의 수급 데이터 수집이 완료되었습니다!")

        return True

    except Exception as e:
        print(f"❌ 수집 실행 중 오류: {e}")
        return False


def main():
    """메인 실행 함수 - 스마트 재시작 기능 추가"""
    # 명령행 인수 파싱
    parser = argparse.ArgumentParser(description='수급 데이터 수집 (스마트 재시작 지원)')
    parser.add_argument('--force-full', action='store_true',
                        help='강제 전체 수집 (모든 종목)')
    parser.add_argument('--restart-analysis', action='store_true',
                        help='재시작 분석 (실행하지 않고 분석만)')
    parser.add_argument('--codes', type=str,
                        help='특정 종목 코드들 (쉼표 구분)')
    parser.add_argument('--date', type=str,
                        help='기준 날짜 (YYYYMMDD, 기본값: 오늘)')
    parser.add_argument('--no-log', action='store_true',
                        help='로그 파일 생성 안함')

    args = parser.parse_args()

    # 로깅 설정
    if not args.no_log:
        setup_logging()

    # 헤더 출력
    print_header()

    try:
        # 1. 재시작 분석만 수행
        if args.restart_analysis:
            print("\n🔍 수급 데이터 재시작 분석 수행 중...")
            db_service = SupplyDemandDatabaseService()
            db_service.show_supply_demand_restart_analysis(args.date)
            return

        # 2. 시스템 상태 확인
        if not check_system_status():
            print("\n❌ 시스템 상태 확인 실패로 종료")
            sys.exit(1)

        # 3. 특정 종목 수집
        if args.codes:
            stock_codes = [code.strip() for code in args.codes.split(',')]
            print(f"\n🎯 특정 종목 수집: {stock_codes}")

            success = run_supply_demand_collection(
                force_update=True,
                specific_codes=stock_codes
            )

            if success:
                print("\n✅ 특정 종목 수집 완료!")
            else:
                print("\n❌ 특정 종목 수집 실패")
                sys.exit(1)
            return

        # 4. 🎯 스마트 재시작으로 전체 수집 실행
        print(f"\n🚀 스마트 재시작으로 수급 데이터 수집 시작")
        print(f"🗓️ 기준 날짜: {args.date or datetime.now().strftime('%Y%m%d')}")

        if args.force_full:
            print("🔄 강제 모드: 모든 종목 재수집")
        else:
            print("🎯 스마트 모드: 미완료 종목만 수집")

        success = run_supply_demand_collection(force_update=args.force_full)

        # 5. 결과에 따른 종료
        if success:
            print("\n🎉 수급 데이터 수집 성공적으로 완료!")
            logging.info("수급 데이터 수집 성공")
        else:
            print("\n❌ 수급 데이터 수집 실패")
            logging.error("수급 데이터 수집 실패")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다")
        print("💡 다시 실행하면 중단된 지점부터 이어서 수집됩니다.")
        logging.warning("사용자 중단")
        sys.exit(1)

    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        logging.error(f"예상치 못한 오류: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()