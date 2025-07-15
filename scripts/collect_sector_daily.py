#!/usr/bin/env python3
"""
파일 경로: scripts/collect_sector_daily.py

업종 일봉 데이터 수집 실행 스크립트
- KOSPI(001), KOSDAQ(101) 종합지수 수집
- OPT20006 TR 코드 사용
- 5년치 데이터 수집 지원
- 프로젝트 표준 패턴 준수
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.api.tr_codes import get_tr_info, show_tr_info
from src.collectors.sector_daily_collector import SectorDailyCollector
from src.core.sector_database import get_sector_database_service
from src.api.base_session import create_kiwoom_session

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def show_current_status():
    """현재 업종 데이터 상태 출력"""
    try:
        print("📊 현재 업종 데이터 상태")
        print("=" * 60)

        db_service = get_sector_database_service()

        # 연결 테스트
        if not db_service.test_connection():
            print("❌ 데이터베이스 연결 실패")
            return False

        # 전체 통계
        stats = db_service.get_sector_statistics()
        print(f"📈 총 업종 수: {stats['total_sectors']}개")
        print()

        # 업종별 상세 정보
        for sector_code, info in stats['sectors'].items():
            completeness = db_service.get_data_completeness(sector_code)

            print(f"🏛️ {info['name']} ({sector_code})")
            print(f"   📊 레코드 수: {info['records']:,}개")
            print(f"   📈 완성도: {completeness['completion_rate']:.1f}%")
            print(f"   📅 최신 날짜: {info['latest_date'] or '없음'}")
            print(f"   🎯 수집 모드: {completeness['collection_mode']}")
            print()

        return True

    except Exception as e:
        print(f"❌ 상태 확인 실패: {e}")
        return False


def show_tr_info_detail():
    """OPT20006 TR 정보 출력"""
    try:
        print("🔍 OPT20006 TR 코드 정보")
        print("=" * 60)

        show_tr_info('opt20006')

        print("\n📋 업종 코드 매핑:")
        print("   001: KOSPI 종합지수")
        print("   101: KOSDAQ 종합지수")

        print("\n📊 예상 수집량:")
        print("   5년치 데이터: 약 1,250개 레코드 (업종당)")
        print("   API 요청 수: 약 10-15회 (업종당)")
        print("   예상 소요시간: 약 3-5분")

    except Exception as e:
        print(f"❌ TR 정보 출력 실패: {e}")


def run_collection(force_full: bool = False, test_mode: bool = False):
    """업종 일봉 수집 실행"""
    try:
        print("🚀 업종 일봉 데이터 수집 시작")
        print("=" * 60)
        print(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"강제 전체 수집: {'예' if force_full else '아니오'}")
        print(f"테스트 모드: {'예' if test_mode else '아니오'}")
        print()

        # 1. TR 정보 확인
        print("1️⃣ TR 코드 정보 확인")
        try:
            tr_info = get_tr_info('opt20006')
            print(f"   ✅ {tr_info['name']} 확인 완료")
        except Exception as e:
            print(f"   ❌ TR 정보 확인 실패: {e}")
            return False

        # 2. 데이터베이스 서비스 초기화
        print("\n2️⃣ 데이터베이스 서비스 초기화")
        db_service = get_sector_database_service()

        if not db_service.test_connection():
            print("   ❌ 데이터베이스 연결 실패")
            return False
        print("   ✅ 데이터베이스 연결 성공")

        if not db_service.create_schema_if_not_exists():
            print("   ❌ 스키마 생성 실패")
            return False
        print("   ✅ 스키마 준비 완료")

        # 테스트 모드인 경우 여기서 종료
        if test_mode:
            print("\n🧪 테스트 모드 완료 - 실제 수집은 하지 않습니다")
            return True

        # 3. 키움 세션 초기화
        print("\n3️⃣ 키움 API 세션 초기화")
        session = create_kiwoom_session(auto_login=True, show_progress=True)

        if not session or not session.is_ready():
            print("   ❌ 키움 세션 준비 실패")
            return False
        print("   ✅ 키움 세션 준비 완료")

        # 4. 수집기 초기화
        print("\n4️⃣ 업종 일봉 수집기 초기화")
        collector = SectorDailyCollector(session)
        print("   ✅ 수집기 초기화 완료")

        # 5. 수집 실행
        print("\n5️⃣ 업종 데이터 수집 실행")
        print("-" * 40)

        result = collector.collect_all_sectors(force_full=force_full)

        # 6. 결과 출력
        print("\n6️⃣ 수집 결과")
        print("=" * 60)

        if result['success']:
            print("🎉 업종 일봉 수집 완료!")
            print(f"📊 전체 업종: {result['total_sectors']}개")
            print(f"✅ 성공: {result['completed_sectors']}개")
            print(f"❌ 실패: {result['failed_sectors']}개")
            print(f"📈 총 레코드: {result['total_records']:,}개")
            print(f"⏱️ 소요시간: {result['elapsed_time']:.1f}초")

            # 업종별 상세 결과
            print("\n📋 업종별 수집 결과:")
            for sector_code, detail in result['sectors_detail'].items():
                status = "✅" if detail['success'] else "❌"
                print(f"   {status} {detail['name']} ({sector_code}): {detail['records']:,}개")

            return True

        else:
            print("❌ 업종 일봉 수집 실패")
            print(f"오류: {result.get('error', '알 수 없는 오류')}")
            return False

    except KeyboardInterrupt:
        print("\n⚠️ 사용자에 의해 중단되었습니다")
        return False
    except Exception as e:
        print(f"\n❌ 수집 실행 실패: {e}")
        logger.exception("수집 실행 중 오류 발생")
        return False


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description='업종 일봉 데이터 수집 스크립트',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python collect_sector_daily.py              # 일반 수집 (자동 모드)
  python collect_sector_daily.py --force      # 강제 전체 수집
  python collect_sector_daily.py --status     # 현재 상태만 확인
  python collect_sector_daily.py --test       # 테스트 모드
  python collect_sector_daily.py --tr-info    # TR 정보 출력
        """
    )

    parser.add_argument('--force', action='store_true',
                        help='강제 전체 수집 (완성도 무시)')
    parser.add_argument('--status', action='store_true',
                        help='현재 상태만 확인')
    parser.add_argument('--test', action='store_true',
                        help='테스트 모드 (키움 연결 없이)')
    parser.add_argument('--tr-info', action='store_true',
                        help='OPT20006 TR 정보 출력')

    args = parser.parse_args()

    try:
        print("🏛️ 업종 일봉 데이터 수집 시스템")
        print("=" * 60)
        print("대상: KOSPI(001), KOSDAQ(101) 종합지수")
        print("TR 코드: OPT20006 (업종별지수요청)")
        print("데이터: 5년치 일봉 데이터")
        print()

        if args.tr_info:
            # TR 정보 출력
            show_tr_info_detail()

        elif args.status:
            # 상태 확인만
            success = show_current_status()
            sys.exit(0 if success else 1)

        elif args.test:
            # 테스트 모드
            success = run_collection(force_full=args.force, test_mode=True)
            sys.exit(0 if success else 1)

        else:
            # 실제 수집 실행
            success = run_collection(force_full=args.force)

            if success:
                print("\n🎯 수집 완료 후 상태 확인:")
                show_current_status()
                sys.exit(0)
            else:
                sys.exit(1)

    except KeyboardInterrupt:
        print("\n⚠️ 프로그램이 중단되었습니다")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 실행 실패: {e}")
        logger.exception("메인 실행 중 오류 발생")
        sys.exit(1)


if __name__ == "__main__":
    main()