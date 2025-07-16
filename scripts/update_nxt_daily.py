#!/usr/bin/env python3
"""
파일 경로: scripts/update_nxt_daily.py

NXT 일일 업데이트 스크립트
- 간단한 실행용 스크립트
- 매일 자동 실행 가능
- 로깅 및 에러 처리 포함
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.collectors.nxt_daily_price_collector import NXTDailyPriceCollector
from src.core.nxt_database import NXTDatabaseService


def setup_logging():
    """로깅 설정"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"nxt_daily_update_{datetime.now().strftime('%Y%m%d')}.log"

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
    print("🚀 NXT 일일 업데이트 시스템")
    print("=" * 60)
    print(f"⏰ 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🎯 대상: stock_codes 테이블의 NXT 종목")
    print("📊 방식: 600개 요청으로 최신 데이터 교체")
    print("=" * 60)


def check_system_status() -> bool:
    """시스템 상태 확인"""
    try:
        print("\n🔍 시스템 상태 확인")
        print("-" * 40)

        nxt_db = NXTDatabaseService()

        # DB 연결 테스트
        if not nxt_db.test_connection():
            print("❌ 데이터베이스 연결 실패")
            return False

        # NXT 종목 통계
        stats = nxt_db.get_nxt_statistics()
        print(f"📋 NXT 종목: {stats.get('active_stocks', 0)}개")

        # 수집 현황
        status = nxt_db.get_nxt_collection_status()
        print(f"📊 현재 완료율: {status.get('completion_rate', 0)}%")
        print(f"🔄 업데이트 필요: {status.get('need_update', 0)}개")

        if stats.get('active_stocks', 0) == 0:
            print("❌ NXT 종목이 없습니다")
            return False

        print("✅ 시스템 상태 정상")
        return True

    except Exception as e:
        print(f"❌ 시스템 상태 확인 실패: {e}")
        return False


def run_daily_update(force_update: bool = False) -> bool:
    """일일 업데이트 실행"""
    try:
        print(f"\n🚀 일일 업데이트 시작 (강제모드: {force_update})")
        print("-" * 40)

        # 수집기 생성
        collector = NXTDailyPriceCollector()

        # 전체 수집 실행
        result = collector.collect_all_nxt_stocks(force_update=force_update)

        if 'error' in result:
            print(f"❌ 업데이트 실패: {result['error']}")
            return False

        print("✅ 일일 업데이트 완료")
        return True

    except Exception as e:
        print(f"❌ 업데이트 실행 중 오류: {e}")
        logging.error(f"업데이트 실행 오류: {e}")
        return False


def print_final_status():
    """최종 상태 출력"""
    try:
        print("\n📊 업데이트 후 최종 상태")
        print("-" * 40)

        nxt_db = NXTDatabaseService()
        status = nxt_db.get_nxt_collection_status()

        print(f"📈 완료율: {status.get('completion_rate', 0)}%")
        print(f"✅ 완료 종목: {status.get('completed_stocks', 0)}개")
        print(f"🔄 업데이트 필요: {status.get('need_update', 0)}개")
        print(f"📀 총 레코드: {status.get('total_records', 0):,}개")

    except Exception as e:
        print(f"❌ 최종 상태 확인 실패: {e}")


def main():
    """메인 실행 함수 - 스마트 재시작 기능 추가"""
    import argparse

    # 명령행 인수 파싱
    parser = argparse.ArgumentParser(description='NXT 일일 업데이트 (스마트 재시작 지원)')
    parser.add_argument('--force', action='store_true',
                        help='강제 업데이트 (모든 종목)')
    parser.add_argument('--status-only', action='store_true',
                        help='상태 확인만 수행')
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
            print("\n🔍 NXT 재시작 분석 수행 중...")
            nxt_db = NXTDatabaseService()
            nxt_db.show_restart_analysis(args.date)
            return

        # 2. 시스템 상태 확인
        if not check_system_status():
            print("\n❌ 시스템 상태 확인 실패로 종료")
            sys.exit(1)

        # 3. 상태 확인만 하는 경우
        if args.status_only:
            print("\n✅ 상태 확인 완료 (업데이트 미실행)")
            return

        # 4. 특정 종목 수집
        if args.codes:
            stock_codes = [code.strip() for code in args.codes.split(',')]
            print(f"\n🎯 특정 종목 수집: {stock_codes}")

            collector = NXTDailyPriceCollector()
            result = collector.collect_specific_stocks(stock_codes)

            if 'error' in result:
                print(f"\n❌ 특정 종목 수집 실패: {result['error']}")
                sys.exit(1)
            else:
                print("\n✅ 특정 종목 수집 완료!")
                return

        # 5. 🎯 스마트 재시작으로 전체 업데이트 실행
        print(f"\n🚀 스마트 재시작으로 NXT 업데이트 시작")
        print(f"🗓️ 기준 날짜: {args.date or datetime.now().strftime('%Y%m%d')}")

        if args.force:
            print("🔄 강제 모드: 모든 종목 재수집")
        else:
            print("🎯 스마트 모드: 미완료 종목만 수집")

        success = run_daily_update(force_update=args.force)

        # 6. 최종 상태 출력
        print_final_status()

        # 7. 결과에 따른 종료
        if success:
            print("\n🎉 NXT 일일 업데이트 성공적으로 완료!")
            logging.info("NXT 일일 업데이트 성공")
        else:
            print("\n❌ NXT 일일 업데이트 실패")
            logging.error("NXT 일일 업데이트 실패")
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