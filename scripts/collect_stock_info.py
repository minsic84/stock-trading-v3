#!/usr/bin/env python3
"""
주식 기본정보 수집 실행 스크립트
stock_codes 테이블의 활성 종목을 대상으로 stocks 테이블 업데이트
"""
import sys
import signal
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.collectors.stock_info_collector import StockInfoCollector
from src.api.base_session import create_kiwoom_session
from src.core.config import Config


class StockInfoCollectionManager:
    """주식정보 수집 관리자"""

    def __init__(self):
        self.session = None
        self.collector = None
        self.interrupted = False

        # Ctrl+C 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """중단 신호 처리"""
        print("\n⚠️ 중단 신호를 받았습니다. 안전하게 종료 중...")
        self.interrupted = True

    def run(self, market_filter=None):
        """수집 실행"""
        try:
            print("🚀 주식 기본정보 수집 시스템 시작")
            print("=" * 60)

            # 1. 키움 세션 생성
            print("🔗 키움 API 연결 중...")
            self.session = create_kiwoom_session()

            if not self.session or not self.session.is_ready():
                print("❌ 키움 API 연결 실패")
                return False

            print("✅ 키움 API 연결 성공")

            # 2. 수집기 초기화
            config = Config()
            self.collector = StockInfoCollector(self.session, config)

            # 3. 수집 실행
            print(f"📊 API 요청 간격: {config.api_request_delay_ms / 1000:.1f}초")
            if market_filter:
                print(f"📈 시장 필터: {market_filter}")
            print()

            result = self.collector.collect_all_active_stocks(market_filter)

            # 4. 결과 확인
            if 'error' in result:
                print(f"❌ 수집 실패: {result['error']}")
                return False
            else:
                print("✅ 수집 성공!")
                return True

        except KeyboardInterrupt:
            print("\n⚠️ 사용자 요청으로 중단되었습니다.")
            return False
        except Exception as e:
            print(f"❌ 예상치 못한 오류: {e}")
            return False
        finally:
            # 5. 정리
            if self.session:
                print("🔌 키움 API 연결 해제 중...")
                # session.disconnect() # 필요시 구현

            print("👋 프로그램을 종료합니다.")


def show_usage():
    """사용법 출력"""
    print("📋 사용법:")
    print("  python scripts/collect_stock_info.py [시장]")
    print("")
    print("📊 시장 옵션:")
    print("  (없음)  : NXT 시장 (786개 종목)")
    print("  NXT     : NXT 시장 (동일)")
    print("  KOSPI   : 코스피 종목만 (사용 안함)")
    print("  KOSDAQ  : 코스닥 종목만 (사용 안함)")
    print("")
    print("🔧 예시:")
    print("  python scripts/collect_stock_info.py")
    print("  python scripts/collect_stock_info.py NXT")


def main():
    """메인 실행 함수"""
    # 명령행 인수 처리
    market_filter = None
    if len(sys.argv) > 1:
        market_arg = sys.argv[1].upper()
        if market_arg in ['NXT', 'KOSPI', 'KOSDAQ']:
            market_filter = market_arg
        elif market_arg in ['HELP', '-H', '--HELP']:
            show_usage()
            return
        else:
            print(f"❌ 잘못된 시장 옵션: {market_arg}")
            show_usage()
            return

    # 시작 정보 출력
    print("🎯 주식 기본정보 수집 (stock_codes → stocks)")
    print("💡 대상: stock_codes.is_active = TRUE 종목")
    print("🔄 방식: 키움 API OPT10001 (주식기본정보요청)")

    print("📈 시장: NXT (786개 종목)")
    print("⚡ 예상 소요시간: 약 47분 (기존 4시간 대비 80% 단축)")
    print("🆕 우선주 제외됨 (name NOT LIKE '%우%')")

    print()

    # 사용자 확인
    try:
        response = input("계속 진행하시겠습니까? (y/N): ")
        if response.lower() not in ['y', 'yes']:
            print("❌ 사용자가 취소했습니다.")
            return
    except KeyboardInterrupt:
        print("\n❌ 사용자가 취소했습니다.")
        return

    # 수집 실행
    manager = StockInfoCollectionManager()
    success = manager.run(market_filter)

    # 종료 코드 반환
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()