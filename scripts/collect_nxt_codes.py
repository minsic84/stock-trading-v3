#!/usr/bin/env python3
"""
NXT 종목코드 수집 실행 스크립트
키움 API를 통해 NXT 종목코드를 수집하고 stock_codes 테이블에 저장
"""
import sys
import signal
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.market.nxt_code_collector import NXTCodeCollector
from src.api.base_session import create_kiwoom_session
from src.core.database import get_database_service


class NXTCollectionManager:
    """NXT 종목코드 수집 관리자"""

    def __init__(self):
        self.session = None
        self.collector = None
        self.db_service = None
        self.interrupted = False

        # Ctrl+C 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """중단 신호 처리"""
        print("\n⚠️ 중단 신호를 받았습니다. 안전하게 종료 중...")
        self.interrupted = True

    def run(self):
        """NXT 종목코드 수집 실행"""
        try:
            print("🆕 NXT 종목코드 수집 시스템 시작")
            print("=" * 60)

            # 1. 키움 세션 생성
            print("🔗 키움 API 연결 중...")
            self.session = create_kiwoom_session()

            if not self.session or not self.session.is_ready():
                print("❌ 키움 API 연결 실패")
                return False

            print("✅ 키움 API 연결 성공")

            # 2. 수집기 초기화
            self.collector = NXTCodeCollector(self.session.get_connector())

            # 3. 데이터베이스 서비스 초기화
            self.db_service = get_database_service()

            # 4. NXT 연결 테스트
            print("\n🧪 NXT 시장 연결 테스트...")
            if not self.collector.test_nxt_connection():
                print("❌ NXT 시장 연결 실패")
                print("💡 NXT 시장이 현재 지원되지 않을 수 있습니다.")

                response = input("그래도 계속 진행하시겠습니까? (y/N): ")
                if response.lower() not in ['y', 'yes']:
                    return False

            # 5. 기존 데이터 확인
            self._check_existing_data()

            # 6. NXT 종목코드 수집
            print("\n📊 NXT 종목코드 수집 중...")
            nxt_data = self.collector.collect_nxt_with_names()

            if not nxt_data:
                print("❌ NXT 종목코드를 수집하지 못했습니다.")
                return False

            print(f"✅ NXT 종목 수집 완료: {len(nxt_data)}개")

            # 7. 데이터베이스 저장
            if not self.interrupted:
                success = self._save_to_database(nxt_data)
                if success:
                    print("✅ 데이터베이스 저장 완료!")
                    return True
                else:
                    print("❌ 데이터베이스 저장 실패")
                    return False

            return False

        except KeyboardInterrupt:
            print("\n⚠️ 사용자 요청으로 중단되었습니다.")
            return False
        except Exception as e:
            print(f"❌ 예상치 못한 오류: {e}")
            return False
        finally:
            # 정리
            if self.session:
                print("🔌 키움 API 연결 해제 중...")

            print("👋 프로그램을 종료합니다.")

    def _check_existing_data(self):
        """기존 stock_codes 데이터 확인"""
        try:
            print("\n📊 기존 stock_codes 데이터 확인...")

            conn = self.db_service._get_connection('main')
            cursor = conn.cursor()

            # 전체 데이터 수 확인
            cursor.execute("SELECT COUNT(*) FROM stock_codes")
            total_count = cursor.fetchone()[0]

            # NXT 데이터 수 확인 (있다면)
            cursor.execute("SELECT COUNT(*) FROM stock_codes WHERE market = 'NXT'")
            nxt_count = cursor.fetchone()[0]

            cursor.close()

            print(f"   📈 전체 종목: {total_count:,}개")
            print(f"   🆕 NXT 종목: {nxt_count:,}개")

            if total_count > 0:
                print("\n⚠️ 기존 데이터가 있습니다.")
                print("💡 옵션:")
                print("   1. 기존 데이터 유지하고 NXT 추가")
                print("   2. 모든 데이터 삭제하고 NXT만 저장")

                choice = input("선택하세요 (1/2): ").strip()

                if choice == '2':
                    print("🗑️ 기존 데이터 삭제 중...")
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM stock_codes")
                    conn.commit()
                    cursor.close()
                    print("✅ 기존 데이터 삭제 완료")
                elif choice == '1':
                    if nxt_count > 0:
                        print("🔄 기존 NXT 데이터 삭제 중...")
                        cursor = conn.cursor()
                        cursor.execute("DELETE FROM stock_codes WHERE market = 'NXT'")
                        conn.commit()
                        cursor.close()
                        print("✅ 기존 NXT 데이터 삭제 완료")
                else:
                    print("❌ 잘못된 선택입니다.")
                    return False

            return True

        except Exception as e:
            print(f"❌ 기존 데이터 확인 실패: {e}")
            return False

    def _save_to_database(self, nxt_data: dict) -> bool:
        """NXT 데이터를 stock_codes 테이블에 저장"""
        try:
            print(f"\n💾 데이터베이스 저장 중... ({len(nxt_data)}개)")

            conn = self.db_service._get_connection('main')
            cursor = conn.cursor()

            success_count = 0
            error_count = 0

            for code, info in nxt_data.items():
                try:
                    cursor.execute("""
                        INSERT INTO stock_codes (code, name, market, is_active, collected_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        market = VALUES(market),
                        is_active = VALUES(is_active),
                        updated_at = NOW()
                    """, (code, info['name'], 'NXT', True))

                    success_count += 1

                    if success_count % 10 == 0:
                        print(f"   진행률: {success_count}/{len(nxt_data)} ({success_count / len(nxt_data) * 100:.1f}%)")

                except Exception as e:
                    error_count += 1
                    print(f"   ❌ {code} 저장 실패: {e}")

            # 커밋
            conn.commit()
            cursor.close()

            # 결과 출력
            print(f"\n📊 저장 결과:")
            print(f"   ✅ 성공: {success_count}개")
            print(f"   ❌ 실패: {error_count}개")
            print(f"   📈 성공률: {(success_count / len(nxt_data) * 100):.1f}%")

            return success_count > 0

        except Exception as e:
            print(f"❌ 데이터베이스 저장 실패: {e}")
            return False


def show_usage():
    """사용법 출력"""
    print("📋 사용법:")
    print("  python scripts/collect_nxt_codes.py")
    print("")
    print("🆕 기능:")
    print("  - NXT 종목코드 수집")
    print("  - 종목명 자동 조회")
    print("  - stock_codes 테이블 저장")
    print("  - 기존 데이터 옵션 제공")


def main():
    """메인 실행 함수"""
    # 명령행 인수 처리
    if len(sys.argv) > 1:
        if sys.argv[1].lower() in ['help', '-h', '--help']:
            show_usage()
            return

    # 시작 정보 출력
    print("🎯 NXT 종목코드 수집 (키움 API → stock_codes)")
    print("💡 대상: NXT 시장 전체 종목")
    print("🔄 방식: GetCodeListByMarket('NXT') + GetMasterCodeName()")
    print("💾 저장: stock_codes 테이블")
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
    manager = NXTCollectionManager()
    success = manager.run()

    # 종료 코드 반환
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()