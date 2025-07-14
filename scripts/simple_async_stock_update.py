#!/usr/bin/env python3
"""
파일 경로: scripts/simple_async_stock_update.py

단순한 비동기 주식정보 업데이트 스크립트
- 복잡한 배치 처리 제거
- 기본적인 semaphore + asyncio.gather 사용
- 안정성과 단순함 우선
"""

import sys
import asyncio
import argparse
from pathlib import Path
from datetime import datetime
import logging
import time

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import Config
from src.core.database import get_database_service
from src.api.base_session import create_kiwoom_session
from src.collectors.stock_info import StockInfoCollector

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SimpleAsyncUpdater:
    """단순한 비동기 업데이트 클래스"""

    def __init__(self, concurrency=5):
        self.concurrency = concurrency
        self.session = None
        self.collector = None
        self.db_service = None

        # 통계
        self.total_stocks = 0
        self.completed = 0
        self.successful = 0
        self.failed = 0
        self.start_time = None

        # 세마포어
        self.semaphore = asyncio.Semaphore(concurrency)

    async def initialize(self):
        """초기화"""
        print("🔧 서비스 초기화 중...")

        # DB 연결
        self.db_service = get_database_service()
        if not self.db_service.test_connection():
            raise Exception("데이터베이스 연결 실패")
        print("✅ 데이터베이스 연결 완료")

        # 키움 세션
        self.session = create_kiwoom_session(auto_login=True, show_progress=True)
        if not self.session or not self.session.is_ready():
            raise Exception("키움 세션 준비 실패")
        print("✅ 키움 세션 준비 완료")

        # 수집기
        self.collector = StockInfoCollector(self.session)
        print("✅ StockInfoCollector 초기화 완료")

    async def process_single_stock(self, stock_code: str):
        """단일 종목 비동기 처리"""
        async with self.semaphore:
            try:
                # API 제한 준수 (3.6초)
                await asyncio.sleep(3.6)

                # 동기 메서드를 스레드풀에서 실행
                loop = asyncio.get_event_loop()
                success, is_new = await loop.run_in_executor(
                    None,
                    self.collector.collect_single_stock_info,
                    stock_code
                )

                # 통계 업데이트
                self.completed += 1
                if success:
                    self.successful += 1
                    action = "신규" if is_new else "업데이트"
                    logger.info(f"✅ {stock_code}: {action} 완료 ({self.completed}/{self.total_stocks})")
                else:
                    self.failed += 1
                    logger.warning(f"❌ {stock_code}: 실패 ({self.completed}/{self.total_stocks})")

                # 주기적 진행상황 출력
                if self.completed % 50 == 0:
                    await self.show_progress()

                return success

            except Exception as e:
                self.completed += 1
                self.failed += 1
                logger.error(f"❌ {stock_code}: 예외 발생 - {e}")
                return False

    async def show_progress(self):
        """진행상황 출력"""
        if self.start_time:
            elapsed = time.time() - self.start_time
            rate = self.completed / elapsed if elapsed > 0 else 0
            remaining = self.total_stocks - self.completed
            eta = remaining / rate if rate > 0 else 0

            success_rate = (self.successful / self.completed * 100) if self.completed > 0 else 0

            print(f"\n📊 진행상황:")
            print(
                f"   📈 완료: {self.completed:,}/{self.total_stocks:,} ({self.completed / self.total_stocks * 100:.1f}%)")
            print(f"   ✅ 성공: {self.successful:,} ({success_rate:.1f}%)")
            print(f"   ❌ 실패: {self.failed:,}")
            print(f"   ⚡ 속도: {rate:.1f} 종목/초")
            print(f"   ⏱️ 예상 완료: {eta / 60:.1f}분 후")

    async def run_update(self, stock_codes=None, market_filter=None):
        """메인 업데이트 실행"""
        try:
            self.start_time = time.time()

            # 대상 종목 조회
            if stock_codes:
                # 지정된 종목들
                stock_codes_data = []
                for code in stock_codes:
                    stock_codes_data.append({'code': code})
            else:
                # DB에서 활성 종목 조회
                if market_filter:
                    stock_codes_data = await self.db_service.get_active_stock_codes_by_market_async(market_filter)
                else:
                    stock_codes_data = await self.db_service.get_active_stock_codes_async()

            if not stock_codes_data:
                print("❌ 대상 종목이 없습니다")
                return False

            codes = [item['code'] for item in stock_codes_data]
            self.total_stocks = len(codes)

            # 예상 시간 계산
            estimated_minutes = (self.total_stocks * 3.6) / self.concurrency / 60

            print(f"\n📊 업데이트 계획:")
            print(f"   🎯 대상 종목: {self.total_stocks:,}개")
            print(f"   ⚡ 동시 처리: {self.concurrency}개")
            print(f"   ⏱️ 예상 시간: {estimated_minutes:.1f}분")

            # 사용자 확인
            response = input("\n계속 진행하시겠습니까? (y/N): ")
            if response.lower() != 'y':
                print("❌ 사용자가 취소했습니다")
                return False

            print(f"\n🚀 비동기 업데이트 시작!")

            # 비동기 처리 - 간단한 gather 방식
            tasks = [self.process_single_stock(code) for code in codes]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 최종 결과
            elapsed = time.time() - self.start_time

            print(f"\n{'=' * 60}")
            print(f"🎉 비동기 업데이트 완료!")
            print(f"{'=' * 60}")
            print(f"📊 최종 결과:")
            print(f"   📈 전체 종목: {self.total_stocks:,}개")
            print(f"   ✅ 성공: {self.successful:,}개 ({self.successful / self.total_stocks * 100:.1f}%)")
            print(f"   ❌ 실패: {self.failed:,}개")
            print(f"   ⏱️ 총 시간: {elapsed / 60:.1f}분")
            print(f"   🚀 처리량: {self.total_stocks / elapsed * 60:.1f} 종목/분")

            return True

        except Exception as e:
            logger.error(f"업데이트 실행 실패: {e}")
            return False


async def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="단순 비동기 주식정보 업데이트")
    parser.add_argument("--concurrency", "-c", type=int, default=5, help="동시 처리 수 (기본: 5)")
    parser.add_argument("--market", "-m", choices=["KOSPI", "KOSDAQ"], help="시장 필터")
    parser.add_argument("--codes", nargs="+", help="특정 종목코드들")

    args = parser.parse_args()

    print("🚀 단순 비동기 주식정보 업데이트")
    print("=" * 50)

    try:
        # 업데이터 생성
        updater = SimpleAsyncUpdater(concurrency=args.concurrency)

        # 초기화
        await updater.initialize()

        # 실행
        success = await updater.run_update(
            stock_codes=args.codes,
            market_filter=args.market
        )

        if success:
            print("\n✅ 업데이트 성공!")
        else:
            print("\n❌ 업데이트 실패!")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⚠️ 사용자가 중단했습니다")
    except Exception as e:
        print(f"\n❌ 오류: {e}")
        logger.exception("실행 중 오류")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())