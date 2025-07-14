#!/usr/bin/env python3
"""
파일 경로: src/utils/async_helpers.py

비동기 처리를 위한 공통 유틸리티 모듈
- 키움 API 제한 준수 (3.6초 간격)
- Semaphore 기반 동시성 제어
- 배치 처리 및 진행상황 추적
- 에러 격리 및 재시도 로직
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional, Callable, Union, Tuple
from datetime import datetime, timedelta
import time
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
import threading

logger = logging.getLogger(__name__)


# ================================
# 🎯 비동기 처리 핵심 클래스들
# ================================

@dataclass
class AsyncTaskResult:
    """비동기 작업 결과"""
    item: Any
    success: bool
    result: Any = None
    error: Optional[Exception] = None
    elapsed_time: float = 0.0
    retry_count: int = 0


@dataclass
class AsyncBatchStats:
    """배치 처리 통계"""
    total_items: int = 0
    completed: int = 0
    successful: int = 0
    failed: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

    @property
    def elapsed_seconds(self) -> float:
        """총 소요시간 (초)"""
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()

    @property
    def success_rate(self) -> float:
        """성공률 (%)"""
        return (self.successful / self.completed * 100) if self.completed > 0 else 0.0

    @property
    def items_per_second(self) -> float:
        """초당 처리량"""
        elapsed = self.elapsed_seconds
        return self.completed / elapsed if elapsed > 0 else 0.0


class AsyncRateLimiter:
    """비동기 속도 제한기 (키움 API 3.6초 간격 준수)"""

    def __init__(self, delay_seconds: float = 3.6, max_concurrent: int = 5):
        """
        Args:
            delay_seconds: API 요청 간격 (기본: 3.6초)
            max_concurrent: 최대 동시 요청 수 (기본: 5개)
        """
        self.delay_seconds = delay_seconds
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.last_request_times = {}
        self.lock = asyncio.Lock()

    async def acquire(self, request_id: str = "default") -> None:
        """속도 제한 적용하여 요청 허가"""
        async with self.semaphore:
            async with self.lock:
                now = time.time()
                last_time = self.last_request_times.get(request_id, 0)
                time_since_last = now - last_time

                if time_since_last < self.delay_seconds:
                    sleep_time = self.delay_seconds - time_since_last
                    logger.debug(f"⏱️ API 제한: {sleep_time:.1f}초 대기")
                    await asyncio.sleep(sleep_time)

                self.last_request_times[request_id] = time.time()


class AsyncProgressTracker:
    """비동기 진행상황 추적기"""

    def __init__(self, total_items: int, description: str = "Processing"):
        self.total_items = total_items
        self.description = description
        self.completed = 0
        self.successful = 0
        self.failed = 0
        self.start_time = datetime.now()
        self.last_report_time = self.start_time
        self.report_interval = 10  # 10초마다 리포트
        self.lock = asyncio.Lock()

    async def update(self, success: bool = True, item_info: str = "") -> None:
        """진행상황 업데이트"""
        async with self.lock:
            self.completed += 1
            if success:
                self.successful += 1
            else:
                self.failed += 1

            # 주기적 리포트
            now = datetime.now()
            if (now - self.last_report_time).total_seconds() >= self.report_interval:
                await self._show_progress(item_info)
                self.last_report_time = now

    async def _show_progress(self, current_item: str = "") -> None:
        """진행상황 출력"""
        progress_pct = (self.completed / self.total_items) * 100
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.completed / elapsed if elapsed > 0 else 0

        eta_seconds = (self.total_items - self.completed) / rate if rate > 0 else 0
        eta_time = timedelta(seconds=int(eta_seconds))

        print(f"\n📊 {self.description} 진행상황:")
        print(f"   📈 완료: {self.completed:,}/{self.total_items:,} ({progress_pct:.1f}%)")
        print(f"   ✅ 성공: {self.successful:,} | ❌ 실패: {self.failed:,}")
        print(f"   ⚡ 속도: {rate:.1f} 항목/초 | ⏱️ 예상 완료: {eta_time}")
        if current_item:
            print(f"   🔄 현재: {current_item}")


# ================================
# 🚀 핵심 비동기 처리 함수들
# ================================

async def create_semaphore_manager(limit: int = 5) -> asyncio.Semaphore:
    """Semaphore 생성 (동시 실행 수 제한)"""
    if limit <= 0:
        raise ValueError("Semaphore limit은 1 이상이어야 합니다")

    logger.info(f"📊 Semaphore 생성: 최대 {limit}개 동시 실행")
    return asyncio.Semaphore(limit)


async def batch_processor(
        items: List[Any],
        processor_func: Callable,
        batch_size: int = 5,
        max_concurrent: int = 5,
        delay_seconds: float = 3.6,
        max_retries: int = 3,
        progress_description: str = "배치 처리"
) -> Tuple[List[AsyncTaskResult], AsyncBatchStats]:
    """
    배치 단위로 비동기 처리

    Args:
        items: 처리할 항목 리스트
        processor_func: 각 항목을 처리할 비동기 함수
        batch_size: 배치 크기 (기본: 5개)
        max_concurrent: 최대 동시 실행 수
        delay_seconds: API 요청 간격
        max_retries: 최대 재시도 횟수
        progress_description: 진행상황 설명

    Returns:
        (결과 리스트, 통계 정보)
    """
    if not items:
        return [], AsyncBatchStats()

    # 통계 및 추적기 초기화
    stats = AsyncBatchStats(total_items=len(items))
    progress = AsyncProgressTracker(len(items), progress_description)
    rate_limiter = AsyncRateLimiter(delay_seconds, max_concurrent)

    # 배치 나누기
    batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
    all_results = []

    logger.info(f"🚀 배치 처리 시작: {len(items):,}개 항목을 {len(batches)}개 배치로 처리")

    try:
        for batch_idx, batch in enumerate(batches):
            logger.info(f"\n📦 배치 {batch_idx + 1}/{len(batches)} 처리 중... ({len(batch)}개 항목)")

            # 배치 내 비동기 처리
            batch_tasks = [
                _process_single_item_with_retry(
                    item, processor_func, rate_limiter, max_retries, progress
                )
                for item in batch
            ]

            # 배치 완료 대기
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            all_results.extend(batch_results)

            # 통계 업데이트
            for result in batch_results:
                if isinstance(result, AsyncTaskResult):
                    stats.completed += 1
                    if result.success:
                        stats.successful += 1
                    else:
                        stats.failed += 1
                else:
                    stats.failed += 1

            logger.info(f"✅ 배치 {batch_idx + 1} 완료")

    except Exception as e:
        logger.error(f"❌ 배치 처리 중 치명적 오류: {e}")

    finally:
        stats.end_time = datetime.now()
        await _show_final_stats(stats, progress_description)

    return all_results, stats


async def _process_single_item_with_retry(
        item: Any,
        processor_func: Callable,
        rate_limiter: AsyncRateLimiter,
        max_retries: int,
        progress: AsyncProgressTracker
) -> AsyncTaskResult:
    """재시도 로직이 포함된 단일 항목 처리"""
    result = AsyncTaskResult(item=item, success=False)
    start_time = time.time()

    for retry in range(max_retries + 1):
        try:
            # API 제한 적용
            await rate_limiter.acquire(str(item))

            # 실제 처리 함수 호출
            if asyncio.iscoroutinefunction(processor_func):
                processed_result = await processor_func(item)
            else:
                # 동기 함수를 비동기로 실행
                loop = asyncio.get_event_loop()
                processed_result = await loop.run_in_executor(None, processor_func, item)

            # 성공
            result.success = True
            result.result = processed_result
            result.retry_count = retry
            break

        except asyncio.CancelledError:
            logger.warning(f"⚠️ {item}: 작업 취소됨")
            result.error = Exception("작업 취소됨")
            break

        except Exception as e:
            result.error = e
            result.retry_count = retry

            if retry < max_retries:
                wait_time = min(2 ** retry, 30)  # 지수적 백오프, 최대 30초
                logger.warning(f"⚠️ {item}: {retry + 1}회 시도 실패, {wait_time}초 후 재시도 - {e}")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"❌ {item}: {max_retries + 1}회 모두 실패 - {e}")

    result.elapsed_time = time.time() - start_time

    # 진행상황 업데이트
    await progress.update(result.success, str(item))

    return result


async def _show_final_stats(stats: AsyncBatchStats, description: str) -> None:
    """최종 통계 출력"""
    print(f"\n{'=' * 60}")
    print(f"🎉 {description} 완료!")
    print(f"{'=' * 60}")
    print(f"📊 처리 결과:")
    print(f"   📈 전체: {stats.total_items:,}개")
    print(f"   ✅ 성공: {stats.successful:,}개 ({stats.success_rate:.1f}%)")
    print(f"   ❌ 실패: {stats.failed:,}개")
    print(f"   ⚡ 속도: {stats.items_per_second:.1f} 항목/초")
    print(f"   ⏱️ 총 시간: {stats.elapsed_seconds:.1f}초")


# ================================
# 🔧 특화된 헬퍼 함수들
# ================================

async def api_rate_limiter(delay_seconds: float = 3.6) -> Callable:
    """API 속도 제한 데코레이터 팩토리"""
    last_call_time = {'time': 0}
    lock = asyncio.Lock()

    def decorator(func):
        async def wrapper(*args, **kwargs):
            async with lock:
                now = time.time()
                time_since_last = now - last_call_time['time']

                if time_since_last < delay_seconds:
                    sleep_time = delay_seconds - time_since_last
                    await asyncio.sleep(sleep_time)

                last_call_time['time'] = time.time()

            return await func(*args, **kwargs)

        return wrapper

    return decorator


async def parallel_processor(
        items: List[Any],
        processor_func: Callable,
        max_concurrent: int = 5,
        timeout_seconds: Optional[float] = None
) -> List[Union[Any, Exception]]:
    """
    단순 병렬 처리 (재시도 없음, 빠른 처리용)

    Args:
        items: 처리할 항목들
        processor_func: 처리 함수
        max_concurrent: 최대 동시 실행 수
        timeout_seconds: 타임아웃 (None이면 무제한)

    Returns:
        결과 또는 예외 리스트
    """
    if not items:
        return []

    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_with_semaphore(item):
        async with semaphore:
            if asyncio.iscoroutinefunction(processor_func):
                return await processor_func(item)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, processor_func, item)

    # 모든 작업 실행
    tasks = [process_with_semaphore(item) for item in items]

    if timeout_seconds:
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=timeout_seconds
        )
    else:
        results = await asyncio.gather(*tasks, return_exceptions=True)

    return results


# ================================
# 🎯 컨텍스트 매니저들
# ================================

class AsyncTimer:
    """비동기 타이머 컨텍스트 매니저"""

    def __init__(self, description: str = "작업"):
        self.description = description
        self.start_time = None
        self.end_time = None

    async def __aenter__(self):
        self.start_time = time.time()
        logger.info(f"⏱️ {self.description} 시작")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        elapsed = self.end_time - self.start_time

        if exc_type is None:
            logger.info(f"✅ {self.description} 완료 ({elapsed:.1f}초)")
        else:
            logger.error(f"❌ {self.description} 실패 ({elapsed:.1f}초): {exc_val}")

    @property
    def elapsed_seconds(self) -> float:
        """경과 시간 (초)"""
        if self.start_time is None:
            return 0.0
        end = self.end_time or time.time()
        return end - self.start_time


class AsyncResourceManager:
    """비동기 리소스 관리자"""

    def __init__(self, setup_func: Callable, cleanup_func: Callable):
        self.setup_func = setup_func
        self.cleanup_func = cleanup_func
        self.resource = None

    async def __aenter__(self):
        if asyncio.iscoroutinefunction(self.setup_func):
            self.resource = await self.setup_func()
        else:
            self.resource = self.setup_func()
        return self.resource

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.resource and self.cleanup_func:
            try:
                if asyncio.iscoroutinefunction(self.cleanup_func):
                    await self.cleanup_func(self.resource)
                else:
                    self.cleanup_func(self.resource)
            except Exception as e:
                logger.error(f"❌ 리소스 정리 실패: {e}")


# ================================
# 🔧 유틸리티 함수들
# ================================

def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """리스트를 청크 단위로 나누기"""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


async def run_in_thread_pool(func: Callable, *args, max_workers: int = 4, **kwargs) -> Any:
    """CPU 집약적 작업을 스레드 풀에서 실행"""
    loop = asyncio.get_event_loop()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return await loop.run_in_executor(executor, func, *args)


async def async_filter(predicate: Callable, items: List[Any]) -> List[Any]:
    """비동기 필터링"""
    if not items:
        return []

    if asyncio.iscoroutinefunction(predicate):
        # 비동기 predicate
        results = await asyncio.gather(*[predicate(item) for item in items])
        return [item for item, keep in zip(items, results) if keep]
    else:
        # 동기 predicate
        return [item for item in items if predicate(item)]


async def async_map(func: Callable, items: List[Any], max_concurrent: int = 10) -> List[Any]:
    """비동기 매핑"""
    if not items:
        return []

    semaphore = asyncio.Semaphore(max_concurrent)

    async def process_item(item):
        async with semaphore:
            if asyncio.iscoroutinefunction(func):
                return await func(item)
            else:
                return func(item)

    return await asyncio.gather(*[process_item(item) for item in items])


# ================================
# 📊 성능 측정 도구들
# ================================

class AsyncPerformanceMonitor:
    """비동기 성능 모니터"""

    def __init__(self):
        self.metrics = {}
        self.start_times = {}

    def start_timer(self, name: str) -> None:
        """타이머 시작"""
        self.start_times[name] = time.time()

    def end_timer(self, name: str) -> float:
        """타이머 종료 및 측정값 반환"""
        if name not in self.start_times:
            return 0.0

        elapsed = time.time() - self.start_times[name]

        if name not in self.metrics:
            self.metrics[name] = {'count': 0, 'total_time': 0, 'min_time': float('inf'), 'max_time': 0}

        stats = self.metrics[name]
        stats['count'] += 1
        stats['total_time'] += elapsed
        stats['min_time'] = min(stats['min_time'], elapsed)
        stats['max_time'] = max(stats['max_time'], elapsed)

        del self.start_times[name]
        return elapsed

    def get_stats(self) -> Dict[str, Dict[str, float]]:
        """통계 조회"""
        result = {}
        for name, stats in self.metrics.items():
            if stats['count'] > 0:
                result[name] = {
                    'count': stats['count'],
                    'total_time': stats['total_time'],
                    'avg_time': stats['total_time'] / stats['count'],
                    'min_time': stats['min_time'],
                    'max_time': stats['max_time']
                }
        return result

    def print_stats(self) -> None:
        """통계 출력"""
        stats = self.get_stats()
        if not stats:
            print("📊 성능 통계 없음")
            return

        print("\n📊 성능 통계:")
        print("-" * 60)
        for name, data in stats.items():
            print(f"🔧 {name}:")
            print(f"   📈 호출 횟수: {data['count']:,}회")
            print(f"   ⏱️ 평균 시간: {data['avg_time']:.3f}초")
            print(f"   ⚡ 최소 시간: {data['min_time']:.3f}초")
            print(f"   🐌 최대 시간: {data['max_time']:.3f}초")
            print(f"   📊 총 시간: {data['total_time']:.1f}초")


# ================================
# 🧪 테스트 및 예제 함수들
# ================================

async def test_batch_processing():
    """배치 처리 테스트"""

    async def sample_processor(item):
        """샘플 처리 함수"""
        await asyncio.sleep(0.1)  # 작업 시뮬레이션
        if item % 10 == 0:  # 10% 실패율
            raise Exception(f"테스트 실패: {item}")
        return f"처리됨: {item}"

    # 테스트 데이터
    test_items = list(range(50))

    print("🧪 배치 처리 테스트 시작...")

    results, stats = await batch_processor(
        items=test_items,
        processor_func=sample_processor,
        batch_size=5,
        max_concurrent=3,
        delay_seconds=0.5,
        progress_description="테스트 배치 처리"
    )

    print(f"\n🎯 테스트 결과:")
    print(f"   처리량: {stats.items_per_second:.1f} 항목/초")
    print(f"   성공률: {stats.success_rate:.1f}%")


if __name__ == "__main__":
    # 테스트 실행
    asyncio.run(test_batch_processing())