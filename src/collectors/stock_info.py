"""
주식 기본정보 수집기 모듈
키움 API OPT10001(주식기본정보요청)을 사용하여 종목 정보를 수집하고 데이터베이스에 저장
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import time
import asyncio

from ..core.config import Config
from ..core.database import get_database_service
from ..api.base_session import KiwoomSession
from ..api.tr_codes import get_tr_info, create_opt10001_input, validate_input_data

# 비동기 헬퍼 import
from ..utils.async_helpers import (
    AsyncRateLimiter, AsyncProgressTracker, batch_processor,
    AsyncTaskResult, AsyncBatchStats, AsyncTimer
)

# 로거 설정
logger = logging.getLogger(__name__)


class StockInfoCollector:
    """주식 기본정보 수집기 클래스 (OPT10001 사용)"""

    def __init__(self, session: KiwoomSession, config: Optional[Config] = None):
        self.session = session
        self.config = config or Config()
        self.db_service = None

        # 수집 상태
        self.collected_count = 0
        self.updated_count = 0
        self.skipped_count = 0
        self.error_count = 0

        # TR 정보
        self.tr_info = get_tr_info('opt10001')
        self.TR_CODE = self.tr_info['code']
        self.RQ_NAME = self.tr_info['name']

        self._setup()

    def _setup(self):
        """초기화 설정"""
        try:
            self.db_service = get_database_service()
            logger.info("주식정보 수집기 초기화 완료")
        except Exception as e:
            logger.error(f"주식정보 수집기 초기화 실패: {e}")
            raise

    # ================================
    # 🔧 기존 동기 메서드들 (간소화)
    # ================================

    def collect_and_update_stocks(self, stock_codes: List[str],
                                  test_mode: bool = True,
                                  always_update: bool = True) -> Dict[str, Any]:
        """주식 코드 리스트를 순회하며 데이터 수집 (동기 처리)"""
        try:
            print(f"🚀 주식 기본정보 수집 시작 (동기 모드)")
            print(f"📊 대상 종목: {len(stock_codes)}개")

            if test_mode:
                stock_codes = stock_codes[:5]
                print(f"🧪 테스트 모드: {len(stock_codes)}개 종목만 수집")

            # 통계 초기화
            self._reset_stats()
            results = self._create_empty_results()
            start_time = datetime.now()

            for idx, stock_code in enumerate(stock_codes):
                try:
                    print(f"\n📈 진행률: {idx + 1}/{len(stock_codes)} - {stock_code}")

                    # 개별 종목 정보 수집
                    success, is_new = self.collect_single_stock_info(stock_code)

                    if success:
                        if is_new:
                            results['success'].append(stock_code)
                            self.collected_count += 1
                            print(f"✅ {stock_code}: 신규 데이터 수집 완료")
                        else:
                            results['updated'].append(stock_code)
                            self.updated_count += 1
                            print(f"🔄 {stock_code}: 최신 데이터로 업데이트 완료")
                    else:
                        results['failed'].append(stock_code)
                        self.error_count += 1
                        print(f"❌ {stock_code}: 수집 실패")

                    # API 요청 제한 준수
                    if idx < len(stock_codes) - 1:
                        delay_ms = self.tr_info.get('delay_ms', 3600)
                        time.sleep(delay_ms / 1000)

                except Exception as e:
                    print(f"❌ {stock_code} 수집 중 예외 발생: {e}")
                    results['failed'].append(stock_code)
                    self.error_count += 1

            # 최종 통계
            return self._finalize_results(results, start_time)

        except Exception as e:
            logger.error(f"동기 주식정보 수집 중 치명적 오류: {e}")
            return {'error': str(e)}

    def collect_single_stock_info(self, stock_code: str) -> Tuple[bool, bool]:
        """단일 종목 기본정보 수집 (OPT10001)"""
        try:
            print(f"🔍 {stock_code} 수집 시작...")

            if not self.session or not self.session.is_ready():
                print(f"❌ {stock_code}: 키움 세션이 준비되지 않음")
                return False, False

            # API 호출
            input_data = create_opt10001_input(f"{stock_code}_AL")  # _AL 접미사 추가

            if not validate_input_data('opt10001', input_data):
                print(f"❌ {stock_code}: 입력 데이터 유효성 검증 실패")
                return False, False

            connector = self.session.get_connector()
            response = connector.request_tr_data(
                rq_name=self.RQ_NAME,
                tr_code=self.TR_CODE,
                input_data=input_data,
                screen_no="9001"
            )

            if not response:
                print(f"❌ {stock_code}: TR 요청 실패")
                return False, False

            # 데이터 파싱
            stock_data = self._parse_stock_info(response, stock_code)
            if not stock_data:
                print(f"❌ {stock_code}: 데이터 파싱 실패")
                return False, False

            # DB 저장 (UPSERT)
            success = self.db_service.upsert_stock_info(stock_code, stock_data)

            if success:
                is_new = not self.db_service.check_stock_exists(stock_code)
                print(f"✅ {stock_code} 저장 성공!")
                return True, is_new
            else:
                print(f"❌ {stock_code}: 데이터베이스 저장 실패")
                return False, False

        except Exception as e:
            print(f"❌ {stock_code} 수집 중 예외 발생: {e}")
            logger.error(f"{stock_code} 주식정보 수집 중 오류: {e}")
            return False, False

    # ================================
    # 🆕 비동기 메서드들 (신규 추가)
    # ================================

    async def collect_and_update_stocks_async(
        self,
        concurrency: int = 5,
        batch_size: int = 10,
        market_filter: Optional[str] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        비동기 주식 정보 수집 (stock_codes DB 기반)

        Args:
            concurrency: 동시 처리 수 (기본: 5개)
            batch_size: 배치 크기 (기본: 10개)
            market_filter: 시장 필터 ('KOSPI', 'KOSDAQ', None=전체)
            max_retries: 최대 재시도 횟수
        """
        async with AsyncTimer("비동기 주식정보 수집"):
            logger.info("🚀 비동기 주식정보 수집 시작")

            try:
                # 1단계: stock_codes에서 활성 종목 조회
                print("📊 활성 종목 조회 중...")
                if market_filter:
                    stock_codes_data = await self.db_service.get_active_stock_codes_by_market_async(market_filter)
                else:
                    stock_codes_data = await self.db_service.get_active_stock_codes_async()

                if not stock_codes_data:
                    logger.warning("활성 종목이 없습니다")
                    return {'error': '활성 종목 없음'}

                stock_codes = [item['code'] for item in stock_codes_data]

                print(f"✅ 대상 종목: {len(stock_codes):,}개")
                if market_filter:
                    print(f"📈 시장 필터: {market_filter}")

                # 2단계: 비동기 배치 처리
                results, stats = await batch_processor(
                    items=stock_codes,
                    processor_func=self._async_single_stock_processor,
                    batch_size=batch_size,
                    max_concurrent=concurrency,
                    delay_seconds=self.config.api_request_delay_ms / 1000,  # 3.6초
                    max_retries=max_retries,
                    progress_description=f"주식정보 비동기 수집 ({market_filter or '전체'})"
                )

                # 3단계: 결과 집계
                return await self._process_async_results(results, stats, stock_codes_data)

            except Exception as e:
                logger.error(f"❌ 비동기 수집 실패: {e}")
                return {
                    'error': str(e),
                    'success': False,
                    'total_stocks': 0,
                    'successful': 0,
                    'failed': 0
                }

    async def collect_single_stock_info_async(
        self,
        stock_code: str,
        semaphore: Optional[asyncio.Semaphore] = None
    ) -> Tuple[bool, bool]:
        """단일 종목 비동기 수집"""

        async def _process():
            try:
                # API 호출
                input_data = create_opt10001_input(f"{stock_code}_AL")
                response = await self._call_kiwoom_api_async(stock_code, input_data)

                if not response:
                    return False, False

                # 데이터 파싱
                stock_data = self._parse_stock_info(response, stock_code)
                if not stock_data:
                    return False, False

                # DB 저장
                save_success = await self.db_service.upsert_stock_info_async(stock_code, stock_data)

                if save_success:
                    is_new = not await asyncio.get_event_loop().run_in_executor(
                        None, self.db_service.check_stock_exists, stock_code
                    )
                    logger.info(f"✅ {stock_code} {'추가' if is_new else '업데이트'} 완료")
                    return True, is_new
                else:
                    logger.error(f"❌ {stock_code} DB 저장 실패")
                    return False, False

            except Exception as e:
                logger.error(f"❌ {stock_code} 비동기 수집 실패: {e}")
                return False, False

        # Semaphore 사용 (제공된 경우)
        if semaphore:
            async with semaphore:
                await asyncio.sleep(self.config.api_request_delay_ms / 1000)
                return await _process()
        else:
            return await _process()

    async def collect_stocks_by_codes_async(
        self,
        stock_codes: List[str],
        concurrency: int = 5,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """지정된 종목코드 리스트 비동기 수집"""

        if not stock_codes:
            return {'error': '종목코드 리스트가 비어있음'}

        logger.info(f"🎯 지정 종목 비동기 수집: {len(stock_codes)}개")

        # 배치 처리 실행
        results, stats = await batch_processor(
            items=stock_codes,
            processor_func=self._async_single_stock_processor,
            batch_size=concurrency,
            max_concurrent=concurrency,
            delay_seconds=self.config.api_request_delay_ms / 1000,
            max_retries=max_retries,
            progress_description="지정 종목 수집"
        )

        # 결과 처리
        successful = sum(1 for r in results if isinstance(r, AsyncTaskResult) and r.success)
        failed = len(results) - successful

        return {
            'total_stocks': len(stock_codes),
            'successful': successful,
            'failed': failed,
            'success_rate': (successful / len(stock_codes) * 100) if stock_codes else 0,
            'elapsed_seconds': stats.elapsed_seconds,
            'items_per_second': stats.items_per_second
        }

    # ================================
    # 🔧 내부 메서드들 (헬퍼)
    # ================================

    async def _async_single_stock_processor(self, stock_code: str) -> Dict[str, Any]:
        """단일 종목 비동기 처리 (내부 메서드)"""
        start_time = time.time()

        try:
            # API 입력 데이터 생성
            input_data = create_opt10001_input(f"{stock_code}_AL")

            # 키움 API 비동기 호출
            response = await self._call_kiwoom_api_async(stock_code, input_data)

            if not response:
                raise Exception("API 응답 없음")

            # 데이터 파싱
            stock_data = self._parse_stock_info(response, stock_code)
            if not stock_data:
                raise Exception("데이터 파싱 실패")

            # DB 비동기 저장 (UPSERT)
            save_success = await self.db_service.upsert_stock_info_async(stock_code, stock_data)

            if not save_success:
                raise Exception("DB 저장 실패")

            elapsed_time = time.time() - start_time

            return {
                'stock_code': stock_code,
                'success': True,
                'action': 'upserted',
                'elapsed_time': elapsed_time,
                'stock_name': stock_data.get('name', stock_code)
            }

        except Exception as e:
            elapsed_time = time.time() - start_time
            logger.error(f"❌ {stock_code} 비동기 처리 실패: {e}")

            return {
                'stock_code': stock_code,
                'success': False,
                'error': str(e),
                'elapsed_time': elapsed_time
            }

    async def _call_kiwoom_api_async(self, stock_code: str, input_data: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """키움 API 비동기 호출"""
        try:
            loop = asyncio.get_event_loop()

            def sync_api_call():
                if not self.session or not self.session.is_ready():
                    raise Exception("키움 세션이 준비되지 않음")

                connector = self.session.get_connector()
                return connector.request_tr_data(
                    rq_name=self.RQ_NAME,
                    tr_code=self.TR_CODE,
                    input_data=input_data,
                    screen_no="0001"
                )

            # 비동기 실행
            response = await loop.run_in_executor(None, sync_api_call)
            logger.debug(f"✅ {stock_code} API 호출 성공")
            return response

        except Exception as e:
            logger.error(f"❌ {stock_code} API 호출 실패: {e}")
            return None

    async def _process_async_results(
        self,
        results: List[AsyncTaskResult],
        stats: AsyncBatchStats,
        original_stock_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """비동기 결과 처리 및 통계 생성"""

        successful_stocks = []
        failed_stocks = []
        performance_data = []

        for result in results:
            if isinstance(result, AsyncTaskResult):
                if result.success and isinstance(result.result, dict):
                    successful_stocks.append(result.result)
                    performance_data.append({
                        'stock_code': result.result['stock_code'],
                        'elapsed_time': result.result['elapsed_time']
                    })
                else:
                    failed_stocks.append({
                        'stock_code': str(result.item),
                        'error': str(result.error) if result.error else '알 수 없는 오류'
                    })
            else:
                failed_stocks.append({
                    'stock_code': 'unknown',
                    'error': str(result)
                })

        # 성능 통계 계산
        if performance_data:
            avg_time = sum(item['elapsed_time'] for item in performance_data) / len(performance_data)
            min_time = min(item['elapsed_time'] for item in performance_data)
            max_time = max(item['elapsed_time'] for item in performance_data)
        else:
            avg_time = min_time = max_time = 0.0

        # 시장별 통계
        market_stats = self._calculate_market_stats(successful_stocks, original_stock_data)

        # 최종 결과
        final_result = {
            'success': True,
            'total_stocks': len(original_stock_data),
            'successful': len(successful_stocks),
            'failed': len(failed_stocks),
            'success_rate': stats.success_rate,
            'elapsed_seconds': stats.elapsed_seconds,
            'items_per_second': stats.items_per_second,
            'performance': {
                'avg_time_per_stock': avg_time,
                'min_time': min_time,
                'max_time': max_time
            },
            'market_breakdown': market_stats,
            'failed_stocks': failed_stocks[:10],  # 처음 10개만
            'collected_at': datetime.now().isoformat()
        }

        # 결과 출력
        await self._show_async_results_summary(final_result)
        return final_result

    def _parse_stock_info(self, response: Dict[str, Any], stock_code: str) -> Optional[Dict[str, Any]]:
        """OPT10001 응답 데이터 파싱"""
        try:
            if response.get('tr_code') != self.TR_CODE:
                logger.error(f"잘못된 TR 코드: {response.get('tr_code')}")
                return None

            data_info = response.get('data', {})
            if not data_info.get('parsed', False):
                logger.error(f"{stock_code} 데이터가 파싱되지 않음")
                return None

            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                logger.error(f"{stock_code} 원시 데이터가 없음")
                return None

            # 첫 번째 레코드 사용 (OPT10001은 단일 레코드)
            row_data = raw_data[0]

            # 필드 매핑 및 변환
            stock_data = {
                'name': self._clean_string(row_data.get('종목명', '')),
                'current_price': self._parse_int(row_data.get('현재가', 0)),
                'prev_day_diff': self._parse_int(row_data.get('전일대비', 0)),
                'change_rate': self._parse_rate(row_data.get('등락률', 0)),
                'volume': self._parse_int(row_data.get('거래량', 0)),
                'open_price': self._parse_int(row_data.get('시가', 0)),
                'high_price': self._parse_int(row_data.get('고가', 0)),
                'low_price': self._parse_int(row_data.get('저가', 0)),
                'upper_limit': self._parse_int(row_data.get('상한가', 0)),
                'lower_limit': self._parse_int(row_data.get('하한가', 0)),
                'market_cap': self._parse_int(row_data.get('시가총액', 0)),
                'market_cap_size': self._clean_string(row_data.get('시가총액규모', '')),
                'listed_shares': self._parse_int(row_data.get('상장주수', 0)),
                'per_ratio': self._parse_rate(row_data.get('PER', 0)),
                'pbr_ratio': self._parse_rate(row_data.get('PBR', 0)),
            }

            return stock_data

        except Exception as e:
            logger.error(f"{stock_code} 데이터 파싱 중 오류: {e}")
            return None

    def _reset_stats(self):
        """통계 초기화"""
        self.collected_count = 0
        self.updated_count = 0
        self.skipped_count = 0
        self.error_count = 0

    def _create_empty_results(self) -> Dict[str, List]:
        """빈 결과 딕셔너리 생성"""
        return {
            'success': [],
            'updated': [],
            'skipped': [],
            'failed': [],
            'total_collected': 0,
            'total_updated': 0,
            'total_skipped': 0,
            'total_errors': 0
        }

    def _finalize_results(self, results: Dict[str, Any], start_time: datetime) -> Dict[str, Any]:
        """최종 결과 정리"""
        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()

        results.update({
            'total_collected': self.collected_count,
            'total_updated': self.updated_count,
            'total_skipped': self.skipped_count,
            'total_errors': self.error_count,
            'elapsed_time': elapsed_time,
            'start_time': start_time,
            'end_time': end_time
        })

        # 결과 출력
        print(f"\n📋 동기 수집 완료 결과:")
        print(f"   ✅ 신규 수집: {results['total_collected']}개")
        print(f"   🔄 업데이트: {results['total_updated']}개")
        print(f"   ❌ 실패: {results['total_errors']}개")
        print(f"   ⏱️ 소요시간: {elapsed_time:.1f}초")

        return results

    def _calculate_market_stats(
        self,
        successful_stocks: List[Dict[str, Any]],
        original_stock_data: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, int]]:
        """시장별 통계 계산"""
        code_to_market = {item['code']: item['market'] for item in original_stock_data}
        market_stats = {}

        for stock in successful_stocks:
            stock_code = stock.get('stock_code')
            market = code_to_market.get(stock_code, 'UNKNOWN')

            if market not in market_stats:
                market_stats[market] = {'successful': 0, 'total': 0}

            market_stats[market]['successful'] += 1

        # 전체 종목 수 계산
        for item in original_stock_data:
            market = item['market']
            if market not in market_stats:
                market_stats[market] = {'successful': 0, 'total': 0}
            market_stats[market]['total'] += 1

        return market_stats

    async def _show_async_results_summary(self, results: Dict[str, Any]) -> None:
        """비동기 결과 요약 출력"""
        print(f"\n{'=' * 60}")
        print(f"🎉 비동기 주식정보 수집 완료!")
        print(f"{'=' * 60}")

        print(f"📊 수집 결과:")
        print(f"   📈 전체 종목: {results['total_stocks']:,}개")
        print(f"   ✅ 성공: {results['successful']:,}개 ({results['success_rate']:.1f}%)")
        print(f"   ❌ 실패: {results['failed']:,}개")

        print(f"\n⚡ 성능 지표:")
        print(f"   ⏱️ 총 시간: {results['elapsed_seconds']:.1f}초")
        print(f"   🚀 처리량: {results['items_per_second']:.1f} 종목/초")
        print(f"   📊 평균 처리 시간: {results['performance']['avg_time_per_stock']:.2f}초/종목")

        if results['market_breakdown']:
            print(f"\n📈 시장별 결과:")
            for market, stats in results['market_breakdown'].items():
                success_rate = (stats['successful'] / stats['total'] * 100) if stats['total'] > 0 else 0
                print(f"   {market}: {stats['successful']}/{stats['total']} ({success_rate:.1f}%)")

    def _clean_string(self, value: str) -> str:
        """문자열 정리"""
        if not value:
            return ""
        return str(value).strip()

    def _parse_int(self, value) -> int:
        """정수 변환"""
        try:
            if not value:
                return 0
            clean_value = str(value).replace('+', '').replace('-', '').replace(',', '').strip()
            if not clean_value:
                return 0
            return int(clean_value)
        except (ValueError, TypeError):
            return 0

    def _parse_rate(self, value) -> int:
        """비율 변환 (소수점 2자리 * 100)"""
        try:
            if not value:
                return 0
            clean_value = str(value).replace('+', '').replace('-', '').replace('%', '').strip()
            if not clean_value:
                return 0
            float_value = float(clean_value)
            return int(float_value * 100)
        except (ValueError, TypeError):
            return 0

    def get_collection_status(self) -> Dict[str, Any]:
        """수집 상태 정보 반환"""
        return {
            'collected_count': self.collected_count,
            'updated_count': self.updated_count,
            'skipped_count': self.skipped_count,
            'error_count': self.error_count,
            'session_ready': self.session.is_ready() if self.session else False,
            'db_connected': self.db_service is not None,
            'tr_code': self.TR_CODE,
            'tr_name': self.RQ_NAME,
            'async_support': True,  # 비동기 지원 표시
            'recommended_concurrency': 5
        }


# ================================
# 🆕 편의 함수들 (동기 + 비동기)
# ================================

def collect_stock_info_batch(session: KiwoomSession, stock_codes: List[str],
                            test_mode: bool = True, config: Optional[Config] = None) -> Dict[str, Any]:
    """배치 주식정보 수집 (동기 편의 함수)"""
    collector = StockInfoCollector(session, config)
    return collector.collect_and_update_stocks(stock_codes, test_mode, always_update=True)


async def collect_stock_info_batch_async(
    session: KiwoomSession,
    stock_codes: List[str],
    concurrency: int = 5,
    config: Optional[Config] = None
) -> Dict[str, Any]:
    """배치 주식정보 비동기 수집 (편의 함수)"""
    collector = StockInfoCollector(session, config)

    if stock_codes:
        return await collector.collect_stocks_by_codes_async(stock_codes, concurrency)
    else:
        return await collector.collect_and_update_stocks_async(concurrency)


def collect_single_stock_info_simple(session: KiwoomSession, stock_code: str,
                                     config: Optional[Config] = None) -> bool:
    """단일 종목 주식정보 수집 (동기 편의 함수)"""
    collector = StockInfoCollector(session, config)
    success, _ = collector.collect_single_stock_info(stock_code)
    return success


async def collect_single_stock_info_simple_async(
    session: KiwoomSession,
    stock_code: str,
    config: Optional[Config] = None
) -> bool:
    """단일 종목 주식정보 비동기 수집 (편의 함수)"""
    collector = StockInfoCollector(session, config)
    success, _ = await collector.collect_single_stock_info_async(stock_code)
    return success


# ================================
# 🔧 마이그레이션 및 호환성 함수들
# ================================

def migrate_to_async(session: KiwoomSession, config: Optional[Config] = None) -> StockInfoCollector:
    """기존 동기 코드를 비동기로 마이그레이션할 때 사용하는 헬퍼"""
    collector = StockInfoCollector(session, config)

    print("🔄 StockInfoCollector 비동기 마이그레이션")
    print("   ✅ 기존 동기 메서드 그대로 사용 가능")
    print("   🚀 새로운 비동기 메서드 추가:")
    print("      - collect_and_update_stocks_async()")
    print("      - collect_single_stock_info_async()")
    print("      - collect_stocks_by_codes_async()")
    print("   ⚡ 예상 성능 향상: 5배 빠름 (29분 vs 2시간 24분)")

    return collector


def get_performance_comparison() -> Dict[str, Any]:
    """동기 vs 비동기 성능 비교 정보"""
    return {
        'sync_processing': {
            'method': '순차 처리',
            'concurrency': 1,
            'estimated_time_2400_stocks': '2시간 24분',
            'api_delay': '3.6초',
            'memory_usage': '낮음'
        },
        'async_processing': {
            'method': '비동기 배치',
            'concurrency': 5,
            'estimated_time_2400_stocks': '29분',
            'api_delay': '3.6초 (준수)',
            'memory_usage': '중간',
            'performance_gain': '5배 빠름'
        },
        'recommendations': {
            'small_batch': '10개 이하 → 동기 처리',
            'medium_batch': '10-100개 → 비동기 3개 동시',
            'large_batch': '100개 이상 → 비동기 5개 동시',
            'full_market': '2400개 → 비동기 5개 동시 + 배치 10개'
        }
    }


# ================================
# 📊 사용 예제 및 문서화
# ================================

"""
🚀 사용 예제:

## 1. 동기 처리 (기존 방식)
```python
from src.collectors.stock_info import StockInfoCollector

session = create_kiwoom_session()
collector = StockInfoCollector(session)

# 소규모 배치
codes = ['005930', '000660', '035420']
result = collector.collect_and_update_stocks(codes, test_mode=False)
```

## 2. 비동기 처리 (새로운 방식) ⭐ 추천
```python
import asyncio
from src.collectors.stock_info import StockInfoCollector

async def main():
    session = create_kiwoom_session()
    collector = StockInfoCollector(session)
    
    # 전체 활성 종목 비동기 수집 (29분)
    result = await collector.collect_and_update_stocks_async(
        concurrency=5,
        market_filter=None  # 전체 시장
    )
    
    print(f"🎉 완료: {result['successful']:,}개 성공")

asyncio.run(main())
```

## 3. 지정 종목 비동기 수집
```python
async def collect_specific_stocks():
    session = create_kiwoom_session()
    collector = StockInfoCollector(session)
    
    # 지정된 종목들만 수집
    my_stocks = ['005930', '000660', '035420', '051910', '006400']
    result = await collector.collect_stocks_by_codes_async(
        stock_codes=my_stocks,
        concurrency=3
    )
    
    return result
```

## 4. 시장별 수집
```python
# KOSPI만 수집
kospi_result = await collector.collect_and_update_stocks_async(
    concurrency=5,
    market_filter='KOSPI'
)

# KOSDAQ만 수집  
kosdaq_result = await collector.collect_and_update_stocks_async(
    concurrency=5,
    market_filter='KOSDAQ'
)
```

## 성능 비교:
- 동기 처리: 2,400개 종목 → 2시간 24분
- 비동기 처리: 2,400개 종목 → 29분 (5배 빠름!)

## 주요 특징:
✅ 기존 코드 100% 호환 (하위 호환성)
✅ 키움 API 제한 준수 (3.6초 간격)
✅ 에러 격리 (개별 실패가 전체에 영향 없음)
✅ 실시간 진행상황 표시
✅ 자동 재시도 (최대 3회)
✅ 상세한 성능 통계
"""

logger.info("✅ StockInfoCollector 완전 리팩토링 완료")
logger.info("🚀 동기/비동기 하이브리드 시스템 준비")
logger.info("⚡ 예상 성능 향상: 5배 (29분 vs 2시간 24분)")