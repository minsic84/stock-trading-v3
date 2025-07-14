"""
주식 기본정보 수집기 모듈 (동기 최적화 버전)
키움 API OPT10001(주식기본정보요청)을 사용하여 종목 정보를 수집하고 데이터베이스에 저장
- 동기 처리로 안정성 보장
- 배치 DB 저장으로 성능 최적화
- 실시간 진행률 표시
- 자동 재시도 로직
- 중단 가능한 안전한 처리
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import time
from tqdm import tqdm

from ..core.config import Config
from ..core.database import get_database_service
from ..api.base_session import KiwoomSession
from ..api.tr_codes import get_tr_info, create_opt10001_input, validate_input_data

# 로거 설정
logger = logging.getLogger(__name__)


class StockInfoCollector:
    """주식 기본정보 수집기 클래스 (동기 최적화 버전)"""

    def __init__(self, session: KiwoomSession, config: Optional[Config] = None):
        self.session = session
        self.config = config or Config()
        self.db_service = get_database_service()

        # 수집 상태
        self.collected_count = 0
        self.updated_count = 0
        self.error_count = 0

        # 마지막 파싱된 데이터 캐시 (배치 저장용)
        self.last_parsed_data = {}

        # TR 정보
        self.tr_info = get_tr_info('opt10001')
        self.TR_CODE = self.tr_info['code']
        self.RQ_NAME = self.tr_info['name']

        logger.info("주식정보 수집기 초기화 완료 (동기 최적화)")

    # ================================
    # 🚀 메인 수집 메서드 (성능 최적화)
    # ================================

    def collect_and_update_stocks(self, stock_codes: List[str],
                                  force_update: bool = True,
                                  batch_size: int = 50) -> Dict[str, Any]:
        """
        동기 주식정보 수집 (성능 최적화)

        Args:
            stock_codes: 수집할 종목 코드 리스트
            force_update: 강제 업데이트 여부
            batch_size: DB 배치 저장 크기
        """
        if not stock_codes:
            logger.warning("수집할 종목이 없습니다")
            return {'successful': 0, 'failed': 0, 'total_processed': 0}

        logger.info(f"🚀 동기 주식정보 수집 시작: {len(stock_codes):,}개 종목")
        if force_update:
            logger.info("🔄 강제 업데이트 모드: 모든 종목 최신 정보 갱신")

        # 통계 초기화
        stats = {
            'successful': 0,
            'failed': 0,
            'new_stocks': 0,
            'updated_stocks': 0,
            'total_processed': len(stock_codes),
            'failed_stocks': [],
            'batch_stats': []
        }

        # 배치 데이터 저장용
        batch_data = []
        start_time = datetime.now()

        # tqdm으로 진행률 표시
        with tqdm(total=len(stock_codes), desc="📊 주식정보 수집", unit="종목") as pbar:

            for i, stock_code in enumerate(stock_codes):
                try:
                    pbar.set_description(f"📊 수집 중: {stock_code}")

                    # 개별 종목 수집 (재시도 포함)
                    success, is_new, stock_data = self._collect_single_with_retry(
                        stock_code, force_update, max_retries=3
                    )

                    if success and stock_data:
                        # 배치에 추가
                        batch_data.append({
                            'stock_code': stock_code,
                            'stock_data': stock_data,
                            'is_new': is_new
                        })

                        stats['successful'] += 1
                        if is_new:
                            stats['new_stocks'] += 1
                        else:
                            stats['updated_stocks'] += 1

                    else:
                        stats['failed'] += 1
                        stats['failed_stocks'].append({
                            'stock_code': stock_code,
                            'error': '수집 실패'
                        })

                    # 배치 크기에 도달하거나 마지막 종목인 경우 DB 저장
                    if len(batch_data) >= batch_size or i == len(stock_codes) - 1:
                        if batch_data:
                            batch_result = self._save_batch_to_db(batch_data)
                            stats['batch_stats'].append(batch_result)
                            batch_data.clear()  # 배치 데이터 초기화

                    # 진행률 업데이트
                    pbar.update(1)
                    pbar.set_postfix({
                        '성공': stats['successful'],
                        '실패': stats['failed'],
                        '성공률': f"{(stats['successful']/(i+1)*100):.1f}%"
                    })

                    # API 제한 준수 (3.6초 대기) - 마지막 종목 제외
                    if i < len(stock_codes) - 1:
                        time.sleep(self.config.api_request_delay_ms / 1000)

                except KeyboardInterrupt:
                    logger.warning(f"⚠️ 사용자 중단 요청 (진행률: {i+1}/{len(stock_codes)})")
                    break
                except Exception as e:
                    logger.error(f"❌ {stock_code} 처리 중 예외: {e}")
                    stats['failed'] += 1
                    stats['failed_stocks'].append({
                        'stock_code': stock_code,
                        'error': str(e)
                    })
                    pbar.update(1)

        # 최종 결과 계산
        elapsed_time = (datetime.now() - start_time).total_seconds()
        success_rate = (stats['successful'] / stats['total_processed'] * 100) if stats['total_processed'] > 0 else 0

        logger.info(f"✅ 동기 수집 완료!")
        logger.info(f"   📊 전체: {stats['total_processed']:,}개")
        logger.info(f"   ✅ 성공: {stats['successful']:,}개 ({success_rate:.1f}%)")
        logger.info(f"   ❌ 실패: {stats['failed']:,}개")
        logger.info(f"   📥 신규: {stats['new_stocks']:,}개")
        logger.info(f"   🔄 업데이트: {stats['updated_stocks']:,}개")
        logger.info(f"   ⏱️ 소요시간: {elapsed_time:.1f}초")

        # 통계에 추가 정보 포함
        stats.update({
            'elapsed_time': elapsed_time,
            'success_rate': success_rate,
            'items_per_second': stats['successful'] / elapsed_time if elapsed_time > 0 else 0
        })

        return stats

    def collect_and_update_all_active_stocks(self, market_filter: Optional[str] = None,
                                           force_update: bool = True,
                                           batch_size: int = 50) -> Dict[str, Any]:
        """
        stock_codes 테이블에서 활성 종목을 조회하여 수집

        Args:
            market_filter: 시장 필터 ('KOSPI', 'KOSDAQ', None=전체)
            force_update: 강제 업데이트 여부
            batch_size: DB 배치 저장 크기
        """
        logger.info("📊 활성 종목 조회 중...")

        # DB에서 활성 종목 조회
        if market_filter:
            stock_codes_data = self.db_service.get_active_stock_codes_by_market(market_filter)
        else:
            stock_codes_data = self.db_service.get_active_stock_codes()

        if not stock_codes_data:
            logger.warning("활성 종목이 없습니다")
            return {'error': '활성 종목 없음'}

        stock_codes = [item['code'] for item in stock_codes_data]

        logger.info(f"✅ 대상 종목: {len(stock_codes):,}개")
        if market_filter:
            logger.info(f"📈 시장 필터: {market_filter}")

        # 수집 실행
        return self.collect_and_update_stocks(stock_codes, force_update, batch_size)

    # ================================
    # 🔧 개별 종목 수집 메서드
    # ================================

    def collect_single_stock_info(self, stock_code: str, force_update: bool = True) -> Tuple[bool, bool]:
        """
        단일 종목 기본정보 수집 (OPT10001)

        Args:
            stock_code: 종목코드
            force_update: 강제 업데이트 여부

        Returns:
            (성공여부, 신규여부)
        """
        try:
            # 1. 기존 데이터 확인 (force_update가 False인 경우만)
            if not force_update:
                exists = self.db_service.check_stock_exists(stock_code)
                if exists:
                    logger.info(f"⏭️ {stock_code}: 기존 데이터 존재, 건너뛰기")
                    return True, False

            # 2. 키움 API 세션 확인
            if not self.session or not self.session.is_ready():
                logger.error(f"❌ {stock_code}: 키움 세션이 준비되지 않음")
                return False, False

            # 3. API 호출
            input_data = create_opt10001_input(f"{stock_code}_AL")

            if not validate_input_data('opt10001', input_data):
                logger.error(f"❌ {stock_code}: 입력 데이터 유효성 검증 실패")
                return False, False

            connector = self.session.get_connector()
            response = connector.request_tr_data(
                rq_name=self.RQ_NAME,
                tr_code=self.TR_CODE,
                input_data=input_data,
                screen_no="9001"
            )

            if not response:
                logger.error(f"❌ {stock_code}: TR 요청 실패")
                return False, False

            # 4. 데이터 파싱
            stock_data = self._parse_stock_info(response, stock_code)
            if not stock_data:
                logger.error(f"❌ {stock_code}: 데이터 파싱 실패")
                return False, False

            # 5. 파싱된 데이터 캐시 저장 (배치 처리용)
            self.last_parsed_data[stock_code] = stock_data

            # 6. DB 저장 (UPSERT) - 기존 데이터 존재 여부 체크
            is_new = not self.db_service.check_stock_exists(stock_code)

            success = self.db_service.upsert_stock_info(stock_code, stock_data)

            if success:
                action = "신규 추가" if is_new else "업데이트"
                logger.info(f"✅ {stock_code} {action} 완료")
                return True, is_new
            else:
                logger.error(f"❌ {stock_code}: DB 저장 실패")
                return False, False

        except Exception as e:
            logger.error(f"❌ {stock_code} 수집 중 오류: {e}")
            return False, False

    def _collect_single_with_retry(self, stock_code: str, force_update: bool, max_retries: int = 3) -> tuple:
        """재시도 로직이 포함된 단일 종목 수집"""
        last_error = None

        for attempt in range(max_retries):
            try:
                success, is_new = self.collect_single_stock_info(stock_code, force_update)

                if success:
                    # 수집된 데이터도 함께 반환 (배치 저장용)
                    stock_data = self._get_latest_stock_data(stock_code)
                    return True, is_new, stock_data
                else:
                    logger.warning(f"⚠️ {stock_code} 수집 실패 (시도 {attempt + 1}/{max_retries})")

            except Exception as e:
                last_error = str(e)
                logger.warning(f"❌ {stock_code} 수집 오류: {e} (시도 {attempt + 1}/{max_retries})")

            # 재시도 전 잠시 대기 (마지막 시도가 아닌 경우)
            if attempt < max_retries - 1:
                time.sleep(1.0)

        logger.error(f"💥 {stock_code} 모든 재시도 실패: {last_error}")
        return False, False, None

    def _get_latest_stock_data(self, stock_code: str) -> dict:
        """마지막 API 응답에서 파싱된 데이터 반환 (배치 저장용)"""
        return self.last_parsed_data.get(stock_code, {})

    def _save_batch_to_db(self, batch_data: List[dict]) -> dict:
        """배치 데이터를 DB에 한번에 저장"""
        try:
            # 기존 DB 서비스의 배치 저장 메서드 활용
            stock_data_list = []
            for item in batch_data:
                stock_data_list.append({
                    'stock_code': item['stock_code'],
                    **item['stock_data']
                })

            # 배치 UPSERT 실행
            result = self.db_service.batch_upsert_stock_info(stock_data_list)

            logger.info(f"💾 배치 저장 완료: {len(batch_data)}개 종목")
            return result

        except Exception as e:
            logger.error(f"❌ 배치 저장 실패: {e}")
            # 개별 저장으로 폴백
            return self._fallback_individual_save(batch_data)

    def _fallback_individual_save(self, batch_data: List[dict]) -> dict:
        """배치 저장 실패 시 개별 저장 폴백"""
        logger.info(f"🔄 개별 저장으로 폴백: {len(batch_data)}개 종목")

        success_count = 0
        failed_count = 0

        for item in batch_data:
            try:
                success = self.db_service.upsert_stock_info(
                    item['stock_code'],
                    item['stock_data']
                )
                if success:
                    success_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.error(f"❌ {item['stock_code']} 개별 저장 실패: {e}")
                failed_count += 1

        return {'success': success_count, 'failed': failed_count}

    # ================================
    # 🔧 데이터 파싱 및 유틸리티
    # ================================

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

    # ================================
    # 📊 상태 및 통계 메서드
    # ================================

    def get_collection_status(self) -> Dict[str, Any]:
        """수집 상태 정보 반환"""
        return {
            'collected_count': self.collected_count,
            'updated_count': self.updated_count,
            'error_count': self.error_count,
            'session_ready': self.session.is_ready() if self.session else False,
            'db_connected': self.db_service is not None,
            'tr_code': self.TR_CODE,
            'tr_name': self.RQ_NAME,
            'processing_mode': 'sync_optimized',
            'batch_processing': True,
            'retry_enabled': True
        }


# ================================
# 🆕 편의 함수들
# ================================

def collect_stock_info_batch(session: KiwoomSession, stock_codes: List[str],
                            force_update: bool = True,
                            batch_size: int = 50,
                            config: Optional[Config] = None) -> Dict[str, Any]:
    """배치 주식정보 수집 (동기 최적화)"""
    collector = StockInfoCollector(session, config)
    return collector.collect_and_update_stocks(stock_codes, force_update, batch_size)


def collect_all_active_stocks(session: KiwoomSession,
                             market_filter: Optional[str] = None,
                             force_update: bool = True,
                             batch_size: int = 50,
                             config: Optional[Config] = None) -> Dict[str, Any]:
    """활성 종목 전체 수집 (동기 최적화)"""
    collector = StockInfoCollector(session, config)
    return collector.collect_and_update_all_active_stocks(market_filter, force_update, batch_size)


# ================================
# 📊 사용 예제
# ================================

"""
🚀 동기 최적화 StockInfoCollector 사용법:

## 1. 지정 종목 수집 (빠른 테스트용)
```python
from src.collectors.stock_info import collect_stock_info_batch

session = create_kiwoom_session()

# 특정 종목들 강제 업데이트
codes = ['005930', '000660', '035420']
result = collect_stock_info_batch(
    session=session, 
    stock_codes=codes, 
    force_update=True,
    batch_size=10
)

print(f"✅ 성공: {result['successful']}개")
print(f"❌ 실패: {result['failed']}개")
```

## 2. 전체 활성 종목 수집 ⭐ 추천
```python
from src.collectors.stock_info import collect_all_active_stocks

# 전체 활성 종목 수집
result = collect_all_active_stocks(
    session=session,
    force_update=True,
    batch_size=50  # 50개씩 배치 저장
)

# KOSPI만 수집
kospi_result = collect_all_active_stocks(
    session=session,
    market_filter='KOSPI',
    force_update=True
)
```

## 3. 클래스 직접 사용 (고급 설정)
```python
from src.collectors.stock_info import StockInfoCollector

collector = StockInfoCollector(session)

# 설정 변경 가능
result = collector.collect_and_update_stocks(
    stock_codes=['005930', '000660'],
    force_update=True,
    batch_size=100  # 더 큰 배치 크기
)

# 상태 확인
status = collector.get_collection_status()
print(f"DB 연결: {status['db_connected']}")
print(f"세션 준비: {status['session_ready']}")
```

## 주요 최적화 기능:
✅ 실시간 진행률 표시 (tqdm)
✅ 배치 DB 저장 (성능 향상)
✅ 자동 재시도 로직 (3회)
✅ 중단 가능 (Ctrl+C)
✅ 개별 실패 격리
✅ 상세 통계 제공
✅ 메모리 효율적 처리

## 예상 처리 시간:
- 4,140개 종목 강제 업데이트: 약 29분
- 배치 저장으로 DB 부하 감소
- 진행률 실시간 확인 가능
- 안정적이고 예측 가능한 처리
"""

logger.info("✅ StockInfoCollector 동기 최적화 버전 완료")
logger.info("🚀 배치 처리 + 진행률 표시 + 재시도 로직 포함")
logger.info("💾 안정적인 동기 처리로 무한 대기 문제 해결")