"""
주식 기본정보 수집기 - 간결화 버전
키움 API OPT10001을 사용하여 stock_codes 테이블 기반으로 stocks 테이블 업데이트
- 핵심 기능에 집중
- 불필요한 코드 제거
- stock_codes.is_active = TRUE 종목만 처리
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import time

from ..core.config import Config
from ..core.database import get_database_service
from ..api.base_session import KiwoomSession
from ..api.tr_codes import get_tr_info, create_opt10001_input, validate_input_data

# 로거 설정
logger = logging.getLogger(__name__)


class StockInfoCollector:
    """간결한 주식 기본정보 수집기"""

    def __init__(self, session: KiwoomSession, config: Optional[Config] = None):
        self.session = session
        self.config = config or Config()
        self.db_service = get_database_service()

        # TR 정보
        self.tr_info = get_tr_info('opt10001')
        self.TR_CODE = self.tr_info['code']
        self.RQ_NAME = self.tr_info['name']

        # 수집 통계
        self.total_count = 0
        self.success_count = 0
        self.error_count = 0

        logger.info("📊 StockInfoCollector 초기화 완료")

    def collect_all_active_stocks(self, market_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        활성 종목 전체 수집 (stock_codes.is_active = TRUE)

        Args:
            market_filter: 'KOSPI', 'KOSDAQ' 또는 None(전체)
        """
        start_time = datetime.now()

        print("🚀 주식 기본정보 수집 시작")
        print("=" * 50)

        try:
            # 1. 활성 종목 조회
            active_stocks = self._get_active_stocks(market_filter)
            if not active_stocks:
                print("❌ 활성 종목이 없습니다.")
                return {'error': '활성 종목 없음'}

            self.total_count = len(active_stocks)
            print(f"📊 대상 종목: {self.total_count:,}개")
            if market_filter:
                print(f"📈 시장 필터: {market_filter}")

            # 2. 종목별 수집 실행
            for idx, stock_data in enumerate(active_stocks, 1):
                stock_code = stock_data['code']
                stock_name = stock_data.get('name', '알 수 없음')

                print(f"\n📈 [{idx:,}/{self.total_count:,}] {stock_code} - {stock_name}")

                # 개별 종목 수집
                success = self._collect_single_stock(stock_code)

                if success:
                    self.success_count += 1
                    print(f"✅ 완료")
                else:
                    self.error_count += 1
                    print(f"❌ 실패")

                # API 제한 준수 (마지막 종목 제외)
                if idx < self.total_count:
                    print(f"⏳ {self.config.api_request_delay_ms/1000:.1f}초 대기...")
                    time.sleep(self.config.api_request_delay_ms / 1000)

            # 3. 최종 결과
            elapsed = datetime.now() - start_time
            return self._create_result_summary(elapsed)

        except Exception as e:
            logger.error(f"전체 수집 실패: {e}")
            print(f"❌ 수집 중 오류: {e}")
            return {'error': str(e)}

    def _get_active_stocks(self, market_filter: Optional[str] = None) -> list:
        """활성 종목 조회"""
        try:
            conn = self.db_service._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            if market_filter:
                query = """
                    SELECT code, name, market 
                    FROM stock_codes 
                    WHERE is_active = TRUE AND market = %s
                    ORDER BY code
                """
                cursor.execute(query, (market_filter,))
            else:
                query = """
                    SELECT code, name, market 
                    FROM stock_codes 
                    WHERE is_active = TRUE
                    ORDER BY code
                """
                cursor.execute(query)

            stocks = cursor.fetchall()
            cursor.close()

            return stocks

        except Exception as e:
            logger.error(f"활성 종목 조회 실패: {e}")
            return []

    def _collect_single_stock(self, stock_code: str) -> bool:
        """단일 종목 정보 수집"""
        try:
            # 1. 입력 데이터 생성
            input_data = create_opt10001_input(stock_code)

            # 2. 입력 검증
            if not validate_input_data('opt10001', input_data):
                logger.error(f"{stock_code}: 입력 데이터 검증 실패")
                return False

            # 3. API 호출
            response = self._call_api(stock_code, input_data)
            if not response:
                return False

            # 4. 데이터 파싱
            stock_data = self._parse_response(response, stock_code)
            if not stock_data:
                return False

            # 5. DB 저장
            return self._save_to_db(stock_code, stock_data)

        except Exception as e:
            logger.error(f"{stock_code} 수집 실패: {e}")
            return False

    def _call_api(self, stock_code: str, input_data: dict) -> Optional[dict]:
        """키움 API 호출"""
        try:
            if not self.session or not self.session.is_ready():
                logger.error("키움 세션이 준비되지 않음")
                return None

            connector = self.session.get_connector()
            response = connector.request_tr_data(
                rq_name=self.RQ_NAME,
                tr_code=self.TR_CODE,
                input_data=input_data,
                screen_no="0001"
            )

            if response and response.get('tr_code') == self.TR_CODE:
                return response
            else:
                logger.error(f"{stock_code}: API 응답 검증 실패")
                return None

        except Exception as e:
            logger.error(f"{stock_code} API 호출 실패: {e}")
            return None

    def _parse_response(self, response: dict, stock_code: str) -> Optional[dict]:
        """응답 데이터 파싱"""
        try:
            data_info = response.get('data', {})
            if not data_info.get('parsed', False):
                logger.error(f"{stock_code}: 데이터 파싱되지 않음")
                return None

            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                logger.error(f"{stock_code}: 원시 데이터 없음")
                return None

            # 첫 번째 레코드 사용 (OPT10001은 단일 레코드)
            row_data = raw_data[0]

            # 핵심 필드만 파싱
            stock_data = {
                'name': self._clean_string(row_data.get('종목명', '')),
                'current_price': self._parse_int(row_data.get('현재가', 0)),
                'prev_day_diff': self._parse_int(row_data.get('전일대비', 0)),
                'change_rate': self._parse_float(row_data.get('등락률', 0)),
                'volume': self._parse_int(row_data.get('거래량', 0)),
                'open_price': self._parse_int(row_data.get('시가', 0)),
                'high_price': self._parse_int(row_data.get('고가', 0)),
                'low_price': self._parse_int(row_data.get('저가', 0)),
                'market_cap': self._parse_int(row_data.get('시가총액', 0)),
                'listed_shares': self._parse_int(row_data.get('상장주수', 0)),
                'per_ratio': self._parse_float(row_data.get('PER', 0)),
                'pbr_ratio': self._parse_float(row_data.get('PBR', 0)),
            }

            return stock_data

        except Exception as e:
            logger.error(f"{stock_code} 데이터 파싱 실패: {e}")
            return None

    def _save_to_db(self, stock_code: str, stock_data: dict) -> bool:
        """DB에 저장 (UPSERT)"""
        try:
            return self.db_service.upsert_stock_info(stock_code, stock_data)
        except Exception as e:
            logger.error(f"{stock_code} DB 저장 실패: {e}")
            return False

    def _create_result_summary(self, elapsed) -> Dict[str, Any]:
        """결과 요약 생성"""
        success_rate = (self.success_count / self.total_count * 100) if self.total_count > 0 else 0

        print("\n" + "=" * 50)
        print("🎉 주식 기본정보 수집 완료!")
        print(f"📊 처리 결과:")
        print(f"   ✅ 성공: {self.success_count:,}개 ({success_rate:.1f}%)")
        print(f"   ❌ 실패: {self.error_count:,}개")
        print(f"   📈 전체: {self.total_count:,}개")
        print(f"   ⏱️ 소요시간: {elapsed}")
        print("=" * 50)

        return {
            'total': self.total_count,
            'success': self.success_count,
            'failed': self.error_count,
            'success_rate': success_rate,
            'elapsed_time': str(elapsed)
        }

    # 유틸리티 메서드들
    def _clean_string(self, value: str) -> str:
        """문자열 정리"""
        if not value:
            return ""
        return str(value).strip()

    def _parse_int(self, value) -> int:
        """정수 파싱"""
        try:
            if not value:
                return 0
            # 문자열에서 숫자만 추출
            cleaned = str(value).replace(',', '').replace('+', '').replace('-', '')
            return int(float(cleaned)) if cleaned else 0
        except:
            return 0

    def _parse_float(self, value) -> float:
        """실수 파싱"""
        try:
            if not value:
                return 0.0
            cleaned = str(value).replace(',', '').replace('%', '')
            return float(cleaned) if cleaned else 0.0
        except:
            return 0.0