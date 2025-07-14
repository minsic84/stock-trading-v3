"""
수급 데이터 수집기 모듈 (매뉴얼 기반 수정)
키움 API OPT10060(일별수급데이터요청)을 사용하여 투자자별 수급 데이터 수집
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import time

from ..core.config import Config
from ..core.database import get_database_service
from ..api.base_session import KiwoomSession
from ..api.tr_codes import get_tr_info, validate_input_data

# 로거 설정
logger = logging.getLogger(__name__)


class SupplyDemandCollector:
    """수급 데이터 수집기 클래스 (OPT10060 사용)"""

    def __init__(self, session: KiwoomSession, config: Optional[Config] = None):
        self.session = session
        self.config = config or Config()
        self.db_service = None

        # 수집 상태
        self.collected_count = 0
        self.error_count = 0

        # TR 정보
        self.TR_CODE = 'opt10060'
        self.RQ_NAME = '일별수급데이터요청'

        self._setup()

    def _setup(self):
        """초기화 설정"""
        try:
            # 데이터베이스 서비스 초기화
            self.db_service = get_database_service()
            logger.info("수급데이터 수집기 초기화 완료")
        except Exception as e:
            logger.error(f"수급데이터 수집기 초기화 실패: {e}")
            raise

    def collect_single_stock_supply_demand(self, stock_code: str,
                                          target_date: str = "",
                                          amount_type: str = "1",
                                          trade_type: str = "0",
                                          unit_type: str = "1000") -> Tuple[bool, bool]:
        """
        단일 종목 수급 데이터 수집 (매뉴얼 기반)

        Args:
            stock_code: 종목코드 (예: '005930')
            target_date: 일자 YYYYMMDD (빈값이면 최근일)
            amount_type: 금액수량구분 (1:금액, 2:수량)
            trade_type: 매매구분 (0:순매수, 1:매수, 2:매도)
            unit_type: 단위구분 (1000:천주, 1:단주)

        Returns:
            (성공여부, 신규데이터여부)
        """
        try:
            print(f"🔄 {stock_code} 수급데이터 수집 시작...")

            # 🔧 매뉴얼 기반 입력 데이터 생성
            input_data = self._create_supply_demand_input(
                stock_code, target_date, amount_type, trade_type, unit_type
            )

            print(f"📋 입력 데이터: {input_data}")

            # 입력 데이터 수동 검증 (tr_codes.py 검증 건너뛰기)
            if not self._manual_validate_input(input_data):
                print(f"❌ {stock_code}: 수급데이터 입력 검증 실패")
                return False, False

            # TR 요청
            connector = self.session.get_connector()
            print(f"🔄 {stock_code} TR 요청 중... (TR: {self.TR_CODE})")

            response = connector.request_tr_data(
                rq_name=self.RQ_NAME,
                tr_code=self.TR_CODE,
                input_data=input_data,
                screen_no="9060"
            )

            if not response:
                print(f"❌ {stock_code}: 수급데이터 TR 요청 실패")
                return False, False

            print(f"📥 {stock_code} TR 응답 받음: {response is not None}")
            print(f"🔍 응답 내용: {response}")

            # 데이터 파싱
            supply_data = self._parse_supply_demand_data(response, stock_code)
            if not supply_data:
                print(f"❌ {stock_code}: 수급데이터 파싱 실패")
                return False, False

            print(f"✅ {stock_code} 수급데이터 파싱 성공: {len(supply_data)}개")

            # 간단 저장 (실제 DB 저장은 나중에)
            print(f"💾 {stock_code} 수급데이터 저장 시뮬레이션 완료")
            self.collected_count += 1
            return True, True

        except Exception as e:
            print(f"❌ {stock_code} 수급데이터 수집 오류: {e}")
            logger.error(f"{stock_code} 수급데이터 수집 실패: {e}")
            self.error_count += 1
            return False, False

        finally:
            # API 요청 제한 준수
            time.sleep(self.config.api_request_delay_ms / 1000)

    def _create_supply_demand_input(self, stock_code: str, date: str = "",
                                   amount_type: str = "1", trade_type: str = "0",
                                   unit_type: str = "1000") -> dict:
        """매뉴얼 기반 OPT10060 입력 데이터 생성"""
        return {
            '일자': '20250710',             # YYYYMMDD (빈값이면 최근일)
            '종목코드': stock_code,          # 종목코드
            '금액수량구분': amount_type,     # 1:금액, 2:수량
            '매매구분': trade_type,          # 0:순매수, 1:매수, 2:매도
            '단위구분': unit_type            # 1000:천주, 1:단주
        }

    def _manual_validate_input(self, input_data: dict) -> bool:
        """수동 입력 데이터 검증"""
        required_fields = ['일자', '종목코드', '금액수량구분', '매매구분', '단위구분']

        for field in required_fields:
            if field not in input_data:
                print(f"❌ 필수 입력 필드 누락: {field}")
                return False

        # 종목코드는 반드시 있어야 함
        if not input_data['종목코드']:
            print(f"❌ 종목코드가 비어있음")
            return False

        print(f"✅ 입력 데이터 검증 통과")
        return True

    # supply_demand_collector.py의 _parse_supply_demand_data 메서드 수정

    def _parse_supply_demand_data(self, response: Dict[str, Any], stock_code: str) -> List[Dict[str, Any]]:
        """실제 필드 구조 기반 수급데이터 파싱"""
        try:
            print(f"=== {stock_code} 수급데이터 파싱 시작 ===")

            # 기본 검증
            data_info = response.get('data', {})
            if not data_info.get('parsed', False):
                return []

            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                return []

            supply_data = []

            for i, row_data in enumerate(raw_data):
                try:
                    # 🔧 실제 확인된 필드들로 파싱
                    parsed_row = {
                        # 기본 정보
                        'date': row_data.get('일자', '').strip(),
                        'current_price': self._parse_price(row_data.get('현재가', '')),
                        'price_diff': self._parse_price(row_data.get('전일대비', '')),
                        'trading_value': self._parse_int(row_data.get('누적거래대금', '0')),

                        # 수급 정보 (실제 필드명 사용)
                        'individual': self._parse_int(row_data.get('개인투자자', '0')),  # 개인
                        'foreign': self._parse_int(row_data.get('내외국인', '0')),  # 외국인 (실제 데이터)
                        'institution_total': self._parse_int(row_data.get('기관계', '0')),  # 기관 합계

                        # 세부 기관별
                        'financial_investment': self._parse_int(row_data.get('금융투자', '0')),
                        'insurance': self._parse_int(row_data.get('보험', '0')),
                        'investment_trust': self._parse_int(row_data.get('투신', '0')),
                        'other_financial': self._parse_int(row_data.get('기타금융', '0')),
                        'bank': self._parse_int(row_data.get('은행', '0')),
                        'pension': self._parse_int(row_data.get('연기금등', '0')),
                        'private_fund': self._parse_int(row_data.get('사모펀드', '0')),
                        'government': self._parse_int(row_data.get('국가', '0')),
                        'other_corporation': self._parse_int(row_data.get('기타법인', '0')),

                        # 메타 정보
                        'stock_code': stock_code,
                        'parsed_at': datetime.now(),
                        'raw_data': row_data  # 원시 데이터 보존
                    }

                    # 유효한 데이터만 추가 (날짜가 있는 경우)
                    if parsed_row['date']:
                        supply_data.append(parsed_row)

                except Exception as e:
                    print(f"❌ 행 {i} 파싱 오류: {e}")
                    continue

            print(f"✅ 수급데이터 파싱 완료: {len(supply_data)}개")

            # 샘플 데이터 출력 (정리된 형태)
            if supply_data:
                sample = supply_data[0]
                print(
                    f"📊 샘플 (정리됨): {sample['date']} - 개인:{sample['individual']:,}, 외국인:{sample['foreign']:,}, 기관:{sample['institution_total']:,}")

            return supply_data

        except Exception as e:
            print(f"❌ 수급데이터 파싱 실패: {e}")
            return []

    def _parse_price(self, price_str: str) -> int:
        """가격 문자열 파싱 (+61000, -1000 등)"""
        if not price_str:
            return 0

        try:
            # + 또는 - 부호 처리
            clean_price = price_str.replace('+', '').replace('-', '').replace(',', '')
            sign = -1 if price_str.strip().startswith('-') else 1
            return int(clean_price) * sign if clean_price.isdigit() else 0
        except:
            return 0

    # 유틸리티 메서드들
    def _clean_string(self, value) -> str:
        """문자열 정리"""
        if not value:
            return ""
        return str(value).strip()

    def _parse_int(self, value) -> int:
        """정수 변환 (키움 API 특수 처리)"""
        if not value:
            return 0

        try:
            # 문자열에서 숫자만 추출
            if isinstance(value, str):
                # 부호 처리
                sign = -1 if value.strip().startswith('-') else 1
                numeric_str = ''.join(c for c in value if c.isdigit())

                if numeric_str:
                    return int(numeric_str) * sign
                else:
                    return 0
            else:
                return int(value)
        except (ValueError, TypeError):
            return 0

    def get_collection_stats(self) -> Dict[str, Any]:
        """수집 통계 반환"""
        return {
            'collected_count': self.collected_count,
            'error_count': self.error_count,
            'tr_code': self.TR_CODE,
            'tr_name': self.RQ_NAME
        }


# 테스트 편의 함수
def test_single_supply_demand(stock_code: str = "005930", session: Optional[KiwoomSession] = None) -> bool:
    """단일 종목 수급데이터 테스트 함수"""
    try:
        print(f"🧪 {stock_code} 수급데이터 테스트 시작")

        if not session:
            from ..api.base_session import create_kiwoom_session
            session = create_kiwoom_session()

        if not session:
            print("❌ 키움 세션 생성 실패")
            return False

        collector = SupplyDemandCollector(session)

        # 다양한 매개변수로 테스트
        test_cases = [
            {"amount_type": "1", "trade_type": "0", "unit_type": "1000"},  # 금액, 순매수, 천주
            {"amount_type": "2", "trade_type": "0", "unit_type": "1000"},  # 수량, 순매수, 천주
        ]

        for i, params in enumerate(test_cases):
            print(f"\n📋 테스트 케이스 {i+1}: {params}")
            success, _ = collector.collect_single_stock_supply_demand(stock_code, **params)

            if success:
                print(f"✅ 테스트 케이스 {i+1} 성공")
            else:
                print(f"❌ 테스트 케이스 {i+1} 실패")
                break

        # 통계 출력
        stats = collector.get_collection_stats()
        print(f"\n📊 테스트 통계: {stats}")

        return stats['collected_count'] > 0

    except Exception as e:
        print(f"❌ 수급데이터 테스트 실패: {e}")
        return False


if __name__ == "__main__":
    print("🧪 수급데이터 수집기 단독 테스트")
    test_single_supply_demand()