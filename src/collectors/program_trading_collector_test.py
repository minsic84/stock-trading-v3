"""
프로그램매매 데이터 수집기 모듈 (OPT90013 기반)
키움 API OPT90013(프로그램매매추이요청)을 사용하여 프로그램매매 데이터 수집
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import time

from ..core.config import Config
from ..core.database import get_database_service
from ..api.base_session import KiwoomSession

# 로거 설정
logger = logging.getLogger(__name__)


class ProgramTradingCollector:
    """프로그램매매 데이터 수집기 클래스 (OPT90013 사용)"""

    def __init__(self, session: KiwoomSession, config: Optional[Config] = None):
        self.session = session
        self.config = config or Config()
        self.db_service = None

        # 수집 상태
        self.collected_count = 0
        self.error_count = 0

        # TR 정보 (🔧 OPT90013으로 변경)
        self.TR_CODE = 'opt90013'
        self.RQ_NAME = 'program_trading_req'  # 영문으로 설정

        self._setup()

    def _setup(self):
        """초기화 설정"""
        try:
            # 데이터베이스 서비스 초기화
            self.db_service = get_database_service()
            logger.info("프로그램매매 수집기 초기화 완료")
        except Exception as e:
            logger.error(f"프로그램매매 수집기 초기화 실패: {e}")
            raise

    def collect_single_stock_program_trading(self, stock_code: str,
                                           target_date: str = "20250710",
                                           time_type: str = "2",
                                           amount_type: str = "1") -> Tuple[bool, bool]:
        """
        단일 종목 프로그램매매 데이터 수집 (OPT90013 기반)

        Args:
            stock_code: 종목코드
            target_date: 날짜 (YYYYMMDD)
            time_type: 시간일자구분 (2:일자별)
            amount_type: 금액수량구분 (1:금액, 2:수량)

        Returns:
            (성공여부, 신규데이터여부)
        """
        try:
            print(f"🔄 {stock_code} 프로그램매매 데이터 수집 시작...")

            # 🔧 OPT90013 매뉴얼 기반 입력 데이터 생성
            input_data = self._create_program_trading_input(
                stock_code, target_date, time_type, amount_type
            )

            print(f"📋 입력 데이터: {input_data}")

            # 입력 데이터 수동 검증
            if not self._manual_validate_input(input_data):
                print(f"❌ {stock_code}: 프로그램매매 입력 검증 실패")
                return False, False

            # TR 요청
            connector = self.session.get_connector()
            print(f"🔄 {stock_code} TR 요청 중... (TR: {self.TR_CODE})")

            response = connector.request_tr_data(
                rq_name=self.RQ_NAME,
                tr_code=self.TR_CODE,
                input_data=input_data,
                screen_no="9013"  # OPT90013용 화면번호
            )

            if not response:
                print(f"❌ {stock_code}: 프로그램매매 TR 요청 실패")
                return False, False

            print(f"📥 {stock_code} TR 응답 받음: {response is not None}")
            print(f"🔍 응답 내용: {response}")

            # 데이터 파싱
            program_data = self._parse_program_trading_data(response, stock_code)
            if not program_data:
                print(f"❌ {stock_code}: 프로그램매매 파싱 실패")
                return False, False

            print(f"✅ {stock_code} 프로그램매매 파싱 성공: {len(program_data)}개")

            # 간단 저장 시뮬레이션
            print(f"💾 {stock_code} 프로그램매매 저장 시뮬레이션 완료")
            self.collected_count += 1
            return True, True

        except Exception as e:
            print(f"❌ {stock_code} 프로그램매매 수집 오류: {e}")
            logger.error(f"{stock_code} 프로그램매매 수집 실패: {e}")
            self.error_count += 1
            return False, False

        finally:
            # API 요청 제한 준수
            time.sleep(self.config.api_request_delay_ms / 1000)

    def _create_program_trading_input(self, stock_code: str, date: str = "20250710",
                                    time_type: str = "2", amount_type: str = "1") -> dict:
        """OPT90013 매뉴얼 기반 입력 데이터 생성"""
        return {
            '시간일자구분': time_type,      # 2:일자별
            '금액수량구분': amount_type,     # 1:금액, 2:수량
            '종목코드': stock_code,          # 종목코드
            '날짜': date                     # YYYYMMDD
        }

    def _manual_validate_input(self, input_data: dict) -> bool:
        """수동 입력 데이터 검증"""
        required_fields = ['시간일자구분', '금액수량구분', '종목코드', '날짜']

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

    # program_trading_collector.py의 파싱 함수 수정

    def _parse_program_trading_data(self, response: Dict[str, Any], stock_code: str) -> List[Dict[str, Any]]:
        """OPT90013 실제 필드 구조 기반 프로그램매매 데이터 파싱"""
        try:
            print(f"=== {stock_code} 프로그램매매 파싱 시작 ===")

            # 기본 검증
            data_info = response.get('data', {})
            if not data_info.get('parsed', False):
                return []

            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                return []

            program_data = []

            for i, row_data in enumerate(raw_data):
                try:
                    # 🔧 실제 확인된 필드들로 정확한 파싱
                    parsed_row = {
                        # 기본 정보
                        'date': row_data.get('일자', '').strip(),
                        'current_price': self._parse_price(row_data.get('현재가', '')),
                        'price_symbol': row_data.get('대비기호', '').strip(),
                        'price_diff': self._parse_price(row_data.get('전일대비', '')),
                        'change_rate': self._parse_rate(row_data.get('등락율', '')),
                        'volume': self._parse_int(row_data.get('거래량', '0')),

                        # 프로그램매매 금액 (🔧 정확한 필드 매핑)
                        'program_sell_amount': self._parse_int(row_data.get('프로그램매도금액', '0')),
                        'program_buy_amount': self._parse_int(row_data.get('프로그램매수금액', '0')),
                        'program_net_amount': self._parse_int(row_data.get('프로그램순매수금액', '0')),  # ← 정확한 순매수
                        'program_net_amount_change': self._parse_int(row_data.get('프로그램순매수금액증감', '0')),  # ← 증감분 별도

                        # 프로그램매매 수량 (🔧 정확한 필드 매핑)
                        'program_sell_volume': self._parse_int(row_data.get('프로그램매도수량', '0')),
                        'program_buy_volume': self._parse_int(row_data.get('프로그램매수수량', '0')),
                        'program_net_volume': self._parse_int(row_data.get('프로그램순매수수량', '0')),  # ← 정확한 순매수
                        'program_net_volume_change': self._parse_int(row_data.get('프로그램순매수수량증감', '0')),  # ← 증감분 별도

                        # 기타 정보
                        'reference_time': row_data.get('기준가시간', '').strip(),
                        'short_repay_total': row_data.get('대차거래상환주수합', '').strip(),
                        'balance_total': row_data.get('잔고수주합', '').strip(),
                        'exchange_type': row_data.get('거래소구분', '').strip(),

                        # 메타 정보
                        'stock_code': stock_code,
                        'parsed_at': datetime.now(),
                        'raw_data': row_data  # 원시 데이터 보존
                    }

                    # 유효한 데이터만 추가 (날짜가 있는 경우)
                    if parsed_row['date']:
                        program_data.append(parsed_row)

                except Exception as e:
                    print(f"❌ 행 {i} 파싱 오류: {e}")
                    continue

            print(f"✅ 프로그램매매 파싱 완료: {len(program_data)}개")

            # 샘플 데이터 출력 (정리된 형태)
            if program_data:
                sample = program_data[0]
                print(
                    f"📊 샘플 (정리됨): {sample['date']} - 매수:{sample['program_buy_amount']:,}, 매도:{sample['program_sell_amount']:,}, 순매수:{sample['program_net_amount']:,}")

            return program_data

        except Exception as e:
            print(f"❌ 프로그램매매 파싱 실패: {e}")
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

    def _parse_rate(self, rate_str: str) -> float:
        """등락율 문자열 파싱 (+0.99, -1.63 등)"""
        if not rate_str:
            return 0.0

        try:
            # + 또는 - 부호 처리
            clean_rate = rate_str.replace('+', '').replace('-', '')
            sign = -1 if rate_str.strip().startswith('-') else 1
            return float(clean_rate) * sign if clean_rate else 0.0
        except:
            return 0.0

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
def test_single_program_trading(stock_code: str = "005930", session: Optional[KiwoomSession] = None) -> bool:
    """단일 종목 프로그램매매 테스트 함수"""
    try:
        print(f"🧪 {stock_code} 프로그램매매 테스트 시작")

        if not session:
            from ..api.base_session import create_kiwoom_session
            session = create_kiwoom_session()

        if not session:
            print("❌ 키움 세션 생성 실패")
            return False

        collector = ProgramTradingCollector(session)

        # 다양한 매개변수로 테스트
        test_cases = [
            {"time_type": "2", "amount_type": "1"},  # 일자별, 금액
            {"time_type": "2", "amount_type": "2"},  # 일자별, 수량
        ]

        for i, params in enumerate(test_cases):
            print(f"\n📋 테스트 케이스 {i+1}: {params}")
            success, _ = collector.collect_single_stock_program_trading(stock_code, **params)

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
        print(f"❌ 프로그램매매 테스트 실패: {e}")
        return False


if __name__ == "__main__":
    print("🧪 프로그램매매 수집기 단독 테스트 (OPT90013)")
    test_single_program_trading()