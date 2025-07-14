"""
분봉 데이터 수집기 모듈 (지정 종목용) - tr_codes.py 완전 연동
키움 API OPT10080(분봉차트조회)을 사용하여 1분, 3분, 5분 등 분봉 데이터 수집
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import time

from ..core.config import Config
from ..core.database import get_database_service
from ..api.base_session import KiwoomSession
from ..api.tr_codes import get_tr_info, create_opt10080_input, validate_input_data

# 로거 설정
logger = logging.getLogger(__name__)


class MinuteDataCollector:
    """분봉 데이터 수집기 클래스 (OPT10080 사용)"""

    def __init__(self, session: KiwoomSession, config: Optional[Config] = None):
        self.session = session
        self.config = config or Config()
        self.db_service = None

        # 수집 상태
        self.collected_count = 0
        self.error_count = 0

        # TR 정보 (tr_codes.py에서 가져오기)
        self.tr_info = get_tr_info('opt10080')
        self.TR_CODE = self.tr_info['code']
        self.RQ_NAME = self.tr_info['name']

        # 지원하는 분봉 타입 (tr_codes.py와 일치)
        self.MINUTE_TYPES = {
            '1': '1분봉',
            '3': '3분봉',
            '5': '5분봉',
            '10': '10분봉',
            '15': '15분봉',
            '30': '30분봉',
            '45': '45분봉',
            '60': '60분봉'
        }

        self._setup()

    def _setup(self):
        """초기화 설정"""
        try:
            # 데이터베이스 서비스 초기화 (테스트 단계에서는 선택적)
            try:
                self.db_service = get_database_service()
                logger.info("분봉데이터 수집기 - DB 서비스 연결 완료")
            except Exception as db_error:
                self.db_service = None
                print(f"⚠️ 데이터베이스 서비스 초기화 건너뜀 (테스트 모드): {db_error}")

            logger.info(f"분봉데이터 수집기 초기화 완료 - TR: {self.TR_CODE}")
        except Exception as e:
            logger.error(f"분봉데이터 수집기 초기화 실패: {e}")
            raise

    def collect_single_stock_minute_data(self, stock_code: str,
                                         minute_type: str = "3",
                                         target_date: str = "",
                                         adj_price: str = "1") -> Tuple[bool, bool]:
        """
        단일 종목 분봉 데이터 수집

        Args:
            stock_code: 종목코드
            minute_type: 시간구분 (1:1분, 3:3분, 5:5분, 10:10분, 15:15분, 30:30분, 45:45분, 60:60분)
            target_date: 조회일자 (사용하지 않음, 호환성 유지)
            adj_price: 수정주가구분 (1:수정주가, 0:원주가)

        Returns:
            (성공여부, 신규데이터여부)
        """
        try:
            minute_name = self.MINUTE_TYPES.get(minute_type, f"{minute_type}분봉")
            print(f"🔄 {stock_code} {minute_name} 데이터 수집 시작...")

            # 분봉 타입 검증
            if minute_type not in self.MINUTE_TYPES:
                print(f"❌ 지원하지 않는 분봉 타입: {minute_type}")
                print(f"💡 지원 가능한 분봉 타입: {list(self.MINUTE_TYPES.keys())}")
                return False, False

            # tr_codes.py의 함수 사용하여 입력 데이터 생성
            input_data = create_opt10080_input(stock_code, minute_type, adj_price)
            print(f"📥 입력 데이터: {input_data}")

            # tr_codes.py의 함수 사용하여 입력 데이터 유효성 검증
            if not validate_input_data('opt10080', input_data):
                print(f"❌ {stock_code}: {minute_name} 입력 검증 실패")
                return False, False

            print(f"✅ {stock_code} {minute_name} 입력 데이터 검증 통과")

            # TR 요청
            connector = self.session.get_connector()
            print(f"🔄 {stock_code} {minute_name} TR 요청 중... (TR: {self.TR_CODE})")

            response = connector.request_tr_data(
                rq_name=self.RQ_NAME,
                tr_code=self.TR_CODE,
                input_data=input_data,
                screen_no="9080"
            )

            print(f"📥 {stock_code} {minute_name} TR 응답 받음: {response is not None}")

            if not response:
                print(f"❌ {stock_code}: {minute_name} TR 요청 실패")
                self.error_count += 1
                return False, False

            # 데이터 파싱
            print(f"🔧 {stock_code} {minute_name} 데이터 파싱 시작...")
            minute_data = self._parse_minute_data(response, stock_code, minute_type)

            if not minute_data:
                print(f"❌ {stock_code}: {minute_name} 파싱 실패")
                self.error_count += 1
                return False, False

            print(f"✅ {stock_code} {minute_name} 파싱 완료: {len(minute_data)}개 레코드")

            # 데이터베이스 저장 (DB 서비스가 있는 경우에만)
            if self.db_service:
                try:
                    # save_minute_data 메서드가 구현되어 있는지 확인
                    if hasattr(self.db_service, 'save_minute_data'):
                        success = self.db_service.save_minute_data(stock_code, minute_data, minute_type)
                        if success:
                            print(f"✅ {stock_code} {minute_name} DB 저장 성공: {len(minute_data)}개")
                            self.collected_count += 1
                            return True, True
                        else:
                            print(f"❌ {stock_code}: {minute_name} DB 저장 실패")
                            return False, False
                    else:
                        print(f"⚠️ {stock_code}: save_minute_data 메서드가 구현되지 않음")
                        print(f"✅ {stock_code} {minute_name} 데이터 수집 성공 (DB 저장 건너뜀): {len(minute_data)}개")
                        self.collected_count += 1
                        return True, True
                except Exception as db_error:
                    print(f"❌ {stock_code}: {minute_name} DB 저장 오류: {db_error}")
                    print(f"✅ {stock_code} {minute_name} 데이터 수집 성공 (DB 저장 실패, 계속 진행): {len(minute_data)}개")
                    self.collected_count += 1
                    return True, True
            else:
                # 테스트 모드: DB 저장 없이 성공 처리
                print(f"✅ {stock_code} {minute_name} 테스트 완료 (DB 저장 건너뜀): {len(minute_data)}개")
                self.collected_count += 1
                return True, True

        except Exception as e:
            print(f"❌ {stock_code} {minute_name} 수집 오류: {e}")
            logger.error(f"{stock_code} {minute_name} 수집 실패: {e}")
            self.error_count += 1
            return False, False

        finally:
            # API 요청 제한 준수 (tr_codes.py에서 딜레이 시간 가져오기)
            delay_ms = self.tr_info.get('delay_ms', 3600)
            delay_sec = delay_ms / 1000
            print(f"⏱️ API 제한 준수 대기: {delay_sec}초")
            time.sleep(delay_sec)

    def _parse_minute_data(self, response: Dict[str, Any], stock_code: str, minute_type: str) -> List[Dict[str, Any]]:
        """키움 API 응답 분봉데이터 파싱"""
        try:
            minute_name = self.MINUTE_TYPES.get(minute_type, f"{minute_type}분봉")
            print(f"=== {stock_code} {minute_name} 파싱 시작 ===")

            tr_code = response.get('tr_code')
            if tr_code != self.TR_CODE:
                print(f"❌ 잘못된 TR 코드: {tr_code} (예상: {self.TR_CODE})")
                return []

            # connector에서 파싱된 데이터 사용
            data_info = response.get('data', {})
            print(f"📊 데이터 정보: 파싱됨={data_info.get('parsed', False)}, 레코드수={data_info.get('repeat_count', 0)}")

            if not data_info.get('parsed', False):
                print(f"❌ 데이터가 파싱되지 않음")
                return []

            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                print(f"❌ 원시 데이터가 없음")
                return []

            print(f"📋 원시 데이터 수: {len(raw_data)}개")

            minute_data = []

            for i, row_data in enumerate(raw_data):
                try:
                    # 첫 번째 샘플 데이터 출력
                    if i == 0:
                        print(f"📊 첫 번째 레코드 샘플: {row_data}")

                    # tr_codes.py의 output_fields에 맞춰 파싱
                    parsed_row = {
                        'time': self._clean_time(row_data.get('체결시간', '')),
                        'open_price': self._parse_int(row_data.get('시가', 0)),
                        'high_price': self._parse_int(row_data.get('고가', 0)),
                        'low_price': self._parse_int(row_data.get('저가', 0)),
                        'close_price': self._parse_int(row_data.get('현재가', 0)),
                        'volume': self._parse_int(row_data.get('거래량', 0)),
                        'trading_value': self._parse_int(row_data.get('거래대금', 0)) if '거래대금' in row_data else 0,
                        'minute_type': int(minute_type),
                        'adj_price_flag': self._parse_int(row_data.get('수정주가구분', 0)),
                        'adj_ratio': self._parse_int(row_data.get('수정비율', 0)),
                        'prev_close': self._parse_int(row_data.get('전일종가', 0))
                    }

                    # 필수 데이터 검증 (음수 가격도 허용)
                    if parsed_row['time'] and parsed_row['close_price'] != 0:
                        minute_data.append(parsed_row)

                        # 첫 번째 파싱된 데이터 출력
                        if i == 0:
                            print(f"📈 첫 번째 파싱 결과: {parsed_row}")
                    else:
                        if i < 5:  # 처음 5개만 출력 (너무 많은 로그 방지)
                            print(f"⚠️ 데이터 검증 실패 {i}: 시간={parsed_row['time']}, 가격={parsed_row['close_price']}")

                except Exception as e:
                    print(f"❌ 데이터 파싱 오류 {i}: {e}")
                    continue

            print(f"✅ {minute_name} 파싱 완료: {len(minute_data)}개 (원본 {len(raw_data)}개 중)")
            return minute_data

        except Exception as e:
            print(f"❌ {minute_name} 파싱 실패: {e}")
            logger.error(f"{stock_code} {minute_name} 파싱 실패: {e}")
            return []

    def collect_designated_stocks_minute_data(self, stock_codes: List[str],
                                              minute_type: str = "3") -> Dict[str, Any]:
        """지정 종목들의 분봉 데이터 수집"""
        try:
            minute_name = self.MINUTE_TYPES.get(minute_type, f"{minute_type}분봉")
            print(f"🚀 지정 {len(stock_codes)}개 종목 {minute_name} 수집 시작")
            print(f"📋 대상 종목: {stock_codes}")

            start_time = datetime.now()
            successful_codes = []
            failed_codes = []

            for i, stock_code in enumerate(stock_codes, 1):
                try:
                    print(f"\n📊 진행: {i}/{len(stock_codes)} - {stock_code} {minute_name}")

                    success, is_new = self.collect_single_stock_minute_data(
                        stock_code, minute_type
                    )

                    if success:
                        successful_codes.append(stock_code)
                        print(f"✅ {stock_code} {minute_name} 완료")
                    else:
                        failed_codes.append(stock_code)
                        print(f"❌ {stock_code} {minute_name} 실패")

                    # 진행률 표시 (5개마다)
                    if i % 5 == 0:
                        elapsed = datetime.now() - start_time
                        success_rate = len(successful_codes) / i * 100
                        remaining = len(stock_codes) - i
                        avg_time_per_stock = elapsed.total_seconds() / i
                        estimated_remaining = remaining * avg_time_per_stock

                        print(f"\n📈 진행률: {i}/{len(stock_codes)} ({i / len(stock_codes) * 100:.1f}%)")
                        print(f"⏱️ 소요시간: {elapsed}")
                        print(f"📊 성공률: {success_rate:.1f}%")
                        print(f"⏳ 예상 남은 시간: {timedelta(seconds=int(estimated_remaining))}")

                except KeyboardInterrupt:
                    print(f"\n⚠️ 사용자에 의해 중단됨 (진행: {i}/{len(stock_codes)})")
                    break
                except Exception as e:
                    print(f"❌ {stock_code} {minute_name} 처리 중 오류: {e}")
                    failed_codes.append(stock_code)
                    continue

            # 결과 요약
            end_time = datetime.now()
            total_time = end_time - start_time

            result = {
                'start_time': start_time,
                'end_time': end_time,
                'total_time': total_time,
                'minute_type': minute_type,
                'minute_name': minute_name,
                'total_stocks': len(stock_codes),
                'successful_count': len(successful_codes),
                'failed_count': len(failed_codes),
                'success_rate': len(successful_codes) / len(stock_codes) * 100 if stock_codes else 0,
                'successful_codes': successful_codes,
                'failed_codes': failed_codes,
                'tr_code': self.TR_CODE
            }

            print(f"\n🎉 지정 종목 {minute_name} 수집 완료!")
            print(f"📊 총 {len(stock_codes)}개 중 {len(successful_codes)}개 성공")
            print(f"⏱️ 총 소요시간: {total_time}")
            print(f"📈 성공률: {result['success_rate']:.1f}%")

            if failed_codes:
                print(f"❌ 실패 종목: {failed_codes}")

            return result

        except Exception as e:
            logger.error(f"지정 종목 {minute_name} 수집 실패: {e}")
            return {}

    def collect_multiple_minute_types(self, stock_codes: List[str],
                                      minute_types: List[str] = ["1", "3", "5"]) -> Dict[str, Any]:
        """지정 종목들의 여러 분봉 타입 동시 수집"""
        try:
            print(f"🚀 {len(stock_codes)}개 종목, {len(minute_types)}개 분봉 타입 동시 수집")
            print(f"📋 수집 대상: {[self.MINUTE_TYPES.get(mt, f'{mt}분봉') for mt in minute_types]}")
            print(f"📊 종목 리스트: {stock_codes}")

            start_time = datetime.now()
            all_results = {}

            for minute_type in minute_types:
                minute_name = self.MINUTE_TYPES.get(minute_type, f"{minute_type}분봉")
                print(f"\n🔄 {minute_name} 수집 시작...")

                result = self.collect_designated_stocks_minute_data(stock_codes, minute_type)
                all_results[minute_type] = result

                print(f"✅ {minute_name} 수집 완료! (성공률: {result.get('success_rate', 0):.1f}%)")

            # 전체 결과 요약
            end_time = datetime.now()
            total_time = end_time - start_time

            summary = {
                'start_time': start_time,
                'end_time': end_time,
                'total_time': total_time,
                'total_stocks': len(stock_codes),
                'minute_types': minute_types,
                'results_by_type': all_results,
                'overall_summary': self._create_overall_summary(all_results, len(stock_codes)),
                'tr_code': self.TR_CODE
            }

            print(f"\n🎉 전체 분봉 수집 완료!")
            print(f"📊 대상: {len(stock_codes)}개 종목 × {len(minute_types)}개 분봉")
            print(f"⏱️ 총 소요시간: {total_time}")

            # 분봉별 성공률 출력
            for minute_type in minute_types:
                if minute_type in all_results:
                    result = all_results[minute_type]
                    minute_name = self.MINUTE_TYPES.get(minute_type, f"{minute_type}분봉")
                    print(f"📈 {minute_name}: {result.get('success_rate', 0):.1f}% 성공")

            return summary

        except Exception as e:
            logger.error(f"다중 분봉 수집 실패: {e}")
            return {}

    def _create_overall_summary(self, all_results: Dict, total_stocks: int) -> Dict[str, Any]:
        """전체 분봉 수집 결과 요약"""
        total_successful = 0
        total_failed = 0

        for result in all_results.values():
            total_successful += result.get('successful_count', 0)
            total_failed += result.get('failed_count', 0)

        total_operations = len(all_results) * total_stocks
        overall_success_rate = (total_successful / total_operations * 100) if total_operations > 0 else 0

        return {
            'total_operations': total_operations,
            'total_successful': total_successful,
            'total_failed': total_failed,
            'overall_success_rate': overall_success_rate
        }

    # 유틸리티 메서드들
    def _clean_time(self, time_str: str) -> str:
        """시간 문자열 정리 (YYYYMMDDHHMMSS -> HHMMSS)"""
        if not time_str:
            return ""

        try:
            # 공백 제거 및 숫자만 추출
            cleaned = ''.join(c for c in str(time_str) if c.isdigit())

            # YYYYMMDDHHMMSS (14자리) -> HHMMSS (6자리) 추출
            if len(cleaned) == 14:  # 20250711153000
                return cleaned[8:14]  # 153000
            elif len(cleaned) == 6:  # 이미 HHMMSS 형태
                return cleaned
            elif len(cleaned) == 4:  # HHMM -> HHMM00
                return cleaned + "00"
            elif len(cleaned) >= 6:  # 6자리 이상이면 뒤 6자리
                return cleaned[-6:]

            return cleaned if len(cleaned) >= 4 else ""

        except Exception as e:
            print(f"⚠️ 시간 파싱 오류: {time_str} -> {e}")
            return ""

    def _parse_int(self, value) -> int:
        """정수 변환 (키움 API 특수 처리)"""
        if not value:
            return 0

        try:
            # 문자열에서 부호와 숫자만 추출
            if isinstance(value, str):
                # 부호 처리
                sign = -1 if value.strip().startswith('-') else 1
                # +/- 제거하고 숫자만 추출
                numeric_str = ''.join(c for c in value.replace('+', '').replace('-', '') if c.isdigit())

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
            'tr_name': self.tr_info['name'],
            'supported_minute_types': self.MINUTE_TYPES,
            'tr_info': self.tr_info
        }


# 편의 함수들
def collect_minute_data_single(stock_code: str, minute_type: str = "3",
                               session: Optional[KiwoomSession] = None) -> bool:
    """단일 종목 분봉데이터 수집 편의 함수"""
    try:
        if not session:
            from ..api.base_session import create_kiwoom_session
            session = create_kiwoom_session()

        if not session:
            return False

        collector = MinuteDataCollector(session)
        success, _ = collector.collect_single_stock_minute_data(stock_code, minute_type)
        return success

    except Exception as e:
        logger.error(f"분봉데이터 수집 편의함수 실패: {e}")
        return False


def collect_designated_stocks_3min(stock_codes: List[str],
                                   session: Optional[KiwoomSession] = None) -> Dict[str, Any]:
    """지정 종목들 3분봉 수집 편의 함수"""
    try:
        if not session:
            from ..api.base_session import create_kiwoom_session
            session = create_kiwoom_session()

        if not session:
            return {}

        collector = MinuteDataCollector(session)
        return collector.collect_designated_stocks_minute_data(stock_codes, "3")

    except Exception as e:
        logger.error(f"지정 종목 3분봉 수집 편의함수 실패: {e}")
        return {}


def collect_multiple_timeframes(stock_codes: List[str],
                                minute_types: List[str] = ["1", "3", "5"],
                                session: Optional[KiwoomSession] = None) -> Dict[str, Any]:
    """지정 종목들 다중 시간대 분봉 수집 편의 함수"""
    try:
        if not session:
            from ..api.base_session import create_kiwoom_session
            session = create_kiwoom_session()

        if not session:
            return {}

        collector = MinuteDataCollector(session)
        return collector.collect_multiple_minute_types(stock_codes, minute_types)

    except Exception as e:
        logger.error(f"다중 시간대 분봉 수집 편의함수 실패: {e}")
        return {}