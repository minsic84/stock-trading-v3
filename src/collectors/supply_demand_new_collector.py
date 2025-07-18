#!/usr/bin/env python3
"""
파일 경로: src/collectors/supply_demand_new_collector.py

새로운 수급 데이터 수집기
- supply_demand_database.py와 연동
- 1년치 데이터 자동 수집 (연속 조회)
- 데이터 완성도 기반 수집 모드 결정
- 진행상황 표시 기능
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import time

from ..core.config import Config
from ..core.supply_demand_database import SupplyDemandDatabaseService
from ..api.base_session import KiwoomSession

logger = logging.getLogger(__name__)


class SupplyDemandNewCollector:
    """새로운 수급 데이터 수집기"""

    def __init__(self, session: KiwoomSession, config: Optional[Config] = None):
        self.session = session
        self.config = config or Config()

        # 데이터베이스 서비스
        self.db_service = SupplyDemandDatabaseService()

        # 수집 통계
        self.stats = {
            'total_stocks': 0,
            'completed_stocks': 0,
            'failed_stocks': 0,
            'skipped_stocks': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None
        }

        # TR 정보
        self.TR_CODE = 'opt10060'
        self.RQ_NAME = '일별수급데이터요청'

        # API 제한 (3.6초)
        self.api_delay = 3.6

    def collect_single_stock(self, stock_code: str, force_full: bool = False) -> Dict[str, Any]:
        """
        단일 종목 수급 데이터 수집

        Args:
            stock_code: 종목코드
            force_full: 강제 전체 수집 여부

        Returns:
            수집 결과 딕셔너리
        """
        try:
            print(f"\n📊 {stock_code} 수급 데이터 수집 시작...")

            # 1. 데이터 완성도 체크
            completeness = self.db_service.get_data_completeness(stock_code)
            print(f"   📈 현재 완성도: {completeness['completion_rate']:.1f}% ({completeness['total_records']}건)")
            print(f"   🎯 수집 모드: {completeness['collection_mode']}")

            # 2. 테이블 생성 (필요한 경우)
            if not completeness['table_exists']:
                print(f"   🔧 테이블 생성 중...")
                if not self.db_service.create_supply_demand_table(stock_code):
                    return self._create_error_result(stock_code, "테이블 생성 실패")

            # 3. 수집 모드 결정
            if force_full:
                collection_mode = 'full'
            else:
                collection_mode = completeness['collection_mode']

            # 4. 수집 실행
            if collection_mode == 'update':
                # 최신 데이터만 업데이트
                result = self._collect_update_mode(stock_code, completeness)
            elif collection_mode in ['continue', 'full']:
                # 연속 수집으로 1년치 데이터 수집
                result = self._collect_continuous_mode(stock_code, completeness)
            else:
                return self._create_error_result(stock_code, f"알 수 없는 수집 모드: {collection_mode}")

            # 5. 결과 처리
            if result['success']:
                self.stats['completed_stocks'] += 1
                self.stats['total_records'] += result.get('saved_records', 0)
                print(f"   ✅ 수집 완료: {result.get('saved_records', 0)}건 저장")
            else:
                self.stats['failed_stocks'] += 1
                print(f"   ❌ 수집 실패: {result.get('error', '알 수 없는 오류')}")

            return result

        except Exception as e:
            logger.error(f"{stock_code} 수집 중 오류: {e}")
            self.stats['failed_stocks'] += 1
            return self._create_error_result(stock_code, str(e))

    def _collect_update_mode(self, stock_code: str, completeness: Dict[str, Any]) -> Dict[str, Any]:
        """업데이트 모드: 최신 데이터만 수집 - 날짜 정렬 기능 추가"""
        try:
            print(f"   🔄 업데이트 모드: 최신 데이터 수집")

            # 단일 요청으로 최신 데이터 조회
            input_data = self._create_supply_demand_input(stock_code)

            response = self._request_tr_data(stock_code, input_data, prev_next=0)
            if not response['success']:
                return response

            # 데이터 파싱
            parsed_data = self._parse_supply_demand_response(response['data'], stock_code)
            if not parsed_data:
                return self._create_error_result(stock_code, "데이터 파싱 실패")

            # 최신 데이터만 필터링 (기존 최신 날짜 이후)
            latest_date = completeness.get('latest_date', '')
            new_data = []

            for item in parsed_data:
                if item.get('일자', '') > latest_date:
                    new_data.append(item)

            # 📅 새 데이터 날짜 오름차순 정렬 (오래된 날짜 → 최신 날짜)
            if new_data:
                print(f"   🔄 저장 전 데이터 정렬 중... ({len(new_data)}개)")
                new_data.sort(key=lambda x: x.get('일자', ''))

                # 정렬 결과 확인
                first_date = new_data[0].get('일자', '')
                last_date = new_data[-1].get('일자', '')
                print(f"   📅 정렬 완료: {first_date} ~ {last_date}")

            # 데이터 저장 (정렬된 순서로)
            saved_count = 0
            if new_data:
                saved_count = self.db_service.save_supply_demand_data(stock_code, new_data)
                print(f"   💾 정렬된 순서로 저장 완료: {saved_count}개")

            return {
                'success': True,
                'stock_code': stock_code,
                'mode': 'update',
                'collected_records': len(parsed_data),
                'new_records': len(new_data),
                'saved_records': saved_count
            }

        except Exception as e:
            return self._create_error_result(stock_code, f"업데이트 모드 실패: {e}")

    def _collect_continuous_mode(self, stock_code: str, completeness: Dict[str, Any]) -> Dict[str, Any]:
        """연속 모드: prev_next=2로 1년치 데이터 수집 (스마트 종료 조건 + 날짜 정렬 추가)"""
        try:
            print(f"   🔄 연속 모드: 1년치 데이터 수집")

            all_data = []
            request_count = 0
            max_requests = 50  # 최대 요청 수 제한
            prev_next = 0  # 첫 요청은 0
            target_records = 250  # 1년치 평일 기준
            previous_batch_dates = set()

            # 1년 전 기준 날짜 설정
            one_year_ago = datetime.now() - timedelta(days=365)
            cutoff_date = one_year_ago.strftime('%Y%m%d')
            print(f"   📅 1년 전 기준 날짜: {cutoff_date}")

            while request_count < max_requests:
                request_count += 1
                print(f"   📡 요청 {request_count}: prev_next={prev_next}")

                # TR 요청
                input_data = self._create_supply_demand_input(stock_code)
                response = self._request_tr_data(stock_code, input_data, prev_next=prev_next)

                if not response['success']:
                    print(f"   ❌ 요청 {request_count} 실패: {response.get('error')}")
                    break

                # 데이터 파싱
                parsed_data = self._parse_supply_demand_response(response['data'], stock_code)
                if not parsed_data:
                    print(f"   ⚠️ 요청 {request_count}: 파싱된 데이터 없음")
                    break

                # 현재 배치 날짜 분석
                current_batch_dates = set(item.get('일자', '') for item in parsed_data if item.get('일자'))
                if not current_batch_dates:
                    print(f"   ⚠️ 요청 {request_count}: 유효한 날짜가 없음")
                    break

                oldest_in_batch = min(current_batch_dates)
                newest_in_batch = max(current_batch_dates)

                # 종료 조건 체크
                # 1. 1년 전 데이터 도달
                if oldest_in_batch <= cutoff_date:
                    print(f"   ✅ 1년 전 데이터 도달 ({oldest_in_batch} <= {cutoff_date})")
                    # 1년 전 이후 데이터만 추가
                    filtered_data = [item for item in parsed_data if item.get('일자', '') > cutoff_date]
                    all_data.extend(filtered_data)
                    print(f"   📊 최종 배치: {len(filtered_data)}건 수집 (누적: {len(all_data)}건)")
                    break

                # 2. 연속 조회 종료 확인
                tr_cont = response.get('tr_cont', '')
                if tr_cont != '2':
                    print(f"   ✅ 연속 조회 완료 (tr_cont: {tr_cont})")
                    all_data.extend(parsed_data)
                    print(f"   📊 최종 배치: {len(parsed_data)}건 수집 (누적: {len(all_data)}건)")
                    break

                # 3. 중복 데이터 감지 (같은 날짜 범위가 반복되면 종료)
                if current_batch_dates and current_batch_dates == previous_batch_dates:
                    print(f"   ✅ 중복 데이터 감지 - 동일한 날짜 범위 반복!")
                    print(f"   📅 반복된 날짜 범위: {min(current_batch_dates)} ~ {max(current_batch_dates)}")
                    break

                # 4. 목표 데이터량 도달 체크
                if len(all_data) >= target_records:
                    print(f"   ✅ 목표 데이터량 도달! ({len(all_data)}/{target_records}건)")
                    # 현재 배치도 추가하고 종료
                    all_data.extend(parsed_data)
                    print(f"   📊 최종 배치: {len(parsed_data)}건 수집 (누적: {len(all_data)}건)")
                    break

                # 정상적으로 데이터 추가
                all_data.extend(parsed_data)
                print(f"   📊 요청 {request_count}: {len(parsed_data)}건 수집 (누적: {len(all_data)}건)")
                print(f"   📅 현재 배치 범위: {oldest_in_batch} ~ {newest_in_batch}")

                # 다음 반복을 위해 현재 배치 날짜 저장
                previous_batch_dates = current_batch_dates.copy()

                # 5. 연속 조회 설정
                prev_next = 2

                # API 제한 준수
                time.sleep(self.api_delay)

            # 📅 전체 수집 데이터 날짜 오름차순 정렬 (오래된 날짜 → 최신 날짜)
            if all_data:
                print(f"   🔄 전체 데이터 정렬 중... ({len(all_data)}개)")
                all_data.sort(key=lambda x: x.get('일자', ''))

                # 정렬 결과 확인
                first_date = all_data[0].get('일자', '')
                last_date = all_data[-1].get('일자', '')
                print(f"   📅 전체 정렬 완료: {first_date} ~ {last_date}")

            # 수집된 데이터 저장 (정렬된 순서로)
            saved_count = 0
            if all_data:
                print(f"   💾 정렬된 데이터 저장 중: {len(all_data)}건")
                saved_count = self.db_service.save_supply_demand_data(stock_code, all_data)
                print(f"   💾 정렬된 순서로 저장 완료: {saved_count}개")

            return {
                'success': True,
                'stock_code': stock_code,
                'mode': 'continuous',
                'requests_made': request_count,
                'collected_records': len(all_data),
                'saved_records': saved_count
            }

        except Exception as e:
            return self._create_error_result(stock_code, f"연속 모드 실패: {e}")

    def _get_termination_reason(self, request_count: int, max_requests: int,
                                collected_count: int, target_records: int) -> str:
        """종료 사유 반환"""
        if collected_count >= target_records:
            return f"목표 데이터량 도달 ({collected_count}/{target_records}건)"
        elif request_count >= max_requests:
            return f"최대 요청 수 제한 ({request_count}/{max_requests}회)"
        else:
            return "정상 완료 (API 또는 날짜 기준)"

    def _request_tr_data(self, stock_code: str, input_data: Dict[str, Any], prev_next: int = 0) -> Dict[str, Any]:
        """TR 요청 실행"""
        try:
            connector = self.session.get_connector()

            response = connector.request_tr_data(
                rq_name=self.RQ_NAME,
                tr_code=self.TR_CODE,
                input_data=input_data,
                prev_next=prev_next,
                screen_no="9060"
            )

            if not response:
                return {'success': False, 'error': 'TR 요청 실패 (응답 없음)'}

            # prev_next 정보 추출 (연속 조회 여부)
            prev_next_value = response.get('prev_next', '')

            return {
                'success': True,
                'data': response,
                'tr_cont': prev_next_value  # '2'면 연속 데이터 있음
            }

        except Exception as e:
            return {'success': False, 'error': f'TR 요청 오류: {e}'}

    def _create_supply_demand_input(self, stock_code: str, target_date: str = "") -> Dict[str, Any]:
        """OPT10060 입력 데이터 생성"""
        from src.utils.trading_date import get_market_today

        # 날짜가 없으면 시장 기준 오늘 사용
        if not target_date:
            today = get_market_today()
            target_date = today.strftime('%Y%m%d')

        return {
            '일자': target_date,  # 빈값이면 최근일부터
            '종목코드': f"{stock_code}_AL",
            '금액수량구분': '1',  # 1:금액
            '매매구분': '0',  # 0:순매수
            '단위구분': '1000'  # 1000:천주
        }

    def _parse_supply_demand_response(self, response: Any, stock_code: str) -> List[Dict[str, Any]]:
        """수급 데이터 응답 파싱"""
        try:
            # 키움 API 응답 구조 확인
            print(f"   🔍 응답 구조 분석: {type(response)}")

            # response가 딕셔너리인지 확인
            if not isinstance(response, dict):
                print(f"   ⚠️ 응답이 딕셔너리가 아님: {type(response)}")
                return []

            # 'data' 키에서 실제 데이터 추출
            data_info = response.get('data', {})
            if not data_info:
                print(f"   ⚠️ 응답에 data 필드 없음")
                return []

            # 파싱 여부 확인
            if not data_info.get('parsed', False):
                print(f"   ⚠️ 데이터가 파싱되지 않음: {data_info.get('error', '알 수 없는 오류')}")
                return []

            # raw_data 추출
            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                print(f"   ⚠️ raw_data가 비어있음 (데이터 없음 또는 비활성 종목)")
                return []

            print(f"   📊 파싱할 데이터: {len(raw_data)}건")
            parsed_data = []

            for i, row_data in enumerate(raw_data):
                try:
                    # 필드 매핑 (API 응답 필드명 → 파싱된 키)
                    parsed_row = {
                        '일자': self._clean_string(row_data.get('일자', '')),
                        '현재가': self._parse_int(row_data.get('현재가', 0)),
                        '전일대비': self._parse_int(row_data.get('전일대비', 0)),
                        '누적거래대금': self._parse_int(row_data.get('누적거래대금', 0)),
                        '개인투자자': self._parse_int(row_data.get('개인투자자', 0)),
                        '외국인투자': self._parse_int(row_data.get('외국인투자', 0)),
                        '기관계': self._parse_int(row_data.get('기관계', 0)),
                        '금융투자': self._parse_int(row_data.get('금융투자', 0)),
                        '보험': self._parse_int(row_data.get('보험', 0)),
                        '투신': self._parse_int(row_data.get('투신', 0)),
                        '기타금융': self._parse_int(row_data.get('기타금융', 0)),
                        '은행': self._parse_int(row_data.get('은행', 0)),
                        '연기금등': self._parse_int(row_data.get('연기금등', 0)),
                        '사모펀드': self._parse_int(row_data.get('사모펀드', 0)),
                        '국가': self._parse_int(row_data.get('국가', 0)),
                        '기타법인': self._parse_int(row_data.get('기타법인', 0)),
                        '내외국인': self._parse_int(row_data.get('내외국인', 0))
                    }

                    # 유효한 날짜가 있는 경우만 추가
                    date_str = parsed_row['일자']
                    if date_str and len(date_str) >= 8:
                        # 날짜 형식 정리 (YYYYMMDD)
                        date_str = date_str.replace('-', '').replace('/', '').strip()
                        if len(date_str) == 8 and date_str.isdigit():
                            parsed_row['일자'] = date_str
                            parsed_data.append(parsed_row)

                            # 첫 번째 데이터 샘플 출력
                            if i == 0:
                                print(f"   📊 샘플: {date_str} - 개인:{parsed_row['개인투자자']:,}, 외국인:{parsed_row['외국인투자']:,}")
                        else:
                            print(f"   ⚠️ 잘못된 날짜 형식: '{date_str}'")
                    else:
                        print(f"   ⚠️ 날짜 없음 (행 {i})")

                except Exception as e:
                    print(f"   ⚠️ 행 {i} 파싱 오류: {e}")
                    continue

            print(f"   ✅ 파싱 완료: {len(parsed_data)}건 유효 데이터")
            return parsed_data

        except Exception as e:
            print(f"   ❌ 응답 파싱 실패: {e}")
            import traceback
            print(f"   📋 상세 오류: {traceback.format_exc()}")
            return []

    def _clean_string(self, value) -> str:
        """문자열 정리"""
        if not value:
            return ""
        return str(value).strip()

    def _parse_int(self, value) -> int:
        """안전한 정수 변환"""
        if value is None or value == '':
            return 0

        try:
            if isinstance(value, str):
                # 콤마, 공백, 부호 처리
                clean_value = value.replace(',', '').replace(' ', '').strip()
                if not clean_value or clean_value == '-':
                    return 0

                # 부호 처리
                sign = -1 if clean_value.startswith('-') else 1
                clean_value = clean_value.lstrip('+-')

                return int(float(clean_value)) * sign
            else:
                return int(value)
        except (ValueError, TypeError):
            return 0

    def _create_error_result(self, stock_code: str, error_msg: str) -> Dict[str, Any]:
        """오류 결과 생성"""
        return {
            'success': False,
            'stock_code': stock_code,
            'error': error_msg,
            'saved_records': 0
        }

    def collect_multiple_stocks(self, stock_codes: List[str] = None, force_full: bool = False) -> Dict[str, Any]:
        """다중 종목 수급 데이터 수집"""
        try:
            self.stats['start_time'] = datetime.now()

            # 대상 종목 결정
            if stock_codes:
                # 지정된 종목들
                target_stocks = []
                for code in stock_codes:
                    if len(code) == 6 and code.isdigit():
                        target_stocks.append({'code': code})
            else:
                # stock_codes 테이블의 모든 활성 종목
                all_stocks = self.db_service.get_all_stock_codes()
                target_stocks = [{'code': stock['code'], 'name': stock['name']} for stock in all_stocks]

            if not target_stocks:
                return {'success': False, 'message': '수집 대상 종목이 없습니다'}

            self.stats['total_stocks'] = len(target_stocks)
            print(f"\n🚀 수급 데이터 수집 시작: {len(target_stocks)}개 종목")
            print("=" * 80)

            # 개별 종목 수집
            results = []
            for i, stock_info in enumerate(target_stocks):
                stock_code = stock_info['code']
                stock_name = stock_info.get('name', stock_code)

                print(f"\n📊 [{i + 1}/{len(target_stocks)}] {stock_code} ({stock_name})")

                # 단일 종목 수집
                result = self.collect_single_stock(stock_code, force_full=force_full)
                results.append(result)

                # API 제한 준수
                if i < len(target_stocks) - 1:  # 마지막이 아니면
                    time.sleep(self.api_delay)

            # 최종 통계
            self.stats['end_time'] = datetime.now()
            elapsed_time = self.stats['end_time'] - self.stats['start_time']

            success_rate = (self.stats['completed_stocks'] / self.stats['total_stocks'] * 100) if self.stats[
                                                                                                      'total_stocks'] > 0 else 0

            final_result = {
                'success': True,
                'total_stocks': self.stats['total_stocks'],
                'completed_stocks': self.stats['completed_stocks'],
                'failed_stocks': self.stats['failed_stocks'],
                'success_rate': success_rate,
                'total_records': self.stats['total_records'],
                'elapsed_time': str(elapsed_time),
                'results': results
            }

            print(f"\n" + "=" * 80)
            print(f"🎉 수급 데이터 수집 완료!")
            print(f"   📊 전체 종목: {self.stats['total_stocks']:,}개")
            print(f"   ✅ 성공: {self.stats['completed_stocks']:,}개")
            print(f"   ❌ 실패: {self.stats['failed_stocks']:,}개")
            print(f"   📈 성공률: {success_rate:.1f}%")
            print(f"   📝 총 레코드: {self.stats['total_records']:,}개")
            print(f"   ⏱️ 소요 시간: {elapsed_time}")

            return final_result

        except Exception as e:
            logger.error(f"다중 종목 수집 실패: {e}")
            return {'success': False, 'error': str(e)}

    def get_collection_stats(self) -> Dict[str, Any]:
        """수집 통계 반환"""
        return self.stats.copy()


# 편의 함수
def create_supply_demand_new_collector(session: KiwoomSession,
                                       config: Optional[Config] = None) -> SupplyDemandNewCollector:
    """새로운 수급 데이터 수집기 생성"""
    return SupplyDemandNewCollector(session, config)


if __name__ == "__main__":
    # 테스트 실행
    print("🧪 새로운 수급 데이터 수집기 테스트")
    print("=" * 50)

    # 데이터베이스 서비스 테스트
    db_service = SupplyDemandDatabaseService()

    print("1. 데이터베이스 연결 테스트...")
    if db_service.test_connection():
        print("   ✅ 연결 성공")
    else:
        print("   ❌ 연결 실패")
        exit(1)

    print("2. 스키마 생성...")
    if db_service.create_schema_if_not_exists():
        print("   ✅ 스키마 준비 완료")
    else:
        print("   ❌ 스키마 생성 실패")
        exit(1)

    print("3. 종목 조회...")
    stocks = db_service.get_all_stock_codes()
    print(f"   📊 조회된 종목: {len(stocks)}개")

    if stocks:
        sample_stock = stocks[0]['code']
        print(f"4. 샘플 종목 완성도 체크: {sample_stock}")
        completeness = db_service.get_data_completeness(sample_stock)
        print(f"   📊 완성도: {completeness['completion_rate']:.1f}%")
        print(f"   🎯 모드: {completeness['collection_mode']}")

    print("\n✅ 테스트 완료!")