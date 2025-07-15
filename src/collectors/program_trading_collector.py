#!/usr/bin/env python3
"""
파일 경로: src/collectors/program_trading_collector.py

프로그램매매 데이터 수집기 (OPT90013 기반)
- supply_demand_new_collector.py 구조 참고
- 동일한 연속조회 로직 적용
- 날짜 관리 및 _AL 종목코드 규칙 준수
"""
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가 (상대 import 해결)
if __name__ == "__main__":
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

from src.api.base_session import KiwoomSession
from src.api.tr_codes import get_tr_info, create_opt90013_input
from src.core.program_trading_database import get_program_trading_database_service
from src.utils.trading_date import get_market_today

logger = logging.getLogger(__name__)


class ProgramTradingCollector:
    """프로그램매매 데이터 수집기 (OPT90013 기반)"""

    def __init__(self, session: KiwoomSession):
        self.session = session
        self.db_service = get_program_trading_database_service()

        # TR 정보 (OPT90013)
        self.TR_CODE = 'opt90013'
        self.RQ_NAME = 'program_trading_request'
        self.tr_info = get_tr_info(self.TR_CODE)

        # 수집 통계
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_stocks': 0,
            'completed_stocks': 0,
            'failed_stocks': 0,
            'total_records': 0,
            'tr_code': self.TR_CODE
        }

        # 수집 설정
        self.api_delay = 3.6  # API 요청 간격 (초)
        self.max_requests_per_stock = 50  # 종목당 최대 요청 수
        self.target_records = 250  # 1년치 데이터 목표

        # 1년 전 기준 날짜 (종료 조건용)
        one_year_ago = datetime.now() - timedelta(days=365)
        self.one_year_ago_str = one_year_ago.strftime('%Y%m%d')

        logger.info(f"프로그램매매 수집기 초기화 완료 (TR: {self.TR_CODE})")

    def collect_single_stock_program_trading(self, stock_code: str, force_full: bool = False) -> Tuple[bool, bool]:
        """
        단일 종목 프로그램매매 데이터 수집

        Args:
            stock_code: 종목코드 (6자리 숫자)
            force_full: 강제 전체 수집 여부

        Returns:
            (성공여부, 신규데이터여부)
        """
        try:
            print(f"\n=== {stock_code} 프로그램매매 수집 시작 ===")

            # 1. 테이블 생성 (필요시)
            if not self.db_service.table_exists(stock_code):
                if not self.db_service.create_program_trading_table(stock_code):
                    print(f"❌ {stock_code}: 테이블 생성 실패")
                    return False, False
                print(f"✅ {stock_code}: 테이블 생성 완료")

            # 2. 데이터 완성도 확인
            completeness = self.db_service.get_data_completeness_info(stock_code)
            print(f"📊 {stock_code} 완성도: {completeness['completion_rate']:.1f}%")

            # 3. 수집 모드 결정
            if force_full or not completeness['is_complete']:
                print(f"🔄 {stock_code}: 연속 수집 모드 (1년치 데이터)")
                result = self._collect_continuous_mode(stock_code, completeness)
            else:
                print(f"📅 {stock_code}: 업데이트 모드 (최신 데이터만)")
                result = self._collect_update_mode(stock_code, completeness)

            # 4. 결과 처리
            if result['success']:
                print(f"✅ {stock_code}: 수집 완료 ({result.get('saved_records', 0)}건 저장)")
                return True, result.get('is_new_data', False)
            else:
                print(f"❌ {stock_code}: 수집 실패 - {result.get('error', '알 수 없는 오류')}")
                return False, False

        except Exception as e:
            print(f"❌ {stock_code}: 수집 중 오류 - {e}")
            logger.error(f"{stock_code} 프로그램매매 수집 실패: {e}")
            return False, False

    def _collect_continuous_mode(self, stock_code: str, completeness: Dict[str, Any]) -> Dict[str, Any]:
        """연속 수집 모드: prev_next=2로 1년치 데이터 수집"""
        try:
            print(f"   🔄 연속 수집 모드: 1년치 데이터 수집")

            all_data = []
            prev_next = 0  # 첫 요청은 0
            request_count = 0
            previous_batch_dates = set()

            while request_count < self.max_requests_per_stock:
                request_count += 1
                print(f"   📡 요청 {request_count}: prev_next={prev_next}")

                # TR 요청
                input_data = self._create_program_trading_input(stock_code)
                response = self._request_tr_data(stock_code, input_data, prev_next=prev_next)

                if not response['success']:
                    print(f"   ❌ 요청 {request_count} 실패: {response.get('error')}")
                    break

                # 데이터 파싱
                parsed_data = self._parse_program_trading_response(response['data'], stock_code)
                if not parsed_data:
                    print(f"   ⚠️ 요청 {request_count}: 파싱된 데이터 없음")
                    break

                # 현재 배치 날짜 집합 생성
                current_batch_dates = set(item.get('일자', '') for item in parsed_data)

                # 날짜 범위 확인
                if current_batch_dates:
                    oldest_in_batch = min(current_batch_dates)
                    newest_in_batch = max(current_batch_dates)
                    print(f"   📅 배치 날짜 범위: {oldest_in_batch} ~ {newest_in_batch}")

                # 1년 전 데이터 도달 시 종료
                if oldest_in_batch and oldest_in_batch <= self.one_year_ago_str:
                    print(f"   ✅ 1년 전 데이터 도달 ({oldest_in_batch} <= {self.one_year_ago_str})")
                    # 1년 전 이후 데이터만 추가
                    filtered_data = [item for item in parsed_data if item.get('일자', '') > self.one_year_ago_str]
                    all_data.extend(filtered_data)
                    print(f"   📊 최종 배치: {len(filtered_data)}건 수집 (누적: {len(all_data)}건)")
                    break

                # 중복 데이터 감지 (같은 날짜 범위 반복)
                if current_batch_dates and current_batch_dates == previous_batch_dates:
                    print(f"   ✅ 중복 데이터 감지 - 동일한 날짜 범위 반복!")
                    break

                # 목표 데이터량 도달 체크
                if len(all_data) >= self.target_records:
                    print(f"   ✅ 목표 데이터량 도달! ({len(all_data)}/{self.target_records}건)")
                    all_data.extend(parsed_data)
                    break

                # 정상적으로 데이터 추가
                all_data.extend(parsed_data)
                print(f"   📊 요청 {request_count}: {len(parsed_data)}건 수집 (누적: {len(all_data)}건)")

                # 다음 반복을 위해 현재 배치 날짜 저장
                previous_batch_dates = current_batch_dates.copy()

                # 연속 조회 여부 확인
                tr_cont = response.get('tr_cont', '')
                if tr_cont != '2':
                    print(f"   ✅ 연속 조회 완료 (tr_cont: {tr_cont})")
                    break

                # 다음 요청은 연속 조회
                prev_next = 2

                # API 요청 간격 준수
                time.sleep(self.api_delay)

            # 수집된 데이터 저장
            saved_count = 0
            if all_data:
                print(f"   💾 데이터 저장 중: {len(all_data)}건")
                saved_count = self.db_service.save_program_trading_data(stock_code, all_data)

            # 종료 사유 출력
            end_reason = self._get_collection_end_reason(request_count, len(all_data))
            print(f"   🏁 수집 종료: {end_reason}")

            return {
                'success': True,
                'stock_code': stock_code,
                'mode': 'continuous',
                'requests_made': request_count,
                'collected_records': len(all_data),
                'saved_records': saved_count,
                'is_new_data': saved_count > 0,
                'end_reason': end_reason
            }

        except Exception as e:
            return self._create_error_result(stock_code, f"연속 수집 모드 실패: {e}")

    def _collect_update_mode(self, stock_code: str, completeness: Dict[str, Any]) -> Dict[str, Any]:
        """업데이트 모드: 최신 데이터만 수집"""
        try:
            print(f"   🔄 업데이트 모드: 최신 데이터 수집")

            # 단일 요청으로 최신 데이터 조회
            input_data = self._create_program_trading_input(stock_code)
            response = self._request_tr_data(stock_code, input_data, prev_next=0)

            if not response['success']:
                return response

            # 데이터 파싱
            parsed_data = self._parse_program_trading_response(response['data'], stock_code)
            if not parsed_data:
                return self._create_error_result(stock_code, "데이터 파싱 실패")

            # 최신 데이터만 필터링 (기존 최신 날짜 이후)
            latest_date = completeness.get('newest_date', '')
            new_data = []

            for item in parsed_data:
                if item.get('일자', '') > latest_date:
                    new_data.append(item)

            # 데이터 저장
            saved_count = 0
            if new_data:
                saved_count = self.db_service.save_program_trading_data(stock_code, new_data)

            return {
                'success': True,
                'stock_code': stock_code,
                'mode': 'update',
                'collected_records': len(parsed_data),
                'new_records': len(new_data),
                'saved_records': saved_count,
                'is_new_data': saved_count > 0
            }

        except Exception as e:
            return self._create_error_result(stock_code, f"업데이트 모드 실패: {e}")

    def _create_program_trading_input(self, stock_code: str, target_date: str = "") -> Dict[str, Any]:
        """OPT90013 입력 데이터 생성 (날짜 관리 및 _AL 규칙 적용)"""
        # 날짜가 없으면 시장 기준 오늘 사용
        if not target_date:
            today = get_market_today()
            target_date = today.strftime('%Y%m%d')

        return {
            '시간일자구분': '2',  # 2:일자별
            '금액수량구분': '1',  # 1:금액, 2:수량
            '종목코드': f"{stock_code}_AL",  # 🔧 _AL 접미사 필수!
            '날짜': target_date  # YYYYMMDD
        }

    def _request_tr_data(self, stock_code: str, input_data: Dict[str, Any], prev_next: int = 0) -> Dict[str, Any]:
        """TR 요청 실행"""
        try:
            connector = self.session.get_connector()

            response = connector.request_tr_data(
                rq_name=self.RQ_NAME,
                tr_code=self.TR_CODE,
                input_data=input_data,
                prev_next=prev_next,
                screen_no="9013"  # OPT90013용 화면번호
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

    def _parse_program_trading_response(self, response: Any, stock_code: str) -> List[Dict[str, Any]]:
        """프로그램매매 API 응답 파싱 (강화된 디버그 모드)"""
        try:
            print(f"   🔍 [{stock_code}] 응답 타입: {type(response)}")

            # 키움 API 응답 구조 확인
            if not isinstance(response, dict):
                print(f"   ❌ [{stock_code}] 응답이 딕셔너리가 아님: {type(response)}")
                return []

            print(f"   🔍 [{stock_code}] 응답 키 목록: {list(response.keys())}")

            # 각 키의 값 타입과 내용 간략히 출력
            for key, value in response.items():
                if isinstance(value, dict):
                    print(f"   📋 [{stock_code}] {key}: dict with keys {list(value.keys())}")
                elif isinstance(value, list):
                    print(f"   📋 [{stock_code}] {key}: list with {len(value)} items")
                    if value and len(value) > 0:
                        print(f"   📋 [{stock_code}] {key}[0] type: {type(value[0])}")
                        if isinstance(value[0], dict):
                            print(f"   📋 [{stock_code}] {key}[0] keys: {list(value[0].keys())}")
                else:
                    print(f"   📋 [{stock_code}] {key}: {type(value)} = {str(value)[:50]}")

            # 다양한 응답 구조 처리 시도
            raw_data = None
            data_source = "unknown"

            # 1. 'data' 키가 있는 경우
            if 'data' in response:
                data_info = response['data']
                print(f"   🔍 [{stock_code}] data 내용 분석 중...")

                if isinstance(data_info, dict):
                    print(f"   📋 [{stock_code}] data 키들: {list(data_info.keys())}")

                    # data 내부에서 실제 데이터 찾기
                    for data_key in ['multi_data', 'output', 'raw_data', 'records', 'items', 'list']:
                        if data_key in data_info:
                            raw_data = data_info[data_key]
                            data_source = f"data.{data_key}"
                            print(
                                f"   ✅ [{stock_code}] {data_source}에서 발견: {len(raw_data) if isinstance(raw_data, list) else type(raw_data)}")
                            break
                elif isinstance(data_info, list):
                    raw_data = data_info
                    data_source = "data (직접 리스트)"
                    print(f"   ✅ [{stock_code}] {data_source}: {len(raw_data)}개")

            # 2. 'raw_data' 키가 직접 있는 경우
            elif 'raw_data' in response:
                raw_data = response['raw_data']
                data_source = "raw_data"
                print(
                    f"   ✅ [{stock_code}] {data_source}: {len(raw_data) if isinstance(raw_data, list) else type(raw_data)}")

            # 3. 'parsed' 키 확인
            elif 'parsed' in response:
                parsed_info = response['parsed']
                print(f"   🔍 [{stock_code}] parsed 분석: {type(parsed_info)}")

                if isinstance(parsed_info, list):
                    raw_data = parsed_info
                    data_source = "parsed"
                    print(f"   ✅ [{stock_code}] {data_source}: {len(raw_data)}개")
                elif isinstance(parsed_info, dict):
                    print(f"   📋 [{stock_code}] parsed 키들: {list(parsed_info.keys())}")
                    # parsed 내부에서 데이터 찾기
                    for parsed_key in ['data', 'records', 'items', 'list']:
                        if parsed_key in parsed_info:
                            raw_data = parsed_info[parsed_key]
                            data_source = f"parsed.{parsed_key}"
                            print(
                                f"   ✅ [{stock_code}] {data_source}: {len(raw_data) if isinstance(raw_data, list) else type(raw_data)}")
                            break

            # 4. 응답 자체가 리스트인 경우
            elif isinstance(response, list):
                raw_data = response
                data_source = "response (직접)"
                print(f"   ✅ [{stock_code}] {data_source}: {len(raw_data)}개")

            # 5. 다른 가능한 키들 시도
            else:
                possible_keys = ['records', 'items', 'list', 'result', 'output']
                for key in possible_keys:
                    if key in response:
                        potential_data = response[key]
                        if isinstance(potential_data, list):
                            raw_data = potential_data
                            data_source = key
                            print(f"   ✅ [{stock_code}] {data_source}에서 발견: {len(raw_data)}개")
                            break

            # 데이터를 찾지 못한 경우
            if raw_data is None:
                print(f"   ❌ [{stock_code}] 데이터 추출 실패")
                print(f"   📋 [{stock_code}] 전체 응답 구조:")
                import json
                try:
                    print(json.dumps(response, indent=2, ensure_ascii=False)[:500] + "...")
                except:
                    print(f"   📋 [{stock_code}] 응답 출력 실패: {response}")
                return []

            # raw_data 검증
            if not isinstance(raw_data, list):
                print(f"   ❌ [{stock_code}] {data_source}가 리스트가 아님: {type(raw_data)}")
                return []

            if len(raw_data) == 0:
                print(f"   ⚠️ [{stock_code}] {data_source}가 비어있음")
                return []

            print(f"   ✅ [{stock_code}] 데이터 추출 성공: {data_source}에서 {len(raw_data)}개")

            # 첫 번째 레코드 구조 확인
            if len(raw_data) > 0:
                first_record = raw_data[0]
                print(f"   📋 [{stock_code}] 첫 번째 레코드 타입: {type(first_record)}")
                if isinstance(first_record, dict):
                    print(f"   📋 [{stock_code}] 첫 번째 레코드 키들: {list(first_record.keys())}")
                    # 샘플 값들 출력
                    for key, value in list(first_record.items())[:5]:  # 처음 5개만
                        print(f"   📋 [{stock_code}] {key}: {str(value)[:30]}")

            # 데이터 파싱 진행
            parsed_data = []
            success_count = 0

            for i, row in enumerate(raw_data):
                try:
                    if not isinstance(row, dict):
                        print(f"   ⚠️ [{stock_code}] 행 {i}: 딕셔너리가 아님 ({type(row)})")
                        continue

                    # 날짜 필드 확인 및 정리
                    date_str = self._clean_string(row.get('일자', ''))
                    if not date_str:
                        print(f"   ⚠️ [{stock_code}] 행 {i}: 일자 필드 없음. 사용 가능한 키: {list(row.keys())}")
                        continue

                    # 날짜 형식 정리
                    if '-' in date_str:
                        date_str = date_str.replace('-', '')

                    # 날짜 검증
                    if len(date_str) != 8 or not date_str.isdigit():
                        print(f"   ⚠️ [{stock_code}] 행 {i}: 잘못된 날짜 형식 ({date_str})")
                        continue

                    # 프로그램매매 데이터 추출 (tr_codes.py 출력 필드 기준)
                    data = {
                        '일자': date_str,
                        # 주가 정보
                        'current_price': self._parse_price(row.get('현재가', 0)),
                        'price_change_sign': self._clean_string(row.get('대비기호', '')),
                        'price_change': self._parse_price(row.get('전일대비', 0)),
                        'change_rate': self._parse_float(row.get('등락율', 0)),
                        'volume': self._parse_int(row.get('거래량', 0)),

                        # 프로그램매매 금액
                        'program_sell_amount': self._parse_int(row.get('프로그램매도금액', 0)),
                        'program_buy_amount': self._parse_int(row.get('프로그램매수금액', 0)),
                        'program_net_amount': self._parse_int(row.get('프로그램순매수금액', 0)),
                        'program_net_amount_change': self._parse_int(row.get('프로그램순매수금액증감', 0)),

                        # 프로그램매매 수량
                        'program_sell_quantity': self._parse_int(row.get('프로그램매도수량', 0)),
                        'program_buy_quantity': self._parse_int(row.get('프로그램매수수량', 0)),
                        'program_net_quantity': self._parse_int(row.get('프로그램순매수수량', 0)),
                        'program_net_quantity_change': self._parse_int(row.get('프로그램순매수수량증감', 0)),

                        # 기타 필드
                        'base_price_time': self._clean_string(row.get('기준가시간', '')),
                        'short_sell_return_stock': self._clean_string(row.get('대차거래상환주수합', '')),
                        'balance_stock': self._clean_string(row.get('잔고수주합', '')),
                        'exchange_type': self._clean_string(row.get('거래소구분', ''))
                    }

                    parsed_data.append(data)
                    success_count += 1

                    # 첫 번째 성공한 데이터 샘플 출력
                    if success_count == 1:
                        print(f"   📊 [{stock_code}] 첫 번째 성공 샘플: 일자={date_str}, 현재가={row.get('현재가', 'N/A')}")

                except Exception as e:
                    print(f"   ⚠️ [{stock_code}] 행 {i} 파싱 오류: {e}")
                    continue

            print(f"   ✅ [{stock_code}] 파싱 완료: {success_count}/{len(raw_data)}건 성공")
            logger.info(f"[{stock_code}] 파싱 완료: {success_count}건")
            return parsed_data

        except Exception as e:
            print(f"   ❌ [{stock_code}] 파싱 실패: {e}")
            logger.error(f"[{stock_code}] 프로그램매매 응답 파싱 실패: {e}")
            import traceback
            traceback.print_exc()
            return []

    def _clean_string(self, value) -> str:
        """문자열 정리"""
        if not value:
            return ""
        return str(value).strip()

    def _parse_price(self, price_str: str) -> int:
        """가격 문자열 파싱 (+61000, -1000 등)"""
        if not price_str:
            return 0

        try:
            # + 또는 - 부호 처리
            clean_price = str(price_str).replace('+', '').replace('-', '').replace(',', '')
            sign = -1 if str(price_str).strip().startswith('-') else 1
            return int(clean_price) * sign if clean_price.isdigit() else 0
        except:
            return 0

    def _parse_int(self, value) -> int:
        """안전한 정수 변환"""
        if value is None or value == '':
            return 0

        try:
            if isinstance(value, str):
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

    def _parse_float(self, value) -> float:
        """안전한 실수 변환"""
        if value is None or value == '':
            return 0.0

        try:
            if isinstance(value, str):
                clean_value = value.replace(',', '').replace(' ', '').strip()
                if not clean_value or clean_value == '-':
                    return 0.0

                # 부호 처리
                sign = -1 if clean_value.startswith('-') else 1
                clean_value = clean_value.lstrip('+-')

                return float(clean_value) * sign
            else:
                return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _get_collection_end_reason(self, request_count: int, collected_count: int) -> str:
        """수집 종료 사유 반환"""
        if collected_count >= self.target_records:
            return f"목표 데이터량 도달 ({collected_count}/{self.target_records}건)"
        elif request_count >= self.max_requests_per_stock:
            return f"최대 요청 수 제한 ({request_count}/{self.max_requests_per_stock}회)"
        else:
            return "정상 완료 (API 또는 날짜 기준)"

    def _create_error_result(self, stock_code: str, error_msg: str) -> Dict[str, Any]:
        """오류 결과 생성"""
        return {
            'success': False,
            'stock_code': stock_code,
            'error': error_msg,
            'saved_records': 0,
            'is_new_data': False
        }

    def collect_multiple_stocks(self, stock_codes: List[str] = None, force_full: bool = False) -> Dict[str, Any]:
        """다중 종목 프로그램매매 데이터 수집"""
        try:
            self.stats['start_time'] = datetime.now()

            # 대상 종목 결정
            if stock_codes:
                target_stocks = [{'code': code} for code in stock_codes]
            else:
                # 활성 종목 조회
                all_stocks = self.db_service.get_all_stock_codes()
                target_stocks = [stock for stock in all_stocks if stock.get('is_active', True)]

            self.stats['total_stocks'] = len(target_stocks)
            print(f"🎯 프로그램매매 수집 대상: {len(target_stocks)}개 종목")

            # 수집 실행
            results = []
            for i, stock_info in enumerate(target_stocks, 1):
                stock_code = stock_info['code']

                print(f"\n[{i}/{len(target_stocks)}] {stock_code} 수집 중...")

                success, is_new = self.collect_single_stock_program_trading(stock_code, force_full)

                result = {
                    'stock_code': stock_code,
                    'success': success,
                    'is_new_data': is_new
                }
                results.append(result)

                if success:
                    self.stats['completed_stocks'] += 1
                else:
                    self.stats['failed_stocks'] += 1

                # 진행률 표시
                progress = (i / len(target_stocks)) * 100
                print(f"📊 진행률: {progress:.1f}% ({i}/{len(target_stocks)})")

            self.stats['end_time'] = datetime.now()

            return {
                'success': True,
                'stats': self.stats,
                'results': results
            }

        except Exception as e:
            logger.error(f"다중 종목 프로그램매매 수집 실패: {e}")
            return {'success': False, 'error': str(e)}

    def get_collection_stats(self) -> Dict[str, Any]:
        """수집 통계 정보 반환"""
        return self.stats.copy()


def collect_program_trading_single(stock_code: str, force_full: bool = False) -> Tuple[bool, bool]:
    """단일 종목 프로그램매매 수집 (편의 함수)"""
    from src.api.base_session import create_kiwoom_session

    session = create_kiwoom_session()
    if not session or not session.is_ready():
        return False, False

    collector = ProgramTradingCollector(session)
    return collector.collect_single_stock_program_trading(stock_code, force_full)


def collect_program_trading_batch(stock_codes: List[str], force_full: bool = False) -> Dict[str, Any]:
    """다중 종목 프로그램매매 수집 (편의 함수)"""
    from src.api.base_session import create_kiwoom_session

    session = create_kiwoom_session()
    if not session or not session.is_ready():
        return {'success': False, 'error': '키움 API 연결 실패'}

    collector = ProgramTradingCollector(session)
    return collector.collect_multiple_stocks(stock_codes, force_full)


# 테스트 함수
if __name__ == "__main__":
    import sys
    from pathlib import Path

    # 프로젝트 루트를 Python 경로에 추가
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

    print("🚀 프로그램매매 수집기 테스트")
    print("=" * 60)

    # 키움 세션 생성
    from src.api.base_session import create_kiwoom_session

    session = create_kiwoom_session(auto_login=True, show_progress=True)
    if not session or not session.is_ready():
        print("❌ 키움 세션 준비 실패")
        exit(1)

    # 수집기 초기화
    collector = ProgramTradingCollector(session)

    # 테스트 종목: 삼성전자
    test_code = "005930"
    print(f"\n📊 테스트 종목: {test_code} (삼성전자)")

    # 프로그램매매 데이터 수집
    success, is_new = collector.collect_single_stock_program_trading(test_code, force_full=True)

    if success:
        print(f"✅ {test_code} 프로그램매매 수집 성공!")

        # 통계 출력
        stats = collector.get_collection_stats()
        print(f"📈 수집 통계: {stats}")
    else:
        print(f"❌ {test_code} 프로그램매매 수집 실패")

    print("\n✅ 프로그램매매 수집기 테스트 완료!")