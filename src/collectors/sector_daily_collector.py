#!/usr/bin/env python3
"""
파일 경로: src/collectors/sector_daily_collector.py

업종 일봉 데이터 수집기 (OPT20006 기반)
- KOSPI(001), KOSDAQ(101) 종합지수 수집
- 5년치 연속조회 지원
- 기존 일봉 시스템 패턴 일관성 유지
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import time
import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.api.base_session import KiwoomSession
from src.api.tr_codes import get_tr_info, create_opt20006_input
from src.core.sector_database import get_sector_database_service
from src.utils.trading_date import get_market_today

logger = logging.getLogger(__name__)


class SectorDailyCollector:
    """업종 일봉 데이터 수집기"""

    def __init__(self, session: KiwoomSession):
        self.session = session
        self.db_service = get_sector_database_service()

        # TR 정보
        self.TR_CODE = 'opt20006'
        self.RQ_NAME = '업종별지수요청'
        self.tr_info = get_tr_info(self.TR_CODE)

        # 수집 설정
        self.api_delay = 3.6  # API 요청 간격 (초)
        self.max_requests_per_sector = 50  # 업종당 최대 요청 수
        self.target_records = 1250  # 5년치 데이터 목표

        # 지원하는 업종 코드
        self.sector_codes = ['001', '101']  # KOSPI, KOSDAQ
        self.sector_names = {
            '001': 'KOSPI 종합지수',
            '101': 'KOSDAQ 종합지수'
        }

        # 5년 전 기준 날짜 (종료 조건용)
        five_years_ago = datetime.now() - timedelta(days=5 * 365)
        self.five_years_ago_str = five_years_ago.strftime('%Y%m%d')

        # 수집 통계
        self.stats = {
            'start_time': None,
            'end_time': None,
            'total_sectors': len(self.sector_codes),
            'completed_sectors': 0,
            'failed_sectors': 0,
            'total_records': 0,
            'sectors_detail': {}
        }

        logger.info(f"업종 일봉 수집기 초기화 완료 (TR: {self.TR_CODE})")

    def collect_all_sectors(self, force_full: bool = False) -> Dict[str, Any]:
        """
        전체 업종 수집 (KOSPI → KOSDAQ 순차)

        Args:
            force_full: 강제 전체 수집 여부

        Returns:
            수집 결과 딕셔너리
        """
        try:
            print("\n🏛️ 업종 일봉 데이터 수집 시작")
            print("=" * 60)

            self.stats['start_time'] = datetime.now()

            # 스키마 및 테이블 준비
            if not self._prepare_database():
                return self._create_error_result("데이터베이스 준비 실패")

            # 각 업종별 순차 수집
            for sector_code in self.sector_codes:
                sector_name = self.sector_names[sector_code]
                print(f"\n📊 {sector_name} ({sector_code}) 수집 시작...")

                try:
                    success, records_collected = self.collect_single_sector(
                        sector_code, force_full
                    )

                    if success:
                        self.stats['completed_sectors'] += 1
                        self.stats['total_records'] += records_collected
                        self.stats['sectors_detail'][sector_code] = {
                            'success': True,
                            'records': records_collected,
                            'name': sector_name
                        }
                        print(f"✅ {sector_name} 수집 완료: {records_collected}개 레코드")
                    else:
                        self.stats['failed_sectors'] += 1
                        self.stats['sectors_detail'][sector_code] = {
                            'success': False,
                            'records': 0,
                            'name': sector_name
                        }
                        print(f"❌ {sector_name} 수집 실패")

                except Exception as e:
                    logger.error(f"{sector_name} 수집 중 오류: {e}")
                    self.stats['failed_sectors'] += 1
                    continue

            self.stats['end_time'] = datetime.now()
            return self._create_success_result()

        except Exception as e:
            logger.error(f"전체 업종 수집 실패: {e}")
            return self._create_error_result(str(e))

    def collect_single_sector(self, sector_code: str, force_full: bool = False) -> Tuple[bool, int]:
        """
        단일 업종 수집

        Args:
            sector_code: 업종코드 ('001', '101')
            force_full: 강제 전체 수집 여부

        Returns:
            (성공여부, 수집된_레코드수)
        """
        try:
            if sector_code not in self.sector_codes:
                logger.error(f"지원하지 않는 업종 코드: {sector_code}")
                return False, 0

            sector_name = self.sector_names[sector_code]
            print(f"\n📈 {sector_name} ({sector_code}) 데이터 수집 중...")

            # 1. 데이터 완성도 확인
            completeness = self.db_service.get_data_completeness(sector_code)
            print(f"   📊 현재 완성도: {completeness['completion_rate']:.1f}% ({completeness['total_records']}건)")
            print(f"   🎯 수집 모드: {completeness['collection_mode']}")

            # 2. 테이블 생성 (필요시)
            if not completeness['table_exists']:
                print(f"   🔧 테이블 생성 중...")
                if not self.db_service.create_sector_table(sector_code):
                    print(f"   ❌ 테이블 생성 실패")
                    return False, 0
                print(f"   ✅ 테이블 생성 완료")

            # 3. 수집 모드 결정
            if force_full:
                collection_mode = 'full'
            else:
                collection_mode = completeness['collection_mode']

            # 4. 수집 실행
            if collection_mode == 'update':
                # 최신 데이터만 업데이트
                records_collected = self._collect_update_mode(sector_code, completeness)
            elif collection_mode in ['continue', 'full']:
                # 연속 수집으로 5년치 데이터 수집
                records_collected = self._collect_continuous_mode(sector_code, completeness)
            else:
                logger.error(f"알 수 없는 수집 모드: {collection_mode}")
                return False, 0

            if records_collected > 0:
                print(f"   ✅ 수집 완료: {records_collected}개 레코드 저장")
                return True, records_collected
            else:
                print(f"   ⚠️ 수집된 데이터 없음")
                return True, 0  # 성공이지만 데이터 없음

        except Exception as e:
            logger.error(f"업종 {sector_code} 수집 실패: {e}")
            return False, 0

    def _collect_continuous_mode(self, sector_code: str, completeness: Dict[str, Any]) -> int:
        """연속조회로 5년치 데이터 수집"""
        try:
            print(f"   🔄 연속조회 모드: 5년치 데이터 수집 중...")

            all_data = []
            prev_next = "0"  # 첫 요청
            request_count = 0

            while request_count < self.max_requests_per_sector:
                # OPT20006 TR 요청
                input_data = create_opt20006_input(sector_code)

                print(f"   📡 TR 요청 {request_count + 1}/{self.max_requests_per_sector} (prev_next: {prev_next})")

                # 키움 커넥터를 통한 TR 요청
                connector = self.session.get_connector()
                response = connector.request_tr_data(
                    rq_name=self.RQ_NAME,
                    tr_code=self.TR_CODE,
                    input_data=input_data,
                    screen_no="9003",
                    prev_next=prev_next
                )

                if not response or 'error' in response:
                    logger.error(f"TR 요청 실패: {response}")
                    break

                # 응답 데이터 파싱
                parsed_data = self._parse_sector_response(response, sector_code)

                if not parsed_data:
                    print(f"   ⚠️ 파싱된 데이터 없음")
                    break

                # 종료 조건 체크
                if self._should_stop_collection(parsed_data, sector_code):
                    print(f"   🛑 5년치 데이터 수집 완료")
                    all_data.extend(parsed_data)
                    break

                all_data.extend(parsed_data)
                print(f"   📊 수신: {len(parsed_data)}건 (누적: {len(all_data)}건)")

                # 연속조회 설정
                prev_next = "2"
                request_count += 1

                # API 딜레이
                time.sleep(self.api_delay)

            # 데이터 저장 전 날짜순 정렬
            if all_data:
                # 날짜순 오름차순 정렬 (오래된 날짜부터)
                all_data.sort(key=lambda x: x['date'])
                print(f"   📅 데이터 정렬 완료: {all_data[0]['date']} ~ {all_data[-1]['date']}")

                saved_count = self.db_service.save_sector_data(sector_code, all_data)
                print(f"   💾 저장 완료: {saved_count}/{len(all_data)}개")
                return saved_count
            else:
                return 0

        except Exception as e:
            logger.error(f"연속조회 수집 실패 ({sector_code}): {e}")
            return 0

    def _collect_update_mode(self, sector_code: str, completeness: Dict[str, Any]) -> int:
        """최신 데이터만 업데이트"""
        try:
            print(f"   📅 업데이트 모드: 최신 데이터 확인 중...")

            # 단일 요청으로 최신 데이터 확인
            input_data = create_opt20006_input(sector_code)

            # 키움 커넥터를 통한 TR 요청
            connector = self.session.get_connector()
            response = connector.request_tr_data(
                rq_name=self.RQ_NAME,
                tr_code=self.TR_CODE,
                input_data=input_data,
                screen_no="9003",
                prev_next="0"
            )

            if not response or 'error' in response:
                logger.error(f"TR 요청 실패: {response}")
                return 0

            # 응답 파싱
            parsed_data = self._parse_sector_response(response, sector_code)

            if not parsed_data:
                return 0

            # 기존 최신 날짜 이후 데이터만 필터링
            latest_date = completeness.get('latest_date')
            if latest_date:
                latest_date_str = latest_date.strftime('%Y%m%d')
                new_data = [
                    item for item in parsed_data
                    if item['date'] > latest_date_str
                ]
            else:
                new_data = parsed_data

            if new_data:
                # 날짜순 정렬 후 저장
                new_data.sort(key=lambda x: x['date'])
                print(f"   📅 신규 데이터 정렬 완료: {len(new_data)}개")

                saved_count = self.db_service.save_sector_data(sector_code, new_data)
                print(f"   📅 최신 데이터 저장: {saved_count}개")
                return saved_count
            else:
                print(f"   ✅ 최신 데이터 이미 완료")
                return 0

        except Exception as e:
            logger.error(f"업데이트 모드 실패 ({sector_code}): {e}")
            return 0

    def _parse_sector_response(self, response: Dict[str, Any], sector_code: str) -> List[Dict[str, Any]]:
        """
        OPT20006 응답 데이터 파싱

        Args:
            response: 키움 API 응답
            sector_code: 업종코드

        Returns:
            파싱된 데이터 리스트
        """
        try:
            parsed_data = []

            # 응답 구조 확인
            if not response or response.get('tr_code') != self.TR_CODE:
                logger.error(f"잘못된 TR 응답: {response.get('tr_code')}")
                return []

            # 데이터 추출
            data_info = response.get('data', {})
            if not data_info.get('parsed', False):
                logger.error("응답 데이터가 파싱되지 않음")
                return []

            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                logger.warning("원시 데이터가 없습니다")
                return []

            # 개별 레코드 파싱
            for i, row in enumerate(raw_data):
                try:
                    # 🔍 디버깅: 첫 번째 레코드의 실제 구조 확인
                    if i == 0:
                        print(f"   🔍 첫 번째 레코드 구조: {row}")
                        print(f"   🔍 사용 가능한 키들: {list(row.keys()) if isinstance(row, dict) else 'dict가 아님'}")

                    # API 필드 → DB 필드 매핑 (문자열 데이터 처리)
                    data_item = {
                        'date': self._clean_string(row.get('일자', '')),
                        'open_index': self._safe_float(row.get('시가', '0')),
                        'high_index': self._safe_float(row.get('고가', '0')),
                        'low_index': self._safe_float(row.get('저가', '0')),
                        'close_index': self._safe_float(row.get('현재가', '0')),
                        'volume': self._safe_int(row.get('거래량', '0')),
                        'trading_value': self._safe_int(row.get('거래대금', '0'))
                    }

                    # 🔍 디버깅: 파싱된 데이터 확인
                    if i == 0:
                        print(f"   🔍 파싱된 첫 번째 데이터: {data_item}")

                    # 필수 데이터 검증
                    if (data_item['date'] and
                            data_item['close_index'] > 0):
                        parsed_data.append(data_item)
                    elif i < 3:  # 처음 3개만 디버깅 출력
                        print(f"   ⚠️ 레코드 {i} 검증 실패: date='{data_item['date']}', close={data_item['close_index']}")

                except Exception as e:
                    logger.warning(f"개별 레코드 파싱 오류 (#{i}): {e}")
                    if i < 3:  # 처음 3개만 디버깅 출력
                        print(f"   ⚠️ 레코드 {i} 원본 데이터: {row}")
                    continue

            logger.info(f"{sector_code} 파싱 완료: {len(parsed_data)}개 레코드")
            return parsed_data

        except Exception as e:
            logger.error(f"응답 파싱 실패 ({sector_code}): {e}")
            return []

    def _should_stop_collection(self, parsed_data: List[Dict[str, Any]], sector_code: str) -> bool:
        """수집 종료 조건 체크"""
        try:
            if not parsed_data:
                return True

            # 가장 오래된 데이터의 날짜 확인
            oldest_date = min(item['date'] for item in parsed_data)

            # 5년 전 날짜보다 오래된 경우 종료
            if oldest_date <= self.five_years_ago_str:
                logger.info(f"{sector_code}: 5년 전 날짜 도달 ({oldest_date})")
                return True

            return False

        except Exception as e:
            logger.error(f"종료 조건 체크 실패: {e}")
            return True

    def _prepare_database(self) -> bool:
        """데이터베이스 준비"""
        try:
            # 연결 테스트
            if not self.db_service.test_connection():
                logger.error("데이터베이스 연결 실패")
                return False

            # 스키마 생성
            if not self.db_service.create_schema_if_not_exists():
                logger.error("스키마 생성 실패")
                return False

            return True

        except Exception as e:
            logger.error(f"데이터베이스 준비 실패: {e}")
            return False

    def _clean_string(self, value: Any) -> str:
        """문자열 정리"""
        return str(value).strip() if value else ""

    def _safe_int(self, value: Any) -> int:
        """안전한 정수 변환 (문자열 지원)"""
        try:
            if isinstance(value, str):
                # 빈 문자열 처리
                if not value.strip():
                    return 0
                # 부호 제거 및 숫자만 추출
                cleaned = ''.join(c for c in value if c.isdigit())
                return int(cleaned) if cleaned else 0
            return int(value) if value else 0
        except (ValueError, TypeError):
            return 0

    def _safe_float(self, value: Any) -> float:
        """안전한 실수 변환 (문자열 지원)"""
        try:
            if isinstance(value, str):
                # 빈 문자열 처리
                if not value.strip():
                    return 0.0
                # 부호 제거 및 소수점 포함 숫자 추출
                cleaned = ''.join(c for c in value if c.isdigit() or c == '.')
                if cleaned:
                    # 지수값은 100으로 나누어서 실제 지수로 변환
                    raw_value = float(cleaned)
                    return raw_value / 100.0  # 89443 → 894.43
                return 0.0
            return float(value) if value else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _create_success_result(self) -> Dict[str, Any]:
        """성공 결과 생성"""
        elapsed_time = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        return {
            'success': True,
            'total_sectors': self.stats['total_sectors'],
            'completed_sectors': self.stats['completed_sectors'],
            'failed_sectors': self.stats['failed_sectors'],
            'total_records': self.stats['total_records'],
            'elapsed_time': elapsed_time,
            'sectors_detail': self.stats['sectors_detail']
        }

    def _create_error_result(self, error_message: str) -> Dict[str, Any]:
        """오류 결과 생성"""
        return {
            'success': False,
            'error': error_message,
            'total_sectors': self.stats['total_sectors'],
            'completed_sectors': self.stats['completed_sectors'],
            'failed_sectors': self.stats['failed_sectors']
        }

    def get_collection_stats(self) -> Dict[str, Any]:
        """수집 통계 반환"""
        return self.stats.copy()


# 편의 함수
def create_sector_daily_collector(session: KiwoomSession) -> SectorDailyCollector:
    """업종 일봉 수집기 인스턴스 생성"""
    return SectorDailyCollector(session)


# 테스트 함수
def test_sector_collector():
    """수집기 테스트 (키움 세션 없이)"""
    try:
        print("🔍 업종 일봉 수집기 테스트")
        print("=" * 50)

        # 데이터베이스 서비스 테스트
        from src.core.sector_database import test_sector_database

        if test_sector_database():
            print("✅ 데이터베이스 서비스 테스트 통과")
        else:
            print("❌ 데이터베이스 서비스 테스트 실패")
            return False

        print("\n🚀 수집기 테스트 완료!")
        print("💡 실제 수집은 키움 세션과 함께 실행하세요.")
        return True

    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        return False


if __name__ == "__main__":
    test_sector_collector()