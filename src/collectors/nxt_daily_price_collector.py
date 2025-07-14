#!/usr/bin/env python3
"""
파일 경로: src/collectors/nxt_daily_price_collector.py

NXT 전용 일봉 데이터 수집기
- stock_codes 테이블의 NXT 종목만 대상
- 5년치 일봉 데이터 기본 수집
- 600개 요청으로 최신 데이터 교체 업데이트
- date_specific_updater.py 로직 활용
"""

import sys
import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.nxt_database import NXTDatabaseService
from src.api.base_session import create_kiwoom_session
from src.core.config import Config

logger = logging.getLogger(__name__)


class NXTDailyPriceCollector:
    """NXT 전용 일봉 데이터 수집기"""

    def __init__(self, config: Optional[Config] = None):
        """NXT 수집기 초기화"""
        self.config = config or Config()
        self.nxt_db = NXTDatabaseService()
        self.session = None
        self.kiwoom = None

        # 수집 통계
        self.stats = {
            'total_stocks': 0,
            'collected_stocks': 0,
            'updated_stocks': 0,
            'failed_stocks': 0,
            'start_time': None,
            'end_time': None
        }

    def connect_kiwoom(self) -> bool:
        """키움 API 연결"""
        try:
            print("🔌 키움 API 연결 중...")

            self.session = create_kiwoom_session(auto_login=True, show_progress=True)
            if not self.session or not self.session.is_ready():
                print("❌ 키움 API 연결 실패")
                return False

            self.kiwoom = self.session.get_connector()
            print("✅ 키움 API 연결 완료")
            return True

        except Exception as e:
            logger.error(f"❌ 키움 API 연결 실패: {e}")
            print(f"❌ 키움 API 연결 실패: {e}")
            return False

    def collect_single_stock_daily_5years(self, stock_code: str) -> List[Dict[str, Any]]:
        """5년치 일봉 데이터 수집 (연속 요청) - daily_price.py 로직 참고"""
        try:
            print(f"📊 {stock_code} 5년치 데이터 수집 시작")

            all_daily_data = []
            prev_next = "0"  # 첫 요청은 0
            request_count = 0
            max_requests = 10  # 최대 10회 요청

            # 오늘 날짜 기준
            today = datetime.now().strftime('%Y%m%d')

            while request_count < max_requests:
                try:
                    print(f"  📥 {request_count + 1}차 요청 (prev_next: {prev_next})")

                    # TR 요청 데이터 (_AL 추가)
                    input_data = {
                        "종목코드": f"{stock_code}_AL",  # _AL 접미사 추가
                        "기준일자": today,
                        "수정주가구분": "1"
                    }

                    # API 요청 (prev_next 매개변수 추가)
                    response = self.kiwoom.request_tr_data(
                        rq_name="일봉차트조회",
                        tr_code="opt10081",
                        input_data=input_data,
                        screen_no="9002",  # daily_price.py와 동일
                        prev_next=prev_next
                    )

                    if not response:
                        print(f"  ❌ {request_count + 1}차 요청 응답 없음")
                        break

                    # 응답 데이터 파싱
                    daily_data = self._parse_daily_response(response)

                    if not daily_data:
                        print(f"  ❌ {request_count + 1}차 요청 데이터 없음")
                        break

                    all_daily_data.extend(daily_data)
                    print(f"  ✅ {request_count + 1}차 수집: {len(daily_data)}개 (누적: {len(all_daily_data)}개)")

                    # 연속조회 여부 확인 (daily_price.py 로직)
                    prev_next = response.get('prev_next', '0').strip()
                    print(f"  🔄 다음 prev_next: '{prev_next}'")

                    # prev_next가 '2'가 아니면 더 이상 데이터 없음
                    if prev_next != '2':
                        print(f"  🏁 연속조회 종료 (prev_next: '{prev_next}')")
                        break

                    # 5년치 충분히 수집되었는지 확인 (약 1,200개)
                    if len(all_daily_data) >= 1200:
                        print(f"  🎯 5년치 데이터 충분히 수집됨: {len(all_daily_data)}개")
                        break

                    request_count += 1

                    # API 제한 준수 (마지막이 아닌 경우)
                    if request_count < max_requests and prev_next == '2':
                        print(f"  ⏳ API 제한 대기 (3.6초)...")
                        time.sleep(self.config.api_request_delay_ms / 1000)

                except Exception as e:
                    print(f"  ❌ {request_count + 1}차 요청 실패: {e}")
                    break

            print(f"✅ {stock_code} 5년치 수집 완료: {len(all_daily_data)}개 ({request_count + 1}회 요청)")
            return all_daily_data

        except Exception as e:
            logger.error(f"❌ {stock_code} 5년치 수집 실패: {e}")
            return []

    def collect_single_stock_daily_recent(self, stock_code: str, days: int = 600) -> List[Dict[str, Any]]:
        """최근 일봉 데이터 수집 (1회 요청, 600개) - prev_next = '0'"""
        try:
            if not self.kiwoom:
                logger.error("키움 API가 연결되지 않았습니다")
                return []

            print(f"📊 {stock_code} 최근 {days}일 수집 시작 (prev_next = '0')")

            # 오늘 날짜 기준
            today = datetime.now().strftime('%Y%m%d')

            # TR 요청 데이터 (_AL 추가)
            input_data = {
                "종목코드": f"{stock_code}_AL",  # _AL 접미사 추가
                "기준일자": today,
                "수정주가구분": "1"  # 수정주가
            }

            # API 요청 (prev_next = "0"으로 최근 데이터만)
            response = self.kiwoom.request_tr_data(
                rq_name="일봉차트조회",
                tr_code="opt10081",
                input_data=input_data,
                screen_no="9002",  # daily_price.py와 동일
                prev_next="0"  # 최근 데이터만 (연속조회 안함)
            )

            if not response or 'error' in response:
                logger.error(f"{stock_code} API 응답 오류: {response}")
                return []

            # 응답 데이터 파싱
            daily_data = self._parse_daily_response(response)

            # 600개 제한 (최근 데이터 우선)
            if len(daily_data) > days:
                daily_data = daily_data[:days]

            logger.info(f"✅ {stock_code} 최근 데이터 수집 완료: {len(daily_data)}개")
            return daily_data

        except Exception as e:
            logger.error(f"❌ {stock_code} 최근 데이터 수집 실패: {e}")
            return []

    def _parse_daily_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """키움 API 응답 데이터 파싱"""
        try:
            daily_data = []

            # 응답 구조 확인
            if not response:
                return []

            # TR 코드 확인
            if response.get('tr_code') != 'opt10081':
                logger.error(f"잘못된 TR 코드: {response.get('tr_code')}")
                return []

            # 데이터 구조 확인 (기존 코드 참고)
            data_info = response.get('data', {})
            if not data_info.get('parsed', False):
                logger.error("응답 데이터가 파싱되지 않음")
                return []

            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                logger.error("원시 데이터가 없습니다")
                return []

            for row in raw_data:
                try:
                    # 데이터 변환 (기존 daily_price.py 로직 참고)
                    data_item = {
                        'date': self._clean_string(row.get('일자', '')),
                        'open_price': self._safe_int(row.get('시가', 0)),
                        'high_price': self._safe_int(row.get('고가', 0)),
                        'low_price': self._safe_int(row.get('저가', 0)),
                        'close_price': self._safe_int(row.get('현재가', 0)),
                        'volume': self._safe_int(row.get('거래량', 0)),
                        'trading_value': self._safe_int(row.get('거래대금', 0)),
                        'prev_day_diff': self._safe_int(row.get('전일대비', 0)),
                        'change_rate': self._safe_int(row.get('등락율', 0)),
                        'data_source': 'OPT10081'
                    }

                    # 유효성 검증
                    if (data_item['date'] and
                            len(data_item['date']) == 8 and
                            data_item['close_price'] > 0):
                        daily_data.append(data_item)

                except Exception as e:
                    logger.warning(f"행 파싱 오류: {e}")
                    continue

            return daily_data

        except Exception as e:
            logger.error(f"응답 파싱 실패: {e}")
            return []

    def _clean_string(self, value: str) -> str:
        """문자열 정리"""
        if not value:
            return ""
        return str(value).strip().replace('+', '').replace('-', '')

    def _safe_int(self, value) -> int:
        """안전한 정수 변환"""
        try:
            if not value:
                return 0
            cleaned = str(value).strip().replace('+', '').replace(',', '')
            return int(float(cleaned))
        except:
            return 0

    def collect_single_stock(self, stock_code: str, force_update: bool = True) -> bool:
        """단일 NXT 종목 수집 (스마트 업데이트 모드)"""
        try:
            print(f"\n=== {stock_code} 수집 시작 ===")

            # 기존 데이터 상태 확인
            exists = self.nxt_db.daily_table_exists(stock_code)
            data_count = self.nxt_db.get_daily_data_count(stock_code) if exists else 0
            latest_date = self.nxt_db.get_latest_date(stock_code) if exists else None

            print(f"📋 기존 상태: 테이블={exists}, 데이터={data_count}개, 최신={latest_date}")

            # 수집 모드 결정
            if not exists or data_count < 1000:
                # 5년치 데이터 부족 → 전체 수집
                print(f"🔄 5년치 데이터 부족 → 전체 수집 모드")
                daily_data = self.collect_single_stock_daily_5years(stock_code)

                if not daily_data:
                    print(f"❌ {stock_code} 5년치 데이터 수집 실패")
                    self.stats['failed_stocks'] += 1
                    return False

                # 전체 교체 모드로 저장
                saved_count = self.nxt_db.save_daily_data_batch(
                    stock_code=stock_code,
                    daily_data=daily_data,
                    replace_mode=True,  # 전체 교체
                    update_recent_only=False
                )

                if saved_count > 0:
                    action = "신규수집" if not exists else "전체교체"
                    print(f"✅ {stock_code} {action} 완료: {saved_count}개 저장")

                    if exists:
                        self.stats['updated_stocks'] += 1
                    else:
                        self.stats['collected_stocks'] += 1
                    return True
                else:
                    print(f"❌ {stock_code} 저장 실패")
                    self.stats['failed_stocks'] += 1
                    return False

            else:
                # 5년치 데이터 충분 → 최근 데이터만 업데이트
                print(f"✅ 5년치 데이터 충분 → 최근 600개 업데이트 모드")
                daily_data = self.collect_single_stock_daily_recent(stock_code, days=600)

                if not daily_data:
                    print(f"❌ {stock_code} 최근 데이터 수집 실패")
                    self.stats['failed_stocks'] += 1
                    return False

                # 최근 데이터 업데이트 모드로 저장
                saved_count = self.nxt_db.save_daily_data_batch(
                    stock_code=stock_code,
                    daily_data=daily_data,
                    replace_mode=False,  # 전체 교체 안함
                    update_recent_only=True  # 최근 데이터만 업데이트
                )

                if saved_count > 0:
                    print(f"✅ {stock_code} 최근데이터 업데이트 완료: {saved_count}개 처리")
                    self.stats['updated_stocks'] += 1
                    return True
                else:
                    print(f"❌ {stock_code} 업데이트 저장 실패")
                    self.stats['failed_stocks'] += 1
                    return False

        except Exception as e:
            logger.error(f"❌ {stock_code} 수집 중 오류: {e}")
            print(f"❌ {stock_code} 수집 중 오류: {e}")
            self.stats['failed_stocks'] += 1
            return False

    def collect_all_nxt_stocks(self, force_update: bool = False) -> Dict[str, Any]:
        """모든 NXT 종목 수집"""
        try:
            print("🚀 NXT 전체 종목 일봉 수집 시작")
            print("=" * 60)

            self.stats['start_time'] = datetime.now()

            # NXT 종목 리스트 조회
            if force_update:
                # 강제 업데이트: 모든 NXT 종목
                nxt_codes = self.nxt_db.get_nxt_stock_codes()
                print(f"🔄 강제 업데이트 모드: 전체 {len(nxt_codes)}개 종목")
            else:
                # 일반 모드: 업데이트 필요한 종목만
                nxt_codes = self.nxt_db.get_nxt_stocks_need_update()
                total_nxt = len(self.nxt_db.get_nxt_stock_codes())
                print(f"📊 업데이트 필요: {len(nxt_codes)}개 / 전체 {total_nxt}개")

            if not nxt_codes:
                print("✅ 업데이트 필요한 종목이 없습니다")
                return {'message': '업데이트 불필요'}

            self.stats['total_stocks'] = len(nxt_codes)

            # 키움 API 연결
            if not self.connect_kiwoom():
                return {'error': '키움 API 연결 실패'}

            print(f"\n📈 수집 시작 - 예상 소요시간: {len(nxt_codes) * 3.6 / 60:.1f}분")

            # 종목별 수집 실행
            for idx, stock_code in enumerate(nxt_codes):
                try:
                    print(f"\n[{idx + 1}/{len(nxt_codes)}] {stock_code} 처리 중...")

                    success = self.collect_single_stock(stock_code, force_update=True)

                    # 진행률 표시
                    progress = (idx + 1) / len(nxt_codes) * 100
                    print(f"📊 진행률: {progress:.1f}% ({idx + 1}/{len(nxt_codes)})")

                    # API 제한 준수 (3.6초 대기)
                    if idx < len(nxt_codes) - 1:  # 마지막이 아닌 경우
                        print(f"⏳ API 제한 준수 대기 (3.6초)...")
                        time.sleep(self.config.api_request_delay_ms / 1000)

                except KeyboardInterrupt:
                    print(f"\n⚠️ 사용자 중단 요청")
                    break
                except Exception as e:
                    logger.error(f"❌ {stock_code} 처리 중 오류: {e}")
                    self.stats['failed_stocks'] += 1
                    continue

            self.stats['end_time'] = datetime.now()

            # 최종 결과 출력
            self._print_final_report()

            return {
                'success': True,
                'stats': self.stats,
                'total_processed': self.stats['collected_stocks'] + self.stats['updated_stocks'],
                'failed_count': self.stats['failed_stocks']
            }

        except Exception as e:
            logger.error(f"❌ 전체 수집 실패: {e}")
            return {'error': str(e)}

    def collect_specific_stocks(self, stock_codes: List[str]) -> Dict[str, Any]:
        """특정 NXT 종목들만 수집"""
        try:
            print(f"🎯 특정 종목 수집: {len(stock_codes)}개")
            print("=" * 50)

            self.stats['start_time'] = datetime.now()
            self.stats['total_stocks'] = len(stock_codes)

            # NXT 종목인지 확인
            nxt_codes = self.nxt_db.get_nxt_stock_codes()
            valid_codes = [code for code in stock_codes if code in nxt_codes]
            invalid_codes = [code for code in stock_codes if code not in nxt_codes]

            if invalid_codes:
                print(f"⚠️ NXT 종목이 아님: {invalid_codes}")

            if not valid_codes:
                return {'error': '유효한 NXT 종목이 없습니다'}

            # 키움 API 연결
            if not self.connect_kiwoom():
                return {'error': '키움 API 연결 실패'}

            # 종목별 수집
            for idx, stock_code in enumerate(valid_codes):
                try:
                    print(f"\n[{idx + 1}/{len(valid_codes)}] {stock_code} 수집...")
                    self.collect_single_stock(stock_code, force_update=True)

                    # API 대기
                    if idx < len(valid_codes) - 1:
                        time.sleep(self.config.api_request_delay_ms / 1000)

                except Exception as e:
                    logger.error(f"❌ {stock_code} 수집 실패: {e}")
                    continue

            self.stats['end_time'] = datetime.now()
            self._print_final_report()

            return {'success': True, 'stats': self.stats}

        except Exception as e:
            logger.error(f"❌ 특정 종목 수집 실패: {e}")
            return {'error': str(e)}

    def _print_final_report(self):
        """최종 수집 결과 리포트"""
        print("\n" + "=" * 60)
        print("🎉 NXT 일봉 수집 완료 리포트")
        print("=" * 60)

        # 기본 통계
        print(f"📊 수집 통계:")
        print(f"   전체 대상: {self.stats['total_stocks']}개")
        print(f"   신규 수집: {self.stats['collected_stocks']}개")
        print(f"   업데이트: {self.stats['updated_stocks']}개")
        print(f"   실패: {self.stats['failed_stocks']}개")

        total_processed = self.stats['collected_stocks'] + self.stats['updated_stocks']
        if self.stats['total_stocks'] > 0:
            success_rate = (total_processed / self.stats['total_stocks']) * 100
            print(f"   성공률: {success_rate:.1f}%")

        # 시간 통계
        if self.stats['start_time'] and self.stats['end_time']:
            elapsed = self.stats['end_time'] - self.stats['start_time']
            print(f"\n⏱️ 시간 통계:")
            print(f"   소요시간: {elapsed}")

            if total_processed > 0:
                avg_time = elapsed.total_seconds() / total_processed
                print(f"   평균 처리시간: {avg_time:.1f}초/종목")

        # NXT 전체 현황
        nxt_status = self.nxt_db.get_nxt_collection_status()
        print(f"\n📈 NXT 전체 현황:")
        print(f"   전체 NXT 종목: {nxt_status.get('total_nxt_stocks', 0)}개")
        print(f"   완료 종목: {nxt_status.get('completed_stocks', 0)}개")
        print(f"   완료율: {nxt_status.get('completion_rate', 0)}%")
        print(f"   업데이트 필요: {nxt_status.get('need_update', 0)}개")

        print("\n✅ 수집 작업 완료!")


# 편의 함수들
def collect_all_nxt_daily(force_update: bool = False) -> Dict[str, Any]:
    """모든 NXT 종목 일봉 수집 (편의 함수)"""
    collector = NXTDailyPriceCollector()
    return collector.collect_all_nxt_stocks(force_update=force_update)


def collect_nxt_daily_codes(stock_codes: List[str]) -> Dict[str, Any]:
    """특정 NXT 종목들 일봉 수집 (편의 함수)"""
    collector = NXTDailyPriceCollector()
    return collector.collect_specific_stocks(stock_codes)


def test_nxt_collector():
    """NXT 수집기 테스트"""
    print("🧪 NXT 수집기 테스트")
    print("=" * 50)

    try:
        collector = NXTDailyPriceCollector()

        # DB 연결 테스트
        if not collector.nxt_db.test_connection():
            print("❌ DB 연결 실패")
            return False

        # NXT 종목 조회 테스트
        nxt_codes = collector.nxt_db.get_nxt_stock_codes()
        print(f"✅ NXT 종목 조회: {len(nxt_codes)}개")

        if nxt_codes:
            print(f"   샘플: {nxt_codes[:5]}")

        # 수집 현황 테스트
        status = collector.nxt_db.get_nxt_collection_status()
        print(f"✅ 수집 현황:")
        print(f"   완료율: {status.get('completion_rate', 0)}%")
        print(f"   업데이트 필요: {status.get('need_update', 0)}개")

        print("\n✅ 테스트 완료!")
        return True

    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        return False


def main():
    """메인 실행 함수"""
    import argparse

    parser = argparse.ArgumentParser(description='NXT 전용 일봉 데이터 수집기')
    parser.add_argument('--test', action='store_true', help='테스트 모드 실행')
    parser.add_argument('--force', action='store_true', help='강제 업데이트 (모든 종목)')
    parser.add_argument('--codes', type=str, help='특정 종목 코드들 (쉼표 구분)')
    parser.add_argument('--status', action='store_true', help='현재 상태만 확인')

    args = parser.parse_args()

    if args.test:
        # 테스트 모드
        test_nxt_collector()

    elif args.status:
        # 상태 확인만
        try:
            nxt_db = NXTDatabaseService()
            status = nxt_db.get_nxt_collection_status()
            stats = nxt_db.get_nxt_statistics()

            print("📊 NXT 시스템 현재 상태")
            print("=" * 50)
            print(f"NXT 종목: {stats.get('total_stocks', 0)}개")
            print(f"완료율: {status.get('completion_rate', 0)}%")
            print(f"완료 종목: {status.get('completed_stocks', 0)}개")
            print(f"업데이트 필요: {status.get('need_update', 0)}개")
            print(f"총 레코드: {status.get('total_records', 0):,}개")

        except Exception as e:
            print(f"❌ 상태 확인 실패: {e}")

    elif args.codes:
        # 특정 종목 수집
        stock_codes = [code.strip() for code in args.codes.split(',')]
        print(f"🎯 특정 종목 수집: {stock_codes}")

        result = collect_nxt_daily_codes(stock_codes)
        if 'error' in result:
            print(f"❌ 수집 실패: {result['error']}")
        else:
            print("✅ 특정 종목 수집 완료")

    else:
        # 전체 수집 (기본)
        print("🚀 NXT 전체 일봉 수집 시작")

        result = collect_all_nxt_daily(force_update=args.force)
        if 'error' in result:
            print(f"❌ 수집 실패: {result['error']}")
        else:
            print("✅ 전체 수집 완료")


if __name__ == "__main__":
    main()