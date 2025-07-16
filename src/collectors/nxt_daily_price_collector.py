#!/usr/bin/env python3
"""
파일 경로: src/collectors/nxt_daily_price_collector.py

NXT 전용 일봉 데이터 수집기 (날짜 정렬 기능 추가)
- stock_codes 테이블의 NXT 종목만 대상
- 5년치 일봉 데이터 기본 수집 (날짜 오름차순 정렬)
- 600개 요청으로 최신 데이터 교체 업데이트 (날짜 오름차순 정렬)
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
        """5년치 일봉 데이터 수집 (연속 요청) - 날짜 오름차순 정렬 적용"""
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

                    # API 요청
                    response = self.kiwoom.request_tr_data(
                        rq_name="nxt_daily_5years",
                        tr_code="opt10081",
                        input_data=input_data,
                        prev_next=int(prev_next),
                        screen_no="9999"
                    )

                    if not response:
                        print(f"  ❌ {request_count + 1}차 요청 실패: 응답 없음")
                        break

                    # 응답 파싱
                    daily_data = self._parse_daily_response(response, stock_code)

                    if not daily_data:
                        print(f"  ❌ {request_count + 1}차 파싱 실패: 데이터 없음")
                        break

                    print(f"  ✅ {request_count + 1}차 수집: {len(daily_data)}개")
                    all_daily_data.extend(daily_data)

                    # 연속 조회 확인
                    prev_next = response.get('prev_next', '0')
                    if prev_next != '2':
                        print(f"  🔚 연속 조회 완료 (prev_next: {prev_next})")
                        break

                    # API 제한 준수
                    time.sleep(3.6)
                    request_count += 1

                except Exception as e:
                    print(f"  ❌ {request_count + 1}차 요청 오류: {e}")
                    break

            # 📅 날짜 오름차순 정렬 (오래된 날짜 → 최신 날짜)
            if all_daily_data:
                print(f"  🔄 데이터 정렬 중...")
                all_daily_data.sort(key=lambda x: x.get('date', ''))
                print(f"✅ 5년치 수집 완료: 총 {len(all_daily_data)}개 (날짜순 정렬)")
                print(f"📅 기간: {all_daily_data[0]['date']} ~ {all_daily_data[-1]['date']}")
                return all_daily_data
            else:
                print("❌ 5년치 데이터 없음")
                return []

        except Exception as e:
            logger.error(f"❌ {stock_code} 5년치 수집 실패: {e}")
            print(f"❌ {stock_code} 5년치 수집 실패: {e}")
            return []

    def collect_single_stock_daily_recent(self, stock_code: str, days: int = 600) -> List[Dict[str, Any]]:
        """최근 N일 일봉 데이터 수집 - 날짜 오름차순 정렬 적용"""
        try:
            print(f"📊 {stock_code} 최근 {days}일 데이터 수집 시작")

            # 오늘 날짜 기준
            today = datetime.now().strftime('%Y%m%d')

            # TR 요청 데이터
            input_data = {
                "종목코드": f"{stock_code}_AL",  # _AL 접미사 추가
                "기준일자": today,
                "수정주가구분": "1"
            }

            # API 요청
            response = self.kiwoom.request_tr_data(
                rq_name="nxt_daily_recent",
                tr_code="opt10081",
                input_data=input_data,
                prev_next=0,
                screen_no="9999"
            )

            if not response:
                print(f"❌ {stock_code} 최근 데이터 요청 실패: 응답 없음")
                return []

            # 응답 파싱
            daily_data = self._parse_daily_response(response, stock_code)

            if not daily_data:
                print(f"❌ {stock_code} 최근 데이터 파싱 실패")
                return []

            # days 개수만큼 제한
            if len(daily_data) > days:
                daily_data = daily_data[:days]

            # 📅 날짜 오름차순 정렬 (오래된 날짜 → 최신 날짜)
            if daily_data:
                print(f"  🔄 데이터 정렬 중...")
                daily_data.sort(key=lambda x: x.get('date', ''))
                print(f"✅ 최근 데이터 수집 완료: {len(daily_data)}개 (날짜순 정렬)")
                print(f"📅 기간: {daily_data[0]['date']} ~ {daily_data[-1]['date']}")
                return daily_data
            else:
                print("❌ 최근 데이터 없음")
                return []

        except Exception as e:
            logger.error(f"❌ {stock_code} 최근 데이터 수집 실패: {e}")
            print(f"❌ {stock_code} 최근 데이터 수집 실패: {e}")
            return []

    def _parse_daily_response(self, response: Dict[str, Any], stock_code: str) -> List[Dict[str, Any]]:
        """일봉 응답 데이터 파싱"""
        try:
            daily_data = []

            # 응답 구조 확인
            data_info = response.get('data', {})
            if not data_info.get('parsed', False):
                logger.warning(f"{stock_code}: 응답이 파싱되지 않음")
                return []

            raw_data = data_info.get('raw_data', [])
            if not raw_data:
                logger.warning(f"{stock_code}: 원시 데이터 없음")
                return []

            print(f"  📊 파싱 중: {len(raw_data)}개 원시 데이터")

            # 개별 레코드 파싱
            for i, row in enumerate(raw_data):
                try:
                    # 날짜 정리 (YYYYMMDD)
                    date_str = self._clean_string(row.get('일자', ''))
                    if not date_str or len(date_str) != 8:
                        continue

                    # OHLCV 데이터 구성
                    data_item = {
                        'date': date_str,
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

                # 전체 교체 모드로 저장 (이미 정렬된 데이터)
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

                # 최근 데이터 업데이트 모드로 저장 (이미 정렬된 데이터)
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
        """모든 NXT 종목 수집 - 스마트 재시작 기능 적용"""
        try:
            print("🚀 NXT 전체 종목 일봉 수집 시작")
            print("=" * 60)

            self.stats['start_time'] = datetime.now()

            # 오늘 날짜 기준
            today = datetime.now().strftime('%Y%m%d')

            # 🎯 스마트 재시작: 미완료 지점부터 시작
            print("🔍 수집 대상 분석 중...")
            nxt_codes = self.nxt_db.get_nxt_stocks_smart_restart(
                force_update=force_update,
                target_date=today
            )

            if not nxt_codes:
                print("✅ 모든 종목이 이미 완료되었습니다!")
                return {
                    'status': 'already_completed',
                    'message': f'{today} 날짜 기준 모든 NXT 종목이 완료됨',
                    'total_stocks': 0,
                    'collected_stocks': 0,
                    'updated_stocks': 0,
                    'failed_stocks': 0
                }

            # 전체 통계 정보 조회
            _, total_count, completed_count = self.nxt_db.find_nxt_restart_position(today)

            print("📊 수집 계획:")
            print(f"   📈 전체 NXT 종목: {total_count}개")
            print(f"   ✅ 이미 완료: {completed_count}개 ({completed_count / total_count * 100:.1f}%)")
            print(f"   🔄 수집 대상: {len(nxt_codes)}개")
            print(f"   📍 시작 종목: {nxt_codes[0] if nxt_codes else 'N/A'}")
            print(f"   ⏱️ 예상 소요시간: {len(nxt_codes) * 3.6 / 60:.1f}분")

            if force_update:
                print("🔄 강제 업데이트 모드: 전체 종목 재수집")
            else:
                print(f"🎯 스마트 재시작 모드: {today} 날짜 기준 미완료 종목만 수집")

            self.stats['total_stocks'] = len(nxt_codes)

            # 키움 API 연결
            if not self.connect_kiwoom():
                return {'error': '키움 API 연결 실패'}

            # 개별 종목 수집
            print(f"\n📊 개별 종목 수집 시작")
            print("-" * 60)

            for i, stock_code in enumerate(nxt_codes, 1):
                try:
                    # 현재 진행상황 표시
                    current_position = completed_count + i
                    overall_progress = current_position / total_count * 100
                    batch_progress = i / len(nxt_codes) * 100

                    print(f"\n[전체: {current_position}/{total_count} ({overall_progress:.1f}%)] " +
                          f"[배치: {i}/{len(nxt_codes)} ({batch_progress:.1f}%)] {stock_code}")

                    # 종목 수집 실행
                    success = self.collect_single_stock(stock_code, force_update)

                    if success:
                        print(f"✅ {stock_code} 완료")
                    else:
                        print(f"❌ {stock_code} 실패")

                    # 중간 통계 출력 (50개마다)
                    if i % 50 == 0:
                        completed = self.stats['collected_stocks'] + self.stats['updated_stocks']
                        failed = self.stats['failed_stocks']
                        print(f"\n📊 중간 통계 ({i}/{len(nxt_codes)}):")
                        print(f"   ✅ 성공: {completed}개")
                        print(f"   ❌ 실패: {failed}개")
                        print(f"   📈 성공률: {completed / (completed + failed) * 100:.1f}%" if (
                                                                                                        completed + failed) > 0 else "   📈 성공률: 0%")

                    # API 제한 준수
                    if i < len(nxt_codes):  # 마지막이 아닌 경우
                        time.sleep(1.0)

                except KeyboardInterrupt:
                    print(f"\n⚠️ 사용자 중단 요청 (Ctrl+C)")
                    print(f"📊 중단 시점: {stock_code} ({i}/{len(nxt_codes)})")
                    print("💡 다시 실행하면 이 지점부터 이어서 수집됩니다.")
                    break

                except Exception as e:
                    logger.error(f"❌ {stock_code} 처리 중 오류: {e}")
                    print(f"❌ [{i}/{len(nxt_codes)}] {stock_code} 오류: {e}")
                    self.stats['failed_stocks'] += 1

            self.stats['end_time'] = datetime.now()

            # 최종 결과
            return self._create_final_result_with_restart_info(total_count, completed_count)

        except Exception as e:
            logger.error(f"❌ 전체 수집 실패: {e}")
            return {'error': str(e)}

    def _create_final_result_with_restart_info(self, total_count: int, initial_completed: int) -> Dict[str, Any]:
        """스마트 재시작 정보가 포함된 최종 결과 생성"""
        duration = self.stats['end_time'] - self.stats['start_time']
        completed = self.stats['collected_stocks'] + self.stats['updated_stocks']
        final_completed = initial_completed + completed

        result = {
            'status': 'completed',
            'restart_info': {
                'total_nxt_stocks': total_count,
                'initial_completed': initial_completed,
                'batch_processed': self.stats['total_stocks'],
                'batch_success': completed,
                'batch_failed': self.stats['failed_stocks'],
                'final_completed': final_completed,
                'overall_progress': final_completed / total_count * 100 if total_count > 0 else 0
            },
            'stats': {
                'total_stocks': self.stats['total_stocks'],
                'collected_stocks': self.stats['collected_stocks'],
                'updated_stocks': self.stats['updated_stocks'],
                'failed_stocks': self.stats['failed_stocks'],
                'success_rate': (completed / self.stats['total_stocks'] * 100) if self.stats['total_stocks'] > 0 else 0,
                'duration': str(duration),
                'start_time': self.stats['start_time'].isoformat(),
                'end_time': self.stats['end_time'].isoformat()
            }
        }

        # 결과 출력
        print("\n" + "=" * 60)
        print("🎉 NXT 일봉 수집 완료!")
        print("=" * 60)

        print("📊 이번 배치 결과:")
        print(f"   🎯 수집 대상: {result['stats']['total_stocks']}개")
        print(f"   ✅ 신규 수집: {result['stats']['collected_stocks']}개")
        print(f"   🔄 업데이트: {result['stats']['updated_stocks']}개")
        print(f"   ❌ 실패: {result['stats']['failed_stocks']}개")
        print(f"   📈 배치 성공률: {result['stats']['success_rate']:.1f}%")
        print(f"   ⏱️ 소요 시간: {result['stats']['duration']}")

        print("\n📊 전체 진행상황:")
        print(f"   📈 전체 NXT 종목: {result['restart_info']['total_nxt_stocks']}개")
        print(f"   ✅ 완료된 종목: {result['restart_info']['final_completed']}개")
        print(f"   📊 전체 진행률: {result['restart_info']['overall_progress']:.1f}%")

        remaining = total_count - final_completed
        if remaining > 0:
            print(f"   🔄 남은 종목: {remaining}개")
            print(f"   ⏱️ 예상 추가 시간: {remaining * 3.6 / 60:.1f}분")
            print("\n💡 다음에 실행하면 남은 종목부터 이어서 수집됩니다.")
        else:
            print("\n🎉 모든 NXT 종목 수집이 완료되었습니다!")

        return result

    def collect_specific_stocks(self, stock_codes: List[str]) -> Dict[str, Any]:
        """특정 종목들만 수집"""
        try:
            print(f"🎯 특정 종목 수집: {len(stock_codes)}개")
            print("=" * 60)

            self.stats['start_time'] = datetime.now()
            self.stats['total_stocks'] = len(stock_codes)

            # 키움 API 연결
            if not self.connect_kiwoom():
                return {'error': '키움 API 연결 실패'}

            # 개별 종목 수집
            for i, stock_code in enumerate(stock_codes, 1):
                try:
                    print(f"\n[{i}/{len(stock_codes)}] {stock_code} 처리 중...")

                    success = self.collect_single_stock(stock_code, force_update=True)

                    if success:
                        print(f"✅ [{i}/{len(stock_codes)}] {stock_code} 완료")
                    else:
                        print(f"❌ [{i}/{len(stock_codes)}] {stock_code} 실패")

                    # API 제한 준수
                    time.sleep(1.0)

                except Exception as e:
                    logger.error(f"❌ {stock_code} 처리 중 오류: {e}")
                    print(f"❌ [{i}/{len(stock_codes)}] {stock_code} 오류: {e}")
                    self.stats['failed_stocks'] += 1

            self.stats['end_time'] = datetime.now()

            # 최종 결과
            return self._create_final_result()

        except Exception as e:
            logger.error(f"❌ 특정 종목 수집 실패: {e}")
            return {'error': str(e)}

    def _create_final_result(self) -> Dict[str, Any]:
        """최종 결과 생성"""
        duration = self.stats['end_time'] - self.stats['start_time']
        completed = self.stats['collected_stocks'] + self.stats['updated_stocks']

        result = {
            'status': 'completed',
            'total_stocks': self.stats['total_stocks'],
            'collected_stocks': self.stats['collected_stocks'],
            'updated_stocks': self.stats['updated_stocks'],
            'failed_stocks': self.stats['failed_stocks'],
            'success_rate': (completed / self.stats['total_stocks'] * 100) if self.stats['total_stocks'] > 0 else 0,
            'duration': str(duration),
            'start_time': self.stats['start_time'].isoformat(),
            'end_time': self.stats['end_time'].isoformat()
        }

        # 결과 출력
        print("\n" + "=" * 60)
        print("🎉 NXT 일봉 수집 완료!")
        print("=" * 60)
        print(f"📊 처리 결과:")
        print(f"   전체 종목: {result['total_stocks']}개")
        print(f"   신규 수집: {result['collected_stocks']}개")
        print(f"   업데이트: {result['updated_stocks']}개")
        print(f"   실패: {result['failed_stocks']}개")
        print(f"   성공률: {result['success_rate']:.1f}%")
        print(f"   소요 시간: {result['duration']}")

        return result


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