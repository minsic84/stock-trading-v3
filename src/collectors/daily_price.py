"""
일봉 데이터 수집기 모듈
키움 API opt10081(일봉차트조회)를 사용하여 일봉 데이터를 수집하고 데이터베이스에 저장
"""
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import time

from ..core.config import Config
from ..core.database import DatabaseService, get_database_manager
from ..api.connector import KiwoomAPIConnector, get_kiwoom_connector
from src.utils.trading_date import get_market_today

# 로거 설정
logger = logging.getLogger(__name__)

class DailyPriceCollector:
    """일봉 데이터 수집기 클래스"""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self.kiwoom = None
        self.db_service = None

        # 수집 상태
        self.collected_count = 0
        self.error_count = 0
        self.skipped_count = 0

        # TR 코드 정의
        self.TR_CODE = "opt10081"  # 일봉차트조회
        self.RQ_NAME = "일봉차트조회"

        self._setup()

    def _setup(self):
        """초기화 설정"""
        try:
            # 데이터베이스 서비스 초기화 (stock_info.py와 동일하게 수정)
            from ..core.database import get_database_service
            self.db_service = get_database_service()  # ← 이렇게 수정

            logger.info("일봉 데이터 수집기 초기화 완료")
        except Exception as e:
            logger.error(f"일봉 데이터 수집기 초기화 실패: {e}")
            raise

    def connect_kiwoom(self, auto_login: bool = True) -> bool:
        """키움 API 연결"""
        try:
            self.kiwoom = get_kiwoom_connector(self.config)

            if auto_login and not self.kiwoom.is_connected:
                logger.info("키움 API 로그인 시도...")
                if self.kiwoom.login():
                    logger.info("키움 API 로그인 성공")
                    return True
                else:
                    logger.error("키움 API 로그인 실패")
                    return False

            return True

        except Exception as e:
            logger.error(f"키움 API 연결 실패: {e}")
            return False

    def _should_continue_for_target_date(self, target_date: str, daily_data: List[Dict]) -> bool:
        """목표 날짜 기준으로 연속조회 여부 결정"""
        try:
            if not daily_data:
                return True  # 데이터 없으면 계속

            first_date = daily_data[0]['date']  # 최신 날짜
            last_date = daily_data[-1]['date']  # 가장 과거 날짜

            # 목표 날짜가 이번 배치에 포함되어 있으면 중단
            if last_date <= target_date <= first_date:
                logger.info(f"목표 날짜 {target_date}가 범위 내 포함: {last_date} ~ {first_date}")
                return False

            # 목표 날짜가 가장 과거보다 더 과거면 계속
            if target_date < last_date:
                logger.info(f"목표 날짜 {target_date}가 더 과거, 연속조회 필요")
                return True

            # 목표 날짜가 최신보다 미래면 중단 (이미 충분)
            logger.info(f"목표 날짜 {target_date}가 범위보다 미래, 연속조회 불필요")
            return False

        except Exception as e:
            logger.error(f"목표 날짜 체크 실패: {e}")
            return True  # 오류 시 기존 로직 유지

    def collect_single_stock(self, stock_code: str, start_date: str = None,
                             end_date: str = None, update_existing: bool = False,
                             target_date: str = None) -> bool:
        """단일 종목 일봉 데이터 수집"""
        try:
            print(f"=== {stock_code} 수집 시작 ===")

            if not self.kiwoom or not self.kiwoom.is_connected:
                print("키움 API가 연결되지 않았습니다")
                logger.error("키움 API가 연결되지 않았습니다")
                return False

            logger.info(f"일봉 데이터 수집 시작: {stock_code}")

            # 종목별 테이블 생성 (필요시)
            from src.utils.data_converter import get_data_converter
            converter = get_data_converter()
            if not converter.create_daily_table_for_stock(stock_code):
                print(f"❌ {stock_code}: 테이블 생성 실패")
                return False

            # 기존 데이터 확인 (업데이트 모드가 아닌 경우)
            if not update_existing:
                latest_date = self._get_latest_date_from_table(stock_code)
                if latest_date:
                    logger.info(f"{stock_code} 기존 데이터 존재 (최신: {latest_date})")
                    if not self._should_update(latest_date):
                        logger.info(f"{stock_code} 데이터가 최신상태, 수집 건너뛰기")
                        self.skipped_count += 1
                        return True

            # 시장 기준 오늘 날짜를 기준일로 사용
            market_today = get_market_today()
            base_date = market_today.strftime('%Y%m%d')

            # input_data에서
            input_data = {
                "종목코드": f"{stock_code}_AL",  # API 요청시에만 _AL 추가
                "기준일자": base_date,
                "수정주가구분": "1"
            }

            print(f"🕐 기준일자: {base_date} (시장 기준 오늘)")

            collected_data = []
            prev_next = "0"
            request_count = 0
            max_requests = 10

            while request_count < max_requests:
                print(f"TR 요청 {request_count + 1}/{max_requests}")

                # TR 요청 (screen_no 추가)
                response = self.kiwoom.request_tr_data(
                    rq_name=self.RQ_NAME,
                    tr_code=self.TR_CODE,
                    input_data=input_data,
                    screen_no="9002",  # 추가
                    prev_next=prev_next
                )

                if not response:
                    print("TR 요청 실패")
                    logger.error(f"{stock_code} TR 요청 실패")
                    self.error_count += 1
                    return False

                print(f"TR 응답 받음: {response}")

                # 데이터 파싱
                daily_data = self._parse_daily_data(response, stock_code)
                if not daily_data:
                    print("파싱된 데이터 없음")
                    logger.warning(f"{stock_code} 파싱된 데이터 없음")
                    break

                collected_data.extend(daily_data)
                print(f"수집된 데이터: {len(daily_data)}개")

                # 연속 조회 여부 확인
                # 연속 조회 여부 확인
                prev_next = response.get('prev_next', '0')
                print(f"연속조회: {prev_next}")

                # 목표 날짜가 설정된 경우 범위 체크
                if target_date and daily_data:
                    should_continue = self._should_continue_for_target_date(target_date, daily_data)
                    if not should_continue:
                        logger.info(f"{stock_code} 목표 날짜 {target_date} 수집 완료")
                        break

                if prev_next != '2':
                    logger.info(f"{stock_code} 모든 데이터 수집 완료")
                    break

                request_count += 1
                time.sleep(self.config.api_request_delay_ms / 1000)

            # 종목별 테이블에 데이터 저장
            if collected_data:
                saved_count = self._save_daily_data_to_table(stock_code, collected_data)
                logger.info(f"{stock_code} 일봉 데이터 저장 완료: {saved_count}개")
                self.collected_count += saved_count
                return True
            else:
                print("수집된 데이터 없음")
                logger.warning(f"{stock_code} 수집된 데이터 없음")
                return False

        except Exception as e:
            print(f"치명적 오류: {e}")
            logger.error(f"{stock_code} 일봉 데이터 수집 중 오류: {e}")
            self.error_count += 1
            return False

    def _save_daily_data_to_table(self, stock_code: str, daily_data: List[Dict[str, Any]]) -> int:
        """종목별 테이블에 일봉 데이터 저장 (MySQL 방식)"""
        saved_count = 0

        try:
            # MySQL daily_prices_db 스키마에 직접 저장
            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"

            # 테이블 존재 확인 및 생성
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            if not cursor.fetchone():
                # 테이블 생성
                create_sql = f"""
                CREATE TABLE {table_name} (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    date VARCHAR(8) NOT NULL COMMENT '일자(YYYYMMDD)',
                    open_price INT COMMENT '시가',
                    high_price INT COMMENT '고가', 
                    low_price INT COMMENT '저가',
                    close_price INT COMMENT '종가/현재가',
                    volume BIGINT COMMENT '거래량',
                    trading_value BIGINT COMMENT '거래대금',
                    prev_day_diff INT DEFAULT 0 COMMENT '전일대비',
                    change_rate INT DEFAULT 0 COMMENT '등락율(소수점2자리*100)',
                    data_source VARCHAR(20) DEFAULT 'OPT10081' COMMENT '데이터 출처',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',

                    UNIQUE KEY idx_date (date),
                    INDEX idx_close_price (close_price),
                    INDEX idx_volume (volume)
                ) ENGINE=InnoDB 
                CHARACTER SET utf8mb4 
                COLLATE utf8mb4_unicode_ci
                COMMENT='{stock_code} 종목 일봉 데이터'
                """
                cursor.execute(create_sql)

            # 데이터 삽입 (MySQL INSERT ... ON DUPLICATE KEY UPDATE)
            insert_sql = f"""
            INSERT INTO {table_name} (
                date, open_price, high_price, low_price, close_price,
                volume, trading_value, prev_day_diff, change_rate,
                data_source, created_at
            ) VALUES (
                %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
                %(volume)s, %(trading_value)s, %(prev_day_diff)s, %(change_rate)s,
                %(data_source)s, %(created_at)s
            ) ON DUPLICATE KEY UPDATE
                open_price = VALUES(open_price),
                high_price = VALUES(high_price),
                low_price = VALUES(low_price),
                close_price = VALUES(close_price),
                volume = VALUES(volume),
                trading_value = VALUES(trading_value),
                prev_day_diff = VALUES(prev_day_diff),
                change_rate = VALUES(change_rate),
                data_source = VALUES(data_source),
                created_at = VALUES(created_at)
            """

            for data in daily_data:
                try:
                    # 데이터 준비
                    insert_data = {
                        'date': data['date'],
                        'open_price': data['start_price'],
                        'high_price': data['high_price'],
                        'low_price': data['low_price'],
                        'close_price': data['current_price'],
                        'volume': data['volume'],
                        'trading_value': data['trading_value'],
                        'prev_day_diff': data['prev_day_diff'],
                        'change_rate': data['change_rate'],
                        'data_source': 'OPT10081',
                        'created_at': datetime.now()
                    }

                    cursor.execute(insert_sql, insert_data)
                    saved_count += 1

                except Exception as e:
                    logger.warning(f"{stock_code} 데이터 저장 실패: {data['date']} - {e}")
                    continue

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"{stock_code} 일봉 데이터 저장 완료: {saved_count}개")

        except Exception as e:
            logger.error(f"{stock_code} 테이블 저장 중 오류: {e}")

        return saved_count

    def _get_latest_date_from_table(self, stock_code: str) -> Optional[str]:
        """종목별 테이블에서 최신 날짜 조회"""
        try:
            table_name = f"daily_prices_{stock_code}"

            with self.db_service.db_manager.get_session() as session:
                from sqlalchemy import text

                # 테이블 존재 확인
                table_exists = session.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table_name"),
                    {"table_name": table_name}
                ).fetchone()

                if not table_exists:
                    return None

                # 최신 날짜 조회
                result = session.execute(
                    text(f"SELECT MAX(date) FROM {table_name}")
                ).fetchone()

                return result[0] if result and result[0] else None

        except Exception as e:
            logger.error(f"{stock_code} 최신 날짜 조회 실패: {e}")
            return None

    def _parse_daily_data(self, response: Dict[str, Any], stock_code: str) -> List[Dict[str, Any]]:
        """키움 API 응답 데이터 파싱 - connector에서 이미 파싱된 데이터 사용"""
        try:
            print(f"=== {stock_code} 데이터 파싱 시작 ===")

            tr_code = response.get('tr_code')
            rq_name = response.get('rq_name')

            print(f"TR 코드: {tr_code}, 요청명: {rq_name}")

            if tr_code != self.TR_CODE:
                print(f"잘못된 TR 코드: {tr_code}")
                return []

            # connector에서 이미 파싱된 데이터 사용
            data_info = response.get('data', {})
            print(f"데이터 정보: {data_info.get('parsed', False)}, 레코드 수: {data_info.get('repeat_count', 0)}")

            if not data_info.get('parsed', False):
                print(f"❌ 데이터가 파싱되지 않음: {data_info}")
                return []

            raw_data = data_info.get('raw_data', [])
            print(f"📊 파싱된 원시 데이터: {len(raw_data)}개")

            if not raw_data:
                print("❌ 원시 데이터가 없습니다")
                return []

            daily_data = []

            for i, row_data in enumerate(raw_data):
                try:
                    if i < 3 or i % 50 == 0:  # 처음 3개와 50개마다 로그 출력
                        print(f"데이터 {i + 1}/{len(raw_data)} 처리 중...")

                    # 원시 데이터에서 필드 추출
                    date = row_data.get("일자", "").strip()
                    current_price = row_data.get("현재가", "").strip()
                    volume = row_data.get("거래량", "").strip()
                    trading_value = row_data.get("거래대금", "").strip()
                    start_price = row_data.get("시가", "").strip()
                    high_price = row_data.get("고가", "").strip()
                    low_price = row_data.get("저가", "").strip()

                    if i < 3:  # 처음 3개 데이터는 상세 로그
                        print(f"원시 데이터 {i}: 날짜='{date}', 현재가='{current_price}', 거래량='{volume}'")

                    # 필수 데이터 확인
                    if not date or not current_price:
                        if i < 3:
                            print(f"필수 데이터 없음: 날짜='{date}', 현재가='{current_price}'")
                        continue

                    # 숫자 변환 (키움 API 특성상 부호나 콤마 제거)
                    try:
                        # 현재가 처리 (+ 또는 - 부호, 콤마 제거)
                        current_price_clean = current_price.replace('+', '').replace('-', '').replace(',', '')
                        current_price_int = int(current_price_clean) if current_price_clean else 0

                        volume_clean = volume.replace(',', '')
                        volume_int = int(volume_clean) if volume_clean else 0

                        trading_value_clean = trading_value.replace(',', '')
                        trading_value_int = int(trading_value_clean) if trading_value_clean else 0

                        start_price_clean = start_price.replace('+', '').replace('-', '').replace(',', '')
                        start_price_int = int(start_price_clean) if start_price_clean else 0

                        high_price_clean = high_price.replace('+', '').replace('-', '').replace(',', '')
                        high_price_int = int(high_price_clean) if high_price_clean else 0

                        low_price_clean = low_price.replace('+', '').replace('-', '').replace(',', '')
                        low_price_int = int(low_price_clean) if low_price_clean else 0

                        if current_price_int == 0:
                            if i < 3:
                                print(f"현재가가 0: '{current_price}' -> '{current_price_clean}'")
                            continue

                        data_item = {
                            'date': date,
                            'current_price': current_price_int,
                            'volume': volume_int,
                            'trading_value': trading_value_int,
                            'start_price': start_price_int,
                            'high_price': high_price_int,
                            'low_price': low_price_int,
                            'prev_day_diff': 0,
                            'change_rate': 0.0
                        }

                        daily_data.append(data_item)

                        if i < 3:  # 처음 3개만 상세 로그
                            print(f"✅ 데이터 추가: {date} - {current_price_int:,}원")

                    except (ValueError, TypeError) as e:
                        if i < 3:
                            print(f"❌ 데이터 변환 오류: {e}")
                            print(f"원시 값들: 현재가='{current_price}', 날짜='{date}'")
                        continue

                except Exception as e:
                    if i < 3:
                        print(f"❌ 행 처리 오류 {i}: {e}")
                    continue

            print(f"✅ 파싱 완료: {len(daily_data)}개 데이터")

            if daily_data:
                first_item = daily_data[0]
                last_item = daily_data[-1]
                print(f"📊 첫 번째 데이터: {first_item['date']} - {first_item['current_price']:,}원")
                print(f"📊 마지막 데이터: {last_item['date']} - {last_item['current_price']:,}원")
            else:
                print("❌ 최종 파싱된 데이터가 없습니다")

            return daily_data

        except Exception as e:
            print(f"❌ 파싱 치명적 오류: {e}")
            import traceback
            print(f"스택 트레이스: {traceback.format_exc()}")
            return []

    def _save_daily_data(self, stock_code: str, daily_data: List[Dict[str, Any]]) -> int:
        """일봉 데이터 데이터베이스 저장"""
        saved_count = 0

        try:
            for data in daily_data:
                success = self.db_service.add_daily_price(
                    stock_code=stock_code,
                    date=data['date'],
                    current_price=data['current_price'],
                    volume=data['volume'],
                    trading_value=data['trading_value'],
                    start_price=data['start_price'],
                    high_price=data['high_price'],
                    low_price=data['low_price'],
                    prev_day_diff=data['prev_day_diff'],
                    change_rate=data['change_rate']
                )

                if success:
                    saved_count += 1
                else:
                    logger.warning(f"{stock_code} 데이터 저장 실패: {data['date']}")

        except Exception as e:
            logger.error(f"{stock_code} 데이터 저장 중 오류: {e}")

        return saved_count

    def _should_update(self, latest_date: str) -> bool:
        """데이터 업데이트 필요 여부 판단"""
        try:
            # 최신 데이터 날짜와 오늘 날짜 비교
            latest_dt = datetime.strptime(latest_date, '%Y%m%d')
            today = datetime.now()

            # 주말 고려 (금요일 데이터가 최신이면 월요일까지 업데이트 불필요)
            if today.weekday() == 0:  # 월요일
                # 금요일 데이터까지 있으면 괜찮음
                friday = today - timedelta(days=3)
                return latest_dt.date() < friday.date()
            elif today.weekday() == 6:  # 일요일
                # 금요일 데이터까지 있으면 괜찮음
                friday = today - timedelta(days=2)
                return latest_dt.date() < friday.date()
            else:
                # 평일: 전일 데이터까지 있으면 괜찮음
                yesterday = today - timedelta(days=1)
                return latest_dt.date() < yesterday.date()

        except Exception as e:
            logger.error(f"업데이트 필요 여부 판단 오류: {e}")
            return True  # 오류 시 업데이트 수행

    def collect_multiple_stocks(self, stock_codes: List[str],
                              start_date: str = None, end_date: str = None,
                              update_existing: bool = False,
                              progress_callback: Optional[callable] = None) -> Dict[str, Any]:
        """다중 종목 일봉 데이터 수집"""

        logger.info(f"다중 종목 일봉 데이터 수집 시작: {len(stock_codes)}개 종목")

        # 통계 초기화
        self.collected_count = 0
        self.error_count = 0
        self.skipped_count = 0

        results = {
            'success': [],
            'failed': [],
            'skipped': [],
            'total_collected': 0,
            'total_errors': 0,
            'total_skipped': 0
        }

        start_time = datetime.now()

        for idx, stock_code in enumerate(stock_codes):
            try:
                logger.info(f"진행률: {idx + 1}/{len(stock_codes)} - {stock_code}")

                # 진행률 콜백 호출
                if progress_callback:
                    progress_callback(idx + 1, len(stock_codes), stock_code)

                # 종목별 데이터 수집
                success = self.collect_single_stock(
                    stock_code, start_date, end_date, update_existing
                )

                if success:
                    results['success'].append(stock_code)
                else:
                    results['failed'].append(stock_code)

                # API 요청 제한 대기
                if idx < len(stock_codes) - 1:  # 마지막이 아닌 경우
                    time.sleep(self.config.api_request_delay_ms / 1000)

            except Exception as e:
                logger.error(f"{stock_code} 수집 중 예외 발생: {e}")
                results['failed'].append(stock_code)
                self.error_count += 1

        # 최종 통계
        results['total_collected'] = self.collected_count
        results['total_errors'] = self.error_count
        results['total_skipped'] = self.skipped_count
        results['elapsed_time'] = (datetime.now() - start_time).total_seconds()

        logger.info(f"다중 종목 수집 완료: 성공 {len(results['success'])}개, "
                   f"실패 {len(results['failed'])}개, 건너뛰기 {len(results['skipped'])}개")

        return results

    def get_stock_list_from_market(self, market: str = "ALL") -> List[str]:
        """시장별 종목 리스트 조회"""
        try:
            from pykrx import stock

            today = datetime.now().strftime('%Y%m%d')

            if market.upper() == "KOSPI":
                stock_codes = stock.get_market_ticker_list(today, market="KOSPI")
            elif market.upper() == "KOSDAQ":
                stock_codes = stock.get_market_ticker_list(today, market="KOSDAQ")
            else:  # ALL
                kospi = stock.get_market_ticker_list(today, market="KOSPI")
                kosdaq = stock.get_market_ticker_list(today, market="KOSDAQ")
                stock_codes = kospi + kosdaq

            logger.info(f"{market} 시장 종목 수: {len(stock_codes)}개")
            return stock_codes

        except Exception as e:
            logger.error(f"종목 리스트 조회 실패: {e}")
            return []

    def get_collection_status(self) -> Dict[str, Any]:
        """수집 상태 정보 반환"""
        return {
            'collected_count': self.collected_count,
            'error_count': self.error_count,
            'skipped_count': self.skipped_count,
            'kiwoom_connected': self.kiwoom.is_connected if self.kiwoom else False,
            'db_connected': self.db_service is not None
        }

# 편의 함수들
def collect_daily_price_single(stock_code: str, config: Optional[Config] = None) -> bool:
    """단일 종목 일봉 데이터 수집 (편의 함수)"""
    collector = DailyPriceCollector(config)

    if not collector.connect_kiwoom():
        return False

    return collector.collect_single_stock(stock_code)

def collect_daily_price_batch(stock_codes: List[str], config: Optional[Config] = None) -> Dict[str, Any]:
    """배치 일봉 데이터 수집 (편의 함수)"""
    collector = DailyPriceCollector(config)

    if not collector.connect_kiwoom():
        return {'error': '키움 API 연결 실패'}

    return collector.collect_multiple_stocks(stock_codes)

def collect_market_daily_prices(market: str = "ALL", config: Optional[Config] = None) -> Dict[str, Any]:
    """전체 시장 일봉 데이터 수집 (편의 함수)"""
    collector = DailyPriceCollector(config)

    if not collector.connect_kiwoom():
        return {'error': '키움 API 연결 실패'}

    stock_codes = collector.get_stock_list_from_market(market)
    if not stock_codes:
        return {'error': '종목 리스트 조회 실패'}

    return collector.collect_multiple_stocks(stock_codes)