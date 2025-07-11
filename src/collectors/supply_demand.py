"""
수급 데이터 수집기 모듈
키움 API OPT10060(상세수급) + OPT10014(프로그램매매)를 사용하여 수급 데이터 수집
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

from src.core.config import Config
from src.core.database import get_database_service
from src.api.base_session import KiwoomSession

logger = logging.getLogger(__name__)


class SupplyDemandCollector:
    """수급 데이터 수집기 클래스"""

    def __init__(self, session: KiwoomSession, config: Optional[Config] = None):
        self.session = session
        self.config = config or Config()
        self.db_service = get_database_service()

        # TR 코드 정의
        self.TR_SUPPLY_DEMAND = "opt10060"  # 상세수급
        self.TR_PROGRAM_TRADE = "opt10014"  # 프로그램매매

        # 수집 통계
        self.stats = {
            'total_stocks': 0,
            'completed_stocks': 0,
            'failed_stocks': 0,
            'total_records': 0,
            'start_time': None,
            'end_time': None
        }

        logger.info("수급 데이터 수집기 초기화 완료")

    def get_target_stocks(self, min_market_cap: int = 2000) -> List[Dict[str, Any]]:
        """
        시가총액 기준으로 대상 종목 조회

        Args:
            min_market_cap: 최소 시가총액 (억원 단위, 기본값: 2000억)

        Returns:
            List[Dict]: 대상 종목 정보 리스트
        """
        try:
            # MySQL 연결
            connection = self.db_service._get_connection('main')
            cursor = connection.cursor(dictionary=True)

            query = """
            SELECT 
                code,
                name,
                market,
                current_price,
                market_cap,
                ROUND(market_cap / 1000, 1) as market_cap_trillion
            FROM stocks 
            WHERE market_cap >= %s
              AND LENGTH(TRIM(code)) = 6
              AND code REGEXP '^[0-9]{6}$'
            ORDER BY market_cap DESC
            """

            cursor.execute(query, (min_market_cap,))
            stocks = cursor.fetchall()

            cursor.close()
            connection.close()

            logger.info(f"시가총액 {min_market_cap}억 이상 종목: {len(stocks)}개")
            return stocks

        except Exception as e:
            logger.error(f"대상 종목 조회 실패: {e}")
            return []

    def create_supply_demand_table(self, stock_code: str) -> bool:
        """종목별 수급 테이블 생성"""
        try:
            table_name = f"supply_demand_{stock_code}"

            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                date VARCHAR(8) PRIMARY KEY COMMENT '거래일자(YYYYMMDD)',

                -- OPT10060 상세 수급 데이터
                securities_buy BIGINT DEFAULT 0 COMMENT '증권자기 매수금액',
                securities_sell BIGINT DEFAULT 0 COMMENT '증권자기 매도금액',
                securities_net BIGINT DEFAULT 0 COMMENT '증권자기 순매수금액',

                bank_buy BIGINT DEFAULT 0 COMMENT '은행 매수금액',
                bank_sell BIGINT DEFAULT 0 COMMENT '은행 매도금액', 
                bank_net BIGINT DEFAULT 0 COMMENT '은행 순매수금액',

                insurance_buy BIGINT DEFAULT 0 COMMENT '보험 매수금액',
                insurance_sell BIGINT DEFAULT 0 COMMENT '보험 매도금액',
                insurance_net BIGINT DEFAULT 0 COMMENT '보험 순매수금액',

                trust_buy BIGINT DEFAULT 0 COMMENT '투신 매수금액',
                trust_sell BIGINT DEFAULT 0 COMMENT '투신 매도금액',
                trust_net BIGINT DEFAULT 0 COMMENT '투신 순매수금액',

                etc_corp_buy BIGINT DEFAULT 0 COMMENT '기타법인 매수금액',
                etc_corp_sell BIGINT DEFAULT 0 COMMENT '기타법인 매도금액',
                etc_corp_net BIGINT DEFAULT 0 COMMENT '기타법인 순매수금액',

                foreign_buy BIGINT DEFAULT 0 COMMENT '외국인 매수금액',
                foreign_sell BIGINT DEFAULT 0 COMMENT '외국인 매도금액', 
                foreign_net BIGINT DEFAULT 0 COMMENT '외국인 순매수금액',

                individual_buy BIGINT DEFAULT 0 COMMENT '개인 매수금액',
                individual_sell BIGINT DEFAULT 0 COMMENT '개인 매도금액',
                individual_net BIGINT DEFAULT 0 COMMENT '개인 순매수금액',

                -- OPT10014 프로그램매매 데이터
                program_buy BIGINT DEFAULT 0 COMMENT '프로그램매매 매수금액',
                program_sell BIGINT DEFAULT 0 COMMENT '프로그램매매 매도금액',
                program_net BIGINT DEFAULT 0 COMMENT '프로그램매매 순매수금액',

                -- 메타정보
                data_source VARCHAR(50) DEFAULT 'OPT10060,OPT10014' COMMENT '데이터출처',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',

                -- 인덱스
                INDEX idx_date (date),
                INDEX idx_foreign_net (foreign_net),
                INDEX idx_program_net (program_net)
            ) ENGINE=InnoDB COMMENT='수급 및 프로그램매매 데이터';
            """

            # supply_demand_db 스키마 사용
            connection = self.db_service._get_connection('supply')
            cursor = connection.cursor()
            cursor.execute(create_sql)
            connection.commit()

            cursor.close()
            connection.close()

            logger.info(f"테이블 생성 완료: {table_name}")
            return True

        except Exception as e:
            logger.error(f"테이블 생성 실패 ({stock_code}): {e}")
            return False

    def collect_supply_demand_data(self, stock_code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """OPT10060 상세수급 데이터 수집"""
        try:
            logger.info(f"[{stock_code}] 상세수급 데이터 수집: {start_date} ~ {end_date}")

            # API 요청 파라미터
            input_data = {
                '종목코드': stock_code,
                '기준일자': end_date,  # 종료일자부터 역순으로
                '금액수량구분': '1'  # 1:금액
            }

            # TR 요청
            response = self.session.get_connector().request_tr_data(
                rq_name="supply_demand_data",
                tr_code=self.TR_SUPPLY_DEMAND,
                prev_next=0,
                screen_no="0001",
                input_data=input_data
            )

            if not response or response.get('data', {}).get('error'):
                error_msg = response.get('data', {}).get('error', '알 수 없는 오류') if response else 'API 응답 없음'
                logger.error(f"[{stock_code}] 상세수급 API 요청 실패: {error_msg}")
                return {'success': False, 'data': []}

            # 응답 데이터 파싱
            data_list = self._parse_supply_demand_response(response, stock_code)

            # 날짜 범위 필터링
            filtered_data = []
            for data in data_list:
                if start_date <= data['date'] <= end_date:
                    filtered_data.append(data)

            logger.info(f"[{stock_code}] 상세수급 데이터 파싱 완료: {len(filtered_data)}개")
            return {'success': True, 'data': filtered_data}

        except Exception as e:
            logger.error(f"[{stock_code}] 상세수급 데이터 수집 실패: {e}")
            return {'success': False, 'data': []}

    def collect_program_trade_data(self, stock_code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """OPT10014 프로그램매매 데이터 수집"""
        try:
            logger.info(f"[{stock_code}] 프로그램매매 데이터 수집: {start_date} ~ {end_date}")

            # API 요청 파라미터
            input_data = {
                '종목코드': stock_code,
                '기준일자': end_date,  # 종료일자부터 역순으로
                '금액수량구분': '1'  # 1:금액
            }

            # TR 요청
            response = self.session.get_connector().request_tr_data(
                rq_name="program_trade_data",
                tr_code=self.TR_PROGRAM_TRADE,
                prev_next=0,
                screen_no="0002",
                input_data=input_data
            )

            if not response.get('success'):
                logger.error(f"[{stock_code}] 프로그램매매 API 요청 실패: {response.get('message')}")
                return {'success': False, 'data': []}

            # 응답 데이터 파싱
            data_list = self._parse_program_trade_response(response, stock_code)

            # 날짜 범위 필터링
            filtered_data = []
            for data in data_list:
                if start_date <= data['date'] <= end_date:
                    filtered_data.append(data)

            logger.info(f"[{stock_code}] 프로그램매매 데이터 파싱 완료: {len(filtered_data)}개")
            if filtered_data:
                return {'success': True, 'data': filtered_data}
            else:
                logger.warning(f"[{stock_code}] 수집된 데이터가 없습니다")
                return {'success': True, 'data': []}  # 빈 데이터도 성공으로 처리

        except Exception as e:
            logger.error(f"[{stock_code}] 프로그램매매 데이터 수집 실패: {e}")
            return {'success': False, 'data': []}

    def _parse_supply_demand_response(self, response: Dict[str, Any], stock_code: str) -> List[Dict[str, Any]]:
        """상세수급 API 응답 파싱"""
        try:
            data_info = response.get('data', {})
            raw_data = data_info.get('raw_data', [])

            if not raw_data:
                logger.warning(f"[{stock_code}] 상세수급 원시 데이터가 없습니다")
                return []

            parsed_data = []
            for row in raw_data:
                try:
                    # 금액 단위: 백만원 → 원 변환 (필요시)
                    data = {
                        'date': row.get('일자', '').replace('-', ''),
                        'securities_buy': self._safe_int(row.get('증권자기매수', 0)),
                        'securities_sell': self._safe_int(row.get('증권자기매도', 0)),
                        'securities_net': self._safe_int(row.get('증권자기', 0)),
                        'bank_buy': self._safe_int(row.get('은행매수', 0)),
                        'bank_sell': self._safe_int(row.get('은행매도', 0)),
                        'bank_net': self._safe_int(row.get('은행', 0)),
                        'insurance_buy': self._safe_int(row.get('보험매수', 0)),
                        'insurance_sell': self._safe_int(row.get('보험매도', 0)),
                        'insurance_net': self._safe_int(row.get('보험', 0)),
                        'trust_buy': self._safe_int(row.get('투신매수', 0)),
                        'trust_sell': self._safe_int(row.get('투신매도', 0)),
                        'trust_net': self._safe_int(row.get('투신', 0)),
                        'etc_corp_buy': self._safe_int(row.get('기타법인매수', 0)),
                        'etc_corp_sell': self._safe_int(row.get('기타법인매도', 0)),
                        'etc_corp_net': self._safe_int(row.get('기타법인', 0)),
                        'foreign_buy': self._safe_int(row.get('외국인매수', 0)),
                        'foreign_sell': self._safe_int(row.get('외국인매도', 0)),
                        'foreign_net': self._safe_int(row.get('외국인', 0)),
                        'individual_buy': self._safe_int(row.get('개인매수', 0)),
                        'individual_sell': self._safe_int(row.get('개인매도', 0)),
                        'individual_net': self._safe_int(row.get('개인', 0))
                    }

                    if data['date'] and len(data['date']) == 8:
                        parsed_data.append(data)

                except Exception as e:
                    logger.warning(f"[{stock_code}] 상세수급 데이터 파싱 오류: {e}")
                    continue

            return parsed_data

        except Exception as e:
            logger.error(f"[{stock_code}] 상세수급 응답 파싱 실패: {e}")
            return []

    def _parse_program_trade_response(self, response: Dict[str, Any], stock_code: str) -> List[Dict[str, Any]]:
        """프로그램매매 API 응답 파싱"""
        try:
            data_info = response.get('data', {})
            raw_data = data_info.get('raw_data', [])

            if not raw_data:
                logger.warning(f"[{stock_code}] 프로그램매매 원시 데이터가 없습니다")
                return []

            parsed_data = []
            for row in raw_data:
                try:
                    data = {
                        'date': row.get('일자', '').replace('-', ''),
                        'program_buy': self._safe_int(row.get('프로그램매수', 0)),
                        'program_sell': self._safe_int(row.get('프로그램매도', 0)),
                        'program_net': self._safe_int(row.get('프로그램매매', 0))
                    }

                    if data['date'] and len(data['date']) == 8:
                        parsed_data.append(data)

                except Exception as e:
                    logger.warning(f"[{stock_code}] 프로그램매매 데이터 파싱 오류: {e}")
                    continue

            return parsed_data

        except Exception as e:
            logger.error(f"[{stock_code}] 프로그램매매 응답 파싱 실패: {e}")
            return []

    def save_supply_demand_data(self, stock_code: str, supply_data: List[Dict], program_data: List[Dict]) -> bool:
        """수급 데이터 저장 (두 TR 데이터 병합)"""
        try:
            if not supply_data and not program_data:
                logger.warning(f"[{stock_code}] 저장할 데이터가 없습니다")
                return False

            # 날짜별로 데이터 병합
            merged_data = {}

            # 상세수급 데이터 추가
            for data in supply_data:
                date = data['date']
                merged_data[date] = data.copy()

            # 프로그램매매 데이터 병합
            for data in program_data:
                date = data['date']
                if date in merged_data:
                    merged_data[date].update({
                        'program_buy': data['program_buy'],
                        'program_sell': data['program_sell'],
                        'program_net': data['program_net']
                    })
                else:
                    # 상세수급 데이터가 없는 날짜는 프로그램매매만
                    merged_data[date] = {
                        'date': date,
                        'securities_buy': 0, 'securities_sell': 0, 'securities_net': 0,
                        'bank_buy': 0, 'bank_sell': 0, 'bank_net': 0,
                        'insurance_buy': 0, 'insurance_sell': 0, 'insurance_net': 0,
                        'trust_buy': 0, 'trust_sell': 0, 'trust_net': 0,
                        'etc_corp_buy': 0, 'etc_corp_sell': 0, 'etc_corp_net': 0,
                        'foreign_buy': 0, 'foreign_sell': 0, 'foreign_net': 0,
                        'individual_buy': 0, 'individual_sell': 0, 'individual_net': 0,
                        'program_buy': data['program_buy'],
                        'program_sell': data['program_sell'],
                        'program_net': data['program_net']
                    }

            # 데이터베이스 저장
            table_name = f"supply_demand_{stock_code}"
            connection = self.db_service._get_connection('supply')
            cursor = connection.cursor()

            insert_sql = f"""
            INSERT INTO {table_name} (
                date, securities_buy, securities_sell, securities_net,
                bank_buy, bank_sell, bank_net,
                insurance_buy, insurance_sell, insurance_net,
                trust_buy, trust_sell, trust_net,
                etc_corp_buy, etc_corp_sell, etc_corp_net,
                foreign_buy, foreign_sell, foreign_net,
                individual_buy, individual_sell, individual_net,
                program_buy, program_sell, program_net
            ) VALUES (
                %(date)s, %(securities_buy)s, %(securities_sell)s, %(securities_net)s,
                %(bank_buy)s, %(bank_sell)s, %(bank_net)s,
                %(insurance_buy)s, %(insurance_sell)s, %(insurance_net)s,
                %(trust_buy)s, %(trust_sell)s, %(trust_net)s,
                %(etc_corp_buy)s, %(etc_corp_sell)s, %(etc_corp_net)s,
                %(foreign_buy)s, %(foreign_sell)s, %(foreign_net)s,
                %(individual_buy)s, %(individual_sell)s, %(individual_net)s,
                %(program_buy)s, %(program_sell)s, %(program_net)s
            ) ON DUPLICATE KEY UPDATE
                securities_buy = VALUES(securities_buy),
                securities_sell = VALUES(securities_sell),
                securities_net = VALUES(securities_net),
                bank_buy = VALUES(bank_buy),
                bank_sell = VALUES(bank_sell),
                bank_net = VALUES(bank_net),
                insurance_buy = VALUES(insurance_buy),
                insurance_sell = VALUES(insurance_sell),
                insurance_net = VALUES(insurance_net),
                trust_buy = VALUES(trust_buy),
                trust_sell = VALUES(trust_sell),
                trust_net = VALUES(trust_net),
                etc_corp_buy = VALUES(etc_corp_buy),
                etc_corp_sell = VALUES(etc_corp_sell),
                etc_corp_net = VALUES(etc_corp_net),
                foreign_buy = VALUES(foreign_buy),
                foreign_sell = VALUES(foreign_sell),
                foreign_net = VALUES(foreign_net),
                individual_buy = VALUES(individual_buy),
                individual_sell = VALUES(individual_sell),
                individual_net = VALUES(individual_net),
                program_buy = VALUES(program_buy),
                program_sell = VALUES(program_sell),
                program_net = VALUES(program_net),
                updated_at = CURRENT_TIMESTAMP
            """

            # 배치 삽입
            data_list = list(merged_data.values())
            cursor.executemany(insert_sql, data_list)
            connection.commit()

            cursor.close()
            connection.close()

            logger.info(f"[{stock_code}] 수급 데이터 저장 완료: {len(data_list)}개")
            return True

        except Exception as e:
            logger.error(f"[{stock_code}] 수급 데이터 저장 실패: {e}")
            return False

    def collect_single_stock(self, stock_code: str, start_date: str = None, end_date: str = None) -> bool:
        """단일 종목 수급 데이터 수집"""
        try:
            # 기본 날짜 설정 (1년치)
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            if not start_date:
                start_dt = datetime.strptime(end_date, '%Y%m%d') - timedelta(days=365)
                start_date = start_dt.strftime('%Y%m%d')

            logger.info(f"[{stock_code}] 수급 데이터 수집 시작: {start_date} ~ {end_date}")

            # 1. 테이블 생성
            if not self.create_supply_demand_table(stock_code):
                return False

            # 2. 상세수급 데이터 수집 (OPT10060)
            supply_result = self.collect_supply_demand_data(stock_code, start_date, end_date)
            if not supply_result['success']:
                logger.error(f"[{stock_code}] 상세수급 데이터 수집 실패")
                return False

            # API 요청 간격 준수 (3.6초)
            time.sleep(3.6)

            # 3. 프로그램매매 데이터 수집 (OPT10014)
            program_result = self.collect_program_trade_data(stock_code, start_date, end_date)
            if not program_result['success']:
                logger.warning(f"[{stock_code}] 프로그램매매 데이터 수집 실패 (상세수급만 저장)")

            # 4. 데이터 저장
            success = self.save_supply_demand_data(
                stock_code,
                supply_result['data'],
                program_result.get('data', [])
            )

            if success:
                self.stats['completed_stocks'] += 1
                self.stats['total_records'] += len(supply_result['data'])
                logger.info(f"[{stock_code}] 수급 데이터 수집 완료")
            else:
                self.stats['failed_stocks'] += 1

            return success

        except Exception as e:
            logger.error(f"[{stock_code}] 수급 데이터 수집 실패: {e}")
            self.stats['failed_stocks'] += 1
            return False

    def collect_multiple_stocks(self, stock_codes: List[str] = None, min_market_cap: int = 2000) -> Dict[str, Any]:
        """다중 종목 수급 데이터 수집"""
        try:
            self.stats['start_time'] = datetime.now()

            # 대상 종목 조회
            if stock_codes:
                # 지정된 종목들
                target_stocks = []
                for code in stock_codes:
                    if len(code) == 6 and code.isdigit():
                        target_stocks.append({'code': code, 'name': f'종목{code}'})
            else:
                # 시가총액 기준 자동 선별
                target_stocks = self.get_target_stocks(min_market_cap)

            if not target_stocks:
                logger.error("수집 대상 종목이 없습니다")
                return {'success': False, 'message': '대상 종목 없음'}

            self.stats['total_stocks'] = len(target_stocks)
            logger.info(f"수급 데이터 수집 시작: {len(target_stocks)}개 종목")

            # 개별 종목 수집
            for i, stock_info in enumerate(target_stocks):
                stock_code = stock_info['code']
                stock_name = stock_info.get('name', stock_code)

                print(f"\n{'=' * 60}")
                print(f"📊 [{i + 1}/{len(target_stocks)}] {stock_code} {stock_name}")
                print(f"{'=' * 60}")

                # 단일 종목 수집
                success = self.collect_single_stock(stock_code)

                if success:
                    print(f"✅ {stock_code} 수급 데이터 수집 성공")
                else:
                    print(f"❌ {stock_code} 수급 데이터 수집 실패")

                # 진행률 출력
                completed_rate = (i + 1) / len(target_stocks) * 100
                print(f"📈 진행률: {completed_rate:.1f}% ({i + 1}/{len(target_stocks)})")

                # 다음 종목 처리 전 API 제한 준수 (3.6초 추가 대기)
                if i < len(target_stocks) - 1:
                    print(f"⏱️ API 제한 준수를 위해 3.6초 대기...")
                    time.sleep(3.6)

            self.stats['end_time'] = datetime.now()
            elapsed_time = self.stats['end_time'] - self.stats['start_time']

            # 최종 결과
            result = {
                'success': True,
                'total_stocks': self.stats['total_stocks'],
                'completed_stocks': self.stats['completed_stocks'],
                'failed_stocks': self.stats['failed_stocks'],
                'total_records': self.stats['total_records'],
                'elapsed_time': str(elapsed_time),
                'success_rate': (self.stats['completed_stocks'] / self.stats['total_stocks'] * 100) if self.stats[
                                                                                                           'total_stocks'] > 0 else 0
            }

            logger.info(f"수급 데이터 수집 완료: {result}")
            return result

        except Exception as e:
            logger.error(f"다중 종목 수급 데이터 수집 실패: {e}")
            return {'success': False, 'message': str(e)}

    def _safe_int(self, value: Any) -> int:
        """안전한 정수 변환"""
        try:
            if isinstance(value, str):
                # 문자열에서 숫자가 아닌 문자 제거 (콤마, 공백 등)
                value = ''.join(filter(lambda x: x.isdigit() or x == '-', value))

            if not value or value == '-' or value == '':
                return 0

            return int(float(value))
        except (ValueError, TypeError):
            return 0

    def get_collection_stats(self) -> Dict[str, Any]:
        """수집 통계 반환"""
        return self.stats.copy()


# 편의 함수들
def create_supply_demand_collector(session: KiwoomSession, config: Optional[Config] = None) -> SupplyDemandCollector:
    """수급 데이터 수집기 생성"""
    return SupplyDemandCollector(session, config)


def collect_supply_demand_single(stock_code: str, session: KiwoomSession = None) -> bool:
    """단일 종목 수급 데이터 수집 (편의 함수)"""
    try:
        if not session:
            from src.api.base_session import create_kiwoom_session
            session = create_kiwoom_session()

        if not session:
            logger.error("키움 API 세션 생성 실패")
            return False

        collector = SupplyDemandCollector(session)
        return collector.collect_single_stock(stock_code)

    except Exception as e:
        logger.error(f"단일 종목 수급 데이터 수집 실패: {e}")
        return False


def collect_supply_demand_market(min_market_cap: int = 2000, session: KiwoomSession = None) -> Dict[str, Any]:
    """시장 전체 수급 데이터 수집 (편의 함수)"""
    try:
        if not session:
            from src.api.base_session import create_kiwoom_session
            session = create_kiwoom_session()

        if not session:
            logger.error("키움 API 세션 생성 실패")
            return {'success': False, 'message': '키움 API 세션 생성 실패'}

        collector = SupplyDemandCollector(session)
        return collector.collect_multiple_stocks(min_market_cap=min_market_cap)

    except Exception as e:
        logger.error(f"시장 수급 데이터 수집 실패: {e}")
        return {'success': False, 'message': str(e)}