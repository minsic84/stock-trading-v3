#!/usr/bin/env python3
"""
파일 경로: src/core/supply_demand_database.py

수급 데이터 전용 데이터베이스 서비스
- supply_demand_db 스키마 관리
- 종목별 테이블 생성 (supply_demand_XXXXXX)
- 1년치 데이터 완성도 체크
- 연속 요청 지원을 위한 데이터 추적
"""
import mysql.connector
from mysql.connector import Error as MySQLError
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime, timedelta
import calendar

logger = logging.getLogger(__name__)


class SupplyDemandDatabaseService:
    """수급 데이터 전용 데이터베이스 서비스"""

    def __init__(self):
        # MySQL 연결 기본 설정
        self.mysql_base_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'stock_user',
            'password': 'StockPass2025!',
            'charset': 'utf8mb4',
            'autocommit': False
        }

        # 수급 데이터 스키마
        self.supply_schema = 'supply_demand_db'

        # 1년치 데이터 기준 (평일 기준 약 250일)
        self.one_year_days = 250

        # 수급 데이터 필드 정의
        self.supply_fields = [
            '일자', '현재가', '전일대비', '누적거래대금', '개인투자자',
            '외국인투자', '기관계', '금융투자', '보험', '투신', '기타금융',
            '은행', '연기금등', '사모펀드', '국가', '기타법인', '내외국인'
        ]

    def _get_connection(self) -> mysql.connector.MySQLConnection:
        """supply_demand_db 스키마 연결 반환"""
        config = self.mysql_base_config.copy()
        config['database'] = self.supply_schema
        return mysql.connector.connect(**config)

    def _get_main_connection(self) -> mysql.connector.MySQLConnection:
        """main 스키마 연결 반환 (stock_codes 조회용)"""
        config = self.mysql_base_config.copy()
        config['database'] = 'stock_trading_db'
        return mysql.connector.connect(**config)

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"수급 DB 연결 테스트 실패: {e}")
            return False

    def create_schema_if_not_exists(self) -> bool:
        """supply_demand_db 스키마 생성"""
        try:
            # 스키마 없는 연결로 시작
            config = self.mysql_base_config.copy()
            config.pop('database', None)  # database 키 제거

            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()

            # 스키마 생성
            cursor.execute(f"""
                CREATE DATABASE IF NOT EXISTS {self.supply_schema}
                CHARACTER SET utf8mb4
                COLLATE utf8mb4_unicode_ci
            """)

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"수급 스키마 '{self.supply_schema}' 준비 완료")
            return True

        except Exception as e:
            logger.error(f"수급 스키마 생성 실패: {e}")
            return False

    def get_all_stock_codes(self) -> List[Dict[str, Any]]:
        """stock_codes 테이블에서 모든 활성 종목 조회"""
        try:
            conn = self._get_main_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute("""
                SELECT code, name, market 
                FROM stock_codes 
                WHERE is_active = TRUE 
                ORDER BY code
            """)

            results = cursor.fetchall()
            cursor.close()
            conn.close()

            logger.info(f"활성 종목 조회 완료: {len(results)}개")
            return results

        except Exception as e:
            logger.error(f"종목 조회 실패: {e}")
            return []

    def table_exists(self, stock_code: str) -> bool:
        """종목별 수급 테이블 존재 여부 확인"""
        try:
            table_name = f"supply_demand_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """, (self.supply_schema, table_name))

            result = cursor.fetchone()
            exists = result[0] > 0 if result else False

            cursor.close()
            conn.close()

            return exists

        except Exception as e:
            logger.error(f"테이블 존재 확인 실패 {stock_code}: {e}")
            return False

    def create_supply_demand_table(self, stock_code: str) -> bool:
        """종목별 수급 데이터 테이블 생성"""
        try:
            table_name = f"supply_demand_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date VARCHAR(8) NOT NULL COMMENT '일자(YYYYMMDD)',
                current_price INT DEFAULT 0 COMMENT '현재가',
                prev_day_diff INT DEFAULT 0 COMMENT '전일대비',
                trading_value BIGINT DEFAULT 0 COMMENT '누적거래대금',

                -- 투자자별 수급 데이터 (API 필드명과 정확히 매칭)
                individual_investor BIGINT DEFAULT 0 COMMENT '개인투자자',
                foreign_investment BIGINT DEFAULT 0 COMMENT '외국인투자',
                institution_total BIGINT DEFAULT 0 COMMENT '기관계',
                financial_investment BIGINT DEFAULT 0 COMMENT '금융투자',
                insurance BIGINT DEFAULT 0 COMMENT '보험',
                investment_trust BIGINT DEFAULT 0 COMMENT '투신',
                other_finance BIGINT DEFAULT 0 COMMENT '기타금융',
                bank BIGINT DEFAULT 0 COMMENT '은행',
                pension_fund BIGINT DEFAULT 0 COMMENT '연기금등',
                private_fund BIGINT DEFAULT 0 COMMENT '사모펀드',
                government BIGINT DEFAULT 0 COMMENT '국가',
                other_corporation BIGINT DEFAULT 0 COMMENT '기타법인',
                foreign_domestic BIGINT DEFAULT 0 COMMENT '내외국인',

                -- 메타 정보
                data_source VARCHAR(20) DEFAULT 'OPT10060' COMMENT '데이터 출처',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',

                -- 인덱스
                UNIQUE KEY uk_date (date),
                INDEX idx_date (date),
                INDEX idx_individual (individual_investor),
                INDEX idx_foreign (foreign_investment),
                INDEX idx_institution_total (institution_total)
            ) ENGINE=InnoDB 
            CHARACTER SET utf8mb4 
            COLLATE utf8mb4_unicode_ci
            COMMENT='{stock_code} 종목 수급 데이터'
            """

            cursor.execute(create_sql)
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"수급 테이블 생성 완료: {table_name}")
            return True

        except Exception as e:
            logger.error(f"수급 테이블 생성 실패 {stock_code}: {e}")
            return False

    def get_data_completeness(self, stock_code: str) -> Dict[str, Any]:
        """종목의 데이터 완성도 체크"""
        try:
            table_name = f"supply_demand_{stock_code}"

            # 테이블이 없으면 완성도 0%
            if not self.table_exists(stock_code):
                return {
                    'stock_code': stock_code,
                    'table_exists': False,
                    'total_records': 0,
                    'latest_date': '',
                    'oldest_date': '',
                    'is_complete': False,
                    'completion_rate': 0.0,
                    'missing_days': self.one_year_days,
                    'needs_update': True,
                    'collection_mode': 'full'  # 전체 수집 필요
                }

            conn = self._get_connection()
            cursor = conn.cursor()

            # 기본 통계 조회
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    MIN(date) as oldest_date,
                    MAX(date) as latest_date
                FROM {table_name}
                WHERE date IS NOT NULL AND date != ''
            """)

            result = cursor.fetchone()
            total_records = result[0] if result else 0
            oldest_date = result[1] if result and result[1] else ''
            latest_date = result[2] if result and result[2] else ''

            cursor.close()
            conn.close()

            # 완성도 계산
            completion_rate = min(total_records / self.one_year_days * 100, 100.0)
            is_complete = total_records >= self.one_year_days
            missing_days = max(self.one_year_days - total_records, 0)

            # 수집 모드 결정
            if is_complete:
                collection_mode = 'update'  # 업데이트만 필요
            else:
                collection_mode = 'continue'  # 연속 수집 필요

            # 최신 데이터 업데이트 필요 여부 (최신 날짜가 3일 이전이면 업데이트 필요)
            needs_update = True
            if latest_date:
                try:
                    latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                    today = datetime.now()
                    days_diff = (today - latest_dt).days
                    needs_update = days_diff > 3  # 3일 이상 차이나면 업데이트 필요
                except:
                    needs_update = True

            return {
                'stock_code': stock_code,
                'table_exists': True,
                'total_records': total_records,
                'latest_date': latest_date,
                'oldest_date': oldest_date,
                'is_complete': is_complete,
                'completion_rate': completion_rate,
                'missing_days': missing_days,
                'needs_update': needs_update,
                'collection_mode': collection_mode
            }

        except Exception as e:
            logger.error(f"데이터 완성도 체크 실패 {stock_code}: {e}")
            return {
                'stock_code': stock_code,
                'table_exists': False,
                'total_records': 0,
                'latest_date': '',
                'oldest_date': '',
                'is_complete': False,
                'completion_rate': 0.0,
                'missing_days': self.one_year_days,
                'needs_update': True,
                'collection_mode': 'full'
            }

    def save_supply_demand_data(self, stock_code: str, data_list: List[Dict[str, Any]]) -> int:
        """수급 데이터 저장 (중복 방지)"""
        try:
            if not data_list:
                return 0

            table_name = f"supply_demand_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            # INSERT ... ON DUPLICATE KEY UPDATE 사용
            insert_sql = f"""
            INSERT INTO {table_name} (
                date, current_price, prev_day_diff, trading_value,
                individual_investor, foreign_investment, institution_total, financial_investment,
                insurance, investment_trust, other_finance, bank,
                pension_fund, private_fund, government, other_corporation, foreign_domestic,
                data_source, created_at
            ) VALUES (
                %(date)s, %(current_price)s, %(prev_day_diff)s, %(trading_value)s,
                %(individual_investor)s, %(foreign_investment)s, %(institution_total)s, %(financial_investment)s,
                %(insurance)s, %(investment_trust)s, %(other_finance)s, %(bank)s,
                %(pension_fund)s, %(private_fund)s, %(government)s, %(other_corporation)s, %(foreign_domestic)s,
                %(data_source)s, %(created_at)s
            ) ON DUPLICATE KEY UPDATE
                current_price = VALUES(current_price),
                prev_day_diff = VALUES(prev_day_diff),
                trading_value = VALUES(trading_value),
                individual_investor = VALUES(individual_investor),
                foreign_investment = VALUES(foreign_investment),
                institution_total = VALUES(institution_total),
                financial_investment = VALUES(financial_investment),
                insurance = VALUES(insurance),
                investment_trust = VALUES(investment_trust),
                other_finance = VALUES(other_finance),
                bank = VALUES(bank),
                pension_fund = VALUES(pension_fund),
                private_fund = VALUES(private_fund),
                government = VALUES(government),
                other_corporation = VALUES(other_corporation),
                foreign_domestic = VALUES(foreign_domestic),
                updated_at = CURRENT_TIMESTAMP
            """

            # 데이터 준비
            save_data = []
            current_time = datetime.now()

            for item in data_list:
                # 필드 매핑 (API 응답 → DB 필드)
                save_record = {
                    'date': item.get('일자', '').replace('-', ''),
                    'current_price': self._parse_int(item.get('현재가', 0)),
                    'prev_day_diff': self._parse_int(item.get('전일대비', 0)),
                    'trading_value': self._parse_int(item.get('누적거래대금', 0)),
                    'individual_investor': self._parse_int(item.get('개인투자자', 0)),
                    'foreign_investment': self._parse_int(item.get('외국인투자', 0)),
                    'institution_total': self._parse_int(item.get('기관계', 0)),
                    'financial_investment': self._parse_int(item.get('금융투자', 0)),
                    'insurance': self._parse_int(item.get('보험', 0)),
                    'investment_trust': self._parse_int(item.get('투신', 0)),
                    'other_finance': self._parse_int(item.get('기타금융', 0)),
                    'bank': self._parse_int(item.get('은행', 0)),
                    'pension_fund': self._parse_int(item.get('연기금등', 0)),
                    'private_fund': self._parse_int(item.get('사모펀드', 0)),
                    'government': self._parse_int(item.get('국가', 0)),
                    'other_corporation': self._parse_int(item.get('기타법인', 0)),
                    'foreign_domestic': self._parse_int(item.get('내외국인', 0)),
                    'data_source': 'OPT10060',
                    'created_at': current_time
                }

                # 날짜가 유효한 경우만 추가
                if save_record['date'] and len(save_record['date']) == 8:
                    save_data.append(save_record)

            # 배치 저장
            if save_data:
                cursor.executemany(insert_sql, save_data)
                conn.commit()

            cursor.close()
            conn.close()

            logger.info(f"수급 데이터 저장 완료 {stock_code}: {len(save_data)}건")
            return len(save_data)

        except Exception as e:
            logger.error(f"수급 데이터 저장 실패 {stock_code}: {e}")
            return 0

    def _parse_int(self, value) -> int:
        """안전한 정수 변환"""
        if value is None or value == '':
            return 0

        try:
            # 문자열에서 숫자만 추출
            if isinstance(value, str):
                # 콤마, 공백 제거
                clean_value = value.replace(',', '').replace(' ', '').strip()
                if not clean_value or clean_value == '-':
                    return 0
                return int(float(clean_value))
            else:
                return int(value)
        except (ValueError, TypeError):
            return 0

    def get_collection_summary(self) -> Dict[str, Any]:
        """전체 수급 데이터 수집 현황 요약"""
        try:
            all_stocks = self.get_all_stock_codes()
            total_stocks = len(all_stocks)

            completed_stocks = 0
            pending_stocks = 0
            total_records = 0

            for stock in all_stocks:
                completeness = self.get_data_completeness(stock['code'])
                if completeness['is_complete']:
                    completed_stocks += 1
                else:
                    pending_stocks += 1
                total_records += completeness['total_records']

            completion_rate = (completed_stocks / total_stocks * 100) if total_stocks > 0 else 0

            return {
                'total_stocks': total_stocks,
                'completed_stocks': completed_stocks,
                'pending_stocks': pending_stocks,
                'completion_rate': completion_rate,
                'total_records': total_records,
                'checked_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"수집 현황 요약 실패: {e}")
            return {
                'total_stocks': 0,
                'completed_stocks': 0,
                'pending_stocks': 0,
                'completion_rate': 0.0,
                'total_records': 0,
                'error': str(e)
            }


# 편의 함수
def get_supply_demand_service() -> SupplyDemandDatabaseService:
    """수급 데이터베이스 서비스 인스턴스 반환"""
    return SupplyDemandDatabaseService()


if __name__ == "__main__":
    # 테스트 실행
    print("🧪 수급 데이터베이스 서비스 테스트")
    print("=" * 50)

    service = SupplyDemandDatabaseService()

    # 1. 연결 테스트
    print("1. 연결 테스트...")
    if service.test_connection():
        print("   ✅ 연결 성공")
    else:
        print("   ❌ 연결 실패")

    # 2. 스키마 생성
    print("2. 스키마 생성...")
    if service.create_schema_if_not_exists():
        print("   ✅ 스키마 준비 완료")
    else:
        print("   ❌ 스키마 생성 실패")

    # 3. 종목 조회 테스트
    print("3. 종목 조회 테스트...")
    stocks = service.get_all_stock_codes()
    print(f"   📊 조회된 종목: {len(stocks)}개")

    # 4. 샘플 종목으로 테스트
    if stocks:
        sample_stock = stocks[0]['code']
        print(f"4. 샘플 종목 테스트: {sample_stock}")

        # 데이터 완성도 체크
        completeness = service.get_data_completeness(sample_stock)
        print(f"   📊 완성도: {completeness['completion_rate']:.1f}%")
        print(f"   📅 수집 모드: {completeness['collection_mode']}")

    print("\n✅ 테스트 완료!")