#!/usr/bin/env python3
"""
파일 경로: src/core/sector_database.py

업종 데이터 전용 데이터베이스 서비스
- sector_data_db 스키마 관리
- kospi, kosdaq 테이블 생성 및 관리
- 5년치 업종 지수 데이터 저장
- 기존 시스템 패턴 일관성 유지
"""
import mysql.connector
from mysql.connector import Error as MySQLError
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class SectorDatabaseService:
    """업종 데이터 전용 데이터베이스 서비스"""

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

        # 업종 데이터 스키마
        self.schema_name = 'sector_data_db'

        # 지원하는 업종 코드와 테이블 매핑
        self.sector_mapping = {
            '001': 'kospi',  # KOSPI 종합지수
            '101': 'kosdaq'  # KOSDAQ 종합지수
        }

        logger.info("업종 데이터베이스 서비스 초기화 완료")

    def _get_connection(self) -> mysql.connector.MySQLConnection:
        """sector_data_db 스키마 연결 반환"""
        try:
            config = self.mysql_base_config.copy()
            config['database'] = self.schema_name

            connection = mysql.connector.connect(**config)
            return connection

        except MySQLError as e:
            logger.error(f"MySQL 연결 실패: {e}")
            raise

    def test_connection(self) -> bool:
        """데이터베이스 연결 테스트"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"연결 테스트 실패: {e}")
            return False

    def create_schema_if_not_exists(self) -> bool:
        """sector_data_db 스키마 생성 (존재하지 않는 경우)"""
        try:
            # 스키마 없이 연결
            config = self.mysql_base_config.copy()
            if 'database' in config:
                del config['database']

            connection = mysql.connector.connect(**config)
            cursor = connection.cursor()

            # 스키마 생성
            create_schema_sql = f"""
            CREATE SCHEMA IF NOT EXISTS {self.schema_name}
            DEFAULT CHARACTER SET utf8mb4 
            DEFAULT COLLATE utf8mb4_unicode_ci
            """

            cursor.execute(create_schema_sql)
            connection.commit()
            cursor.close()
            connection.close()

            logger.info(f"스키마 {self.schema_name} 생성/확인 완료")
            return True

        except MySQLError as e:
            logger.error(f"스키마 생성 실패: {e}")
            return False

    def table_exists(self, sector_code: str) -> bool:
        """업종 테이블 존재 여부 확인"""
        try:
            if sector_code not in self.sector_mapping:
                return False

            table_name = self.sector_mapping[sector_code]

            conn = self._get_connection()
            cursor = conn.cursor()

            check_sql = """
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
            """

            cursor.execute(check_sql, (self.schema_name, table_name))
            count = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            return count > 0

        except Exception as e:
            logger.error(f"테이블 존재 확인 실패 ({sector_code}): {e}")
            return False

    def create_sector_table(self, sector_code: str) -> bool:
        """업종별 테이블 생성"""
        try:
            if sector_code not in self.sector_mapping:
                logger.error(f"지원하지 않는 업종 코드: {sector_code}")
                return False

            table_name = self.sector_mapping[sector_code]
            sector_name = "KOSPI 종합지수" if sector_code == '001' else "KOSDAQ 종합지수"

            create_table_sql = f"""
            CREATE TABLE {table_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                date DATE NOT NULL,

                -- 지수 OHLC 데이터 (소수점 2자리)
                open_index DECIMAL(10,2) NOT NULL,      -- 시가지수
                high_index DECIMAL(10,2) NOT NULL,      -- 고가지수
                low_index DECIMAL(10,2) NOT NULL,       -- 저가지수
                close_index DECIMAL(10,2) NOT NULL,     -- 현재가(종가지수)

                -- 거래 정보
                volume BIGINT NOT NULL DEFAULT 0,       -- 거래량
                trading_value BIGINT NOT NULL DEFAULT 0, -- 거래대금

                -- 메타데이터 (sector_code 제거)
                data_source VARCHAR(20) DEFAULT 'OPT20006',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                -- 인덱스 (날짜별 중복 방지)
                UNIQUE KEY idx_date (date),
                KEY idx_close_index (close_index),
                KEY idx_volume (volume)
            ) ENGINE=InnoDB 
            CHARSET=utf8mb4 
            COMMENT='{sector_name} 일봉 데이터 (업종코드: {sector_code})'
            """

            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"테이블 생성 완료: {table_name} (업종: {sector_code})")
            return True

        except MySQLError as e:
            logger.error(f"테이블 생성 실패 ({sector_code}): {e}")
            return False

    def save_sector_data(self, sector_code: str, data_list: List[Dict[str, Any]]) -> int:
        """
        업종 데이터 일괄 저장 (중복 시 무시)

        Args:
            sector_code: 업종코드 (001, 101)
            data_list: 저장할 데이터 리스트

        Returns:
            저장된 레코드 수
        """
        try:
            if not data_list:
                return 0

            if sector_code not in self.sector_mapping:
                logger.error(f"지원하지 않는 업종 코드: {sector_code}")
                return 0

            table_name = self.sector_mapping[sector_code]

            # 테이블 생성 (필요시)
            if not self.table_exists(sector_code):
                if not self.create_sector_table(sector_code):
                    return 0

            conn = self._get_connection()
            cursor = conn.cursor()

            # INSERT IGNORE를 사용하여 중복 데이터 무시 (sector_code 제거)
            insert_sql = f"""
            INSERT IGNORE INTO {table_name} 
            (date, open_index, high_index, low_index, close_index, 
             volume, trading_value, data_source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            saved_count = 0
            for i, data in enumerate(data_list):
                try:
                    # 🔍 디버깅: 첫 번째 데이터 확인
                    if i == 0:
                        print(f"   🔍 저장할 첫 번째 데이터: {data}")

                    # 날짜 형식 변환 (YYYYMMDD → YYYY-MM-DD)
                    date_str = str(data['date'])
                    if len(date_str) == 8:
                        formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                    else:
                        formatted_date = date_str

                    insert_data = (
                        formatted_date,
                        float(data.get('open_index', 0)),
                        float(data.get('high_index', 0)),
                        float(data.get('low_index', 0)),
                        float(data.get('close_index', 0)),
                        int(data.get('volume', 0)),
                        int(data.get('trading_value', 0)),
                        'OPT20006'
                        # sector_code 제거
                    )

                    # 🔍 디버깅: 실제 INSERT 데이터 확인
                    if i == 0:
                        print(f"   🔍 실제 INSERT 데이터: {insert_data}")

                    cursor.execute(insert_sql, insert_data)
                    if cursor.rowcount > 0:
                        saved_count += 1
                    elif i < 3:  # 처음 3개만 디버깅
                        print(f"   ⚠️ 레코드 {i} INSERT 실패 (중복?): {insert_data}")

                except Exception as e:
                    logger.warning(f"개별 데이터 저장 오류 (#{i}): {e}")
                    if i < 3:  # 처음 3개만 디버깅
                        print(f"   ⚠️ 저장 실패 데이터: {data}")
                    continue

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"{sector_code} 데이터 저장 완료: {saved_count}/{len(data_list)}개")
            return saved_count

        except Exception as e:
            logger.error(f"업종 데이터 저장 실패 ({sector_code}): {e}")
            return 0

    def get_data_completeness(self, sector_code: str) -> Dict[str, Any]:
        """
        업종 데이터 완성도 확인

        Returns:
            {
                'table_exists': bool,
                'total_records': int,
                'latest_date': date,
                'oldest_date': date,
                'completion_rate': float,
                'collection_mode': str  # 'full', 'update', 'skip'
            }
        """
        try:
            if sector_code not in self.sector_mapping:
                return self._empty_completeness_result()

            table_name = self.sector_mapping[sector_code]

            # 테이블 존재 확인
            if not self.table_exists(sector_code):
                return {
                    'table_exists': False,
                    'total_records': 0,
                    'latest_date': None,
                    'oldest_date': None,
                    'completion_rate': 0.0,
                    'collection_mode': 'full'
                }

            conn = self._get_connection()
            cursor = conn.cursor(dictionary=True)

            # 데이터 통계 조회
            stats_sql = f"""
            SELECT 
                COUNT(*) as total_records,
                MAX(date) as latest_date,
                MIN(date) as oldest_date
            FROM {table_name}
            """

            cursor.execute(stats_sql)
            stats = cursor.fetchone()

            cursor.close()
            conn.close()

            # 완성도 계산 (5년치 기준 약 1250개)
            target_records = 1250
            completion_rate = min((stats['total_records'] / target_records) * 100, 100.0)

            # 수집 모드 결정
            if completion_rate >= 95:
                collection_mode = 'update'  # 최신 데이터만 업데이트
            elif completion_rate >= 10:
                collection_mode = 'continue'  # 기존 데이터에서 이어서
            else:
                collection_mode = 'full'  # 전체 수집

            return {
                'table_exists': True,
                'total_records': stats['total_records'],
                'latest_date': stats['latest_date'],
                'oldest_date': stats['oldest_date'],
                'completion_rate': completion_rate,
                'collection_mode': collection_mode
            }

        except Exception as e:
            logger.error(f"데이터 완성도 확인 실패 ({sector_code}): {e}")
            return self._empty_completeness_result()

    def _empty_completeness_result(self) -> Dict[str, Any]:
        """빈 완성도 결과 반환"""
        return {
            'table_exists': False,
            'total_records': 0,
            'latest_date': None,
            'oldest_date': None,
            'completion_rate': 0.0,
            'collection_mode': 'full'
        }

    def get_sector_statistics(self) -> Dict[str, Any]:
        """전체 업종 데이터 통계"""
        try:
            stats = {
                'total_sectors': len(self.sector_mapping),
                'sectors': {}
            }

            for sector_code, table_name in self.sector_mapping.items():
                completeness = self.get_data_completeness(sector_code)
                sector_name = "KOSPI" if sector_code == '001' else "KOSDAQ"

                stats['sectors'][sector_code] = {
                    'name': sector_name,
                    'table_name': table_name,
                    'records': completeness['total_records'],
                    'completion_rate': completeness['completion_rate'],
                    'latest_date': str(completeness['latest_date']) if completeness['latest_date'] else None
                }

            return stats

        except Exception as e:
            logger.error(f"통계 조회 실패: {e}")
            return {'total_sectors': 0, 'sectors': {}}


# 편의 함수
def get_sector_database_service() -> SectorDatabaseService:
    """업종 데이터베이스 서비스 인스턴스 반환"""
    return SectorDatabaseService()


# 테스트 함수
def test_sector_database():
    """데이터베이스 서비스 테스트"""
    try:
        print("🔍 업종 데이터베이스 서비스 테스트")
        print("=" * 50)

        service = get_sector_database_service()

        # 1. 연결 테스트
        print("1. 연결 테스트...")
        if service.test_connection():
            print("   ✅ 연결 성공")
        else:
            print("   ❌ 연결 실패")
            return False

        # 2. 스키마 생성
        print("2. 스키마 생성...")
        if service.create_schema_if_not_exists():
            print("   ✅ 스키마 생성/확인 완료")
        else:
            print("   ❌ 스키마 생성 실패")
            return False

        # 3. 테이블 생성 테스트
        print("3. 테이블 생성 테스트...")
        for sector_code in ['001', '101']:
            if service.create_sector_table(sector_code):
                print(f"   ✅ {sector_code} 테이블 생성 완료")
            else:
                print(f"   ❌ {sector_code} 테이블 생성 실패")

        # 4. 통계 확인
        print("4. 통계 확인...")
        stats = service.get_sector_statistics()
        print(f"   📊 업종 수: {stats['total_sectors']}")
        for sector_code, info in stats['sectors'].items():
            print(f"   📈 {info['name']}: {info['records']}개 레코드")

        print("\n✅ 모든 테스트 완료!")
        return True

    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        return False


if __name__ == "__main__":
    test_sector_database()