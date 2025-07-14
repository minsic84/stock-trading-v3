#!/usr/bin/env python3
"""
파일 경로: scripts/collect_stock_codes.py

종목코드 수집 및 저장 스크립트
- code_collector로 KOSPI/KOSDAQ 최신 종목코드 수집
- 순수 숫자 6자리 필터링
- GetMasterCodeName으로 종목명 수집
- stock_codes 테이블에 저장/업데이트
"""

import sys
import mysql.connector
from pathlib import Path
from datetime import datetime
import logging
import time
from typing import List, Dict, Tuple

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.api.base_session import create_kiwoom_session
from src.market.code_collector import StockCodeCollector

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockCodeManager:
    """종목코드 수집 및 관리 클래스"""

    def __init__(self):
        # MySQL 연결 설정
        self.db_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'stock_user',
            'password': 'StockPass2025!',
            'database': 'stock_trading_db',
            'charset': 'utf8mb4'
        }

        self.table_name = 'stock_codes'
        self.session = None
        self.code_collector = None

        # 수집 통계
        self.stats = {
            'kospi_total': 0,
            'kosdaq_total': 0,
            'kospi_filtered': 0,
            'kosdaq_filtered': 0,
            'new_codes': 0,
            'updated_codes': 0,
            'deactivated_codes': 0,
            'failed_names': 0
        }

    def connect_database(self):
        """데이터베이스 연결"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            logger.info("✅ MySQL 연결 성공")
            return connection
        except Exception as e:
            logger.error(f"❌ MySQL 연결 실패: {e}")
            return None

    def setup_kiwoom_session(self):
        """키움 세션 및 code_collector 준비"""
        try:
            print("🔌 키움 세션 준비 중...")

            # 키움 세션 생성
            self.session = create_kiwoom_session(auto_login=True, show_progress=True)

            if not self.session or not self.session.is_ready():
                print("❌ 키움 세션 준비 실패")
                return False

            print("✅ 키움 세션 준비 완료")

            # code_collector 초기화
            connector = self.session.get_connector()
            self.code_collector = StockCodeCollector(connector)

            print("✅ StockCodeCollector 초기화 완료")
            return True

        except Exception as e:
            logger.error(f"❌ 키움 세션 준비 실패: {e}")
            return False

    def is_valid_stock_code(self, code: str) -> bool:
        """유효한 주식 종목코드인지 확인 (순수 숫자 6자리)"""
        if not code or len(code) != 6:
            return False

        # 순수 숫자 6자리인지 확인
        if not code.isdigit():
            return False

        # 특수 코드 제외 (0000XX, 9999XX 등)
        if code.startswith('0000') or code.startswith('9999'):
            return False

        return True

    def collect_latest_codes(self) -> Dict[str, List[str]]:
        """최신 종목코드 수집"""
        try:
            print("\n📊 최신 종목코드 수집 시작")
            print("=" * 50)

            # KOSPI 종목코드 수집
            print("📈 KOSPI 종목코드 수집 중...")
            kospi_codes = self.code_collector.get_kospi_codes()

            if not kospi_codes:
                print("❌ KOSPI 종목코드 수집 실패")
                return {}

            print(f"✅ KOSPI 원시 데이터: {len(kospi_codes):,}개")
            self.stats['kospi_total'] = len(kospi_codes)

            # KOSDAQ 종목코드 수집
            print("📈 KOSDAQ 종목코드 수집 중...")
            kosdaq_codes = self.code_collector.get_kosdaq_codes()

            if not kosdaq_codes:
                print("❌ KOSDAQ 종목코드 수집 실패")
                return {}

            print(f"✅ KOSDAQ 원시 데이터: {len(kosdaq_codes):,}개")
            self.stats['kosdaq_total'] = len(kosdaq_codes)

            # 순수 숫자 6자리 필터링
            print("\n🔍 순수 숫자 6자리 종목 필터링 중...")

            kospi_filtered = [code for code in kospi_codes if self.is_valid_stock_code(code)]
            kosdaq_filtered = [code for code in kosdaq_codes if self.is_valid_stock_code(code)]

            self.stats['kospi_filtered'] = len(kospi_filtered)
            self.stats['kosdaq_filtered'] = len(kosdaq_filtered)

            print(f"📈 KOSPI 필터링 결과: {len(kospi_filtered):,}개 (제외: {len(kospi_codes) - len(kospi_filtered):,}개)")
            print(f"📈 KOSDAQ 필터링 결과: {len(kosdaq_filtered):,}개 (제외: {len(kosdaq_codes) - len(kosdaq_filtered):,}개)")
            print(f"📊 전체 유효 종목: {len(kospi_filtered) + len(kosdaq_filtered):,}개")

            return {
                'kospi': kospi_filtered,
                'kosdaq': kosdaq_filtered,
                'all': kospi_filtered + kosdaq_filtered
            }

        except Exception as e:
            logger.error(f"❌ 종목코드 수집 실패: {e}")
            return {}

    def get_stock_name(self, code: str) -> str:
        """종목코드로 종목명 조회"""
        try:
            if not self.session:
                return ""

            # GetMasterCodeName 함수 호출
            connector = self.session.get_connector()
            name = connector.dynamicCall("GetMasterCodeName(QString)", code)

            # 종목명 정리 (공백 제거, 특수문자 처리)
            if name:
                name = name.strip()
                # 빈 문자열이면 코드 반환
                if not name:
                    return code
                return name
            else:
                return code

        except Exception as e:
            logger.debug(f"종목명 조회 실패 {code}: {e}")
            return code

    def collect_stock_names(self, codes: List[str]) -> Dict[str, str]:
        """종목코드 리스트의 종목명 일괄 수집"""
        try:
            print(f"\n📝 종목명 수집 중... ({len(codes):,}개)")

            stock_names = {}
            failed_count = 0

            for i, code in enumerate(codes):
                if i % 100 == 0:  # 100개마다 진행상황 출력
                    progress = (i + 1) / len(codes) * 100
                    print(f"   진행률: {progress:.1f}% ({i + 1:,}/{len(codes):,})")

                name = self.get_stock_name(code)
                stock_names[code] = name

                if name == code:  # 종목명 조회 실패
                    failed_count += 1

                # API 호출 제한 고려 (0.1초 딜레이)
                time.sleep(0.1)

            self.stats['failed_names'] = failed_count

            print(f"✅ 종목명 수집 완료")
            print(f"   📝 성공: {len(codes) - failed_count:,}개")
            print(f"   ❌ 실패: {failed_count:,}개")

            return stock_names

        except Exception as e:
            logger.error(f"❌ 종목명 수집 실패: {e}")
            return {}

    def get_existing_codes(self, connection) -> Dict[str, Dict]:
        """기존 저장된 종목코드 조회"""
        try:
            cursor = connection.cursor(dictionary=True)

            cursor.execute(f"""
                SELECT code, name, market, is_active, collected_at 
                FROM {self.table_name}
            """)

            existing_data = cursor.fetchall()
            cursor.close()

            # 딕셔너리로 변환 (code를 키로)
            existing_dict = {}
            for row in existing_data:
                existing_dict[row['code']] = row

            return existing_dict

        except Exception as e:
            logger.error(f"❌ 기존 종목코드 조회 실패: {e}")
            return {}

    def save_to_database(self, connection, codes_data: Dict[str, List[str]],
                         names_data: Dict[str, str]) -> bool:
        """수집된 데이터를 데이터베이스에 저장"""
        try:
            print(f"\n💾 데이터베이스 저장 시작")
            print("=" * 50)

            cursor = connection.cursor()
            current_time = datetime.now()

            # 기존 데이터 조회
            existing_codes = self.get_existing_codes(connection)
            print(f"📊 기존 데이터: {len(existing_codes):,}개")

            # 새로운 코드들 처리
            new_codes = []
            updated_codes = []

            # KOSPI 처리
            for code in codes_data['kospi']:
                name = names_data.get(code, code)

                if code in existing_codes:
                    # 기존 데이터 업데이트
                    cursor.execute(f"""
                        UPDATE {self.table_name} 
                        SET name = %s, market = %s, is_active = TRUE, 
                            collected_at = %s, updated_at = %s
                        WHERE code = %s
                    """, (name, 'KOSPI', current_time, current_time, code))
                    updated_codes.append(code)
                else:
                    # 신규 데이터 추가
                    cursor.execute(f"""
                        INSERT INTO {self.table_name} 
                        (code, name, market, is_active, collected_at, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (code, name, 'KOSPI', True, current_time, current_time, current_time))
                    new_codes.append(code)

            # KOSDAQ 처리
            for code in codes_data['kosdaq']:
                name = names_data.get(code, code)

                if code in existing_codes:
                    # 기존 데이터 업데이트
                    cursor.execute(f"""
                        UPDATE {self.table_name} 
                        SET name = %s, market = %s, is_active = TRUE, 
                            collected_at = %s, updated_at = %s
                        WHERE code = %s
                    """, (name, 'KOSDAQ', current_time, current_time, code))
                    updated_codes.append(code)
                else:
                    # 신규 데이터 추가
                    cursor.execute(f"""
                        INSERT INTO {self.table_name} 
                        (code, name, market, is_active, collected_at, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (code, name, 'KOSDAQ', True, current_time, current_time, current_time))
                    new_codes.append(code)

            # 현재 수집에서 누락된 기존 코드들 비활성화
            current_codes = set(codes_data['all'])
            deactivated_codes = []

            for existing_code in existing_codes:
                if existing_code not in current_codes and existing_codes[existing_code]['is_active']:
                    cursor.execute(f"""
                        UPDATE {self.table_name} 
                        SET is_active = FALSE, updated_at = %s
                        WHERE code = %s
                    """, (current_time, existing_code))
                    deactivated_codes.append(existing_code)

            # 커밋
            connection.commit()
            cursor.close()

            # 통계 업데이트
            self.stats['new_codes'] = len(new_codes)
            self.stats['updated_codes'] = len(updated_codes)
            self.stats['deactivated_codes'] = len(deactivated_codes)

            print(f"✅ 데이터베이스 저장 완료")
            print(f"   📥 신규 추가: {len(new_codes):,}개")
            print(f"   🔄 업데이트: {len(updated_codes):,}개")
            print(f"   ⏸️ 비활성화: {len(deactivated_codes):,}개")

            return True

        except Exception as e:
            logger.error(f"❌ 데이터베이스 저장 실패: {e}")
            connection.rollback()
            return False

    def show_final_statistics(self, connection):
        """최종 통계 출력"""
        try:
            print(f"\n📊 최종 통계")
            print("=" * 50)

            cursor = connection.cursor(dictionary=True)

            # 전체 통계
            cursor.execute(f"""
                SELECT 
                    market,
                    COUNT(*) as total_count,
                    SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) as active_count,
                    SUM(CASE WHEN is_active = FALSE THEN 1 ELSE 0 END) as inactive_count
                FROM {self.table_name}
                GROUP BY market
                ORDER BY market
            """)

            market_stats = cursor.fetchall()

            print("📈 시장별 현황:")
            total_active = 0
            for stat in market_stats:
                market = stat['market']
                total = stat['total_count']
                active = stat['active_count']
                inactive = stat['inactive_count']
                total_active += active

                print(f"   {market:>7}: {active:,}개 활성 ({inactive:,}개 비활성, 전체 {total:,}개)")

            print(f"   {'전체':>7}: {total_active:,}개 활성")

            # 수집 통계
            print(f"\n🔍 수집 과정:")
            print(f"   📊 KOSPI 원시: {self.stats['kospi_total']:,}개 → 필터링: {self.stats['kospi_filtered']:,}개")
            print(f"   📊 KOSDAQ 원시: {self.stats['kosdaq_total']:,}개 → 필터링: {self.stats['kosdaq_filtered']:,}개")
            print(f"   📝 종목명 실패: {self.stats['failed_names']:,}개")

            print(f"\n💾 저장 결과:")
            print(f"   📥 신규 추가: {self.stats['new_codes']:,}개")
            print(f"   🔄 업데이트: {self.stats['updated_codes']:,}개")
            print(f"   ⏸️ 비활성화: {self.stats['deactivated_codes']:,}개")

            # 최근 수집 시간
            cursor.execute(f"""
                SELECT MAX(collected_at) as last_collected 
                FROM {self.table_name} 
                WHERE is_active = TRUE
            """)

            result = cursor.fetchone()
            if result and result['last_collected']:
                print(f"\n⏰ 최근 수집: {result['last_collected']}")

            cursor.close()

        except Exception as e:
            logger.error(f"❌ 통계 출력 실패: {e}")

    def run(self):
        """전체 실행"""
        print("🚀 종목코드 수집 및 저장 시작")
        print("=" * 60)

        start_time = datetime.now()

        try:
            # 1. 키움 세션 준비
            if not self.setup_kiwoom_session():
                print("❌ 키움 세션 준비 실패")
                return False

            # 2. 데이터베이스 연결
            connection = self.connect_database()
            if not connection:
                print("❌ 데이터베이스 연결 실패")
                return False

            # 3. 최신 종목코드 수집
            codes_data = self.collect_latest_codes()
            if not codes_data:
                print("❌ 종목코드 수집 실패")
                return False

            # 4. 종목명 수집
            names_data = self.collect_stock_names(codes_data['all'])
            if not names_data:
                print("❌ 종목명 수집 실패")
                return False

            # 5. 데이터베이스 저장
            if not self.save_to_database(connection, codes_data, names_data):
                print("❌ 데이터베이스 저장 실패")
                return False

            # 6. 최종 통계
            self.show_final_statistics(connection)

            # 실행 시간 계산
            elapsed = datetime.now() - start_time

            print(f"\n" + "=" * 60)
            print(f"🎉 종목코드 수집 및 저장 완료!")
            print(f"⏱️ 총 실행시간: {elapsed.total_seconds():.1f}초")

            return True

        except Exception as e:
            logger.error(f"❌ 실행 중 오류: {e}")
            return False
        finally:
            if 'connection' in locals():
                connection.close()


def main():
    """메인 실행 함수"""
    manager = StockCodeManager()
    success = manager.run()

    if success:
        print("\n✅ 작업 완료!")
        print("💡 다음 단계: stock_info 수집기에서 해당 종목들 사용 가능")
    else:
        print("\n❌ 작업 실패!")
        sys.exit(1)


if __name__ == "__main__":
    main()