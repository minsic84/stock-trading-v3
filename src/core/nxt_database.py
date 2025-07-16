#!/usr/bin/env python3
"""
파일 경로: src/core/nxt_database.py

NXT 전용 데이터베이스 서비스
- stock_codes 테이블 기반 NXT 종목 관리
- daily_prices_db 스키마의 종목별 테이블 관리
- 기존 MySQLMultiSchemaService 활용하여 간결하게 구현
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from src.core.database import MySQLMultiSchemaService

logger = logging.getLogger(__name__)


class NXTDatabaseService:
    """NXT 전용 데이터베이스 서비스"""

    def __init__(self):
        """NXT 전용 DB 서비스 초기화"""
        self.db_service = MySQLMultiSchemaService()

    # ================================
    # NXT 종목 관리
    # ================================

    def get_nxt_stock_codes(self) -> List[str]:
        """NXT 종목 코드 리스트 조회"""
        try:
            conn = self.db_service._get_connection('main')
            cursor = conn.cursor()

            query = """
                SELECT code 
                FROM stock_codes 
                WHERE is_active = TRUE 
                ORDER BY code
            """

            cursor.execute(query)
            result = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()

            logger.info(f"✅ NXT 종목 조회 완료: {len(result)}개")
            return result

        except Exception as e:
            logger.error(f"❌ NXT 종목 조회 실패: {e}")
            return []

    def get_nxt_stock_info(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """NXT 특정 종목 정보 조회"""
        try:
            conn = self.db_service._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            query = """
                SELECT code, name, market, is_active, created_at, updated_at
                FROM stock_codes 
                WHERE code = %s
            """

            cursor.execute(query, (stock_code,))
            result = cursor.fetchone()
            cursor.close()
            conn.close()

            return result

        except Exception as e:
            logger.error(f"❌ {stock_code} 정보 조회 실패: {e}")
            return None

    def get_nxt_statistics(self) -> Dict[str, Any]:
        """NXT 종목 통계 정보"""
        try:
            conn = self.db_service._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            # 기본 통계
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_stocks,
                    COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_stocks,
                    COUNT(CASE WHEN market = 'KOSPI' THEN 1 END) as kospi_stocks,
                    COUNT(CASE WHEN market = 'KOSDAQ' THEN 1 END) as kosdaq_stocks
                FROM stock_codes
            """)

            stats = cursor.fetchone()
            cursor.close()
            conn.close()

            return stats or {}

        except Exception as e:
            logger.error(f"❌ NXT 통계 조회 실패: {e}")
            return {}

    # ================================
    # NXT 일봉 데이터 관리
    # ================================

    def daily_table_exists(self, stock_code: str) -> bool:
        """종목별 일봉 테이블 존재 여부 확인"""
        try:
            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            exists = cursor.fetchone() is not None

            cursor.close()
            conn.close()

            return exists

        except Exception as e:
            logger.error(f"❌ {stock_code} 테이블 존재 확인 실패: {e}")
            return False

    def get_daily_data_count(self, stock_code: str) -> int:
        """종목별 일봉 데이터 개수 조회"""
        try:
            if not self.daily_table_exists(stock_code):
                return 0

            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            return count

        except Exception as e:
            logger.error(f"❌ {stock_code} 데이터 개수 조회 실패: {e}")
            return 0

    def get_latest_date(self, stock_code: str) -> Optional[str]:
        """종목별 최신 데이터 날짜 조회"""
        try:
            if not self.daily_table_exists(stock_code):
                return None

            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"
            cursor.execute(f"SELECT MAX(date) FROM {table_name}")
            latest_date = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            return latest_date

        except Exception as e:
            logger.error(f"❌ {stock_code} 최신 날짜 조회 실패: {e}")
            return None

    def create_daily_table(self, stock_code: str) -> bool:
        """종목별 일봉 테이블 생성"""
        try:
            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"

            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
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
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"✅ {stock_code} 테이블 생성 완료")
            return True

        except Exception as e:
            logger.error(f"❌ {stock_code} 테이블 생성 실패: {e}")
            return False

    def save_daily_data_batch(self, stock_code: str, daily_data: List[Dict[str, Any]],
                              replace_mode: bool = False, update_recent_only: bool = False) -> int:
        """일봉 데이터 배치 저장 (전체 모드 및 최근 데이터 업데이트 모드 지원) - 날짜 정렬 기능 추가"""
        try:
            if not daily_data:
                return 0

            # 📅 데이터베이스 저장 전 날짜 오름차순 정렬 (오래된 날짜 → 최신 날짜)
            print(f"  🔄 DB 저장 전 데이터 정렬 중... ({len(daily_data)}개)")
            daily_data_sorted = sorted(daily_data, key=lambda x: x.get('date', ''))

            # 정렬 결과 확인
            if daily_data_sorted:
                first_date = daily_data_sorted[0].get('date', '')
                last_date = daily_data_sorted[-1].get('date', '')
                print(f"  📅 정렬 완료: {first_date} ~ {last_date}")

            # 테이블 존재 확인 및 생성
            if not self.daily_table_exists(stock_code):
                if not self.create_daily_table(stock_code):
                    return 0

            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            table_name = f"daily_prices_{stock_code}"

            # 모드별 처리
            if replace_mode:
                # 전체 교체 모드: 기존 데이터 삭제 후 전체 삽입
                cursor.execute(f"DELETE FROM {table_name}")
                logger.info(f"🔄 {stock_code} 전체 교체 모드: 기존 데이터 삭제")
                insert_mode = "INSERT"

            elif update_recent_only:
                # 최근 데이터 업데이트 모드: 중복 날짜는 교체, 새 날짜는 추가
                logger.info(f"🔄 {stock_code} 최근 데이터 업데이트 모드")
                insert_mode = "REPLACE"  # MySQL REPLACE INTO 사용

            else:
                # 일반 모드: 중복 시 무시
                insert_mode = "INSERT IGNORE"

            # 삽입 쿼리 결정
            if insert_mode == "INSERT":
                insert_sql = f"""
                    INSERT INTO {table_name} (
                        date, open_price, high_price, low_price, close_price,
                        volume, trading_value, prev_day_diff, change_rate,
                        data_source, created_at
                    ) VALUES (
                        %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
                        %(volume)s, %(trading_value)s, %(prev_day_diff)s, %(change_rate)s,
                        %(data_source)s, %(created_at)s
                    )
                """
            elif insert_mode == "REPLACE":
                insert_sql = f"""
                    REPLACE INTO {table_name} (
                        date, open_price, high_price, low_price, close_price,
                        volume, trading_value, prev_day_diff, change_rate,
                        data_source, created_at
                    ) VALUES (
                        %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
                        %(volume)s, %(trading_value)s, %(prev_day_diff)s, %(change_rate)s,
                        %(data_source)s, %(created_at)s
                    )
                """
            else:  # INSERT IGNORE
                insert_sql = f"""
                    INSERT IGNORE INTO {table_name} (
                        date, open_price, high_price, low_price, close_price,
                        volume, trading_value, prev_day_diff, change_rate,
                        data_source, created_at
                    ) VALUES (
                        %(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s,
                        %(volume)s, %(trading_value)s, %(prev_day_diff)s, %(change_rate)s,
                        %(data_source)s, %(created_at)s
                    )
                """

            # 📅 정렬된 데이터로 전처리 (오름차순 정렬된 순서 유지)
            processed_data = []
            for data in daily_data_sorted:  # 정렬된 데이터 사용
                processed_data.append({
                    'date': data.get('date', ''),
                    'open_price': data.get('open_price', 0),
                    'high_price': data.get('high_price', 0),
                    'low_price': data.get('low_price', 0),
                    'close_price': data.get('close_price', 0),
                    'volume': data.get('volume', 0),
                    'trading_value': data.get('trading_value', 0),
                    'prev_day_diff': data.get('prev_day_diff', 0),
                    'change_rate': data.get('change_rate', 0),
                    'data_source': data.get('data_source', 'OPT10081'),
                    'created_at': datetime.now()
                })

            # 📅 배치 실행 (오름차순 정렬된 순서로 저장)
            cursor.executemany(insert_sql, processed_data)
            conn.commit()

            saved_count = cursor.rowcount
            cursor.close()
            conn.close()

            mode_desc = "전체교체" if replace_mode else ("최근업데이트" if update_recent_only else "일반삽입")
            logger.info(f"✅ {stock_code} 일봉 데이터 저장 완료 ({mode_desc}): {saved_count}개 (날짜순 정렬)")

            # 저장 결과 상세 출력
            if processed_data:
                first_saved = processed_data[0]['date']
                last_saved = processed_data[-1]['date']
                print(f"  💾 저장 완료: {first_saved} ~ {last_saved} ({saved_count}개)")

            return saved_count

        except Exception as e:
            logger.error(f"❌ {stock_code} 일봉 데이터 저장 실패: {e}")
            return 0

    # ================================
    # NXT 수집 상태 관리
    # ================================

    def get_nxt_stocks_need_update(self) -> List[str]:
        """업데이트가 필요한 NXT 종목 리스트"""
        try:
            # 1. NXT 종목 조회
            conn_main = self.db_service._get_connection('main')
            cursor_main = conn_main.cursor()
            cursor_main.execute("SELECT code FROM stock_codes WHERE is_active = TRUE")
            nxt_codes = [row[0] for row in cursor_main.fetchall()]
            cursor_main.close()
            conn_main.close()

            need_update = []

            # 2. daily_prices_db에서 확인
            conn_daily = self.db_service._get_connection('daily')
            cursor_daily = conn_daily.cursor()

            for code in nxt_codes:
                try:
                    table_name = f"daily_prices_{code}"

                    # 테이블 존재 확인
                    cursor_daily.execute(f"SHOW TABLES LIKE '{table_name}'")
                    if not cursor_daily.fetchone():
                        need_update.append(code)
                        continue

                    # 데이터 개수 확인
                    cursor_daily.execute(f"SELECT COUNT(*) FROM {table_name}")
                    data_count = cursor_daily.fetchone()[0]
                    if data_count < 1000:  # 5년치 미만
                        need_update.append(code)
                        continue

                    # 최신 날짜 확인
                    cursor_daily.execute(f"SELECT MAX(date) FROM {table_name}")
                    latest_date = cursor_daily.fetchone()[0]
                    if latest_date:
                        try:
                            latest_dt = datetime.strptime(latest_date, '%Y%m%d')
                            days_old = (datetime.now() - latest_dt).days
                            if days_old >= 3:  # 3일 이상 오래됨
                                need_update.append(code)
                        except:
                            need_update.append(code)
                    else:
                        need_update.append(code)

                except Exception:
                    need_update.append(code)

            cursor_daily.close()
            conn_daily.close()

            logger.info(f"✅ 업데이트 필요 종목: {len(need_update)}개 / 전체 {len(nxt_codes)}개")
            return need_update

        except Exception as e:
            logger.error(f"❌ 업데이트 필요 종목 조회 실패: {e}")
            return []

    def get_nxt_collection_status(self) -> Dict[str, Any]:
        """NXT 수집 현황 요약"""
        try:
            # 1. NXT 종목 조회
            conn_main = self.db_service._get_connection('main')
            cursor_main = conn_main.cursor()
            cursor_main.execute("SELECT code FROM stock_codes WHERE is_active = TRUE")
            nxt_codes = [row[0] for row in cursor_main.fetchall()]
            cursor_main.close()
            conn_main.close()

            total_count = len(nxt_codes)

            # 2. 완료 종목 확인 (daily_prices_db에서)
            conn_daily = self.db_service._get_connection('daily')
            cursor_daily = conn_daily.cursor()

            completed_count = 0
            total_records = 0

            for code in nxt_codes:
                try:
                    table_name = f"daily_prices_{code}"
                    # 테이블 존재 확인
                    cursor_daily.execute(f"SHOW TABLES LIKE '{table_name}'")
                    if cursor_daily.fetchone():
                        # 데이터 개수 확인
                        cursor_daily.execute(f"SELECT COUNT(*) FROM {table_name}")
                        data_count = cursor_daily.fetchone()[0]
                        total_records += data_count

                        if data_count >= 1000:  # 5년치 기준
                            completed_count += 1
                except Exception:
                    continue

            cursor_daily.close()
            conn_daily.close()

            return {
                'total_nxt_stocks': total_count,
                'completed_stocks': completed_count,
                'completion_rate': round(completed_count / total_count * 100, 1) if total_count > 0 else 0,
                'total_records': total_records,
                'need_update': total_count - completed_count,
                'checked_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"❌ NXT 수집 현황 조회 실패: {e}")
            return {}

    # ================================
    # 유틸리티 메서드
    # ================================

    def test_connection(self) -> bool:
        """NXT DB 연결 테스트"""
        try:
            # 1. stock_codes 접근 테스트
            conn_main = self.db_service._get_connection('main')
            cursor_main = conn_main.cursor()
            cursor_main.execute("SELECT COUNT(*) FROM stock_codes WHERE is_active = TRUE")
            nxt_count = cursor_main.fetchone()[0]
            cursor_main.close()
            conn_main.close()

            # 2. daily_prices_db 접근 테스트
            conn_daily = self.db_service._get_connection('daily')
            cursor_daily = conn_daily.cursor()
            cursor_daily.execute("SELECT 1")
            cursor_daily.fetchone()  # 결과 읽기
            cursor_daily.close()
            conn_daily.close()

            logger.info(f"✅ NXT DB 연결 테스트 성공 (NXT 종목: {nxt_count}개)")
            return True

        except Exception as e:
            logger.error(f"❌ NXT DB 연결 테스트 실패: {e}")
            return False

    def get_nxt_stocks_from_position(self, from_code: str = None) -> List[str]:
        """특정 종목 코드부터 시작하여 NXT 종목 리스트 조회 (스마트 재시작용)"""
        try:
            conn = self.db_service._get_connection('main')
            cursor = conn.cursor()

            if from_code:
                # 특정 종목부터 시작
                query = """
                    SELECT code 
                    FROM stock_codes 
                    WHERE is_active = TRUE AND code >= %s
                    ORDER BY code
                """
                cursor.execute(query, (from_code,))
            else:
                # 처음부터 시작
                query = """
                    SELECT code 
                    FROM stock_codes 
                    WHERE is_active = TRUE 
                    ORDER BY code
                """
                cursor.execute(query)

            result = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()

            logger.info(f"✅ NXT 종목 조회 완료 (from {from_code}): {len(result)}개")
            return result

        except Exception as e:
            logger.error(f"❌ NXT 종목 조회 실패: {e}")
            return []

    def find_nxt_restart_position(self, target_date: str = None) -> Tuple[str, int, int]:
        """
        수집을 재시작할 위치 찾기

        Args:
            target_date: 찾을 날짜 (YYYYMMDD), None이면 오늘 날짜

        Returns:
            Tuple[시작할_종목코드, 전체_종목수, 스킵할_종목수]
        """
        try:
            # 기본 날짜 설정 (오늘 날짜)
            if not target_date:
                target_date = datetime.now().strftime('%Y%m%d')

            print(f"🔍 재시작 위치 찾기: {target_date} 날짜 기준")
            print("-" * 50)

            # 1. 전체 NXT 종목 리스트 (순서대로)
            all_nxt_codes = self.get_nxt_stock_codes()
            total_count = len(all_nxt_codes)

            if not all_nxt_codes:
                return None, 0, 0

            print(f"📊 전체 NXT 종목: {total_count}개")

            # 2. DB 연결 (daily_prices_db)
            conn = self.db_service._get_connection('daily')
            cursor = conn.cursor()

            completed_count = 0
            restart_position = None

            # 3. 종목 순서대로 확인
            for i, stock_code in enumerate(all_nxt_codes):
                table_name = f"daily_prices_{stock_code}"

                try:
                    # 테이블 존재 확인
                    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                    table_exists = cursor.fetchone() is not None

                    if not table_exists:
                        # 테이블이 없으면 여기서부터 시작
                        restart_position = stock_code
                        print(f"📍 재시작 위치 발견: {stock_code} (테이블 없음)")
                        break

                    # 해당 날짜 데이터 확인
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE date = %s", (target_date,))
                    date_exists = cursor.fetchone()[0] > 0

                    if not date_exists:
                        # 해당 날짜 데이터가 없으면 여기서부터 시작
                        restart_position = stock_code
                        print(f"📍 재시작 위치 발견: {stock_code} ({target_date} 데이터 없음)")
                        break

                    # 이 종목은 완료됨
                    completed_count += 1

                    # 10개마다 진행상황 출력
                    if (i + 1) % 10 == 0:
                        print(f"   확인 중: {i + 1}/{total_count} ({(i + 1) / total_count * 100:.1f}%)")

                except Exception as e:
                    # 오류 발생 시 이 종목부터 시작
                    print(f"⚠️ {stock_code} 확인 중 오류, 여기서부터 시작: {e}")
                    restart_position = stock_code
                    break

            cursor.close()
            conn.close()

            # 4. 결과 분석
            if restart_position is None:
                # 모든 종목이 완료됨
                print("✅ 모든 종목이 이미 완료되었습니다!")
                return None, total_count, total_count
            else:
                print(f"📊 분석 결과:")
                print(f"   ✅ 완료된 종목: {completed_count}개")
                print(f"   🔄 남은 종목: {total_count - completed_count}개")
                print(f"   📍 시작 위치: {restart_position}")
                print(f"   📈 진행률: {completed_count / total_count * 100:.1f}%")

                return restart_position, total_count, completed_count

        except Exception as e:
            logger.error(f"❌ 재시작 위치 찾기 실패: {e}")
            return None, 0, 0

    def get_nxt_stocks_smart_restart(self, force_update: bool = False, target_date: str = None) -> List[str]:
        """
        스마트 재시작용 NXT 종목 리스트 조회

        Args:
            force_update: 강제 업데이트 (모든 종목)
            target_date: 기준 날짜 (YYYYMMDD), None이면 오늘

        Returns:
            수집해야 할 종목 리스트
        """
        try:
            if force_update:
                # 강제 업데이트: 모든 종목
                print("🔄 강제 업데이트 모드: 전체 종목 대상")
                return self.get_nxt_stock_codes()

            # 스마트 재시작: 미완료 지점부터
            restart_code, total_count, completed_count = self.find_nxt_restart_position(target_date)

            if restart_code is None:
                # 모든 종목 완료
                return []

            # 재시작 위치부터 종목 리스트 조회
            remaining_codes = self.get_nxt_stocks_from_position(restart_code)

            print(f"🚀 스마트 재시작 준비 완료:")
            print(f"   📊 전체: {total_count}개")
            print(f"   ✅ 완료: {completed_count}개")
            print(f"   🔄 남은: {len(remaining_codes)}개")
            print(f"   📍 시작: {restart_code}")

            return remaining_codes

        except Exception as e:
            logger.error(f"❌ 스마트 재시작 준비 실패: {e}")
            # 오류 시 전체 목록 반환
            return self.get_nxt_stock_codes()

    def show_restart_analysis(self, target_date: str = None):
        """재시작 분석 결과 상세 출력 (실행 전 확인용)"""
        try:
            if not target_date:
                target_date = datetime.now().strftime('%Y%m%d')

            print("📊 NXT 일봉 수집 재시작 분석")
            print("=" * 60)
            print(f"🗓️ 기준 날짜: {target_date}")
            print()

            restart_code, total_count, completed_count = self.find_nxt_restart_position(target_date)

            if restart_code is None:
                print("🎉 분석 결과: 모든 종목이 완료되었습니다!")
                print(f"   ✅ 완료된 종목: {completed_count}/{total_count}개 (100%)")
                print("   💡 추가 수집이 필요하지 않습니다.")
            else:
                remaining_count = total_count - completed_count

                print("📊 분석 결과:")
                print(f"   📈 전체 종목: {total_count}개")
                print(f"   ✅ 완료 종목: {completed_count}개 ({completed_count / total_count * 100:.1f}%)")
                print(f"   🔄 남은 종목: {remaining_count}개 ({remaining_count / total_count * 100:.1f}%)")
                print(f"   📍 시작 위치: {restart_code}")
                print(f"   ⏱️ 예상 소요시간: {remaining_count * 3.6 / 60:.1f}분")

                # 샘플 미완료 종목들 표시
                remaining_codes = self.get_nxt_stocks_from_position(restart_code)
                if remaining_codes:
                    sample_codes = remaining_codes[:5]
                    print(f"   📝 미완료 종목 샘플: {', '.join(sample_codes)}")
                    if len(remaining_codes) > 5:
                        print(f"      (외 {len(remaining_codes) - 5}개 더...)")

            print()
            print("💡 재시작 방법:")
            print("   python scripts/update_nxt_daily.py")
            print("   (또는 python scripts/update_nxt_daily.py --force)")
            print("=" * 60)

        except Exception as e:
            print(f"❌ 재시작 분석 실패: {e}")


# 편의 함수들
def get_nxt_database_service():
    """NXT 데이터베이스 서비스 인스턴스 반환"""
    return NXTDatabaseService()


def test_nxt_database():
    """NXT 데이터베이스 서비스 테스트"""
    print("🧪 NXT 데이터베이스 서비스 테스트")
    print("=" * 50)

    nxt_db = NXTDatabaseService()

    # 연결 테스트
    if not nxt_db.test_connection():
        print("❌ 연결 테스트 실패")
        return False

    # NXT 종목 통계
    stats = nxt_db.get_nxt_statistics()
    print(f"📊 NXT 종목 통계:")
    print(f"   전체: {stats.get('total_stocks', 0)}개")
    print(f"   활성: {stats.get('active_stocks', 0)}개")
    print(f"   KOSPI: {stats.get('kospi_stocks', 0)}개")
    print(f"   KOSDAQ: {stats.get('kosdaq_stocks', 0)}개")

    # 수집 현황
    status = nxt_db.get_nxt_collection_status()
    print(f"\n📈 수집 현황:")
    print(f"   완료율: {status.get('completion_rate', 0)}%")
    print(f"   완료 종목: {status.get('completed_stocks', 0)}개")
    print(f"   업데이트 필요: {status.get('need_update', 0)}개")

    print("\n✅ 테스트 완료!")
    return True


if __name__ == "__main__":
    test_nxt_database()