#!/usr/bin/env python3
"""
파일 경로: scripts/create_stock_codes_table.py

stock_codes 테이블 생성 스크립트
- stock_trading_db 스키마에 종목코드 마스터 테이블 생성
- 기존 테이블 확인 및 백업 옵션
- 인덱스 최적화 포함
"""

import sys
import mysql.connector
from pathlib import Path
from datetime import datetime
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockCodesTableCreator:
    """stock_codes 테이블 생성 관리 클래스"""

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

    def connect_database(self):
        """데이터베이스 연결"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            logger.info("✅ MySQL 연결 성공")
            return connection
        except Exception as e:
            logger.error(f"❌ MySQL 연결 실패: {e}")
            return None

    def check_existing_table(self, connection):
        """기존 테이블 존재 여부 확인"""
        try:
            cursor = connection.cursor()

            # 테이블 존재 확인
            cursor.execute("""
                SELECT COUNT(*) as cnt
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """, (self.db_config['database'], self.table_name))

            result = cursor.fetchone()
            exists = result[0] > 0

            if exists:
                # 기존 테이블 정보 조회
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                row_count = cursor.fetchone()[0]

                print(f"⚠️ 테이블 '{self.table_name}'이 이미 존재합니다.")
                print(f"📊 현재 데이터: {row_count:,}개 레코드")

                # 백업 여부 확인
                response = input("\n기존 테이블을 백업하시겠습니까? (y/N): ")
                if response.lower() == 'y':
                    self.backup_existing_table(connection)

                # 재생성 여부 확인
                response = input("기존 테이블을 삭제하고 새로 만드시겠습니까? (y/N): ")
                if response.lower() == 'y':
                    cursor.execute(f"DROP TABLE {self.table_name}")
                    print(f"🗑️ 기존 테이블 '{self.table_name}' 삭제됨")
                    return False  # 새로 생성 필요
                else:
                    print("ℹ️ 기존 테이블을 유지합니다.")
                    return True  # 이미 존재

            cursor.close()
            return False  # 테이블이 없음, 새로 생성 필요

        except Exception as e:
            logger.error(f"❌ 테이블 확인 실패: {e}")
            return False

    def backup_existing_table(self, connection):
        """기존 테이블 백업"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_table = f"{self.table_name}_backup_{timestamp}"

            cursor = connection.cursor()
            cursor.execute(f"""
                CREATE TABLE {backup_table} AS 
                SELECT * FROM {self.table_name}
            """)

            # 백업된 레코드 수 확인
            cursor.execute(f"SELECT COUNT(*) FROM {backup_table}")
            backup_count = cursor.fetchone()[0]

            print(f"💾 백업 완료: {backup_table} ({backup_count:,}개 레코드)")
            cursor.close()

        except Exception as e:
            logger.error(f"❌ 테이블 백업 실패: {e}")

    def create_table(self, connection):
        """stock_codes 테이블 생성"""
        try:
            cursor = connection.cursor()

            print(f"\n🔧 테이블 '{self.table_name}' 생성 중...")

            # 테이블 생성 SQL
            create_sql = f"""
            CREATE TABLE {self.table_name} (
                code VARCHAR(10) PRIMARY KEY COMMENT '종목코드 (6자리 숫자)',
                name VARCHAR(100) NOT NULL COMMENT '종목명',
                market VARCHAR(10) NOT NULL COMMENT '시장구분 (KOSPI/KOSDAQ)',
                is_active BOOLEAN DEFAULT TRUE COMMENT '활성 여부',
                collected_at DATETIME NOT NULL COMMENT '수집 시점',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',

                INDEX idx_market (market) COMMENT '시장별 조회 최적화',
                INDEX idx_active (is_active) COMMENT '활성 종목 조회 최적화',
                INDEX idx_collected (collected_at) COMMENT '수집 시점별 조회 최적화',
                INDEX idx_market_active (market, is_active) COMMENT '시장별 활성 종목 조회 최적화'

            ) ENGINE=InnoDB 
              DEFAULT CHARSET=utf8mb4 
              COLLATE=utf8mb4_unicode_ci 
              COMMENT='종목코드 마스터 테이블 (코스피/코스닥 순수 6자리 숫자 종목만)'
            """

            cursor.execute(create_sql)
            connection.commit()

            print(f"✅ 테이블 '{self.table_name}' 생성 완료")
            cursor.close()
            return True

        except Exception as e:
            logger.error(f"❌ 테이블 생성 실패: {e}")
            return False

    def verify_table(self, connection):
        """생성된 테이블 검증"""
        try:
            cursor = connection.cursor()

            print(f"\n🔍 테이블 '{self.table_name}' 검증 중...")

            # 테이블 구조 확인
            cursor.execute(f"DESCRIBE {self.table_name}")
            columns = cursor.fetchall()

            print(f"📋 테이블 구조:")
            for column in columns:
                field, type_info, null, key, default, extra = column
                key_info = f" ({key})" if key else ""
                print(f"   📄 {field}: {type_info}{key_info}")

            # 인덱스 확인
            cursor.execute(f"SHOW INDEX FROM {self.table_name}")
            indexes = cursor.fetchall()

            unique_indexes = set()
            for index in indexes:
                index_name = index[2]  # Key_name
                unique_indexes.add(index_name)

            print(f"\n🔑 인덱스: {len(unique_indexes)}개")
            for idx_name in sorted(unique_indexes):
                print(f"   🗝️ {idx_name}")

            # 테이블 상태 확인
            cursor.execute(f"""
                SELECT 
                    table_rows,
                    data_length,
                    index_length,
                    (data_length + index_length) as total_size
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """, (self.db_config['database'], self.table_name))

            table_info = cursor.fetchone()
            if table_info:
                rows, data_size, index_size, total_size = table_info
                print(f"\n📊 테이블 상태:")
                print(f"   📝 레코드 수: {rows or 0:,}개")
                print(f"   💾 데이터 크기: {(data_size or 0) / 1024:.1f} KB")
                print(f"   🗝️ 인덱스 크기: {(index_size or 0) / 1024:.1f} KB")
                print(f"   📦 전체 크기: {(total_size or 0) / 1024:.1f} KB")

            cursor.close()
            return True

        except Exception as e:
            logger.error(f"❌ 테이블 검증 실패: {e}")
            return False

    def show_sample_queries(self):
        """샘플 쿼리 예시 출력"""
        print(f"\n📝 샘플 쿼리 예시:")
        print(f"")
        print(f"# 전체 종목 조회")
        print(f"SELECT * FROM {self.table_name} WHERE is_active = TRUE;")
        print(f"")
        print(f"# 코스피 종목만 조회")
        print(f"SELECT * FROM {self.table_name} WHERE market = 'KOSPI' AND is_active = TRUE;")
        print(f"")
        print(f"# 종목코드로 검색")
        print(f"SELECT * FROM {self.table_name} WHERE code = '005930';")
        print(f"")
        print(f"# 종목명으로 검색 (like)")
        print(f"SELECT * FROM {self.table_name} WHERE name LIKE '%삼성%' AND is_active = TRUE;")
        print(f"")
        print(f"# 시장별 종목 수 통계")
        print(f"SELECT market, COUNT(*) as count FROM {self.table_name} WHERE is_active = TRUE GROUP BY market;")

    def run(self):
        """전체 실행"""
        print("🚀 stock_codes 테이블 생성 스크립트 시작")
        print("=" * 60)

        # 1. 데이터베이스 연결
        connection = self.connect_database()
        if not connection:
            print("❌ 데이터베이스 연결 실패로 종료합니다.")
            return False

        try:
            # 2. 기존 테이블 확인
            table_exists = self.check_existing_table(connection)

            # 3. 테이블 생성 (필요한 경우)
            if not table_exists:
                if not self.create_table(connection):
                    print("❌ 테이블 생성 실패")
                    return False

            # 4. 테이블 검증
            if not self.verify_table(connection):
                print("❌ 테이블 검증 실패")
                return False

            # 5. 샘플 쿼리 출력
            self.show_sample_queries()

            print("\n" + "=" * 60)
            print("🎉 stock_codes 테이블 준비 완료!")
            print("💡 다음 단계: scripts/collect_stock_codes.py 실행")

            return True

        except Exception as e:
            logger.error(f"❌ 실행 중 오류: {e}")
            return False
        finally:
            connection.close()


def main():
    """메인 실행 함수"""
    creator = StockCodesTableCreator()
    success = creator.run()

    if success:
        print("\n✅ 작업 완료!")
    else:
        print("\n❌ 작업 실패!")
        sys.exit(1)


if __name__ == "__main__":
    main()