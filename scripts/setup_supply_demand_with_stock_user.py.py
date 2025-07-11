#!/usr/bin/env python3
"""
stock_user 계정을 사용한 MySQL 수급 스키마 설정 스크립트
scripts/setup_supply_demand_with_stock_user.py
"""
import sys
import os
import mysql.connector
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def check_user_permissions():
    """stock_user 권한 확인"""
    try:
        print("🔍 stock_user 권한 확인 중...")

        connection = mysql.connector.connect(
            host='localhost',
            user='stock_user',
            password='StockPass2025!',
            charset='utf8mb4'
        )

        cursor = connection.cursor()

        # 현재 사용자 권한 확인
        cursor.execute("SHOW GRANTS FOR CURRENT_USER")
        grants = cursor.fetchall()

        print("📋 stock_user 권한:")
        can_create_db = False
        for grant in grants:
            print(f"   {grant[0]}")
            if 'ALL PRIVILEGES' in grant[0] or 'CREATE' in grant[0]:
                can_create_db = True

        cursor.close()
        connection.close()

        return can_create_db

    except mysql.connector.Error as e:
        print(f"❌ 권한 확인 실패: {e}")
        return False


def create_schema_with_stock_user():
    """stock_user로 스키마 생성 시도"""
    try:
        print("🚀 stock_user로 수급 스키마 생성 시도...")

        connection = mysql.connector.connect(
            host='localhost',
            user='stock_user',
            password='StockPass2025!',
            charset='utf8mb4'
        )

        cursor = connection.cursor()

        # 1. 수급 데이터 스키마 생성 시도
        try:
            cursor.execute("""
                CREATE DATABASE IF NOT EXISTS supply_demand_db 
                CHARACTER SET utf8mb4 
                COLLATE utf8mb4_unicode_ci
            """)
            print("✅ 1. supply_demand_db 스키마 생성 성공")
        except mysql.connector.Error as e:
            print(f"❌ 스키마 생성 실패: {e}")
            print("💡 root 계정으로 권한 부여가 필요합니다")
            return False

        # 2. 스키마 변경
        cursor.execute("USE supply_demand_db")

        # 3. 템플릿 테이블 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supply_demand_template (
                date VARCHAR(8) PRIMARY KEY COMMENT '거래일자(YYYYMMDD)',
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
                program_buy BIGINT DEFAULT 0 COMMENT '프로그램매매 매수금액',
                program_sell BIGINT DEFAULT 0 COMMENT '프로그램매매 매도금액',
                program_net BIGINT DEFAULT 0 COMMENT '프로그램매매 순매수금액',
                data_source VARCHAR(50) DEFAULT 'OPT10060,OPT10014' COMMENT '데이터출처',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
                INDEX idx_date (date),
                INDEX idx_foreign_net (foreign_net),
                INDEX idx_program_net (program_net)
            ) ENGINE=InnoDB COMMENT='수급 및 프로그램매매 데이터 템플릿'
        """)
        print("✅ 2. 템플릿 테이블 생성 완료")

        # 4. 진행상황 테이블 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supply_demand_collection_progress (
                stock_code VARCHAR(6) PRIMARY KEY COMMENT '종목코드',
                stock_name VARCHAR(100) COMMENT '종목명',
                market VARCHAR(10) COMMENT '시장구분',
                market_cap BIGINT COMMENT '시가총액',
                table_created BOOLEAN DEFAULT FALSE COMMENT '테이블 생성 여부',
                last_collected_date VARCHAR(8) COMMENT '마지막 수집일자',
                total_records INT DEFAULT 0 COMMENT '수집 레코드 수',
                status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending' COMMENT '수집 상태',
                attempt_count INT DEFAULT 0 COMMENT '시도 횟수',
                last_attempt_time DATETIME COMMENT '마지막 시도 시간',
                error_message TEXT COMMENT '오류 메시지',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
                INDEX idx_status (status),
                INDEX idx_market_cap (market_cap),
                INDEX idx_last_collected_date (last_collected_date)
            ) ENGINE=InnoDB COMMENT='수급 데이터 수집 진행상황'
        """)
        print("✅ 3. 진행상황 테이블 생성 완료")

        # 5. 대상 종목 데이터 초기화
        cursor.execute("""
            INSERT INTO supply_demand_collection_progress (stock_code, stock_name, market, market_cap, status)
            SELECT 
                code,
                name,
                market,
                market_cap,
                'pending'
            FROM stock_trading_db.stocks 
            WHERE market_cap >= 300000
              AND LENGTH(TRIM(code)) = 6
              AND code REGEXP '^[0-9]{6}$'
            ON DUPLICATE KEY UPDATE
                stock_name = VALUES(stock_name),
                market = VALUES(market),
                market_cap = VALUES(market_cap),
                updated_at = CURRENT_TIMESTAMP
        """)
        connection.commit()
        print("✅ 4. 대상 종목 데이터 초기화 완료")

        # 6. 결과 확인
        cursor.execute("""
            SELECT 
                COUNT(*) as target_stocks,
                SUM(CASE WHEN market = 'KOSPI' THEN 1 ELSE 0 END) as kospi_count,
                SUM(CASE WHEN market = 'KOSDAQ' THEN 1 ELSE 0 END) as kosdaq_count,
                MIN(market_cap) as min_market_cap,
                MAX(market_cap) as max_market_cap
            FROM supply_demand_collection_progress
        """)

        result = cursor.fetchone()

        print("\n📊 설정 완료 결과:")
        print(f"   📈 총 대상 종목: {result[0]:,}개")
        print(f"   📊 코스피: {result[1]:,}개")
        print(f"   📊 코스닥: {result[2]:,}개")
        print(f"   💰 최소 시가총액: {result[3]:,}억원")
        print(f"   💰 최대 시가총액: {result[4]:,}억원")

        cursor.close()
        connection.close()

        print("\n🎉 수급 데이터 스키마 설정 완료!")
        return True

    except mysql.connector.Error as e:
        print(f"❌ MySQL 오류: {e}")
        return False
    except Exception as e:
        print(f"❌ 실행 중 오류 발생: {e}")
        return False


def provide_root_commands():
    """root 권한 부여 명령어 안내"""
    print("\n💡 root 계정으로 다음 명령어를 실행해주세요:")
    print("=" * 60)
    print("mysql -u root -p")
    print("")
    print("-- 접속 후 다음 명령어들 실행:")
    print("CREATE DATABASE IF NOT EXISTS supply_demand_db CHARACTER SET utf8mb4;")
    print("GRANT ALL PRIVILEGES ON supply_demand_db.* TO 'stock_user'@'localhost';")
    print("FLUSH PRIVILEGES;")
    print("EXIT;")
    print("=" * 60)
    print("\n그 다음에 이 스크립트를 다시 실행해주세요.")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 MySQL 수급 데이터 스키마 설정 (stock_user)")
    print("=" * 60)

    # 1. 권한 확인
    can_create = check_user_permissions()

    if can_create:
        print("✅ stock_user에게 CREATE 권한이 있습니다.")
        # 스키마 생성 시도
        if create_schema_with_stock_user():
            print("🎯 다음 단계: python scripts/collect_supply_demand.py")
        else:
            provide_root_commands()
    else:
        print("❌ stock_user에게 CREATE DATABASE 권한이 없습니다.")
        provide_root_commands()