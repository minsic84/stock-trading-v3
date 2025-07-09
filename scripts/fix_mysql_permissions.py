#!/usr/bin/env python3
"""
파일 경로: scripts/fix_mysql_permissions.py

MySQL 권한 문제 해결 스크립트
- root 권한으로 새 스키마 생성 및 권한 부여
- 스키마 분리를 위한 사전 준비
"""
import sys
import mysql.connector
from mysql.connector import Error as MySQLError
import getpass


def fix_permissions():
    """MySQL 권한 문제 해결"""
    print("🔐 MySQL 권한 문제 해결")
    print("=" * 40)

    # root 비밀번호 입력
    root_password = getpass.getpass("MySQL root 비밀번호를 입력하세요: ")

    # root 연결 설정
    root_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': root_password,
        'charset': 'utf8mb4',
        'autocommit': True
    }

    try:
        # root로 연결
        print("🔗 root 계정으로 연결 중...")
        conn = mysql.connector.connect(**root_config)
        cursor = conn.cursor()

        # 1. 새 스키마들 생성
        print("\n📁 1단계: 새 스키마 생성")
        schemas = [
            'daily_prices_db',
            'supply_demand_db',
            'minute_data_db'
        ]

        for schema in schemas:
            try:
                cursor.execute(f"""
                    CREATE DATABASE IF NOT EXISTS {schema} 
                    DEFAULT CHARACTER SET utf8mb4 
                    DEFAULT COLLATE utf8mb4_unicode_ci
                """)
                print(f"   ✅ {schema}: 생성 완료")
            except Exception as e:
                print(f"   ❌ {schema}: {e}")

        # 2. stock_user에게 권한 부여
        print("\n🔐 2단계: stock_user 권한 부여")
        all_schemas = ['stock_trading_db'] + schemas

        for schema in all_schemas:
            try:
                cursor.execute(f"GRANT ALL PRIVILEGES ON {schema}.* TO 'stock_user'@'localhost'")
                print(f"   ✅ {schema}: 권한 부여 완료")
            except Exception as e:
                print(f"   ❌ {schema}: {e}")

        # 3. 권한 적용
        cursor.execute("FLUSH PRIVILEGES")
        print("\n✅ 권한 적용 완료")

        # 4. 권한 확인
        print("\n📋 부여된 권한 확인:")
        cursor.execute("SHOW GRANTS FOR 'stock_user'@'localhost'")
        grants = cursor.fetchall()
        for grant in grants:
            print(f"   {grant[0]}")

        conn.close()

        print(f"\n🎉 권한 문제 해결 완료!")
        print(f"💡 이제 스키마 분리 스크립트를 다시 실행하세요.")

        return True

    except MySQLError as e:
        print(f"❌ MySQL 오류: {e}")
        return False
    except Exception as e:
        print(f"❌ 실행 오류: {e}")
        return False


def main():
    """메인 실행 함수"""
    print("🚀 MySQL 권한 문제 해결 도구")
    print("=" * 50)
    print("⚠️  이 스크립트는 MySQL root 권한이 필요합니다.")
    print("📋 작업 내용:")
    print("   1. daily_prices_db, supply_demand_db, minute_data_db 스키마 생성")
    print("   2. stock_user에게 모든 스키마 권한 부여")
    print("   3. 권한 적용 및 확인")

    response = input("\n계속 진행하시겠습니까? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("❌ 작업이 취소되었습니다.")
        return False

    success = fix_permissions()

    if success:
        print(f"\n🎯 다음 단계:")
        print(f"   python scripts/separate_mysql_schemas.py")

    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)