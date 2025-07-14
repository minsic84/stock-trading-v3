#!/usr/bin/env python3
"""
파일명: setup_dev_environment.py
개발 환경 설정 스크립트 - VSCode 설정, Git 훅, 코드 품질 도구
"""

import json
import os
from pathlib import Path


def setup_vscode_settings():
    """VSCode 설정 생성"""
    print("🔧 VSCode 설정 생성")

    vscode_dir = Path(".vscode")
    vscode_dir.mkdir(exist_ok=True)

    # settings.json
    settings = {
        "python.defaultInterpreterPath": "./venv-32bit/Scripts/python.exe",
        "python.terminal.activateEnvironment": True,
        "python.linting.enabled": True,
        "python.linting.pylintEnabled": True,
        "python.formatting.provider": "black",
        "python.formatting.blackArgs": ["--line-length=100"],
        "editor.formatOnSave": True,
        "editor.rulers": [100],
        "files.encoding": "utf8",
        "files.autoSave": "onFocusChange",
        "terminal.integrated.shell.windows": "powershell.exe",
        "python.analysis.typeCheckingMode": "basic",
        "sqltools.connections": [
            {
                "name": "Stock Trading DB",
                "driver": "MySQL",
                "server": "localhost",
                "port": 3306,
                "database": "stock_trading_db",
                "username": "stock_user"
            }
        ]
    }

    with open(vscode_dir / "settings.json", "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2, ensure_ascii=False)

    # extensions.json (권장 확장프로그램)
    extensions = {
        "recommendations": [
            "ms-python.python",
            "ms-python.black-formatter",
            "ms-python.pylint",
            "mtxr.sqltools",
            "mtxr.sqltools-driver-mysql",
            "ms-vscode.vscode-json",
            "redhat.vscode-yaml",
            "ms-vscode.powershell",
            "github.github-vscode-theme",
            "ms-python.debugpy"
        ]
    }

    with open(vscode_dir / "extensions.json", "w", encoding="utf-8") as f:
        json.dump(extensions, f, indent=2)

    # launch.json (디버깅 설정)
    launch = {
        "version": "0.2.0",
        "configurations": [
            {
                "name": "데이터 수집 디버그",
                "type": "python",
                "request": "launch",
                "program": "${workspaceFolder}/scripts/collect_all_stocks.py",
                "console": "integratedTerminal",
                "justMyCode": True,
                "env": {
                    "PYTHONPATH": "${workspaceFolder}"
                }
            },
            {
                "name": "스마트 재시작 디버그",
                "type": "python",
                "request": "launch",
                "program": "${workspaceFolder}/scripts/smart_restart_collection.py",
                "console": "integratedTerminal",
                "justMyCode": True
            }
        ]
    }

    with open(vscode_dir / "launch.json", "w", encoding="utf-8") as f:
        json.dump(launch, f, indent=2)

    print("   ✅ .vscode/settings.json")
    print("   ✅ .vscode/extensions.json")
    print("   ✅ .vscode/launch.json")


def setup_env_template():
    """환경변수 템플릿 생성"""
    print("\n🔐 환경변수 템플릿 생성")

    env_example = """# ========================================
# 주식 트레이딩 시스템 v3 - 환경변수 설정
# ========================================

# 환경 설정
ENVIRONMENT=development
DEBUG=True
LOG_LEVEL=INFO

# MySQL 데이터베이스 설정
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_USER=stock_user
DB_PASSWORD=StockPass2025!

# MySQL 다중 스키마
MYSQL_MAIN_SCHEMA=stock_trading_db
MYSQL_DAILY_SCHEMA=daily_prices_db
MYSQL_SUPPLY_SCHEMA=supply_demand_db
MYSQL_MINUTE_SCHEMA=minute_data_db

# 키움 API 설정 (보안상 비워둠 - 수동 입력 필요)
KIWOOM_USER_ID=
KIWOOM_PASSWORD=
KIWOOM_CERT_PASSWORD=

# API 요청 설정
API_REQUEST_DELAY_MS=3600
MAX_RETRY_ATTEMPTS=3
RETRY_DELAY_SECONDS=3.6

# 로깅 설정
LOG_DIR=./logs
LOG_FILE_MAX_SIZE=10MB
LOG_FILE_BACKUP_COUNT=5

# 텔레그램 알림 (선택사항)
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
"""

    with open(".env.example", "w", encoding="utf-8") as f:
        f.write(env_example)

    print("   ✅ .env.example 생성 완료")
    print("   💡 실제 사용시 .env로 복사 후 값 입력")


def setup_gitignore_enhancement():
    """gitignore 강화"""
    print("\n🚫 .gitignore 강화")

    additional_ignores = """
# ========================================
# 개발 환경 추가 제외 항목
# ========================================

# VSCode 사용자 설정
.vscode/settings.json.user

# Python 가상환경들
venv-*/
env-*/

# 개발용 임시 파일
*.dev.py
test_*.py
debug_*.py

# 성능 프로파일링 결과
*.prof
*.pstats

# 메모리 프로파일링
*.mprof

# Jupyter Notebook
.ipynb_checkpoints/
*.ipynb

# 개발 데이터베이스
*.dev.db
*.test.db

# 백업 파일들
backup_*/
archives/
"""

    with open(".gitignore", "a", encoding="utf-8") as f:
        f.write(additional_ignores)

    print("   ✅ .gitignore 강화 완료")


def setup_development_scripts():
    """개발용 스크립트 생성"""
    print("\n🛠️ 개발용 스크립트 생성")

    scripts_dir = Path("scripts/dev")
    scripts_dir.mkdir(exist_ok=True)

    # 코드 품질 검사 스크립트
    quality_check = """#!/usr/bin/env python3
\"\"\"
개발용 코드 품질 검사 스크립트
\"\"\"

import subprocess
import sys

def run_command(cmd, description):
    print(f"🔍 {description}...")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"✅ {description} 통과")
    else:
        print(f"❌ {description} 실패:")
        print(result.stdout)
        print(result.stderr)
    return result.returncode == 0

def main():
    print("🚀 코드 품질 검사 시작")
    print("=" * 50)

    all_passed = True

    # Black 포매팅 체크
    if not run_command("black --check --diff src/ scripts/", "Black 포매팅 체크"):
        all_passed = False

    # 타입 체크 (mypy가 설치된 경우)
    if not run_command("mypy src/ --ignore-missing-imports", "타입 체크"):
        print("   💡 mypy가 없으면 건너뜀")

    if all_passed:
        print("\\n🎉 모든 검사 통과!")
    else:
        print("\\n❌ 일부 검사 실패 - 수정 필요")
        sys.exit(1)

if __name__ == "__main__":
    main()
"""

    with open(scripts_dir / "quality_check.py", "w", encoding="utf-8") as f:
        f.write(quality_check)

    print("   ✅ scripts/dev/quality_check.py")


def setup_development_commands():
    """개발용 명령어 모음 생성"""
    print("\n📋 개발 명령어 가이드 생성")

    dev_commands = """# 🛠️ 개발 명령어 모음

## 🚀 빠른 시작
```bash
# 가상환경 활성화
venv-32bit\\Scripts\\activate

# 개발 서버 시작
python scripts/collect_all_stocks.py

# 스마트 재시작
python scripts/smart_restart_collection.py
```

## 🔍 데이터 확인
```bash
# 수집 상태 확인
python scripts/check_collection_status.py

# 데이터베이스 분석
python scripts/analyze_database_status.py
```

## 🧪 테스트 및 품질 검사
```bash
# 코드 포매팅
black src/ scripts/

# 코드 품질 검사
python scripts/dev/quality_check.py

# 타입 체크
mypy src/ --ignore-missing-imports
```

## 🌿 브랜치 작업
```bash
# 새 기능 개발 시작
git checkout develop
git pull origin develop
git checkout -b feature/새기능명

# 개발 완료 후 병합
git checkout develop
git merge feature/새기능명
git push origin develop
```

## 🗄️ 데이터베이스 관리
```bash
# MySQL 접속
mysql -u stock_user -p stock_trading_db

# 데이터베이스 백업
mysqldump -u stock_user -p stock_trading_db > backup.sql

# 백업 복원
mysql -u stock_user -p stock_trading_db < backup.sql
```
"""

    with open("DEV_COMMANDS.md", "w", encoding="utf-8") as f:
        f.write(dev_commands)

    print("   ✅ DEV_COMMANDS.md")


def main():
    """메인 실행 함수"""
    print("🛠️ 개발 환경 설정 시작")
    print("=" * 60)

    setup_vscode_settings()
    setup_env_template()
    setup_gitignore_enhancement()
    setup_development_scripts()
    setup_development_commands()

    print("\n" + "=" * 60)
    print("🎉 개발 환경 설정 완료!")
    print("\n📋 다음 단계:")
    print("   1. VSCode에서 권장 확장프로그램 설치")
    print("   2. .env.example을 .env로 복사 후 설정")
    print("   3. DEV_COMMANDS.md 참고하여 개발 시작")
    print("   4. Git 커밋 및 푸시")


if __name__ == "__main__":
    main()