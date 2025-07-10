# 🛠️ 개발 명령어 모음

## 🚀 빠른 시작
```bash
# 가상환경 활성화
venv-32bit\Scripts\activate

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
