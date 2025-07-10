@echo off
REM 파일명: quick_backup.bat
REM 위치: C:\project\stock-trading-v3\quick_backup.bat

echo 🚀 빠른 Git 백업 시작...
echo.

echo 📦 파일 추가 중...
git add .

echo 💾 백업 커밋 생성 중...
git commit -m "파일정리 전 백업 - %date% %time%"

echo 🏷️ 백업 태그 생성 중...
git tag backup-%date:~0,4%%date:~5,2%%date:~8,2%-%time:~0,2%%time:~3,2%%time:~6,2%

echo.
echo ✅ 백업 완료!
echo 💡 이제 안전하게 파일 정리를 시작할 수 있습니다!
echo.
pause