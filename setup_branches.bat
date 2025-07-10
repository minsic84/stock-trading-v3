#!/bin/bash
# 파일명: setup_branches.bat
# Git 브랜치 전략 설정 스크립트

echo "🌿 Git 브랜치 전략 설정 시작"
echo "=" * 50

# 현재 브랜치 확인
echo "📍 현재 브랜치 상태:"
git branch -a
echo ""

# 1. develop 브랜치 생성 (개발 통합 브랜치)
echo "🔧 1단계: develop 브랜치 생성"
git checkout -b develop
git push -u origin develop
echo "✅ develop 브랜치 생성 완료"
echo ""

# 2. 기능별 브랜치들 생성
echo "🚀 2단계: 기능별 브랜치 생성"

# 일일 업데이트 기능
echo "📅 feature/daily-update 브랜치 생성..."
git checkout -b feature/daily-update
git push -u origin feature/daily-update

# 수급 데이터 기능
echo "📊 feature/supply-demand 브랜치 생성..."
git checkout -b feature/supply-demand
git push -u origin feature/supply-demand

# 웹 UI 기능
echo "🖥️ feature/web-ui 브랜치 생성..."
git checkout -b feature/web-ui
git push -u origin feature/web-ui

# 3분봉 데이터 기능
echo "⏱️ feature/3min-data 브랜치 생성..."
git checkout -b feature/3min-data
git push -u origin feature/3min-data

echo "✅ 기능 브랜치들 생성 완료"
echo ""

# 3. develop 브랜치로 복귀
echo "🔄 3단계: develop 브랜치로 복귀"
git checkout develop
echo "✅ develop 브랜치로 전환 완료"
echo ""

# 4. 브랜치 구조 확인
echo "📋 4단계: 최종 브랜치 구조"
echo "🌳 브랜치 목록:"
git branch -a
echo ""

echo "🎯 브랜치 역할:"
echo "  📍 main      : 프로덕션 배포용 (안정 버전)"
echo "  🔧 develop   : 개발 통합 브랜치 (기본 작업용)"
echo "  📅 feature/daily-update   : 일일 업데이트 시스템"
echo "  📊 feature/supply-demand  : 수급 데이터 수집"
echo "  🖥️ feature/web-ui        : 웹 대시보드"
echo "  ⏱️ feature/3min-data     : 3분봉 데이터 수집"
echo ""

echo "💡 작업 흐름:"
echo "  1. feature/* 브랜치에서 개발"
echo "  2. develop으로 병합"
echo "  3. 테스트 완료 후 main으로 병합"
echo ""

echo "🎉 Git 브랜치 전략 설정 완료!"