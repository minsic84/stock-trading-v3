#!/usr/bin/env python3
"""
파일 경로: scripts/backup_and_cleanup.py

프로젝트 백업 생성 및 불필요한 파일 정리 스크립트
- 현재 상태 백업
- 불필요한 파일 안전 삭제
- 폴더 구조 최적화
"""

import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
import json


class ProjectCleanup:
    """프로젝트 정리 및 백업 클래스"""

    def __init__(self):
        self.project_root = Path.cwd()
        self.backup_dir = self.project_root / "backups"
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 삭제 대상 파일들
        self.files_to_remove = [
            "scripts/clean_database.py",
            "scripts/analyze_database_status.py",
            "scripts/restructure_mysql_to_separate_tables.py",
            "scripts/sync_sqlite_to_mysql_incremental.py",
            "config/database.yaml"  # SQLite 전용 설정
        ]

        # 삭제 대상 폴더들
        self.dirs_to_remove = [
            "logs",  # 임시 로그들 (필요시 재생성)
            "__pycache__",  # Python 캐시
            ".pytest_cache"  # 테스트 캐시
        ]

        # 백업에서 제외할 항목들
        self.backup_exclude = [
            "data/*.db",
            "data/*.sqlite*",
            "logs/*.log",
            "**/__pycache__",
            "**/*.pyc",
            ".git",
            "venv*",
            "env*",
            "backups"
        ]

    def create_backup(self) -> bool:
        """현재 프로젝트 상태 백업"""
        print("📦 1단계: 프로젝트 백업 생성")
        print("=" * 50)

        try:
            # 백업 디렉토리 생성
            self.backup_dir.mkdir(exist_ok=True)

            # 백업 파일 이름
            backup_filename = f"stock_trading_backup_{self.timestamp}.zip"
            backup_path = self.backup_dir / backup_filename

            print(f"📁 백업 위치: {backup_path}")
            print(f"⏳ 백업 생성 중...")

            # ZIP 파일 생성
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                self._add_to_zip(zipf, self.project_root)

            # 백업 메타데이터 생성
            metadata = {
                "backup_date": datetime.now().isoformat(),
                "backup_purpose": "Git 환경 정리 전 백업",
                "project_state": "MySQL 마이그레이션 완료, 62.7% 데이터 수집 완료",
                "files_count": self._count_files_in_zip(backup_path)
            }

            metadata_path = self.backup_dir / f"backup_metadata_{self.timestamp}.json"
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            file_size = backup_path.stat().st_size / (1024 * 1024)  # MB
            print(f"✅ 백업 완료!")
            print(f"   📁 파일: {backup_filename}")
            print(f"   📊 크기: {file_size:.1f}MB")
            print(f"   📋 메타데이터: backup_metadata_{self.timestamp}.json")

            return True

        except Exception as e:
            print(f"❌ 백업 실패: {e}")
            return False

    def _add_to_zip(self, zipf, folder):
        """폴더를 ZIP에 추가 (제외 패턴 적용)"""
        for file_path in folder.rglob("*"):
            if file_path.is_file() and not self._should_exclude(file_path):
                # 상대 경로로 추가
                relative_path = file_path.relative_to(self.project_root)
                zipf.write(file_path, relative_path)

    def _should_exclude(self, file_path: Path) -> bool:
        """파일이 백업에서 제외되어야 하는지 확인"""
        relative_path = file_path.relative_to(self.project_root)
        path_str = str(relative_path).replace('\\', '/')

        for pattern in self.backup_exclude:
            if self._match_pattern(path_str, pattern):
                return True
        return False

    def _match_pattern(self, path: str, pattern: str) -> bool:
        """간단한 패턴 매칭"""
        if '**' in pattern:
            # **/*.pyc 같은 패턴
            parts = pattern.split('**/')
            if len(parts) == 2:
                return path.endswith(parts[1].replace('*', ''))
        elif '*' in pattern:
            # data/*.db 같은 패턴
            import fnmatch
            return fnmatch.fnmatch(path, pattern)
        else:
            # 정확한 매칭
            return path == pattern or path.endswith('/' + pattern)
        return False

    def _count_files_in_zip(self, zip_path: Path) -> int:
        """ZIP 파일 내 파일 수 카운트"""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zipf:
                return len(zipf.namelist())
        except:
            return 0

    def cleanup_files(self) -> bool:
        """불필요한 파일들 정리"""
        print(f"\n🗑️ 2단계: 불필요한 파일 정리")
        print("=" * 50)

        removed_files = []
        removed_dirs = []

        try:
            # 파일 삭제
            print("📄 불필요한 파일 삭제:")
            for file_path in self.files_to_remove:
                full_path = self.project_root / file_path
                if full_path.exists():
                    full_path.unlink()
                    removed_files.append(file_path)
                    print(f"   ✅ 삭제: {file_path}")
                else:
                    print(f"   ⚠️ 없음: {file_path}")

            # 폴더 삭제
            print(f"\n📁 불필요한 폴더 삭제:")
            for dir_path in self.dirs_to_remove:
                full_path = self.project_root / dir_path
                if full_path.exists() and full_path.is_dir():
                    shutil.rmtree(full_path)
                    removed_dirs.append(dir_path)
                    print(f"   ✅ 삭제: {dir_path}/")
                else:
                    print(f"   ⚠️ 없음: {dir_path}/")

            # Python 캐시 재귀적 삭제
            print(f"\n🧹 Python 캐시 정리:")
            cache_count = self._remove_python_cache()
            print(f"   ✅ __pycache__ 폴더 {cache_count}개 삭제")

            # 정리 결과 저장
            cleanup_report = {
                "cleanup_date": datetime.now().isoformat(),
                "removed_files": removed_files,
                "removed_directories": removed_dirs,
                "python_cache_cleaned": cache_count,
                "status": "success"
            }

            report_path = self.backup_dir / f"cleanup_report_{self.timestamp}.json"
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(cleanup_report, f, indent=2, ensure_ascii=False)

            print(f"\n✅ 정리 완료!")
            print(f"   📄 삭제된 파일: {len(removed_files)}개")
            print(f"   📁 삭제된 폴더: {len(removed_dirs)}개")
            print(f"   📋 정리 리포트: cleanup_report_{self.timestamp}.json")

            return True

        except Exception as e:
            print(f"❌ 정리 실패: {e}")
            return False

    def _remove_python_cache(self) -> int:
        """Python 캐시 폴더들 재귀적 삭제"""
        count = 0
        for cache_dir in self.project_root.rglob("__pycache__"):
            if cache_dir.is_dir():
                shutil.rmtree(cache_dir)
                count += 1
        return count

    def optimize_structure(self) -> bool:
        """폴더 구조 최적화"""
        print(f"\n📁 3단계: 폴더 구조 최적화")
        print("=" * 50)

        try:
            # 필수 폴더들 생성
            essential_dirs = [
                "data",
                "logs",
                "scripts/utils",
                "src/core",
                "src/api",
                "src/collectors",
                "src/utils"
            ]

            created_dirs = []
            for dir_path in essential_dirs:
                full_path = self.project_root / dir_path
                if not full_path.exists():
                    full_path.mkdir(parents=True, exist_ok=True)
                    created_dirs.append(dir_path)
                    print(f"   ✅ 생성: {dir_path}/")

            # .gitkeep 파일 생성 (빈 폴더 유지용)
            gitkeep_dirs = ["data", "logs"]
            for dir_name in gitkeep_dirs:
                gitkeep_path = self.project_root / dir_name / ".gitkeep"
                if not gitkeep_path.exists():
                    gitkeep_path.touch()
                    print(f"   📌 .gitkeep 생성: {dir_name}/")

            if created_dirs:
                print(f"\n✅ 구조 최적화 완료! ({len(created_dirs)}개 폴더 생성)")
            else:
                print(f"\n✅ 폴더 구조 이미 최적화됨!")

            return True

        except Exception as e:
            print(f"❌ 구조 최적화 실패: {e}")
            return False

    def show_final_structure(self):
        """최종 프로젝트 구조 표시"""
        print(f"\n📋 4단계: 최종 프로젝트 구조")
        print("=" * 50)

        def print_tree(path: Path, prefix: str = "", max_depth: int = 3, current_depth: int = 0):
            if current_depth >= max_depth:
                return

            items = sorted([p for p in path.iterdir() if not p.name.startswith('.')],
                           key=lambda x: (x.is_file(), x.name.lower()))

            for i, item in enumerate(items):
                is_last = i == len(items) - 1
                current_prefix = "└── " if is_last else "├── "
                next_prefix = "    " if is_last else "│   "

                if item.is_dir():
                    print(f"{prefix}{current_prefix}📁 {item.name}/")
                    if current_depth < max_depth - 1:
                        print_tree(item, prefix + next_prefix, max_depth, current_depth + 1)
                else:
                    icon = "🐍" if item.suffix == ".py" else "📄"
                    print(f"{prefix}{current_prefix}{icon} {item.name}")

        print("🏗️ stock-trading-system/")
        print_tree(self.project_root, max_depth=3)

        # 요약 정보
        py_files = list(self.project_root.rglob("*.py"))
        print(f"\n📊 프로젝트 요약:")
        print(f"   🐍 Python 파일: {len(py_files)}개")
        print(f"   📁 주요 폴더: src/, scripts/, data/, logs/")
        print(f"   🎯 상태: Git 환경 최적화 완료!")


def main():
    """메인 실행 함수"""
    print("🚀 주식 트레이딩 시스템 - Git 환경 정리")
    print("=" * 60)
    print("📋 작업 계획:")
    print("   1️⃣ 현재 상태 백업 생성")
    print("   2️⃣ 불필요한 파일 정리")
    print("   3️⃣ 폴더 구조 최적화")
    print("   4️⃣ 최종 구조 확인")
    print("=" * 60)

    # 사용자 확인
    response = input("\n계속 진행하시겠습니까? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("❌ 작업이 취소되었습니다.")
        return False

    cleanup = ProjectCleanup()

    # 1단계: 백업 생성
    if not cleanup.create_backup():
        print("❌ 백업 실패로 인해 작업을 중단합니다.")
        return False

    # 2단계: 파일 정리
    if not cleanup.cleanup_files():
        print("⚠️ 파일 정리에 문제가 있었지만 계속 진행합니다.")

    # 3단계: 구조 최적화
    if not cleanup.optimize_structure():
        print("⚠️ 구조 최적화에 문제가 있었지만 계속 진행합니다.")

    # 4단계: 최종 구조 표시
    cleanup.show_final_structure()

    print("\n" + "=" * 60)
    print("🎉 Git 환경 정리 완료!")
    print("💡 다음 단계: README 및 문서 업데이트")
    print("📦 백업 위치: backups/ 폴더")

    return True


if __name__ == "__main__":
    main()