#!/usr/bin/env python3
"""
파일 경로: cleanup_files.py
불필요한 파일 정리 스크립트 (백업 완료 후 실행)
"""

import os
import shutil
from pathlib import Path
import json


def cleanup_project():
    """프로젝트 파일 정리"""
    print("🗑️ 불필요한 파일 정리 시작")
    print("=" * 50)

    project_root = Path.cwd()

    # 삭제할 파일들
    files_to_remove = [
        "scripts/clean_database.py",
        "scripts/analyze_database_status.py",
        "scripts/restructure_mysql_to_separate_tables.py",
        "scripts/sync_sqlite_to_mysql_incremental.py",
        "config/database.yaml"
    ]

    # 삭제할 폴더들
    dirs_to_remove = [
        "logs",
        "__pycache__",
        "backups"  # 중단된 백업 파일들 완전 삭제
    ]

    removed_files = []
    removed_dirs = []

    # 파일 삭제
    print("📄 불필요한 파일 삭제:")
    for file_path in files_to_remove:
        full_path = project_root / file_path
        if full_path.exists():
            full_path.unlink()
            removed_files.append(file_path)
            print(f"   ✅ 삭제: {file_path}")
        else:
            print(f"   ⚠️ 없음: {file_path}")

    # 폴더 삭제
    print(f"\n📁 불필요한 폴더 삭제:")
    for dir_path in dirs_to_remove:
        full_path = project_root / dir_path
        if full_path.exists() and full_path.is_dir():
            shutil.rmtree(full_path)
            removed_dirs.append(dir_path)
            print(f"   ✅ 삭제: {dir_path}/")
        else:
            print(f"   ⚠️ 없음: {dir_path}/")

    # Python 캐시 정리
    print(f"\n🧹 Python 캐시 정리:")
    cache_count = 0
    for cache_dir in project_root.rglob("__pycache__"):
        if cache_dir.is_dir():
            shutil.rmtree(cache_dir)
            cache_count += 1
    print(f"   ✅ __pycache__ 폴더 {cache_count}개 삭제")

    # 추가 공간 확보 - 임시 파일들 삭제
    print(f"\n🧹 추가 공간 확보:")
    temp_patterns = [
        "*.tmp",
        "*.temp",
        "*.log",
        "*.db-journal",
        "*.sqlite-wal",
        "*.sqlite-shm"
    ]

    temp_count = 0
    freed_space = 0
    for pattern in temp_patterns:
        for temp_file in project_root.rglob(pattern):
            if temp_file.is_file():
                try:
                    file_size = temp_file.stat().st_size / (1024 * 1024)  # MB
                    temp_file.unlink()
                    temp_count += 1
                    freed_space += file_size
                    if file_size > 1:  # 1MB 이상만 표시
                        print(f"   ✅ 삭제: {temp_file.name} ({file_size:.1f}MB)")
                except:
                    pass

    if temp_count == 0:
        print("   ℹ️ 삭제할 임시 파일 없음")
    else:
        print(f"   🗑️ 총 {temp_count}개 임시 파일 삭제 ({freed_space:.1f}MB 확보)")

    # 필수 폴더 생성
    print(f"\n📁 필수 폴더 생성:")
    essential_dirs = ["data", "logs", "scripts/utils"]
    for dir_path in essential_dirs:
        full_path = project_root / dir_path
        if not full_path.exists():
            full_path.mkdir(parents=True, exist_ok=True)
            print(f"   ✅ 생성: {dir_path}/")

    # .gitkeep 파일 생성
    gitkeep_dirs = ["data", "logs"]
    for dir_name in gitkeep_dirs:
        gitkeep_path = project_root / dir_name / ".gitkeep"
        if not gitkeep_path.exists():
            gitkeep_path.touch()
            print(f"   📌 .gitkeep 생성: {dir_name}/")

    print(f"\n✅ 파일 정리 완료!")
    print(f"   📄 삭제된 파일: {len(removed_files)}개")
    print(f"   📁 삭제된 폴더: {len(removed_dirs)}개")

    return True


if __name__ == "__main__":
    cleanup_project()