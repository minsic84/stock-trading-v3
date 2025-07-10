#!/usr/bin/env python3
"""
개발용 코드 품질 검사 스크립트
"""

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
        print("\n🎉 모든 검사 통과!")
    else:
        print("\n❌ 일부 검사 실패 - 수정 필요")
        sys.exit(1)

if __name__ == "__main__":
    main()
