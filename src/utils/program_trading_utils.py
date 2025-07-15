# src/utils/program_trading_utils.py - 새 파일 생성

"""
프로그램매매 수집 관련 유틸리티 함수들
"""
from datetime import datetime, timedelta
from typing import Optional


def get_market_yesterday() -> str:
    """
    시장 기준 어제 날짜 반환 (YYYYMMDD)
    주말과 공휴일을 고려한 마지막 거래일
    """
    today = datetime.now()

    # 월요일(0)이면 금요일로, 나머지는 하루 전
    if today.weekday() == 0:  # 월요일
        yesterday = today - timedelta(days=3)  # 금요일
    elif today.weekday() == 6:  # 일요일
        yesterday = today - timedelta(days=2)  # 금요일
    else:
        yesterday = today - timedelta(days=1)  # 평일 하루 전

    return yesterday.strftime('%Y%m%d')


def get_one_year_ago_date() -> str:
    """
    1년 전 날짜 반환 (YYYYMMDD)
    """
    one_year_ago = datetime.now() - timedelta(days=365)
    return one_year_ago.strftime('%Y%m%d')


def calculate_missing_days(latest_date_str: str) -> int:
    """
    최신 날짜부터 어제까지의 누락 일수 계산

    Args:
        latest_date_str: 최신 날짜 (YYYYMMDD)

    Returns:
        누락 일수
    """
    try:
        latest_date = datetime.strptime(latest_date_str, '%Y%m%d').date()
        yesterday = datetime.strptime(get_market_yesterday(), '%Y%m%d').date()

        if latest_date >= yesterday:
            return 0

        return (yesterday - latest_date).days

    except Exception:
        return 0


def format_collection_summary(stats: dict) -> str:
    """
    수집 통계를 보기 좋은 문자열로 포맷팅
    """
    summary = []
    summary.append(f"📊 수집 결과 요약:")
    summary.append(f"   총 종목: {stats.get('total_stocks', 0):,}개")
    summary.append(f"   완료: {stats.get('completed_stocks', 0):,}개")
    summary.append(f"   신규 데이터: {stats.get('new_data_stocks', 0):,}개")
    summary.append(f"   스킵: {stats.get('skipped_stocks', 0):,}개")
    summary.append(f"   실패: {stats.get('failed_stocks', 0):,}개")

    if stats.get('total_stocks', 0) > 0:
        success_rate = (stats.get('completed_stocks', 0) / stats['total_stocks']) * 100
        summary.append(f"   성공률: {success_rate:.1f}%")

    return '\n'.join(summary)


def is_collection_needed(latest_date_str: Optional[str]) -> tuple[bool, str]:
    """
    수집이 필요한지 판단

    Args:
        latest_date_str: 최신 데이터 날짜 (YYYYMMDD) 또는 None

    Returns:
        (수집필요여부, 사유)
    """
    if latest_date_str is None:
        return True, "데이터 없음"

    missing_days = calculate_missing_days(latest_date_str)

    if missing_days == 0:
        return False, "최신 상태"
    elif missing_days <= 3:
        return True, f"{missing_days}일 누락"
    else:
        return True, f"{missing_days}일 누락 (대량)"


def get_collection_start_date(latest_date_str: Optional[str]) -> Optional[str]:
    """
    수집 시작 날짜 계산

    Args:
        latest_date_str: 최신 데이터 날짜 (YYYYMMDD) 또는 None

    Returns:
        수집 시작 날짜 (YYYYMMDD) 또는 None (수집 불필요)
    """
    if latest_date_str is None:
        # 데이터 없음 → 1년 전부터
        return get_one_year_ago_date()

    missing_days = calculate_missing_days(latest_date_str)

    if missing_days == 0:
        # 최신 상태 → 수집 불필요
        return None

    # 다음 날부터 수집
    latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
    next_date = latest_date + timedelta(days=1)
    return next_date.strftime('%Y%m%d')


def validate_date_format(date_str: str) -> bool:
    """
    날짜 형식 검증 (YYYYMMDD)
    """
    try:
        if len(date_str) != 8 or not date_str.isdigit():
            return False

        datetime.strptime(date_str, '%Y%m%d')
        return True

    except ValueError:
        return False


def get_smart_collection_mode(latest_date_str: Optional[str], force_full: bool = False) -> tuple[str, str]:
    """
    스마트 수집 모드 결정

    Args:
        latest_date_str: 최신 데이터 날짜
        force_full: 강제 전체 수집 여부

    Returns:
        (수집모드, 설명)
    """
    if force_full:
        return "full", "강제 전체 수집"

    if latest_date_str is None:
        return "full", "신규 종목 전체 수집"

    missing_days = calculate_missing_days(latest_date_str)

    if missing_days == 0:
        return "skip", "최신 상태 (스킵)"
    elif missing_days <= 30:
        return "incremental", f"증분 수집 ({missing_days}일)"
    else:
        return "full", f"대량 누락으로 전체 수집 ({missing_days}일)"