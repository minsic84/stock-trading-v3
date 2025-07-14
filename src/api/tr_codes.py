"""
키움 API TR 코드 정보 관리 모듈
각 TR의 입력/출력 필드 및 메타정보 중앙 관리
매뉴얼 기반으로 정확한 필드명 사용
"""

# TR 코드별 상세 정보
TR_INFO = {
    'opt10001': {
        'name': '주식기본정보요청',
        'description': '종목의 현재가, 등락률, 거래량 등 기본 정보 조회',
        'input_fields': {
            '종목코드': 'string'  # 6자리 종목코드
        },
        'output_fields': {
            '종목명': 'string',
            '현재가': 'int',
            '전일대비': 'int',
            '등락률': 'float',
            '거래량': 'int',
            '시가': 'int',
            '고가': 'int',
            '저가': 'int',
            '상한가': 'int',
            '하한가': 'int',
            '시가총액': 'int',
            '시가총액규모': 'string',
            '상장주수': 'int',
            'PER': 'float',
            'PBR': 'float'
        },
        'delay_ms': 3600  # API 요청 간격
    },

    'opt10081': {
        'name': '일봉차트조회',
        'description': '주식 일봉 데이터 조회',
        'input_fields': {
            '종목코드': 'string',
            '기준일자': 'string',  # YYYYMMDD
            '수정주가구분': 'string'  # 1:수정주가, 0:원주가
        },
        'output_fields': {
            '일자': 'string',
            '현재가': 'int',
            '거래량': 'int',
            '거래대금': 'int',
            '시가': 'int',
            '고가': 'int',
            '저가': 'int',
            '전일종가': 'int'
        },
        'delay_ms': 3600
    },

    # 🔧 수정된 수급 데이터 TR (매뉴얼 기반)
    'opt10060': {
        'name': '일별수급데이터요청',
        'description': '일별 투자자별 매수/매도 수급 데이터 조회',
        'input_fields': {
            '일자': 'string',           # YYYYMMDD (연도4자리, 월 2자리, 일 2자리)
            '종목코드': 'string',       # KRX:039490, NXT:039490_NX, 통합:039490_AL
            '금액수량구분': 'string',   # 1:금액, 2:수량
            '매매구분': 'string',       # 0:순매수, 1:매수, 2:매도
            '단위구분': 'string'        # 1000:천주, 1:단주
        },
        'output_fields': {
            # 실제 출력 필드는 API 호출해서 확인 필요
            '일자': 'string',
            '현재가': 'int',
            '전일대비': 'int',
            '누적거래대금': 'int',
            '개인투자자': 'int',
            '외국인투자': 'int',
            '기관계': 'int',
            '금융투자': 'int',
            '보험': 'int',
            '투신': 'int',
            '기타금융': 'int',
            '은행': 'int',
            '연기금등': 'int',
            '사모펀드': 'int',
            '국가': 'int',
            '기타법인': 'int',
            '내외국인': 'int'
            # 추가 필드는 실제 응답 구조 확인 후 업데이트
        },
        'delay_ms': 3600
    },

    # 🔧 프로그램매매 TR을 OPT90013으로 변경
    'opt90013': {
        'name': '프로그램매매추이요청',
        'description': '프로그램매매 추이 데이터 조회',
        'input_fields': {
            '시간일자구분': 'string',   # 2:일자별
            '금액수량구분': 'string',   # 1:금액, 2:수량
            '종목코드': 'string',       # 종목코드
            '날짜': 'string'           # YYYYMMDD
        },
        'output_fields': {
            # 🔍 실제 API 응답에서 확인된 필드들
            '일자': 'string',
            '현재가': 'string',               # +61000 형태
            '대비기호': 'string',             # 2, 5 등
            '전일대비': 'string',             # +600 형태
            '등락율': 'string',               # +0.99 형태
            '거래량': 'int',                  # 14768473
            '프로그램매도금액': 'int',        # 265487
            '프로그램매수금액': 'int',        # 336139
            '프로그램순매수금액': 'int',      # 70652 (실제 순매수)
            '프로그램순매수금액증감': 'int',  # 263674 (증감분)
            '프로그램매도수량': 'int',        # 4362777
            '프로그램매수수량': 'int',        # 5517403
            '프로그램순매수수량': 'int',      # 1154626 (실제 순매수)
            '프로그램순매수수량증감': 'int',  # 4340008 (증감분)
            '기준가시간': 'string',           # 빈값
            '대차거래상환주수합': 'string',   # 빈값
            '잔고수주합': 'string',           # 빈값
            '거래소구분': 'string'            # KRX
        },
        'delay_ms': 3600
    },

    # 🔧 수정된 분봉차트 TR (매뉴얼 기반)
    'opt10080': {
        'name': '분봉차트조회',
        'description': '주식 분봉 데이터 조회 (최대 900개)',
        'input_fields': {
            '종목코드': 'string',       # KRX:039490, NXT:039490_NX, 통합:039490_AL
            '틱범위': 'string',         # 1:1분, 3:3분, 5:5분, 10:10분, 15:15분, 30:30분, 45:45분, 60:60분
            '수정주가구분': 'string'    # 0 or 1
        },
        'output_fields': {
            # 실제 출력 필드는 API 호출해서 확인 필요
            '현재가': 'string',
            '거래량': 'int',
            '체결시간': 'int',
            '시가': 'int',
            '고가': 'int',
            '저가': 'int',
            '수정주가구분': 'int',
            '수정비율': 'int',
            '대업종구분': 'int',
            '소업종구분': 'int',
            '종목정보': 'int',
            '수정주가이벤트': 'int',
            '전일종가': 'int'
            # 추가 필드는 실제 응답 구조 확인 후 업데이트
        },
        'delay_ms': 3600
    }
}


def get_tr_info(tr_code: str) -> dict:
    """TR 코드 정보 반환"""
    tr_code_lower = tr_code.lower()
    if tr_code_lower not in TR_INFO:
        raise ValueError(f"지원하지 않는 TR 코드: {tr_code}")

    info = TR_INFO[tr_code_lower].copy()
    info['code'] = tr_code_lower
    return info


def get_tr_name(tr_code: str) -> str:
    """TR 코드의 이름 반환"""
    return get_tr_info(tr_code)['name']


def get_input_fields(tr_code: str) -> dict:
    """TR 코드의 입력 필드 반환"""
    return get_tr_info(tr_code)['input_fields']


def get_output_fields(tr_code: str) -> dict:
    """TR 코드의 출력 필드 반환"""
    return get_tr_info(tr_code)['output_fields']


def get_delay_ms(tr_code: str) -> int:
    """TR 코드의 권장 딜레이 시간 반환"""
    return get_tr_info(tr_code)['delay_ms']


def validate_input_data(tr_code: str, input_data: dict) -> bool:
    """입력 데이터 유효성 검증"""
    try:
        required_fields = get_input_fields(tr_code)

        for field in required_fields:
            if field not in input_data:
                print(f"❌ 필수 입력 필드 누락: {field}")
                return False

            if not input_data[field]:
                print(f"❌ 입력 필드 값이 비어있음: {field}")
                return False

        return True

    except Exception as e:
        print(f"❌ 입력 데이터 검증 실패: {e}")
        return False


def get_all_tr_codes() -> list:
    """지원하는 모든 TR 코드 반환"""
    return list(TR_INFO.keys())


def show_tr_info(tr_code: str = None):
    """TR 코드 정보 출력 (디버깅용)"""
    if tr_code:
        try:
            info = get_tr_info(tr_code)
            print(f"🔍 TR 코드: {info['code'].upper()}")
            print(f"📝 이름: {info['name']}")
            print(f"📄 설명: {info['description']}")
            print(f"📥 입력 필드: {list(info['input_fields'].keys())}")
            print(f"📤 출력 필드: {list(info['output_fields'].keys())}")
            print(f"⏱️ 딜레이: {info['delay_ms']}ms")
        except ValueError as e:
            print(f"❌ {e}")
    else:
        print("🎯 지원하는 TR 코드 목록:")
        for code in get_all_tr_codes():
            info = TR_INFO[code]
            print(f"   📊 {code.upper()}: {info['name']}")


# 편의 함수들
def create_opt10001_input(stock_code: str) -> dict:
    """OPT10001 입력 데이터 생성"""
    return {
        '종목코드': stock_code
    }


def create_opt10081_input(stock_code: str, base_date: str = "", adj_price: str = "1") -> dict:
    """OPT10081 입력 데이터 생성"""
    return {
        '종목코드': stock_code,
        '기준일자': base_date,
        '수정주가구분': adj_price
    }


def create_opt10060_input(stock_code: str, date: str = "", amount_type: str = "1",
                         trade_type: str = "0", unit_type: str = "1000") -> dict:
    """OPT10060 수급데이터 입력 데이터 생성 (매뉴얼 기반)"""
    return {
        '일자': date,                    # YYYYMMDD (빈값이면 최근일)
        '종목코드': stock_code,          # 종목코드
        '금액수량구분': amount_type,     # 1:금액, 2:수량
        '매매구분': trade_type,          # 0:순매수, 1:매수, 2:매도
        '단위구분': unit_type            # 1000:천주, 1:단주
    }


# 🔧 실제 필드 기반 입력 데이터 생성 함수
def create_opt90013_input(stock_code: str, date: str = "20250710",
                         time_type: str = "2", amount_type: str = "1") -> dict:
    """OPT90013 프로그램매매 입력 데이터 생성 (실제 테스트 확인)"""
    return {
        '시간일자구분': time_type,      # 2:일자별
        '금액수량구분': amount_type,     # 1:금액, 2:수량
        '종목코드': stock_code,          # 종목코드
        '날짜': date                     # YYYYMMDD (테스트: 20250710)
    }


def create_opt10080_input(stock_code: str, tick_range: str = "3", adj_price: str = "1") -> dict:
    """OPT10080 분봉차트 입력 데이터 생성 (매뉴얼 기반)"""
    return {
        '종목코드': stock_code,          # 종목코드
        '틱범위': tick_range,            # 1:1분, 3:3분, 5:5분 등
        '수정주가구분': adj_price        # 0 or 1
    }


# 하위 호환성을 위한 별칭들
def create_opt10014_input(*args, **kwargs):
    """OPT10014는 OPT90013으로 변경됨"""
    print("⚠️ OPT10014는 OPT90013으로 변경되었습니다. create_opt90013_input()을 사용하세요.")
    return create_opt90013_input(*args, **kwargs)


# 테스트 함수
if __name__ == "__main__":
    print("🚀 TR 코드 관리 모듈 테스트 (매뉴얼 기반 수정)")
    print("=" * 60)

    # 전체 TR 코드 출력
    show_tr_info()

    print("\n" + "=" * 60)

    # 수정된 TR 정보 출력
    for tr_code in ['opt10060', 'opt90013', 'opt10080']:
        print(f"\n🔧 수정된 TR: {tr_code.upper()}")
        show_tr_info(tr_code)

    print("\n" + "=" * 60)

    # 입력 데이터 생성 테스트
    print("🔧 입력 데이터 생성 테스트:")

    # 수급 데이터 (OPT10060)
    supply_input = create_opt10060_input('005930')
    print(f"📊 OPT10060 수급: {supply_input}")

    # 프로그램매매 (OPT90013)
    program_input = create_opt90013_input('005930')
    print(f"📈 OPT90013 프로그램매매: {program_input}")

    # 3분봉 (OPT10080)
    minute_input = create_opt10080_input('005930', '3')
    print(f"⏰ OPT10080 3분봉: {minute_input}")

    print(f"\n✅ 총 {len(get_all_tr_codes())}개 TR 코드 지원!")
    print(f"🔧 매뉴얼 기반으로 정확한 필드명 사용")