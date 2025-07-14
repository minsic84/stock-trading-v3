"""
NXT 종목코드 전용 수집 모듈
키움 API GetCodeListByMarket() 함수를 사용하여 NXT 종목만 빠르게 수집
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime

# 로거 설정
logger = logging.getLogger(__name__)


class NXTCodeCollector:
    """NXT 종목코드 전용 수집 클래스"""

    def __init__(self, kiwoom_connector):
        """
        Args:
            kiwoom_connector: 키움 API 커넥터 인스턴스
        """
        self.kiwoom = kiwoom_connector

        # NXT 전용 시장 코드
        self.NXT_MARKET_CODE = 'NXT'

        logger.info("NXT 종목코드 수집기 초기화 완료")

    def get_nxt_codes(self) -> List[str]:
        """
        NXT 종목코드 수집

        Returns:
            List[str]: NXT 종목코드 리스트
        """
        try:
            logger.info("NXT 종목코드 수집 시작")
            print("🆕 NXT 종목코드 수집 중...")

            # GetCodeListByMarket 함수 호출
            codes_str = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", self.NXT_MARKET_CODE)

            if not codes_str:
                logger.warning("NXT 종목코드 수집 결과 없음")
                print("⚠️ NXT 종목코드 수집 결과가 없습니다.")
                return []

            # 세미콜론으로 구분된 문자열을 리스트로 변환
            raw_codes = [code.strip() for code in codes_str.split(';') if code.strip()]

            # 유효한 종목코드만 필터링 (6자리 숫자)
            valid_codes = [code for code in raw_codes if self._is_valid_stock_code(code)]

            # 결과 출력
            print(f"✅ NXT 원시 데이터: {len(raw_codes)}개")
            print(f"✅ NXT 유효 종목: {len(valid_codes)}개")

            if len(raw_codes) != len(valid_codes):
                invalid_count = len(raw_codes) - len(valid_codes)
                print(f"⚠️ 제외된 종목: {invalid_count}개 (형식 오류)")

            logger.info(f"NXT 종목코드 수집 완료: {len(valid_codes)}개")
            return valid_codes

        except Exception as e:
            logger.error(f"NXT 종목코드 수집 실패: {e}")
            print(f"❌ NXT 종목코드 수집 실패: {e}")
            return []

    def collect_nxt_with_names(self) -> Dict[str, Dict[str, str]]:
        """
        NXT 종목코드와 종목명을 함께 수집

        Returns:
            Dict: {종목코드: {'name': 종목명, 'market': 'NXT'}}
        """
        try:
            print("🆕 NXT 종목코드 + 종목명 수집 중...")

            # 종목코드 수집
            nxt_codes = self.get_nxt_codes()

            if not nxt_codes:
                print("❌ NXT 종목코드가 없어 종목명 수집을 건너뜁니다.")
                return {}

            # 종목명 수집
            print(f"📝 {len(nxt_codes)}개 NXT 종목의 종목명 수집 중...")

            result = {}
            success_count = 0

            for i, code in enumerate(nxt_codes, 1):
                name = self._get_stock_name(code)

                if name:
                    result[code] = {
                        'name': name,
                        'market': 'NXT',
                        'code': code
                    }
                    success_count += 1
                    print(f"   {i:3d}/{len(nxt_codes)} ✅ {code}: {name}")
                else:
                    print(f"   {i:3d}/{len(nxt_codes)} ❌ {code}: 종목명 조회 실패")

            print(f"\n📊 NXT 종목명 수집 결과:")
            print(f"   ✅ 성공: {success_count}개")
            print(f"   ❌ 실패: {len(nxt_codes) - success_count}개")
            print(f"   📈 성공률: {(success_count / len(nxt_codes) * 100):.1f}%")

            return result

        except Exception as e:
            logger.error(f"NXT 종목명 수집 실패: {e}")
            print(f"❌ NXT 종목명 수집 실패: {e}")
            return {}

    def get_nxt_summary(self) -> Dict[str, any]:
        """
        NXT 시장 요약 정보

        Returns:
            Dict: NXT 시장 요약
        """
        try:
            print("📊 NXT 시장 요약 조회 중...")

            # 종목코드 수집
            codes = self.get_nxt_codes()

            if not codes:
                return {
                    'market': 'NXT',
                    'total_count': 0,
                    'valid_codes': [],
                    'collected_at': datetime.now(),
                    'status': 'empty'
                }

            # 샘플 종목명 조회 (처음 5개)
            sample_names = {}
            for code in codes[:5]:
                name = self._get_stock_name(code)
                if name:
                    sample_names[code] = name

            summary = {
                'market': 'NXT',
                'total_count': len(codes),
                'valid_codes': codes,
                'sample_stocks': sample_names,
                'first_code': codes[0] if codes else None,
                'last_code': codes[-1] if codes else None,
                'collected_at': datetime.now(),
                'status': 'success'
            }

            # 결과 출력
            print(f"\n📋 NXT 시장 요약:")
            print(f"   🆕 시장: NXT")
            print(f"   📊 총 종목 수: {len(codes):,}개")
            print(f"   🔢 첫 번째 종목: {codes[0] if codes else 'N/A'}")
            print(f"   🔢 마지막 종목: {codes[-1] if codes else 'N/A'}")

            if sample_names:
                print(f"   📝 샘플 종목:")
                for code, name in sample_names.items():
                    print(f"      {code}: {name}")

            return summary

        except Exception as e:
            logger.error(f"NXT 요약 조회 실패: {e}")
            print(f"❌ NXT 요약 조회 실패: {e}")
            return {
                'market': 'NXT',
                'total_count': 0,
                'error': str(e),
                'status': 'error'
            }

    def export_nxt_codes(self, format_type: str = 'list') -> any:
        """
        NXT 종목코드 내보내기

        Args:
            format_type: 'list', 'dict', 'csv_string' 중 선택

        Returns:
            형식에 따른 데이터
        """
        try:
            codes_data = self.collect_nxt_with_names()

            if not codes_data:
                print("❌ 내보낼 NXT 데이터가 없습니다.")
                return None

            if format_type == 'list':
                # 종목코드 리스트만
                result = list(codes_data.keys())
                print(f"📤 NXT 종목코드 리스트 내보내기: {len(result)}개")
                return result

            elif format_type == 'dict':
                # 전체 딕셔너리
                print(f"📤 NXT 종목 딕셔너리 내보내기: {len(codes_data)}개")
                return codes_data

            elif format_type == 'csv_string':
                # CSV 형태 문자열
                lines = ['code,name,market']
                for code, info in codes_data.items():
                    lines.append(f"{code},{info['name']},NXT")

                csv_string = '\n'.join(lines)
                print(f"📤 NXT CSV 문자열 내보내기: {len(codes_data)}개")
                return csv_string

            else:
                print(f"❌ 지원하지 않는 형식: {format_type}")
                return None

        except Exception as e:
            logger.error(f"NXT 데이터 내보내기 실패: {e}")
            print(f"❌ 내보내기 실패: {e}")
            return None

    def test_nxt_connection(self) -> bool:
        """
        NXT 시장 연결 테스트

        Returns:
            bool: 연결 성공 여부
        """
        try:
            print("🧪 NXT 시장 연결 테스트 중...")

            # API 연결 상태 확인
            if not self.kiwoom:
                print("❌ 키움 커넥터가 없습니다.")
                return False

            # NXT 시장 코드로 테스트 호출
            test_result = self.kiwoom.dynamicCall("GetCodeListByMarket(QString)", self.NXT_MARKET_CODE)

            if test_result and len(test_result) > 0:
                print("✅ NXT 시장 연결 성공!")
                print(f"   반환 데이터 길이: {len(test_result)}자")
                print(f"   샘플 데이터: {test_result[:100]}...")
                return True
            else:
                print("❌ NXT 시장에서 데이터를 받지 못했습니다.")
                print("💡 가능한 원인:")
                print("   - NXT 시장이 현재 운영되지 않음")
                print("   - 시장 코드 'NXT'가 잘못됨")
                print("   - 키움 API 권한 문제")
                return False

        except Exception as e:
            print(f"❌ NXT 연결 테스트 실패: {e}")
            return False

    def _is_valid_stock_code(self, code: str) -> bool:
        """
        유효한 종목코드인지 확인

        Args:
            code: 검증할 종목코드

        Returns:
            bool: 유효성 여부
        """
        try:
            # 6자리 숫자인지 확인
            return len(code) == 6 and code.isdigit()
        except:
            return False

    def _get_stock_name(self, code: str) -> Optional[str]:
        """
        종목코드로 종목명 조회

        Args:
            code: 종목코드

        Returns:
            Optional[str]: 종목명 (실패시 None)
        """
        try:
            if not self.kiwoom or not code:
                return None

            # GetMasterCodeName 함수 호출
            name = self.kiwoom.dynamicCall("GetMasterCodeName(QString)", code)

            if name and name.strip():
                # 종목명 정리 (공백 제거, 특수문자 처리)
                cleaned_name = name.strip()
                return cleaned_name if cleaned_name else None
            else:
                return None

        except Exception as e:
            logger.debug(f"종목명 조회 실패 ({code}): {e}")
            return None

    def show_nxt_samples(self, sample_size: int = 10):
        """
        NXT 종목 샘플 출력

        Args:
            sample_size: 출력할 샘플 개수
        """
        try:
            codes_data = self.collect_nxt_with_names()

            if not codes_data:
                print("❌ 출력할 NXT 종목이 없습니다.")
                return

            codes_list = list(codes_data.keys())
            sample_codes = codes_list[:sample_size]

            print(f"\n🆕 NXT 종목 샘플 (처음 {len(sample_codes)}개):")
            print("─" * 50)

            for i, code in enumerate(sample_codes, 1):
                info = codes_data[code]
                print(f"   {i:2d}. {code} - {info['name']}")

            if len(codes_list) > sample_size:
                print(f"   ... 외 {len(codes_list) - sample_size}개 종목")

        except Exception as e:
            logger.error(f"NXT 샘플 출력 실패: {e}")
            print(f"❌ 샘플 출력 실패: {e}")

    def get_api_status(self) -> Dict[str, any]:
        """
        키움 API 상태 확인

        Returns:
            Dict: API 상태 정보
        """
        try:
            status = {
                'connected': False,
                'nxt_available': False,
                'api_version': None,
                'account_count': 0
            }

            if not self.kiwoom:
                status['error'] = '키움 커넥터 없음'
                return status

            # 기본 연결 상태
            status['connected'] = True

            # NXT 시장 사용 가능 여부
            status['nxt_available'] = self.test_nxt_connection()

            # 계좌 수 확인 (가능한 경우)
            try:
                account_cnt = self.kiwoom.dynamicCall("GetLoginInfo(QString)", "ACCOUNT_CNT")
                status['account_count'] = int(account_cnt) if account_cnt else 0
            except:
                pass

            return status

        except Exception as e:
            logger.error(f"API 상태 확인 실패: {e}")
            return {'error': str(e)}