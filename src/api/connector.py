"""
키움증권 OpenAPI 연결 및 관리 모듈
기존 kiwoom.py의 API 연결 부분을 모듈화
"""
import os
import sys
import logging
from typing import Optional, Dict, Any, Callable
from datetime import datetime

from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop, QTimer
from PyQt5.QtWidgets import QApplication
from PyQt5.QtTest import QTest

from ..core.config import Config

# 로거 설정
logger = logging.getLogger(__name__)

class KiwoomAPIConnector(QAxWidget):
    """키움증권 OpenAPI 연결 및 기본 기능 제공 클래스"""

    def __init__(self, config: Optional[Config] = None):
        super().__init__()

        self.config = config or Config()

        # 연결 상태
        self.is_connected = False
        self.account_num = None

        # 이벤트 루프들
        self.login_event_loop = None
        self.tr_event_loop = None

        # TR 요청 관련
        self.tr_data = {}
        self.request_count = 0
        self.last_request_time = None

        # 계좌 정보
        self.account_info = {}

        # 초기화
        self._setup_ocx()
        self._setup_events()

        logger.info("키움 API 커넥터 초기화 완료")

    def _setup_ocx(self):
        """OCX 컨트롤 설정"""
        try:
            self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
            logger.info("키움 OCX 컨트롤 설정 완료")
        except Exception as e:
            logger.error(f"OCX 컨트롤 설정 실패: {e}")
            raise

    def _setup_events(self):
        """이벤트 핸들러 연결"""
        try:
            # 로그인 관련 이벤트
            self.OnEventConnect.connect(self._on_login_event)

            # TR 데이터 수신 이벤트
            self.OnReceiveTrData.connect(self._on_receive_tr_data)

            # 메시지 수신 이벤트
            self.OnReceiveMsg.connect(self._on_receive_msg)

            # 실시간 데이터 수신 이벤트 (추후 확장)
            self.OnReceiveRealData.connect(self._on_receive_real_data)

            logger.info("이벤트 핸들러 연결 완료")
        except Exception as e:
            logger.error(f"이벤트 설정 실패: {e}")
            raise

    def login(self, auto_login: bool = False) -> bool:
        """키움증권 로그인"""
        try:
            logger.info("키움증권 로그인 시작...")

            # 이미 연결되어 있다면 스킵
            if self.is_connected:
                logger.info("이미 로그인되어 있습니다")
                return True

            # 로그인 이벤트 루프 준비
            self.login_event_loop = QEventLoop()

            # 로그인 요청
            self.dynamicCall("CommConnect()")

            # 로그인 완료까지 대기 (최대 30초)
            QTimer.singleShot(30000, self.login_event_loop.quit)
            self.login_event_loop.exec_()

            if self.is_connected:
                self._get_account_info()
                logger.info(f"로그인 성공 - 계좌번호: {self.account_num}")
                return True
            else:
                logger.error("로그인 실패")
                return False

        except Exception as e:
            logger.error(f"로그인 중 오류 발생: {e}")
            return False

    def _on_login_event(self, err_code: int):
        """로그인 이벤트 핸들러"""
        if err_code == 0:
            self.is_connected = True
            logger.info("로그인 성공")
        else:
            self.is_connected = False
            error_msg = self._get_error_message(err_code)
            logger.error(f"로그인 실패: {error_msg}")

        if self.login_event_loop:
            self.login_event_loop.exit()

    def _get_account_info(self):
        """계좌 정보 조회"""
        try:
            # 계좌번호 목록 조회
            account_list = self.dynamicCall("GetLoginInfo(String)", "ACCNO")
            if account_list:
                self.account_num = account_list.split(";")[0]
                logger.info(f"계좌번호 조회 완료: {self.account_num}")

            # 사용자 정보 조회
            user_id = self.dynamicCall("GetLoginInfo(String)", "USER_ID")
            user_name = self.dynamicCall("GetLoginInfo(String)", "USER_NAME")

            self.account_info = {
                "account_num": self.account_num,
                "user_id": user_id,
                "user_name": user_name,
                "login_time": datetime.now()
            }

        except Exception as e:
            logger.error(f"계좌 정보 조회 실패: {e}")

    def logout(self):
        """로그아웃"""
        try:
            if self.is_connected:
                # 실시간 데이터 해제 등 정리 작업
                self._cleanup()
                self.is_connected = False
                logger.info("로그아웃 완료")
        except Exception as e:
            logger.error(f"로그아웃 중 오류: {e}")

    def request_tr_data(self, rq_name: str, tr_code: str,
                       input_data: Dict[str, str],
                       screen_no: str,
                       prev_next: str = "0") -> Optional[Dict[str, Any]]:
        """TR 데이터 요청"""
        try:
            # 요청 제한 체크
            if not self._check_request_limit():
                logger.warning("요청 제한 초과, 대기 중...")
                QTest.qWait(self.config.api_request_delay_ms)

            # 입력 데이터 설정
            for key, value in input_data.items():
                self.dynamicCall("SetInputValue(QString, QString)", key, value)

            # TR 요청
            self.tr_event_loop = QEventLoop()
            self.tr_data.clear()

            ret = self.dynamicCall("CommRqData(QString, QString, int, QString)",
                                 rq_name, tr_code, prev_next, screen_no)
            print(ret)

            if ret == 0:
                # 응답 대기 (최대 10초)
                QTimer.singleShot(10000, self.tr_event_loop.quit)
                self.tr_event_loop.exec_()

                self._update_request_count()
                print(self.tr_data.copy())
                return self.tr_data.copy()


            else:
                error_msg = self._get_error_message(ret)
                logger.error(f"TR 요청 실패: {error_msg}")
                return None

        except Exception as e:
            logger.error(f"TR 데이터 요청 중 오류: {e}")
            return None

    def _on_receive_tr_data(self, screen_no: str, rq_name: str, tr_code: str,
                            record_name: str, prev_next: str):
        """TR 데이터 수신 이벤트 핸들러 - 개선된 버전"""
        try:
            logger.debug(f"TR 데이터 수신: {rq_name} ({tr_code})")
            print(f"🔄 TR 데이터 수신: {rq_name} ({tr_code}) - 레코드명: '{record_name}'")

            # 즉시 실제 데이터 파싱 및 저장
            parsed_data = self._parse_tr_data_immediately(tr_code, record_name, rq_name)

            self.tr_data = {
                "screen_no": screen_no,
                "rq_name": rq_name,
                "tr_code": tr_code,
                "record_name": record_name,
                "prev_next": prev_next,
                "data": parsed_data,  # 실제 파싱된 데이터
                "received_at": datetime.now()
            }

            print(f"✅ TR 데이터 저장 완료: {len(parsed_data.get('raw_data', []))}개 레코드")

        except Exception as e:
            logger.error(f"TR 데이터 처리 중 오류: {e}")
            print(f"❌ TR 데이터 처리 오류: {e}")
        finally:
            if self.tr_event_loop:
                self.tr_event_loop.exit()

    # connector.py 수정 부분 - _parse_tr_data_immediately 메서드 개선

    def _parse_tr_data_immediately(self, tr_code: str, record_name: str, rq_name: str) -> Dict[str, Any]:
        """TR 데이터 즉시 파싱 (이벤트 핸들러에서 호출) - 동적 필드 처리"""
        try:
            print(f"🔍 즉시 파싱 시작: TR={tr_code}, 레코드명='{record_name}'")

            # 🆕 tr_codes.py에서 동적으로 필드 정보 가져오기
            try:
                from ..api.tr_codes import get_tr_info, get_output_fields
                tr_info = get_tr_info(tr_code)
                fields = list(get_output_fields(tr_code).keys())
                print(f"🔧 동적 필드 로드: {tr_code} -> {len(fields)}개 필드")
                print(f"📋 필드 목록: {fields}")
            except Exception as e:
                print(f"⚠️ tr_codes.py에서 필드 로드 실패: {e}")
                # 기존 하드코딩 필드로 폴백
                fields = self._get_fallback_fields(tr_code)
                print(f"🔄 폴백 필드 사용: {fields}")

            # TR별 처리 방식 결정
            is_single_record = tr_code.lower() in ['opt10001']  # 단일 레코드 TR들

            if is_single_record:
                return self._parse_single_record_data(tr_code, record_name, rq_name, fields)
            else:
                return self._parse_multi_record_data(tr_code, record_name, rq_name, fields)

        except Exception as e:
            print(f"❌ 즉시 파싱 오류: {e}")
            import traceback
            print(f"스택 트레이스: {traceback.format_exc()}")
            return {
                "tr_code": tr_code,
                "record_name": record_name,
                "repeat_count": 0,
                "raw_data": [],
                "parsed": False,
                "error": str(e)
            }

    def _get_fallback_fields(self, tr_code: str) -> list:
        """tr_codes.py 로드 실패 시 사용할 폴백 필드들"""
        fallback_fields = {
            'opt10001': ["종목명", "현재가", "전일대비", "등락률", "거래량", "시가", "고가", "저가",
                         "상한가", "하한가", "시가총액", "시가총액규모", "상장주수", "PER", "PBR"],
            'opt10081': ["일자", "현재가", "거래량", "거래대금", "시가", "고가", "저가", "전일종가"],
            'opt10060': ["일자", "개인투자자", "외국인투자자", "기관계", "금융투자", "보험", "투신"],
            'opt10014': ["일자", "차익매수", "차익매도", "비차익매수", "비차익매도", "전체매수", "전체매도"],
            'opt10080': ["시간", "시가", "고가", "저가", "현재가", "거래량", "거래대금"]
        }

        return fallback_fields.get(tr_code.lower(), ["일자", "현재가", "거래량"])

    def _parse_single_record_data(self, tr_code: str, record_name: str, rq_name: str, fields: list) -> Dict[str, Any]:
        """단일 레코드 데이터 파싱 (OPT10001 등)"""
        print(f"🔍 {tr_code} 단일 레코드 파싱 모드")

        # 가능한 레코드명들
        possible_records = [record_name, "", rq_name, tr_code]

        row_data = {}
        used_record = None
        found_data = None

        # 첫 번째 필드로 레코드명 테스트
        first_field = fields[0] if fields else "종목명"

        for test_record in possible_records:
            try:
                print(f"🔍 '{test_record}' 레코드명으로 '{first_field}' 필드 테스트 중...")

                test_value = self.dynamicCall("GetCommData(QString, QString, int, QString)",
                                              tr_code, test_record, 0, first_field)

                print(f"🔍 '{test_record}' {first_field} 결과: '{test_value}'")

                if test_value and test_value.strip():
                    used_record = test_record
                    found_data = test_value.strip()
                    print(f"✅ 사용할 레코드명: '{used_record}' ({first_field}: '{found_data}')")
                    break
                else:
                    print(f"❌ '{test_record}': {first_field} 데이터 없음")

            except Exception as e:
                print(f"❌ '{test_record}' 오류: {e}")
                continue

        # 대안 필드들로 재시도
        if not used_record and len(fields) > 1:
            print(f"🔧 다른 필드들로 재시도...")
            for test_record in possible_records:
                for field in fields[1:4]:  # 처음 몇 개 필드만 시도
                    try:
                        test_value = self.dynamicCall("GetCommData(QString, QString, int, QString)",
                                                      tr_code, test_record, 0, field)

                        if test_value and test_value.strip():
                            used_record = test_record
                            found_data = test_value.strip()
                            print(f"✅ 대안 필드로 발견: '{test_record}' + '{field}' = '{found_data}'")
                            break
                    except Exception:
                        continue
                if used_record:
                    break

        if not used_record:
            print(f"⚠️ {tr_code}: 데이터를 찾을 수 없음 (상장폐지/거래정지 종목일 수 있음)")
            return {
                "tr_code": tr_code,
                "record_name": record_name,
                "repeat_count": 0,
                "raw_data": [],
                "parsed": False,
                "error": "데이터 없음 (비활성 종목 추정)"
            }

        print(f"✅ 데이터 추출 시작 - 레코드명: '{used_record}'")

        # 모든 필드 데이터 추출
        for field in fields:
            try:
                value = self.dynamicCall("GetCommData(QString, QString, int, QString)",
                                         tr_code, used_record, 0, field)
                row_data[field] = value.strip() if value else ""
            except Exception as e:
                print(f"⚠️ 필드 '{field}' 추출 실패: {e}")
                row_data[field] = ""

        # 기본 필드 확인 (TR별로 다름)
        required_field = fields[0] if fields else "종목명"
        if not row_data.get(required_field):
            print(f"❌ 필수 데이터({required_field}) 없음")
            return {
                "tr_code": tr_code,
                "record_name": used_record,
                "repeat_count": 0,
                "raw_data": [],
                "parsed": False,
                "error": "필수 데이터 없음"
            }

        print(
            f"✅ {tr_code} 파싱 완료: {row_data.get(required_field, 'N/A')} - {row_data.get(fields[1] if len(fields) > 1 else 'N/A', 'N/A')}")

        return {
            "tr_code": tr_code,
            "record_name": used_record,
            "repeat_count": 1,
            "raw_data": [row_data],
            "parsed": True,
            "extracted_at": datetime.now()
        }

    def _parse_multi_record_data(self, tr_code: str, record_name: str, rq_name: str, fields: list) -> Dict[str, Any]:
        """다중 레코드 데이터 파싱 (OPT10081, OPT10060, OPT10014, OPT10080 등)"""
        print(f"🔍 {tr_code} 다중 레코드 파싱 모드")

        # 여러 레코드명으로 시도
        possible_records = [record_name, "", rq_name, tr_code]

        repeat_cnt = 0
        used_record = None

        # GetRepeatCnt로 개수 확인
        for test_record in possible_records:
            try:
                cnt = self.dynamicCall("GetRepeatCnt(QString, QString)", tr_code, test_record)
                print(f"레코드명 '{test_record}' 시도: {cnt}개")

                if cnt > 0:
                    repeat_cnt = cnt
                    used_record = test_record
                    break
            except Exception as e:
                print(f"레코드명 '{test_record}' 오류: {e}")
                continue

        # 직접 데이터 접근으로 개수 확인 (GetRepeatCnt 실패 시)
        if repeat_cnt == 0:
            print("❌ GetRepeatCnt로 데이터 개수 확인 실패")
            print("🔄 직접 데이터 접근으로 개수 확인 시도...")

            first_field = fields[0] if fields else "일자"

            for test_record in possible_records:
                try:
                    # 최대 1000개까지 확인
                    for i in range(1000):
                        test_data = self.dynamicCall("GetCommData(QString, QString, int, QString)",
                                                     tr_code, test_record, i, first_field)
                        if test_data and test_data.strip():
                            repeat_cnt = i + 1
                        else:
                            break

                    if repeat_cnt > 0:
                        used_record = test_record
                        print(f"✅ 직접 접근으로 확인: {repeat_cnt}개 (레코드명: '{test_record}')")
                        break

                except Exception:
                    continue

        if repeat_cnt == 0:
            print("❌ 모든 방법으로 데이터 확인 실패")
            return {
                "tr_code": tr_code,
                "record_name": record_name,
                "repeat_count": 0,
                "raw_data": [],
                "parsed": False,
                "error": "데이터 개수 확인 실패"
            }

        print(f"✅ 데이터 개수 확인: {repeat_cnt}개 (레코드명: '{used_record}')")

        # 실제 데이터 추출
        raw_data = []

        for i in range(repeat_cnt):
            try:
                row_data = {}

                for field in fields:
                    try:
                        value = self.dynamicCall("GetCommData(QString, QString, int, QString)",
                                                 tr_code, used_record, i, field)
                        row_data[field] = value.strip() if value else ""
                    except Exception as e:
                        print(f"⚠️ 필드 '{field}' 추출 실패 (행 {i}): {e}")
                        row_data[field] = ""

                # 필수 데이터 확인 (첫 번째 필드가 있으면 유효한 데이터로 간주)
                first_field = fields[0] if fields else "일자"
                if row_data.get(first_field):
                    raw_data.append(row_data)

            except Exception as e:
                print(f"데이터 추출 오류 {i}: {e}")
                continue

        print(f"✅ 실제 데이터 추출 완료: {len(raw_data)}개")

        # 첫 번째 데이터 샘플 출력
        if raw_data and len(fields) >= 2:
            sample = raw_data[0]
            field1 = fields[0]
            field2 = fields[1] if len(fields) > 1 else fields[0]
            print(f"📊 샘플 데이터: {field1}={sample.get(field1)}, {field2}={sample.get(field2)}")

        return {
            "tr_code": tr_code,
            "record_name": used_record,
            "repeat_count": len(raw_data),
            "raw_data": raw_data,
            "parsed": True,
            "extracted_at": datetime.now()
        }

    def get_comm_data(self, tr_code: str, record_name: str, index: int, field_name: str) -> str:
        """TR 데이터에서 특정 필드 값 추출"""
        try:
            data = self.dynamicCall("GetCommData(QString, QString, int, QString)",
                                  tr_code, record_name, index, field_name)
            return data.strip() if data else ""
        except Exception as e:
            logger.error(f"데이터 추출 오류: {e}")
            return ""

    def get_repeat_cnt(self, tr_code: str, record_name: str) -> int:
        """반복 데이터 개수 조회"""
        try:
            return self.dynamicCall("GetRepeatCnt(QString, QString)", tr_code, record_name)
        except Exception as e:
            logger.error(f"반복 카운트 조회 오류: {e}")
            return 0

    def _check_request_limit(self) -> bool:
        """API 요청 제한 체크"""
        current_time = datetime.now()

        # 첫 요청이거나 시간이 충분히 지났으면 허용
        if (self.last_request_time is None or
            (current_time - self.last_request_time).total_seconds() >=
            self.config.api_request_delay_ms / 1000):
            return True

        return False

    def _update_request_count(self):
        """요청 카운트 업데이트"""
        self.request_count += 1
        self.last_request_time = datetime.now()

        if self.request_count % 100 == 0:
            logger.info(f"API 요청 수: {self.request_count}")

    def _on_receive_msg(self, screen_no: str, rq_name: str, tr_code: str, msg: str):
        """메시지 수신 이벤트 핸들러"""
        logger.info(f"키움 메시지: {msg} (화면: {screen_no}, 요청: {rq_name})")

    def _on_receive_real_data(self, code: str, real_type: str, real_data: str):
        """실시간 데이터 수신 이벤트 핸들러 (기본 구현)"""
        # 실시간 데이터는 별도 모듈에서 구현
        logger.debug(f"실시간 데이터 수신: {code} ({real_type})")

    def _get_error_message(self, err_code: int) -> str:
        """에러 코드를 메시지로 변환"""
        error_messages = {
            0: "정상처리",
            -10: "실패",
            -100: "사용자정보교환실패",
            -101: "서버접속실패",
            -102: "버전처리실패",
            -103: "개인방화벽실패",
            -104: "메모리보호실패",
            -105: "함수입력값오류",
            -106: "통신연결종료",
            -200: "시세조회과부하",
            -201: "전문작성초기화실패",
            -202: "전문작성입력값오류",
            -203: "데이터없음",
            -204: "조회가능한종목수초과",
            -205: "데이터수신실패",
            -206: "조회가능한FID수초과",
            -207: "실시간해제오류",
            -300: "입력값오류",
            -301: "계좌비밀번호없음",
            -302: "타인계좌사용오류",
            -303: "주문가격이20억원을초과",
            -304: "주문가격이50억원을초과",
            -305: "주문수량이총발행주수의1%초과오류",
            -306: "주문수량이총발행주수의3%초과오류",
            -307: "주문전송실패",
            -308: "주문전송과부하",
            -309: "주문수량300계약초과",
            -310: "주문수량500계약초과",
            -340: "계좌정보없음",
            -500: "종목코드없음"
        }

        return error_messages.get(err_code, f"알 수 없는 오류: {err_code}")

    def _cleanup(self):
        """리소스 정리"""
        try:
            # 실시간 데이터 해제
            # 이벤트 루프 정리
            if self.login_event_loop:
                self.login_event_loop = None
            if self.tr_event_loop:
                self.tr_event_loop = None

            logger.info("리소스 정리 완료")
        except Exception as e:
            logger.error(f"리소스 정리 중 오류: {e}")

    def get_connection_status(self) -> Dict[str, Any]:
        """연결 상태 정보 반환"""
        return {
            "is_connected": self.is_connected,
            "account_num": self.account_num,
            "request_count": self.request_count,
            "last_request_time": self.last_request_time,
            "account_info": self.account_info.copy()
        }

    def __del__(self):
        """소멸자"""
        try:
            self.logout()
            self._cleanup()
        except:
            pass

class KiwoomAPIManager:
    """키움 API 매니저 (싱글톤 패턴)"""

    _instance = None
    _connector = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._connector is None:
            # QApplication 확인 및 생성
            if not QApplication.instance():
                self.app = QApplication(sys.argv)
            else:
                self.app = QApplication.instance()

    def get_connector(self, config: Optional[Config] = None) -> KiwoomAPIConnector:
        """커넥터 인스턴스 반환"""
        if self._connector is None:
            self._connector = KiwoomAPIConnector(config)
        return self._connector

    def disconnect(self):
        """연결 해제"""
        if self._connector:
            self._connector.logout()
            self._connector = None

# 편의 함수들
def get_kiwoom_connector(config: Optional[Config] = None) -> KiwoomAPIConnector:
    """키움 API 커넥터 인스턴스 반환"""
    manager = KiwoomAPIManager()
    return manager.get_connector(config)

def create_kiwoom_session(auto_login: bool = True, config: Optional[Config] = None) -> Optional[KiwoomAPIConnector]:
    """키움 API 세션 생성 및 로그인"""
    try:
        connector = get_kiwoom_connector(config)

        if auto_login and not connector.is_connected:
            if connector.login():
                logger.info("키움 API 세션 생성 완료")
                return connector
            else:
                logger.error("키움 API 로그인 실패")
                return None

        return connector

    except Exception as e:
        logger.error(f"키움 API 세션 생성 실패: {e}")
        return None