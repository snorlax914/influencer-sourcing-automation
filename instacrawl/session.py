"""인스타그램 세션/로그인 공용 모듈.

main.py, discover.py 등에서 공통으로 쓰는 Instaloader 객체와 로그인 헬퍼.
"""
import os
from collections.abc import Callable
from pathlib import Path

import instaloader
from dotenv import load_dotenv

# 진행 상황을 어디로 보낼지 호출자가 정할 수 있게 하는 타입.
# CLI는 기본값 print를, 웹 대시보드는 로그 큐에 넣는 함수를 넘긴다.
LogFn = Callable[[str], None]

# 터미널에서 매번 환경변수를 세팅하지 않아도 되도록, 이 파일 옆의 .env를 읽는다.
# 어느 디렉터리에서 실행하든 같은 파일을 보도록 __file__ 기준 절대경로를 쓴다.
# override=False: 이미 셸에 IG_SESSIONID가 있으면 그쪽을 우선한다(일회성 덮어쓰기 가능).
load_dotenv(Path(__file__).with_name(".env"), override=False)

# 프로젝트 전역에서 공유하는 Instaloader 객체.
# max_connection_attempts=3: 인스타가 간헐적으로 403을 주므로 몇 번은 재시도해
#   프로필 조회 성공률을 높인다(무한정 매달리지는 않도록 3회로 제한).
# quiet=True: 실제 데이터는 정상인데도 뜨는 '403 retrying' 같은 내부 로그를
#   숨겨 우리 출력만 깔끔하게 보이도록 한다.
L = instaloader.Instaloader(max_connection_attempts=3, quiet=True)


def enable_request_logging(log: LogFn = print) -> None:
    """instaloader가 보내는 모든 HTTP 요청의 URL과 응답 코드를 출력한다.

    requests.Session.request를 감싸서, 어떤 요청이 403을 받는지 URL 단위로
    바로 보이게 한다. 로그인 이후에 호출해도 같은 세션 객체를 감싸므로 유효하다.

    log를 매번 다시 받는 이유: 세션 객체는 프로세스 전역(L)이라 한 번만 래핑되는데,
    웹에서는 잡마다 로그 목적지가 달라진다. 래퍼가 참조하는 목적지를 세션에
    저장해두고 호출 때마다 갱신해, 두 번째 잡의 HTTP 로그가 첫 잡의 큐로
    새어 들어가지 않도록 한다.
    """
    session = L.context._session
    session._log_fn = log  # 래퍼가 매 요청 시 여기서 최신 목적지를 읽는다
    if getattr(session, "_logging_enabled", False):
        return  # 중복 래핑 방지
    original_request = session.request

    def logged_request(method, url, *args, **kwargs):
        emit = getattr(session, "_log_fn", print)
        try:
            resp = original_request(method, url, *args, **kwargs)
        except Exception as e:
            emit(f"    [HTTP] {method} {url[:120]} -> 예외 {type(e).__name__}")
            raise
        status = resp.status_code
        mark = "  <== 403!" if status == 403 else ""
        emit(f"    [HTTP] {method} {status} {url[:120]}{mark}")
        return resp

    session.request = logged_request
    session._logging_enabled = True
    log("[요청 로깅 활성화] 모든 HTTP 요청을 출력합니다.")


def disable_request_logging() -> None:
    """요청 로깅 출력을 멈춘다(래퍼는 그대로 두고 목적지만 버린다).

    웹에서 '상세 HTTP 로그' 체크를 끈 채 다음 잡을 돌릴 때, 이전 잡이 켜둔
    래퍼가 계속 로그를 뱉지 않도록 한다.
    """
    L.context._session._log_fn = lambda _msg: None


def safe(getter, fallback=None):
    """필드 하나가 403 등으로 실패해도 전체가 죽지 않도록 감싼다."""
    try:
        return getter()
    except Exception:
        return fallback


def login_with_browser_cookies(log: LogFn = print) -> str | None:
    """브라우저(크롬)에 로그인된 인스타그램 세션 쿠키를 그대로 재사용한다.

    instaloader로 직접 로그인하면 인스타그램이 자동화로 판단해
    "Please wait a few minutes..." 401로 차단하는 경우가 많다.
    평소 쓰는 브라우저의 로그인 세션을 빌려오면 정상 사용자로 취급되어
    차단을 훨씬 덜 받는다.

    우선순위:
      1) IG_SESSIONID (instacrawl/.env 파일 또는 환경변수. 가장 안정적,
         크롬 개발자도구에서 직접 복사해 .env에 붙여넣으면 된다)
      2) 크롬에 저장된 instagram.com 쿠키 자동 로드
    성공하면 로그인된 사용자명을, 실패하면 None을 반환한다.
    """
    session = L.context._session

    # 1) .env/환경변수로 넣은 sessionid 쿠키 (권장, 복호화 이슈가 없음)
    # 복사/붙여넣기 과정에서 딸려오는 공백과 따옴표는 제거한다.
    sessionid = os.environ.get("IG_SESSIONID", "").strip().strip('"').strip("'")
    if sessionid:
        session.cookies.set("sessionid", sessionid, domain=".instagram.com")
        username = L.test_login()
        if username:
            L.context.username = username
            log(f"IG_SESSIONID로 로그인 확인: {username}")
            return username
        log("IG_SESSIONID가 유효하지 않습니다(만료되었을 수 있음).")
        log("→ 크롬 F12 > Application > Cookies > instagram.com 에서 "
            "'sessionid'를 다시 복사해 instacrawl/.env에 넣어주세요.")
        return None

    # 2) 크롬 쿠키 자동 로드
    try:
        import browser_cookie3
    except ImportError:
        log("browser_cookie3가 설치되어 있지 않습니다. `uv sync`를 실행하세요.")
        return None

    try:
        cookiejar = browser_cookie3.chrome(domain_name="instagram.com")
    except Exception as e:  # 크롬 쿠키 복호화 실패 등
        log(f"크롬 쿠키를 읽지 못했습니다 [{type(e).__name__}]: {e}")
        log("→ 대안: 크롬 개발자도구(F12) > Application > Cookies에서 "
            "'sessionid' 값을 복사해 instacrawl/.env의 IG_SESSIONID에 넣어보세요.")
        return None

    session.cookies.update(cookiejar)
    username = L.test_login()
    if username:
        L.context.username = username
        log(f"크롬 세션으로 로그인 확인: {username}")
        return username

    log("크롬에서 인스타그램 쿠키를 찾았지만 로그인 상태가 아닙니다. "
        "크롬에서 인스타그램에 먼저 로그인했는지 확인하세요.")
    return None
