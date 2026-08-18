"""외국인(관광객) 인플루언서 발굴 파이프라인.

한국 여행/맛집 해시태그 게시물에서 작성자 계정을 모으고,
'외국인 추정 + 팔로워 범위 + 참여율'로 걸러 CSV로 저장한다.

    uv run discover.py          # CLI (기본 설정)
    uv run python webapp.py     # 웹 대시보드에서 설정·실행·결과 조회

설정은 DiscoverConfig 하나로 모아 두었다. CLI는 기본값을 쓰고, 웹은 폼에서
받은 값으로 DiscoverConfig를 만들어 run_discovery()에 넘긴다.

주의:
- 인스타그램은 해시태그 대량 조회를 403/429로 막는다. delay를 넉넉히 두고
  한 번에 소량씩 돌리며 CSV(candidates.csv)에 누적하는 방식을 전제로 한다.
- 이미 CSV에 있는 계정은 건너뛰므로 여러 번 나눠 실행해도 된다.
"""

import csv
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import instaloader

from history import append_snapshot
from session import (
    L,
    LogFn,
    disable_request_logging,
    enable_request_logging,
    login_with_browser_cookies,
    safe,
)

# 모든 데이터 파일은 이 모듈 옆에 둔다. 웹서버는 프로젝트 루트에서 실행될 수도
# 있으므로 cwd에 의존하는 상대경로를 쓰면 CSV를 엉뚱한 곳에 만들게 된다.
BASE_DIR = Path(__file__).parent

# 외국인 관광객이 한국에서 자주 쓰는 해시태그
DEFAULT_HASHTAGS = [
    "visitkorea",
    "koreatravel",
    "koreanfood",
    "seoultravel",
    "koreatrip",
]

CSV_FIELDS = [
    "username",
    "full_name",
    "followers",
    "posts",
    "hangul_ratio",
    "email",
    "external_url",
    "biography",
    "profile_url",
    "captions",  # 최근 게시물 캡션 (CAPTION_SEP로 구분)
    # 갱신 판단용 시각. 이 두 칸이 없으면 '뭐가 오래됐는지'를 알 수 없어
    # 오래된 것부터 골라 다시 크롤하는 게 불가능하다. 값이 비어 있으면
    # 컬럼이 생기기 전에 수집된 행이라는 뜻이고, 갱신 1순위로 취급한다.
    "first_seen_at",
    "updated_at",
]

# 캡션 여러 개를 CSV 한 칸에 담기 위한 구분자. 캡션 본문에 나올 일이 없는 문자열.
CAPTION_SEP = " ||| "
# 캡션 하나당 최대 길이. 인스타 캡션은 해시태그로 수천 자가 되기도 하는데,
# 계정 성격 판단에는 앞부분이면 충분하고 CSV·LLM 입력만 불필요하게 커진다.
CAPTION_MAX_CHARS = 300

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
HANGUL_RE = re.compile(r"[가-힣]")


@dataclass
class DiscoverConfig:
    """한 번의 발굴 실행에 필요한 설정 전부.

    필드 기본값 = 기존 CLI 동작. 웹 대시보드는 이 값들을 폼으로 노출한다.
    """

    # 수집 소스
    seeds: list[str] | None = None  # None이면 seeds.txt에서 읽는다
    expand_from: str = "chuchuperoaichu"  # 이 계정의 팔로잉을 후보로 (비우면 사용 안 함)
    expand_limit: int = 5
    # 인스타그램이 해시태그 조회 API를 막아 현재 자동 해시태그 발굴은 동작하지
    # 않는다(KeyError: medias/more_available). 기본은 seeds 기반으로 두고,
    # 나중에 인스타가 풀리면 True로 켜서 재시도할 수 있게 남겨둔다.
    use_hashtag_discovery: bool = False
    hashtags: list[str] = field(default_factory=lambda: list(DEFAULT_HASHTAGS))
    posts_per_hashtag: int = 5  # 해시태그당 훑을 게시물 수

    # 최근 게시물 캡션 수집.
    # bio만으로는 '한국 여행/음식 콘텐츠를 만드는 계정인가'를 판단하기 어렵다.
    # instaloader는 게시물을 페이지 단위(약 12개)로 받아오고 캡션은 그 응답에
    # 들어 있으므로, 몇 개를 가져오든 추가 요청은 보통 1회다.
    captions_per_account: int = 3  # 0이면 수집 안 함

    # 필터
    min_followers: int = 1000  # 너무 작은 계정 제외
    max_followers: int = 100_000  # 대형 계정은 단가↑·회신율↓ → 마이크로 위주

    # 갱신 — 이미 저장된 계정의 팔로워·소개글·캡션을 다시 받아온다.
    # 대상은 사용자가 화면에서 직접 고른다. 계정 하나에 delay가 두 번
    # (프로필+캡션) 붙어 기본 설정으로 12초씩 걸리므로, 무엇을 갱신할지는
    # 자동으로 정하기보다 고르게 두는 편이 낫다 — 지금 컨택하려는 계정만
    # 최신화하면 되는 경우가 대부분이다.
    refresh_usernames: list[str] = field(default_factory=list)
    # '오래된 계정이 몇 개인가'를 화면에 알려줄 때만 쓰는 기준(갱신 대상 선정과 무관).
    refresh_days: int = 7

    # 실행
    delay: float = 6.0  # 요청 사이 대기(초). 차단 회피용, 줄이지 말 것
    verbose_http: bool = False  # 모든 HTTP 요청/응답코드 출력
    output: str = "candidates.csv"
    seeds_file: str = "seeds.txt"

    @property
    def output_path(self) -> Path:
        return BASE_DIR / self.output

    @property
    def seeds_path(self) -> Path:
        return BASE_DIR / self.seeds_file


class Stopped(Exception):
    """사용자가 중단을 요청했을 때 루프를 빠져나오기 위한 내부 신호."""


# 진행 상황을 받는 콜백들. 웹은 여기서 SSE로 흘려보낸다.
RowFn = Callable[[dict], None]
StopFn = Callable[[], bool]


def hangul_ratio(text: str) -> float:
    """문자열에서 한글이 차지하는 비율(0~1). 문자가 없으면 0."""
    if not text:
        return 0.0
    letters = [c for c in text if c.isalpha()]
    if not letters:
        return 0.0
    hangul = sum(1 for c in letters if HANGUL_RE.match(c))
    return hangul / len(letters)


def parse_seeds(lines: Iterable[str]) -> set[str]:
    """계정명 목록 텍스트를 정규화한다. '@'나 프로필 URL도 허용.

    웹 폼 입력과 seeds.txt가 같은 규칙을 타도록 파싱만 따로 뺐다.
    """
    seeds: set[str] = set()
    for line in lines:
        name = line.strip().lstrip("@")
        if not name or name.startswith("#"):
            continue
        # URL을 넣었으면 마지막 경로 조각만 계정명으로 사용
        name = name.rstrip("/").split("/")[-1].split("?")[0]
        if name:
            seeds.add(name)
    return seeds


def load_seeds(config: DiscoverConfig, log: LogFn = print) -> set[str]:
    """seeds.txt에 수동으로 넣어둔 계정명을 읽는다(한 줄에 하나).

    인스타 앱/웹에서 직접 찾은 계정을 넣으면, 해시태그 자동 수집이
    막혀도 이 계정들은 확실히 보강·점수화된다.
    """
    if config.seeds is not None:  # 웹에서 직접 넘긴 목록이 있으면 파일 대신 사용
        seeds = parse_seeds(config.seeds)
        if seeds:
            log(f"[seeds] 입력받은 계정 {len(seeds)}개")
        return seeds

    if not config.seeds_path.exists():
        return set()
    seeds = parse_seeds(config.seeds_path.read_text(encoding="utf-8").splitlines())
    if seeds:
        log(f"[{config.seeds_file}] 수동 계정 {len(seeds)}개 불러옴")
    return seeds


def _sleep(seconds: float, should_stop: StopFn) -> None:
    """중단 요청에 반응하는 sleep.

    delay가 6초라 통짜로 자면 '중지'를 눌러도 그만큼 늦게 멈춘다.
    0.25초 단위로 쪼개 확인해 바로 반응하게 한다.
    """
    remaining = seconds
    while remaining > 0:
        if should_stop():
            raise Stopped
        step = min(0.25, remaining)
        time.sleep(step)
        remaining -= step


def collect_from_followees(
    seed: str,
    limit: int,
    config: DiscoverConfig,
    log: LogFn,
    should_stop: StopFn,
) -> set[str]:
    """기준 계정(seed)이 팔로우하는 계정들을 최대 limit개 모은다(실험).

    followees 목록은 GraphQL을 사용하므로 인스타가 403으로 막을 수 있다.
    그 경우 예외를 출력하고 빈 집합을 반환한다.
    """
    log(f"[팔로잉 발굴] @{seed} 가 팔로우하는 계정 최대 {limit}개 수집 중...")
    found: set[str] = set()
    try:
        profile = instaloader.Profile.from_username(L.context, seed)
        for i, followee in enumerate(profile.get_followees()):
            if i >= limit:
                break
            name = safe(lambda: followee.username)
            if name:
                found.add(name)
            _sleep(config.delay, should_stop)
    except Stopped:
        raise
    except Exception as e:
        log(f"  팔로잉 수집 실패 [{type(e).__name__}]: {e}")
        log("  → 인스타가 목록 조회를 막은 것입니다. seeds 수동 방식을 쓰세요.")
    log(f"  팔로잉에서 {len(found)}개 수집")
    return found


def collect_usernames(config: DiscoverConfig, log: LogFn, should_stop: StopFn) -> set[str]:
    """seeds + (옵션) 팔로잉/해시태그에서 계정명을 모은다.

    일반 get_posts()는 인스타 API 변경으로 'more_available' KeyError가
    나므로, 페이지네이션이 없는 get_top_posts()를 우선 사용한다.
    """
    found: set[str] = load_seeds(config, log)

    if config.expand_from:
        found |= collect_from_followees(
            config.expand_from, config.expand_limit, config, log, should_stop
        )

    if not config.use_hashtag_discovery:
        return found

    for tag in config.hashtags:
        log(f"[해시태그 #{tag}] 인기 게시물 수집 중...")
        before = len(found)
        try:
            hashtag = instaloader.Hashtag.from_name(L.context, tag)
            for i, post in enumerate(hashtag.get_top_posts()):
                if i >= config.posts_per_hashtag:
                    break
                owner = safe(lambda: post.owner_username)
                if owner:
                    found.add(owner)
                _sleep(config.delay, should_stop)
        except Stopped:
            raise
        except Exception as e:
            log(f"  #{tag} 수집 실패 [{type(e).__name__}]: {e}")
        log(f"  이번 태그에서 +{len(found) - before}개, 누적 {len(found)}개")
    return found


def fetch_captions(profile, limit: int, log: LogFn) -> list[str]:
    """최근 게시물 캡션을 최대 limit개 가져온다.

    get_posts()는 인스타 API 변경으로 'more_available' KeyError가 나기도 한다.
    캡션은 있으면 좋은 보조 정보일 뿐이므로, 실패해도 빈 목록을 주고 넘어간다.
    """
    if limit <= 0:
        return []
    captions: list[str] = []
    try:
        for i, post in enumerate(profile.get_posts()):
            if i >= limit:
                break
            text = safe(lambda: post.caption, "") or ""
            text = " ".join(text.split())  # 줄바꿈·연속 공백 정리
            if text:
                captions.append(text[:CAPTION_MAX_CHARS])
    except Exception as e:
        log(f"    (캡션 조회 실패 [{type(e).__name__}] — 캡션 없이 진행)")
    return captions


def ensure_csv_schema(config: DiscoverConfig, log: LogFn = print) -> None:
    """기존 CSV의 헤더가 현재 CSV_FIELDS와 다르면 파일을 다시 써서 맞춘다.

    컬럼을 추가한 뒤 append 모드로 이어 쓰면, 헤더는 옛 컬럼 수인데 행은 새
    컬럼 수라 파일 전체가 어긋난다. 새 컬럼은 빈 값으로 채워 넣고,
    없어진 컬럼의 값은 버린다.
    """
    path = config.output_path
    if not path.exists():
        return
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames == CSV_FIELDS:
            return  # 이미 최신 스키마
        rows = list(reader)
        old_fields = reader.fieldnames or []

    added = [c for c in CSV_FIELDS if c not in old_fields]
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})
    log(f"[CSV 스키마 갱신] {path.name}: 컬럼 {added} 추가 (기존 {len(rows)}행 유지)")


def load_seen(config: DiscoverConfig) -> set[str]:
    """이미 CSV에 저장된 계정명(중복 방지)."""
    return {row["username"] for row in read_candidates(config)}


def read_candidates(config: DiscoverConfig | None = None) -> list[dict]:
    """저장된 후보 CSV 전체를 dict 목록으로 읽는다(없으면 빈 목록).

    웹 대시보드의 결과 테이블이 이 함수를 쓴다.
    """
    config = config or DiscoverConfig()
    path = config.output_path
    if not path.exists():
        return []
    with open(path, encoding="utf-8-sig", newline="") as f:
        return [row for row in csv.DictReader(f) if row.get("username")]


def now_iso() -> str:
    """수집 시각. reels.csv와 같은 형식(로컬 타임존 포함 ISO)을 쓴다."""
    return datetime.now().astimezone().isoformat(timespec="seconds")


def write_candidates(rows: list[dict], config: DiscoverConfig) -> None:
    """후보 CSV를 통째로 다시 쓴다.

    발굴은 append로 이어 쓰면 되지만 갱신은 **기존 행을 고쳐야** 해서 이어 쓸
    수 없다(그대로 붙이면 같은 계정이 두 줄이 된다). 계정 수가 많아야 수백이라
    전체를 다시 쓰는 편이 부분 수정보다 단순하고 어긋날 여지가 없다.
    reels.py의 save_reel도 같은 이유로 같은 방식을 쓴다.
    """
    with open(config.output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CSV_FIELDS})


def upsert_candidate(row: dict, config: DiscoverConfig) -> None:
    """계정 한 건을 저장한다(있으면 제자리에서 교체, 없으면 뒤에 추가).

    계정마다 파일을 다시 쓰는 게 낭비로 보이지만, 갱신은 계정당 12초씩 걸리는
    작업이라 파일 쓰기는 무시할 수 있는 비용이다. 반대로 한꺼번에 모아 뒀다가
    마지막에 쓰면 중간에 끊겼을 때 그때까지 받아온 것이 전부 날아간다.
    """
    rows = read_candidates(config)
    for i, existing in enumerate(rows):
        if existing["username"] == row["username"]:
            rows[i] = row
            break
    else:
        rows.append(row)
    write_candidates(rows, config)


def _parsed_time(value: str):
    """CSV의 시각 문자열 → datetime. 비었거나 깨졌으면 None."""
    try:
        return datetime.fromisoformat(str(value or "").strip())
    except ValueError:
        return None


def refresh_targets(config: DiscoverConfig) -> tuple[list[dict], list[str]]:
    """고른 계정명 → 후보 CSV의 행. (대상, 후보에 없던 계정명)을 돌려준다.

    갱신은 이미 수집된 계정의 값을 고쳐 쓰는 일이므로, 후보 CSV에 없는 이름은
    갱신할 대상 자체가 없다. 조용히 건너뛰지 않고 돌려줘서 호출부가 알린다.
    """
    by_user = {row["username"]: row for row in read_candidates(config)}
    targets, missing = [], []
    for raw in config.refresh_usernames:
        username = raw.strip().lstrip("@")
        if not username:
            continue
        row = by_user.get(username)
        if row is None:
            missing.append(username)
        else:
            targets.append(row)
    return targets, missing


def stale_candidates(config: DiscoverConfig | None = None) -> list[dict]:
    """오래된 계정을 오래된 순으로 돌려준다(가장 오래된 것이 앞).

    갱신 대상을 정하지는 않는다 — 그건 사용자가 화면에서 고른다. 이 함수는
    '며칠 지난 계정이 몇 개인지'를 알려주는 용도다.

    updated_at이 비어 있는 행 = 시각 컬럼이 생기기 전에 수집된 행이다. 언제
    받아온 값인지 알 수 없으므로 '가장 오래됨'으로 보고 맨 앞에 둔다.
    """
    config = config or DiscoverConfig()
    cutoff = datetime.now().astimezone() - timedelta(days=config.refresh_days)
    stale = []
    for row in read_candidates(config):
        updated = _parsed_time(row.get("updated_at"))
        if updated is None or updated < cutoff:
            stale.append((updated, row))
    # 정렬 키를 (시각을 아는가, 시각)으로 둔다. 시각 미상(None)이 먼저 오고,
    # 두 번째 칸은 같은 그룹끼리만 비교되므로 aware/naive가 섞이지 않는다.
    stale.sort(key=lambda pair: (pair[0] is not None, pair[0] or datetime.min))
    return [row for _updated, row in stale]


def _profile_row(profile, username: str, captions: list[str] | None = None) -> dict:
    """프로필 객체에서 CSV 한 줄을 만든다.

    필드 하나가 403으로 실패해도 나머지는 채워지도록 전부 safe()로 감싼다.
    """
    bio = safe(lambda: profile.biography, "") or ""
    full_name = safe(lambda: profile.full_name, "") or ""
    email_match = EMAIL_RE.search(bio)
    # 한글 비율은 '한국어 계정일 가능성'의 참고 지표로만 컬럼에 넣는다.
    # (자동 제외하지 않음 — 한국어를 쓰는 외국인도 후보에 남긴다.)
    kr = hangul_ratio(f"{full_name} {bio}")
    return {
        "username": username,
        "full_name": full_name,
        "followers": safe(lambda: profile.followers, 0),
        "posts": safe(lambda: profile.mediacount, ""),
        "hangul_ratio": round(kr, 2),
        "email": email_match.group(0) if email_match else "",
        "external_url": safe(lambda: profile.external_url, "") or "",
        "biography": bio.replace("\n", " "),
        "profile_url": f"https://www.instagram.com/{username}/",
        "captions": CAPTION_SEP.join(captions or []),
    }


def fetch_candidate(
    username: str, config: DiscoverConfig, log: LogFn, should_stop: StopFn
) -> dict | None:
    """계정 하나를 조회해 CSV 한 줄을 만든다. 실패하면 None(사유는 로그로).

    발굴과 갱신이 이 함수를 공유한다 — 조회 절차(프로필 → 대기 → 캡션 → 대기)가
    같아야 차단 회피 간격도 같게 유지된다. 시각 컬럼은 호출부가 채운다.
    발굴은 first_seen_at을 새로 찍고, 갱신은 기존 값을 지켜야 하기 때문이다.
    """
    try:
        profile = instaloader.Profile.from_username(L.context, username)
    except instaloader.exceptions.ProfileNotExistsException:
        log(f"  - {username} 건너뜀: 존재하지 않는 계정")
        _sleep(config.delay, should_stop)
        return None
    except Exception as e:
        # 403 등: 실제 원인을 감추지 말고 그대로 출력한다.
        log(f"  - {username} 조회 실패 [{type(e).__name__}]: {e}")
        _sleep(config.delay, should_stop)
        return None

    _sleep(config.delay, should_stop)
    row = _profile_row(profile, username)
    if config.captions_per_account > 0:
        captions = fetch_captions(profile, config.captions_per_account, log)
        row["captions"] = CAPTION_SEP.join(captions)
        _sleep(config.delay, should_stop)
    return row


def run_refresh(
    config: DiscoverConfig | None = None,
    log: LogFn = print,
    on_row: RowFn | None = None,
    should_stop: StopFn | None = None,
) -> dict:
    """이미 저장된 계정의 정보를 다시 받아온다(오래된 것부터 일부만).

    run_discovery와 같은 콜백 규약을 따르므로 웹의 잡 실행기가 그대로 쓴다.

    발굴과 달리 **새 계정을 찾지 않는다.** 팔로워·소개글·캡션은 시간이 지나면
    변하는데, 발굴은 이미 CSV에 있는 계정을 통째로 건너뛰기 때문에(load_seen)
    재실행해도 옛 값이 그대로 남는다. 그 갱신을 이 함수가 맡는다.

    캡션이 새로 들어오면 점수 신호(지역·음식·언어)도 달라진다. 스코어링은
    LLM을 쓰지 않으므로, 갱신 후 judge.rescore_all을 돌리면 API 비용 없이
    점수가 다시 매겨진다.
    """
    config = config or DiscoverConfig()
    should_stop = should_stop or (lambda: False)
    on_row = on_row or (lambda _row: None)
    stats = {"ok": False, "checked": 0, "refreshed": 0, "failed": 0, "stopped": False}

    if config.verbose_http:
        enable_request_logging(log=log)
    else:
        disable_request_logging()

    if not login_with_browser_cookies(log=log):
        log("로그인에 실패해 중단합니다.")
        stats["error"] = "로그인 실패 — instacrawl/.env의 IG_SESSIONID를 확인하세요."
        return stats

    try:
        ensure_csv_schema(config, log)

        targets, missing = refresh_targets(config)
        if missing:
            log(f"후보 목록에 없어 건너뜁니다: {', '.join('@' + m for m in missing)}")
        if not targets:
            log("갱신할 계정이 없습니다. 표에서 계정을 선택한 뒤 다시 실행하세요.")
            stats["ok"] = True
            return stats

        log(f"선택한 계정 {len(targets)}개를 갱신합니다.")
        log(f"예상 소요: 약 {len(targets) * config.delay * 2 / 60:.0f}분\n")

        for old in targets:
            if should_stop():
                raise Stopped
            username = old["username"]
            stats["checked"] += 1
            log(f"[{username}] 갱신 중... ({stats['checked']}/{len(targets)})")

            row = fetch_candidate(username, config, log, should_stop)
            if row is None:
                stats["failed"] += 1
                continue

            # 최초 수집 시각은 갱신해도 유지한다 — '언제 발굴했는가'는 갱신과
            # 무관한 사실이고, 없으면(옛 행) 지금을 최초로 본다.
            row["first_seen_at"] = old.get("first_seen_at") or old.get("updated_at") or now_iso()
            row["updated_at"] = now_iso()
            upsert_candidate(row, config)
            append_snapshot(
                username, "crawl", followers=row["followers"], posts=row["posts"]
            )
            stats["refreshed"] += 1
            on_row(row)

            before = str(old.get("followers") or "").strip()
            after = row["followers"] or 0
            if before.isdigit() and int(before) != after:
                diff = after - int(before)
                log(f"  ✔ 팔로워 {int(before):,} → {after:,} ({diff:+,})")
            else:
                log(f"  ✔ 갱신 완료 (팔로워 {after:,})")

    except Stopped:
        stats["stopped"] = True
        stats["ok"] = True
        log("\n사용자 요청으로 중단했습니다. 지금까지 갱신한 계정은 저장되어 있습니다.")
        return stats
    except Exception as e:
        stats["error"] = f"{type(e).__name__}: {e}"
        log(f"\n예기치 못한 오류로 중단: {type(e).__name__}: {e}")
        return stats

    stats["ok"] = True
    log(f"\n완료. 갱신 {stats['refreshed']}건 / 실패 {stats['failed']}건")
    log("점수를 다시 매기려면 '재채점'을 누르세요(LLM 호출 없음).")
    return stats


def run_discovery(
    config: DiscoverConfig | None = None,
    log: LogFn = print,
    on_row: RowFn | None = None,
    should_stop: StopFn | None = None,
) -> dict:
    """발굴 파이프라인 본체. CLI와 웹이 공유한다.

    log        진행 로그를 받는 콜백 (기본 print)
    on_row     후보 한 건이 저장될 때마다 호출 (웹 테이블 실시간 갱신용)
    should_stop 주기적으로 호출해 True면 중단 (웹 '중지' 버튼용)

    반환: 실행 요약 dict. 예외를 던지지 않고 ok/error로 알린다.
    """
    config = config or DiscoverConfig()
    should_stop = should_stop or (lambda: False)
    on_row = on_row or (lambda _row: None)
    stats = {"ok": False, "checked": 0, "saved": 0, "skipped": 0, "failed": 0, "stopped": False}

    if config.verbose_http:
        enable_request_logging(log=log)
    else:
        disable_request_logging()

    if not login_with_browser_cookies(log=log):
        log("로그인에 실패해 중단합니다.")
        stats["error"] = "로그인 실패 — instacrawl/.env의 IG_SESSIONID를 확인하세요."
        return stats

    try:
        # 컬럼이 늘어난 뒤 옛 CSV에 이어 쓰면 파일이 어긋나므로 먼저 맞춘다.
        ensure_csv_schema(config, log)

        seen = load_seen(config)
        log(f"기존 CSV에 저장된 계정: {len(seen)}개 (건너뜀)")

        usernames = collect_usernames(config, log, should_stop) - seen
        log(f"\n새로 검사할 계정: {len(usernames)}개\n")

        if not usernames:
            log("검사할 계정이 없습니다.")
            log("→ seeds 목록에 인스타 계정명을 한 줄에 하나씩 넣어주세요.")
            log("  (인스타 앱에서 #visitkorea, #koreanfood 등을 검색해 외국인 계정을 찾으면 됩니다.)")
            stats["ok"] = True
            return stats

        # CSV를 append 모드로 열고, 파일이 없으면 헤더를 쓴다.
        write_header = not config.output_path.exists()
        with open(config.output_path, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if write_header:
                writer.writeheader()

            for u in sorted(usernames):
                if should_stop():
                    raise Stopped
                stats["checked"] += 1
                log(f"[{u}] 조회 중... ({stats['checked']}/{len(usernames)})")

                try:
                    profile = instaloader.Profile.from_username(L.context, u)
                except instaloader.exceptions.ProfileNotExistsException:
                    log(f"  - {u} 건너뜀: 존재하지 않는 계정")
                    stats["failed"] += 1
                    _sleep(config.delay, should_stop)
                    continue
                except Exception as e:
                    # 403 등: 실제 원인을 감추지 말고 그대로 출력한다.
                    log(f"  - {u} 조회 실패 [{type(e).__name__}]: {e}")
                    stats["failed"] += 1
                    _sleep(config.delay, should_stop)
                    continue

                _sleep(config.delay, should_stop)

                row = _profile_row(profile, u)
                followers = row["followers"] or 0
                # 팔로워 범위는 명확한 비즈니스 기준이므로 필터로 유지한다.
                if not (config.min_followers <= followers <= config.max_followers):
                    log(
                        f"  - {u} 건너뜀: 팔로워 {followers:,} "
                        f"(허용 범위 {config.min_followers:,}~{config.max_followers:,})"
                    )
                    stats["skipped"] += 1
                    continue

                # 캡션은 팔로워 필터를 통과한 계정만 가져온다.
                # 어차피 버릴 계정에 요청을 더 쓰면 차단 위험만 커진다.
                captions: list[str] = []
                if config.captions_per_account > 0:
                    captions = fetch_captions(profile, config.captions_per_account, log)
                    row["captions"] = CAPTION_SEP.join(captions)
                    _sleep(config.delay, should_stop)

                # 신규 수집이므로 최초·갱신 시각이 같다. 이 값이 있어야
                # 나중에 stale_candidates가 '오래된 것부터'를 고를 수 있다.
                row["first_seen_at"] = row["updated_at"] = now_iso()
                writer.writerow(row)
                f.flush()  # 중간에 끊겨도 지금까지 결과는 남도록 즉시 기록
                stats["saved"] += 1
                on_row(row)
                append_snapshot(u, "crawl", followers=followers, posts=row["posts"])
                log(
                    f"  ✔ 후보 저장: {u} (팔로워 {followers:,}, "
                    f"한글 {row['hangul_ratio']:.0%}, 캡션 {len(captions)}개)"
                )

    except Stopped:
        stats["stopped"] = True
        stats["ok"] = True
        log("\n사용자 요청으로 중단했습니다. 지금까지 결과는 CSV에 저장되어 있습니다.")
        return stats
    except Exception as e:
        stats["error"] = f"{type(e).__name__}: {e}"
        log(f"\n예기치 못한 오류로 중단: {type(e).__name__}: {e}")
        return stats

    stats["ok"] = True
    log(
        f"\n완료. 저장 {stats['saved']}건 / 범위밖 {stats['skipped']}건 / 실패 {stats['failed']}건"
    )
    log(f"결과는 {config.output_path} 에 저장되었습니다.")
    return stats


def main() -> None:
    run_discovery(DiscoverConfig(verbose_http=True))


if __name__ == "__main__":
    main()
