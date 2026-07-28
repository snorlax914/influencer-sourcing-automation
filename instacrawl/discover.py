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
from pathlib import Path

import instaloader

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

                writer.writerow(row)
                f.flush()  # 중간에 끊겨도 지금까지 결과는 남도록 즉시 기록
                stats["saved"] += 1
                on_row(row)
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
