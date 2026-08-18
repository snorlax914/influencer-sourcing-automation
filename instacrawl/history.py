"""계정 지표의 시점 스냅샷 — 갱신할 때마다 한 줄씩 덧붙인다.

candidates.csv와 reels.csv는 계정당 한 행만 두고 갱신 때 덮어쓴다. '지금 이
계정이 어떤가'를 보기에는 그게 맞지만, 그러면 **팔로워가 늘고 있는 계정인지**를
영영 알 수 없다. 덮어쓴 값은 소급해서 복원할 수 없다.

이게 중요한 이유는 스코어링에 도달·신뢰(ER) 축이 비어 있어서다(judge.py 상단
주석 참고 — 크롤러가 좋아요 수를 모으지 않아 계산할 수 없다). 팔로워 증감은
추가 요청 없이 얻을 수 있는, 그 빈자리를 메울 거의 유일한 신호다. 갱신할 때
어차피 프로필을 다시 읽으므로 비용이 0이다.

이 파일은 **덧붙이기 전용**이다. 지금은 내보내기(export.py)에서 증감 표시에만
쓰지만, 나중에 추이 그래프나 '성장 중' 배지의 재료가 된다.
"""

import csv
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
HISTORY_CSV = BASE_DIR / "history.csv"

HISTORY_FIELDS = [
    "recorded_at",
    "username",
    # 어느 수집 단계가 남긴 줄인지. 단계마다 채우는 칸이 다르므로
    # (크롤은 팔로워, 릴스는 조회수) 빈 칸이 있는 게 정상이다.
    "source",  # "crawl" | "reels"
    "followers",
    "posts",
    "views_median",
]


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def append_snapshot(username: str, source: str, **metrics) -> None:
    """계정 하나의 현재 지표를 한 줄 덧붙인다.

    metrics에는 HISTORY_FIELDS에 있는 이름만 넣는다(나머지는 조용히 무시).
    이력 기록이 실패해도 본 작업(크롤·릴스 수집)까지 같이 죽지는 않게 한다 —
    있으면 좋은 부가 정보이지 수집 결과 자체가 아니다.
    """
    row = {
        "recorded_at": _now(),
        "username": username,
        "source": source,
        **{k: v for k, v in metrics.items() if k in HISTORY_FIELDS},
    }
    try:
        write_header = not HISTORY_CSV.exists()
        with open(HISTORY_CSV, "a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=HISTORY_FIELDS, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerow({k: row.get(k, "") for k in HISTORY_FIELDS})
    except OSError:
        pass


def load_history() -> list[dict]:
    """전체 이력을 기록순으로 읽는다(없으면 빈 목록)."""
    if not HISTORY_CSV.exists():
        return []
    with open(HISTORY_CSV, encoding="utf-8-sig", newline="") as f:
        return [row for row in csv.DictReader(f) if row.get("username")]


def _as_int(value) -> int | None:
    text = str(value or "").strip()
    return int(text) if text.lstrip("-").isdigit() else None


def follower_growth() -> dict[str, dict]:
    """계정별 팔로워 증감. {username: {"delta", "from", "to", "days"}}.

    스냅샷이 2개 이상 쌓인 계정만 나온다. 처음 갱신을 돌리기 전에는 비어 있는
    것이 정상이다 — 비교할 과거가 아직 없다는 뜻이지 오류가 아니다.
    """
    by_user: dict[str, list[tuple[datetime, int]]] = {}
    for row in load_history():
        followers = _as_int(row.get("followers"))
        if followers is None:
            continue  # 릴스 스냅샷 등 팔로워를 안 남긴 줄
        try:
            when = datetime.fromisoformat(row["recorded_at"])
        except (ValueError, KeyError):
            continue
        by_user.setdefault(row["username"], []).append((when, followers))

    out: dict[str, dict] = {}
    for username, points in by_user.items():
        if len(points) < 2:
            continue
        points.sort(key=lambda p: p[0])
        (first_at, first), (last_at, last) = points[0], points[-1]
        out[username] = {
            "delta": last - first,
            "from": first,
            "to": last,
            "days": max((last_at - first_at).days, 0),
        }
    return out
