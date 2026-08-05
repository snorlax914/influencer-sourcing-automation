"""릴스 조회수 수집 — 대시보드 버튼과 Claude for Chrome 사이의 다리.

릴스 조회수는 로그인한 브라우저 화면에만 렌더링된다. instaloader로는
안정적으로 못 가져오고(응답에서 자주 빠지고, 비로그인 요청은 바로 막힌다),
서버가 직접 브라우저를 띄우면 인스타 로그인 세션을 하나 더 관리해야 한다.
반면 Claude for Chrome은 사용자가 이미 로그인해 둔 크롬을 그대로 쓴다.

다만 크롬 확장을 조종하는 쪽은 Claude Code이지 이 서버가 아니다. 그래서
서버는 '요청 큐'만 들고 있고, 실제 추출은 Claude Code가 가져가서 한다.

    1. 대시보드 [릴스] 버튼 → POST /api/reels/request      → 큐에 적재
    2. Claude Code → GET /api/reels/queue                  → 할 일 확인
       Claude for Chrome으로 instagram.com/<계정>/reels/ 를 열어 조회수 읽기
    3. Claude Code → POST /api/reels/result                → reels.csv에 저장

큐는 메모리에만 둔다(서버를 껐다 켜면 사라진다). 결과는 CSV라 남는다.
"""

import csv
import re
import statistics
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# discover.py와 같은 이유로 모듈 옆에 파일을 둔다(cwd에 의존하지 않기 위해).
BASE_DIR = Path(__file__).parent
REELS_CSV = BASE_DIR / "reels.csv"

# 조회수 목록을 CSV 한 칸에 담기 위한 구분자.
VIEW_SEP = "|"

REELS_FIELDS = [
    "username",
    "reels_count",  # 실제로 읽어낸 릴스 개수
    "views_median",  # 대표값 — 평균은 한 개의 대박 영상에 끌려간다
    "views_mean",
    "views_max",
    "views_min",
    "views",  # 원본 목록 (VIEW_SEP로 구분, 화면에 뜬 순서 = 최신순)
    # 고정(📌) 릴스는 계정이 맨 위에 박아둔 대표작이라 최신 성과가 아니고,
    # 보통 평소의 10배씩 나온다. 통계에서 빼고 따로 보관한다.
    "views_pinned",
    "collected_at",
    "note",  # 비공개 계정·릴스 없음 등 수집 과정에서 남긴 메모
]

# 인스타는 화면 폭에 따라 "12.3만", "1.2M", "3,456" 등으로 줄여 보여준다.
_MULTIPLIERS = {
    "천": 1_000,
    "만": 10_000,
    "억": 100_000_000,
    "k": 1_000,
    "m": 1_000_000,
    "b": 1_000_000_000,
}
# "조회수 1.2만회" 처럼 앞뒤에 글자가 붙어 와도 첫 숫자 덩어리만 집어낸다.
_VIEW_RE = re.compile(r"(\d[\d,.]*)\s*(천|만|억|[kmb])?", re.IGNORECASE)


def parse_view_count(raw) -> int | None:
    """화면에서 읽은 조회수 문자열을 정수로. 못 읽으면 None.

    Claude for Chrome이 돌려주는 값은 사람이 보는 그대로의 문자열이라
    ("1.2만", "12.3K", "3,456", "조회수 1,234회") 여기서 한 번에 정규화한다.
    """
    if raw is None:
        return None
    if isinstance(raw, bool):  # bool은 int의 하위형이라 먼저 걸러낸다
        return None
    if isinstance(raw, (int, float)):
        return int(raw) if raw >= 0 else None
    match = _VIEW_RE.search(str(raw))
    if not match:
        return None
    number = match.group(1).replace(",", "").rstrip(".")
    try:
        value = float(number)
    except ValueError:
        return None
    value *= _MULTIPLIERS.get((match.group(2) or "").lower(), 1)
    return round(value)


def summarize(views: list[int]) -> dict:
    """조회수 목록 → CSV/표에 넣을 요약 통계."""
    if not views:
        return {
            "reels_count": 0,
            "views_median": "",
            "views_mean": "",
            "views_max": "",
            "views_min": "",
        }
    return {
        "reels_count": len(views),
        "views_median": int(statistics.median(views)),
        "views_mean": round(statistics.fmean(views)),
        "views_max": max(views),
        "views_min": min(views),
    }


def load_reels() -> dict[str, dict]:
    """저장된 릴스 조회수를 {username: row}로 읽는다."""
    if not REELS_CSV.exists():
        return {}
    rows: dict[str, dict] = {}
    with open(REELS_CSV, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            username = (row.get("username") or "").strip()
            if not username:
                continue
            # CSV는 전부 문자열이라, 정렬·계산에 쓰는 수치만 되돌린다.
            for key in ("reels_count", "views_median", "views_mean", "views_max", "views_min"):
                value = str(row.get(key) or "").strip()
                row[key] = int(value) if value.lstrip("-").isdigit() else ""
            for key in ("views", "views_pinned"):
                row[key] = [
                    int(v)
                    for v in str(row.get(key) or "").split(VIEW_SEP)
                    if v.strip().isdigit()
                ]
            rows[username] = row
    return rows


def save_reel(
    username: str, views: list[int], note: str = "", pinned: list[int] | None = None
) -> dict:
    """한 계정의 수집 결과를 저장(있으면 덮어쓰기)하고 저장된 행을 돌려준다.

    같은 계정을 다시 추출하면 최신 결과만 남긴다. 조회수는 계속 오르는 값이라
    이력을 쌓아도 표에서 쓸 일이 없고, 행이 늘면 병합이 번거로워진다.

    pinned(고정 릴스)는 통계에 넣지 않는다 — 계정이 골라 박아둔 대표작이라
    평소 성과가 아니고, 몇 개만 섞여도 중앙값이 자릿수째 달라진다.
    """
    row = {
        "username": username,
        **summarize(views),
        "views": VIEW_SEP.join(str(v) for v in views),
        "views_pinned": VIEW_SEP.join(str(v) for v in (pinned or [])),
        "collected_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "note": note,
    }
    existing = load_reels()
    existing[username] = row
    # 전체 다시 쓰기 — 계정 수가 많아야 수백이라 부분 갱신할 이유가 없다.
    with open(REELS_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REELS_FIELDS)
        writer.writeheader()
        for name, saved in existing.items():
            out = dict(saved)
            out["username"] = name
            for key in ("views", "views_pinned"):
                if isinstance(out.get(key), list):  # load_reels가 되돌려 놓은 것
                    out[key] = VIEW_SEP.join(str(v) for v in out[key])
            writer.writerow({k: out.get(k, "") for k in REELS_FIELDS})
    row["views"] = views
    row["views_pinned"] = list(pinned or [])
    return row


def reels_url(username: str) -> str:
    return f"https://www.instagram.com/{username}/reels/"


@dataclass
class ReelsRequest:
    """대시보드에서 누른 추출 요청 하나."""

    username: str
    state: str = "queued"  # queued | running | error
    requested_at: str = field(
        default_factory=lambda: datetime.now().astimezone().isoformat(timespec="seconds")
    )
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "username": self.username,
            "state": self.state,
            "requested_at": self.requested_at,
            "error": self.error,
            "reels_url": reels_url(self.username),
            "profile_url": f"https://www.instagram.com/{self.username}/",
        }


class ReelsQueue:
    """추출 대기열. 대시보드(쓰기)와 Claude Code(읽기)가 같이 건드리므로 잠근다.

    성공한 요청은 큐에서 빼낸다 — 결과는 reels.csv에 있고, 화면은 거기서 읽는다.
    실패한 요청은 사용자가 이유를 볼 수 있도록 error 상태로 남겨 둔다.
    """

    def __init__(self) -> None:
        self._items: dict[str, ReelsRequest] = {}
        self._lock = threading.Lock()

    def add(self, usernames: list[str]) -> list[dict]:
        added = []
        with self._lock:
            for raw in usernames:
                username = raw.strip().lstrip("@")
                if not username:
                    continue
                item = self._items.get(username)
                # 이미 추출 중인 건 그대로 두고, 실패로 남아 있던 건 재시도로 되살린다.
                if item and item.state == "running":
                    continue
                self._items[username] = ReelsRequest(username)
                added.append(self._items[username].to_dict())
        return added

    def remove(self, username: str) -> bool:
        with self._lock:
            return self._items.pop(username.strip().lstrip("@"), None) is not None

    def clear(self) -> int:
        with self._lock:
            count = len(self._items)
            self._items.clear()
            return count

    def snapshot(self) -> list[dict]:
        """대시보드에 보여줄 큐 전체(대기 + 추출 중 + 실패)."""
        with self._lock:
            return [item.to_dict() for item in self._items.values()]

    def pending(self) -> list[dict]:
        """아직 손대지 않은 요청 = Claude Code가 지금 처리할 것."""
        with self._lock:
            return [i.to_dict() for i in self._items.values() if i.state == "queued"]

    def mark_running(self, username: str) -> dict | None:
        with self._lock:
            item = self._items.get(username.strip().lstrip("@"))
            if not item:
                return None
            item.state = "running"
            item.error = ""
            return item.to_dict()

    def mark_error(self, username: str, error: str) -> dict:
        with self._lock:
            key = username.strip().lstrip("@")
            item = self._items.get(key) or ReelsRequest(key)
            item.state = "error"
            item.error = error
            self._items[key] = item
            return item.to_dict()

    def mark_done(self, username: str) -> None:
        with self._lock:
            self._items.pop(username.strip().lstrip("@"), None)


queue = ReelsQueue()
