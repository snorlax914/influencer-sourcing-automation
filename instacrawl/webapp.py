"""discover.py를 웹에서 돌리기 위한 대시보드 서버.

    uv run python webapp.py      →  http://127.0.0.1:8000

발굴 작업은 계정당 delay(기본 6초)씩 쉬어가며 수 분~수십 분이 걸린다.
그래서 HTTP 요청 안에서 처리하지 않고, 백그라운드 스레드 잡으로 돌리면서
로그와 결과 행을 SSE로 브라우저에 흘려보낸다.

동시 실행은 막는다. instaloader 세션(L)이 프로세스 전역 하나뿐이라 잡을
병렬로 돌리면 쿠키/요청 로깅이 서로 섞이고, 인스타 차단 위험도 커진다.
"""

import json
import os
import threading
from collections import deque
from dataclasses import asdict
from pathlib import Path
from queue import Empty, Queue

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from discover import (
    BASE_DIR,
    DEFAULT_HASHTAGS,
    DiscoverConfig,
    parse_seeds,
    read_candidates,
    run_discovery,
)
from judge import (
    DEFAULT_PRESET,
    MODEL_PRESETS,
    JudgeConfig,
    list_local_models,
    load_verdicts,
    run_judging,
    score_breakdown,
)

STATIC_DIR = BASE_DIR / "static"
# 새로고침해도 지난 로그를 볼 수 있도록 잡별로 보관하는 최대 줄 수.
# 상세 HTTP 로그를 켜면 줄이 빠르게 늘어나므로 상한을 둔다.
MAX_LOG_LINES = 5000


class Job:
    """실행 중이거나 방금 끝난 발굴 작업 하나.

    로그는 두 곳에 쌓는다.
      - lines: 뒤늦게 접속한 브라우저에게 한 번에 돌려줄 히스토리
      - subscribers: 지금 붙어 있는 SSE 연결마다 하나씩 있는 큐
    둘 다 lock 안에서 함께 갱신해야 '히스토리를 읽는 사이에 들어온 줄'이
    누락되지 않는다.
    """

    def __init__(self, runner, config, kind: str):
        # runner는 (config, log, on_row, should_stop) -> stats 규약을 따르는 함수.
        # run_discovery와 run_judging이 같은 규약이라 잡 관리를 공유한다.
        self.runner = runner
        self.config = config
        self.kind = kind  # "discover" | "judge" — UI가 어느 패널이 도는지 안다
        self.state = "running"  # running | done | error | stopped
        self.lines: deque[str] = deque(maxlen=MAX_LOG_LINES)
        self.rows: list[dict] = []
        self.stats: dict = {}
        self.subscribers: list[Queue] = []
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._run, daemon=True)

    # ── 이벤트 발행 ────────────────────────────────────
    def _publish(self, event: dict) -> None:
        with self.lock:
            if event["type"] == "log":
                self.lines.append(event["data"])
            elif event["type"] == "row":
                self.rows.append(event["data"])
            for q in self.subscribers:
                q.put(event)

    def log(self, message: str) -> None:
        # run_discovery는 "\n새로 검사할..." 처럼 줄바꿈을 섞어 보낸다.
        # 브라우저에서 한 줄씩 다루기 쉽도록 여기서 분리해 발행한다.
        for line in str(message).split("\n"):
            self._publish({"type": "log", "data": line})

    def add_row(self, row: dict) -> None:
        self._publish({"type": "row", "data": row})

    def set_state(self, state: str) -> None:
        with self.lock:
            self.state = state
        self._publish({"type": "state", "data": state, "stats": self.stats})

    # ── 실행 ───────────────────────────────────────────
    def start(self) -> None:
        self.thread.start()

    def _run(self) -> None:
        state = "error"  # finally에서 항상 참조되므로 미리 묶어둔다
        try:
            self.stats = self.runner(
                self.config,
                log=self.log,
                on_row=self.add_row,
                should_stop=self.stop_event.is_set,
            )
            if self.stats.get("stopped"):
                state = "stopped"
            elif self.stats.get("ok"):
                state = "done"
            else:
                state = "error"
        except Exception as e:  # 잡 스레드가 조용히 죽는 것만은 막는다
            self.stats = {"ok": False, "error": f"{type(e).__name__}: {e}"}
            self.log(f"서버 내부 오류: {type(e).__name__}: {e}")
            state = "error"
        finally:
            self.set_state(state)
            # 붙어 있는 SSE 연결들에게 '끝났다'는 신호(None)를 보내 닫게 한다.
            with self.lock:
                for q in self.subscribers:
                    q.put(None)
                self.subscribers.clear()

    def subscribe(self) -> tuple[list[str], list[dict], str, Queue | None]:
        """현재 히스토리 스냅샷과, 실행 중이면 이후 이벤트를 받을 큐를 준다."""
        with self.lock:
            history, rows, state = list(self.lines), list(self.rows), self.state
            if state != "running":
                return history, rows, state, None
            q: Queue = Queue()
            self.subscribers.append(q)
            return history, rows, state, q

    def unsubscribe(self, q: Queue) -> None:
        with self.lock:
            if q in self.subscribers:
                self.subscribers.remove(q)


# 프로세스당 잡 하나. job_lock은 '동시에 두 개 시작' 경합만 막는다.
current_job: Job | None = None
job_lock = threading.Lock()

app = FastAPI(title="인플루언서 발굴 대시보드")


class RunRequest(BaseModel):
    """대시보드 폼 → DiscoverConfig. 값 범위는 여기서 한 번에 검증한다."""

    seeds: str = ""  # 줄바꿈으로 구분된 계정명/URL
    expand_from: str = ""
    expand_limit: int = Field(5, ge=0, le=200)
    use_hashtag_discovery: bool = False
    hashtags: list[str] = Field(default_factory=lambda: list(DEFAULT_HASHTAGS))
    posts_per_hashtag: int = Field(5, ge=1, le=50)
    # 캡션은 계정당 요청을 1회 더 쓴다. 상한 12는 instaloader가 한 페이지로
    # 받아오는 게시물 수로, 그 이상은 추가 요청이 발생해 차단 위험이 커진다.
    captions_per_account: int = Field(3, ge=0, le=12)
    min_followers: int = Field(1000, ge=0)
    max_followers: int = Field(100_000, ge=1)
    # delay 하한 1초: 0으로 두면 인스타에 즉시 차단당해 계정이 위험해진다.
    delay: float = Field(6.0, ge=1.0, le=60.0)
    verbose_http: bool = False

    def to_config(self) -> DiscoverConfig:
        if self.min_followers > self.max_followers:
            raise HTTPException(400, "최소 팔로워가 최대 팔로워보다 큽니다.")
        return DiscoverConfig(
            seeds=self.seeds.splitlines(),
            expand_from=self.expand_from.strip().lstrip("@"),
            expand_limit=self.expand_limit,
            use_hashtag_discovery=self.use_hashtag_discovery,
            hashtags=[t.strip().lstrip("#") for t in self.hashtags if t.strip()],
            posts_per_hashtag=self.posts_per_hashtag,
            captions_per_account=self.captions_per_account,
            min_followers=self.min_followers,
            max_followers=self.max_followers,
            delay=self.delay,
            verbose_http=self.verbose_http,
        )


class SeedsRequest(BaseModel):
    seeds: str


class JudgeRequest(BaseModel):
    """LLM 판정 설정."""

    preset: str = DEFAULT_PRESET
    model: str = ""  # 비우면 프리셋 기본 모델
    # 판정은 인스타를 건드리지 않아 차단 위험이 없으므로 병렬로 돌린다.
    # 상한 8은 API 레이트리밋에 부딪히지 않을 정도의 보수적인 값.
    concurrency: int = Field(4, ge=1, le=8)
    rejudge: bool = False  # 이미 판정한 계정도 다시 판정
    max_accounts: int = Field(0, ge=0)  # 0이면 전체

    def to_config(self) -> JudgeConfig:
        if self.preset not in {p.key for p in MODEL_PRESETS}:
            raise HTTPException(400, f"알 수 없는 모델 선택: {self.preset}")
        return JudgeConfig(
            preset=self.preset,
            model=self.model,
            concurrency=self.concurrency,
            rejudge=self.rejudge,
            max_accounts=self.max_accounts,
        )


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse((STATIC_DIR / "index.html").read_text(encoding="utf-8"))


@app.get("/api/config")
def get_config() -> dict:
    """폼 초기값 = discover.py의 기본 설정 + 저장된 seeds.txt."""
    defaults = DiscoverConfig()
    seeds_path = defaults.seeds_path
    return {
        "defaults": {k: v for k, v in asdict(defaults).items() if k != "seeds"},
        "seeds": seeds_path.read_text(encoding="utf-8") if seeds_path.exists() else "",
        "seeds_file": str(seeds_path),
        "output_file": str(defaults.output_path),
    }


@app.put("/api/seeds")
def save_seeds(req: SeedsRequest) -> dict:
    """seeds.txt에 저장해 다음 실행/CLI에서도 그대로 쓰이게 한다."""
    path = DiscoverConfig().seeds_path
    path.write_text(req.seeds.rstrip() + "\n", encoding="utf-8")
    return {"saved": len(parse_seeds(req.seeds.splitlines()))}


@app.get("/api/status")
def status() -> dict:
    job = current_job
    return {
        "state": job.state if job else "idle",
        "kind": job.kind if job else "",
        "stats": job.stats if job else {},
        "saved_count": len(read_candidates()),
        "judged_count": len(load_verdicts()),
    }


def _start_job(runner, config, kind: str) -> dict:
    """잡을 하나만 돌리도록 보장하며 시작한다.

    발굴과 판정은 서로 다른 자원을 쓰지만, instaloader 세션과 로그 스트림이
    프로세스 전역 하나뿐이라 동시에 돌리면 출력이 섞인다.
    """
    global current_job
    with job_lock:
        if current_job and current_job.state == "running":
            raise HTTPException(409, "이미 실행 중입니다. 중지 후 다시 시도하세요.")
        current_job = Job(runner, config, kind)
        current_job.start()
    return {"started": True, "kind": kind}


@app.post("/api/run")
def run(req: RunRequest) -> dict:
    config = req.to_config()  # 검증 먼저 (락 잡기 전에 400을 내도록)
    return _start_job(run_discovery, config, "discover")


@app.get("/api/judge/models")
def judge_models() -> dict:
    """고를 수 있는 판정 모델과, 각각이 지금 바로 쓸 수 있는 상태인지.

    키가 없으면 실행 전에 화면에서 미리 알려주려는 목적이다. 키 '값'은
    절대 내보내지 않고 존재 여부만 boolean으로 준다.
    """
    local = list_local_models()
    presets = []
    for p in MODEL_PRESETS:
        ready = (not p.api_key_env) or bool(os.environ.get(p.api_key_env, "").strip())
        if p.key == "local":
            ready = bool(local)  # 키 대신 Ollama가 떠 있는지로 판단
        presets.append(
            {
                "key": p.key,
                "label": p.label,
                "model": p.model,
                "api_key_env": p.api_key_env,
                "note": p.note,
                "ready": ready,
            }
        )
    return {"presets": presets, "default": DEFAULT_PRESET, "local_models": local}


@app.post("/api/judge/run")
def judge_run(req: JudgeRequest) -> dict:
    return _start_job(run_judging, req.to_config(), "judge")


@app.get("/api/verdicts")
def verdicts() -> dict:
    rows = load_verdicts()
    return {"rows": list(rows.values()), "count": len(rows)}


@app.post("/api/stop")
def stop() -> dict:
    job = current_job
    if not job or job.state != "running":
        raise HTTPException(409, "실행 중인 작업이 없습니다.")
    job.stop_event.set()
    job.log("중지 요청을 받았습니다. 진행 중인 요청을 마치고 멈춥니다...")
    return {"stopping": True}


@app.get("/api/stream")
def stream() -> StreamingResponse:
    """SSE: 접속 시점의 로그·결과 히스토리를 먼저 주고, 이후 실시간 전달."""
    job = current_job
    if not job:
        raise HTTPException(404, "아직 실행된 작업이 없습니다.")
    history, rows, state, q = job.subscribe()

    def event_stream():
        yield _sse({"type": "history", "lines": history, "rows": rows, "state": state})
        if q is None:  # 이미 끝난 잡 — 히스토리만 주고 닫는다
            return
        try:
            while True:
                try:
                    event = q.get(timeout=15)
                except Empty:
                    yield ": keepalive\n\n"  # 프록시가 유휴 연결을 끊지 않도록
                    continue
                if event is None:  # 잡 종료 신호
                    return
                yield _sse(event)
        finally:
            job.unsubscribe(q)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


@app.get("/api/candidates")
def candidates() -> dict:
    """후보 + 판정 결과를 계정명으로 합쳐서 준다.

    두 CSV를 따로 두는 이유는 루브릭만 바꿔 재판정할 때 크롤링 결과를
    건드리지 않기 위해서다. 화면에서는 한 테이블로 보는 편이 낫다.
    """
    verdict_by_user = load_verdicts()
    rows = []
    for row in read_candidates():
        verdict = verdict_by_user.get(row["username"], {})
        # 채점된(선별 합격) 계정만 계정별 채점 표를 곁들인다. 신호로 다시 계산하므로
        # CSV에 표를 저장할 필요가 없다.
        breakdown = score_breakdown(verdict) if verdict.get("tier") else None
        rows.append(
            {
                **row,
                "screen": verdict.get("screen", ""),
                "screen_reason": verdict.get("screen_reason", ""),
                "fit": verdict.get("fit", ""),
                "score": verdict.get("score", ""),
                "grade": verdict.get("grade", ""),
                "tier": verdict.get("tier", ""),
                "gate": verdict.get("gate", ""),
                "score_reason": verdict.get("score_reason", ""),
                "reason": verdict.get("reason", ""),
                "breakdown": breakdown,
            }
        )
    return {"rows": rows, "count": len(rows), "judged": len(verdict_by_user)}


@app.get("/api/candidates.csv")
def download_csv() -> FileResponse:
    path: Path = DiscoverConfig().output_path
    if not path.exists():
        raise HTTPException(404, "아직 저장된 후보가 없습니다.")
    return FileResponse(path, media_type="text/csv", filename="candidates.csv")


if __name__ == "__main__":
    # 기본은 로컬 전용. 같은 공유기 안의 다른 기기에서 열어야 하면
    # HOST=0.0.0.0 으로 띄운다(인증이 없으니 신뢰하는 네트워크에서만).
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8000"))
    print(f"대시보드: http://{'127.0.0.1' if host == '0.0.0.0' else host}:{port}")
    if host == "0.0.0.0":
        print("같은 네트워크의 다른 기기에서는 이 PC의 IP 주소로 접속하세요.")
    # reload 없음: 잡이 메모리에 있어 리로드되면 실행 중 작업이 통째로 날아간다.
    uvicorn.run(app, host=host, port=port, log_level="warning")
