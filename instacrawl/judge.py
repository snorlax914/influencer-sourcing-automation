"""수집된 후보 계정을 LLM으로 1차 선별한다.

    uv run python judge.py        # CLI
    (웹 대시보드의 'LLM 판정' 패널에서도 실행)

이 단계는 **최종 판단이 아니라 발굴 단계**다. 목표는 정밀도가 아니라 재현율 —
사람이 눈으로 볼 후보 목록을 좁혀주되, 애매한 계정을 떨어뜨리지 않는 것이다.
그래서 판정 기준을 일부러 널널하게 잡고, 확신이 없으면 남기는 쪽으로 기울인다.

설계 메모:
- 크롤링(discover.py)과 분리되어 있다. 루브릭을 고쳐도 재크롤 없이 다시 돌린다.
- 결과는 candidates.csv를 건드리지 않고 verdicts.csv에 따로 쌓는다.
- 최종 점수·등급은 LLM이 아니라 코드가 계산한다(티어별 배점표). 배점을 바꿔도
  LLM을 다시 호출할 필요가 없고, 왜 그 점수인지 추적할 수 있다.
- 점수는 회의록 스코어링 기준을 따른다: 팔로워 규모(티어)별로 배점을 달리하고,
  도달(릴스 조회수)·지역(부산 우선)·외국인·음식 네 축을 채점한 뒤 지역 게이트로 거른다.
  **도달이 점수의 40~50%로 가장 무겁다** — 자세한 배분과 이유는 아래 스코어링 절 참고.
"""

import csv
import json
import os
import re
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from discover import BASE_DIR, CAPTION_SEP, hangul_ratio, read_candidates
from reels import load_reels
from session import LogFn  # import 시 session이 .env를 읽어 아래 os.environ에 반영된다

# 로컬 Ollama 서버 주소. instacrawl/.env의 OLLAMA_HOST로 덮어쓸 수 있고,
# 없으면 기본 로컬 주소를 쓴다. 원격/다른 포트의 Ollama도 .env만 바꿔 붙는다.
# (session import가 이미 .env를 로드했으므로 여기서 그 값이 보인다.)
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434").strip().rstrip("/")


@dataclass(frozen=True)
class ModelPreset:
    """대시보드에서 고를 수 있는 판정 모델 하나.

    provider가 'anthropic'이면 Anthropic SDK를, 'openai'면 OpenAI SDK를 쓴다.
    OpenAI SDK는 base_url만 바꾸면 Gemini(호환 엔드포인트)와 Ollama(로컬)도
    같은 코드로 호출할 수 있어, 프로바이더 구현은 둘뿐이다.
    """

    key: str
    label: str
    provider: str  # "anthropic" | "openai"
    model: str  # 기본 모델 ID (UI에서 덮어쓸 수 있음)
    api_key_env: str  # 필요한 환경변수 이름 ("" = 불필요)
    base_url: str | None = None
    # 구조화 출력 방식. 로컬 모델은 json_schema를 제대로 못 지키는 경우가 많아
    # json_object로 낮추고, 스키마는 프롬프트로 알려준 뒤 관대하게 파싱한다.
    structured: str = "json_schema"
    note: str = ""


MODEL_PRESETS: list[ModelPreset] = [
    ModelPreset(
        key="claude-opus",
        label="Claude Opus 5 (권장)",
        provider="anthropic",
        model="claude-opus-5",
        api_key_env="ANTHROPIC_API_KEY",
        note="다국어 혼합 소개글 판단이 가장 안정적입니다.",
    ),
    ModelPreset(
        key="claude-sonnet",
        label="Claude Sonnet 5 (저렴)",
        provider="anthropic",
        model="claude-sonnet-5",
        api_key_env="ANTHROPIC_API_KEY",
        note="Opus보다 저렴합니다. 1차 거르기에는 대개 충분합니다.",
    ),
    ModelPreset(
        key="openai",
        label="OpenAI GPT",
        provider="openai",
        model="gpt-4o",
        api_key_env="OPENAI_API_KEY",
        note="모델 ID는 보유한 계정에서 쓸 수 있는 것으로 바꿔 넣으세요.",
    ),
    ModelPreset(
        key="gemini",
        label="Google Gemini",
        provider="openai",
        model="gemini-2.0-flash",
        api_key_env="GEMINI_API_KEY",
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
        note="OpenAI 호환 엔드포인트로 호출합니다. 모델 ID는 바꿔 넣을 수 있습니다.",
    ),
    ModelPreset(
        key="local",
        label="로컬 (Ollama)",
        provider="openai",
        model="",  # 설치된 모델 중에서 고르게 하므로 기본값 없음
        api_key_env="",  # 로컬이라 키 불필요
        base_url=f"{OLLAMA_HOST}/v1",
        structured="json_object",
        note="API 비용이 들지 않습니다. 다국어 판단 품질은 모델 크기에 좌우됩니다.",
    ),
]

PRESETS_BY_KEY = {p.key: p for p in MODEL_PRESETS}
DEFAULT_PRESET = "claude-opus"

VERDICT_FIELDS = [
    "username",
    "screen",
    "screen_reason",
    "tier",
    "grade",
    "gate",
    "caption_lang",
    "bio_lang",
    "busan_ratio",
    "korea_ratio",
    "food_ratio",
    "view_rate",
    "views_abs",
    "fit",
    "score",
    "score_reason",
    "reason",
]

# ── 스코어링 (현황 회의록 스코어링 기준) ──────────────────────
# 팔로워 규모(티어)마다 배점이 다르다. 최종 점수는 LLM이 아니라 아래 표로
# 코드가 계산하므로, 표만 바꾸면 재호출 없이 순위를 다시 매길 수 있다.
#
# 축은 네 개다: 도달(릴스 조회수) / 지역(부산·한국) / 외국인(캡션·바이오) / 음식.
# **도달이 가장 무겁다** — 티어별로 40·45·50점, 즉 점수의 40~50%다. 나머지 세 축을
# 다 합쳐도 도달 하나를 겨우 넘는다. 지역·외국인·음식은 '이 계정이 우리와 맞는가'를
# 보는 반면, 도달은 '실제로 몇 명에게 닿는가'를 본다. 맞는 계정을 찾아도 아무도
# 안 보면 소용이 없으므로, 광고 효과와 가장 직접 연결되는 축에 배점을 몰았다.
# (원본 회의록의 ④도달×신뢰는 ER=좋아요/팔로워였다. 좋아요는 여전히 미수집이고,
#  릴스 조회수가 도달의 직접 측정치라 프록시인 좋아요보다 낫다.)
#
# 앞의 세 축은 크롤 텍스트만으로 항상 채점되지만, 도달 축은 릴스 조회수를
# 추출한 계정에만 있다(reels.py — 사용자의 크롬으로 계정마다 따로 수집한다).
# 그래서 만점이 두 가지다:
#   - 조회수 있음 → 네 축 전부, 티어별 만점 100 (환산 없이 원점수가 곧 점수)
#   - 조회수 없음 → 세 축만, 티어별 만점 60/55/50 → 100점으로 환산
#
# 조회수 없는 계정을 '도달 0점'으로 깎으면 아직 추출하지 않았다는 이유만으로
# 순위에서 밀려난다. 그래서 축을 빼고 남은 축으로 환산한다(= 도달을 '평균'으로
# 가정). 다만 도달이 점수의 절반인 지금, 미측정 계정의 점수는 절반이 가정값이다.
# 그래서 등급을 UNMEASURED_GRADE_CAP(B)까지만 준다 — 순위에는 남기되 '즉시 컨택'
# 판정은 실제로 재본 계정에만 붙인다. 추출해서 도달이 나쁘면 점수는 실제로
# 내려간다 — 재보니 약점이 드러난 것이라 그게 맞다.


def tier_of(followers: int) -> str:
    """팔로워 수 → 규모 티어. 경계는 원본 기준(~1만 / 1만~10만 / 10만+)."""
    if followers < 10_000:
        return "small"
    if followers < 100_000:
        return "mid"
    return "large"


# 각 표: 신호값 → {티어: 점수}. 소형·중형이 같은 항목은 값도 같게 둔다.
#
# 배점 배분(티어별 만점 100):
#          도달   지역   외국인  음식
#   소형     40     25     18     17
#   중형     45     24     17     14
#   대형     50     24     15     11
# 도달(릴스 조회수)이 모든 티어에서 단일 최대 축이다 — 다음으로 큰 축은
# 소형 부산 22, 중형 부산 18, 대형 한국 13이라 격차가 분명하다.
BUSAN_PTS = {  # 지역 ① 부산 콘텐츠 비율
    "over30": {"small": 22, "mid": 18, "large": 11},
    "b15_30": {"small": 16, "mid": 13, "large": 8},
    "b5_15": {"small": 10, "mid": 9, "large": 5},
    "b1_5": {"small": 5, "mid": 4, "large": 3},
    "zero": {"small": 0, "mid": 0, "large": 0},
}
KOREA_PTS = {  # 지역 ② 한국(부산 외) 콘텐츠
    "over30": {"small": 3, "mid": 6, "large": 13},
    "b10_30": {"small": 2, "mid": 4, "large": 9},
    "seoul_small": {"small": 1, "mid": 2, "large": 5},
    "none": {"small": 0, "mid": 0, "large": 0},
}
CAP_LANG_PTS = {  # 외국인 ① 캡션 언어
    "foreign": {"small": 11, "mid": 11, "large": 9},
    "mixed": {"small": 7, "mid": 7, "large": 5},
    "korean": {"small": 0, "mid": 0, "large": 0},
}
BIO_LANG_PTS = {  # 외국인 ② 바이오·계정명 언어
    "foreign": {"small": 7, "mid": 6, "large": 6},
    "mixed": {"small": 4, "mid": 3, "large": 3},
    "korean": {"small": 0, "mid": 0, "large": 0},
}
FOOD_PTS = {  # 음식 — 최근 게시물 중 음식 콘텐츠 비율
    "over50": {"small": 17, "mid": 14, "large": 11},
    "r30_50": {"small": 13, "mid": 11, "large": 8},
    "r15_30": {"small": 9, "mid": 7, "large": 6},
    "r5_15": {"small": 4, "mid": 4, "large": 3},
    "under5": {"small": 0, "mid": 0, "large": 0},
}
# 도달 ① 조회수 배율 = 릴스 조회수 중앙값 / 팔로워.
# '팔로워가 진짜인가, 알고리즘이 밀어주는가'를 본다. 팔로워를 사서 부풀린 계정은
# 규모가 커도 배율이 바닥이라 여기서 걸린다. 릴스는 비팔로워에게도 뿌려지므로
# 배율이 1을 넘는 것이 이상하지 않다(그래서 좋아요 ER과 기준선이 다르다).
VIEW_RATE_PTS = {
    "excellent": {"small": 26, "mid": 24, "large": 22},
    "good": {"small": 19, "mid": 18, "large": 17},
    "normal": {"small": 12, "mid": 12, "large": 11},
    "low": {"small": 5, "mid": 6, "large": 5},
    "poor": {"small": 0, "mid": 0, "large": 0},
}
# 도달 ② 절대 조회수(중앙값) — 실제로 몇 명에게 닿는가. 배율과 달리 규모를 그대로 본다.
# 배점이 대형에 쏠린 건 대형 계정을 쓰는 이유가 곧 절대 도달이기 때문이다.
# 소형이 손해 보지는 않는다 — 소형이 3만을 찍으면 배율도 만점권이라 축 전체가 만점이 된다.
VIEWS_ABS_PTS = {
    "over100k": {"small": 14, "mid": 21, "large": 28},
    "v30k_100k": {"small": 14, "mid": 19, "large": 23},
    "v10k_30k": {"small": 12, "mid": 15, "large": 17},
    "v3k_10k": {"small": 7, "mid": 8, "large": 9},
    "under3k": {"small": 0, "mid": 0, "large": 0},
}

# 조회수 배율의 구간 경계는 티어마다 다르다. 같은 1.0배라도 팔로워 3천 계정에서는
# 평범하고 10만 계정에서는 대단한 수치다 — 경계를 하나로 두면 대형은 전부 바닥
# 구간에, 소형은 전부 상위 구간에 몰려 축이 순위를 못 가른다.
VIEW_RATE_CUTS = {
    #          excellent  good  normal  low   (미만이면 poor)
    "small": (3.0, 1.5, 0.7, 0.3),
    "mid": (1.5, 0.8, 0.4, 0.15),
    "large": (0.8, 0.4, 0.2, 0.08),
}
# 절대 조회수 경계는 티어와 무관하다 — 3만 회는 어느 계정에서든 3만 명이다.
VIEWS_ABS_CUTS = ((100_000, "over100k"), (30_000, "v30k_100k"), (10_000, "v10k_30k"), (3_000, "v3k_10k"))

# 릴스 표본이 이보다 적으면 도달 축을 채점하지 않는다(= 미측정 취급).
# 릴스 1~2개의 중앙값은 그 계정의 평소 성과가 아니라 그날의 운에 가깝고,
# 그걸로 40~50점(점수의 절반)을 움직이면 표본이 적은 계정만 위아래로 튄다.
MIN_REELS_SAMPLE = 3

# 등급 → 대시보드 배지(keep/maybe/drop). A·B는 컨택, C는 예비, D는 보류.
GRADE_TO_FIT = {"A": "keep", "B": "keep", "C": "maybe", "D": "drop"}


def grade_of(score: int) -> str:
    """원본 2.4 랭킹 기준. A 70+, B 50~69, C 35~49, D 35 미만."""
    if score >= 70:
        return "A"
    if score >= 50:
        return "B"
    if score >= 35:
        return "C"
    return "D"


# 조회수를 아직 안 뽑은 계정이 받을 수 있는 최고 등급.
# 도달이 점수의 40~50%를 차지하게 된 이상, 미측정 계정의 점수는 절반이 '평균이라고
# 가정한 값'이다. 그 상태로 A(즉시 컨택)를 주면 실제로는 도달이 바닥인 계정에
# 사람을 보내게 된다. 그래서 A는 조회수를 재본 계정에만 준다 — 화면에서 B로 뜬
# 계정의 조회수를 뽑으면 A로 올라가거나 아래로 떨어지고, 그게 곧 검토 순서가 된다.
UNMEASURED_GRADE_CAP = "B"
_GRADE_ORDER = ["D", "C", "B", "A"]


def passes_region_gate(tier: str, busan_ratio: str, korea_ratio: str) -> bool:
    """원본 2.2 지역 게이트. 통과 못 하면 후보에서 사실상 밀려난다(0점 처리)."""
    has_busan = busan_ratio != "zero"
    if tier == "small":
        return has_busan  # 부산 콘텐츠 1건 이상 필수(한국 일반만으론 탈락)
    if tier == "mid":
        return has_busan or korea_ratio in ("over30", "b10_30")  # 부산 or 한국 3건+
    return has_busan or korea_ratio != "none"  # 대형: 한국 콘텐츠 1건 이상(서울만도 OK)


# 채점 이유(사람이 읽는 점수 내역)에 쓰는 한글 라벨.
TIER_LABEL = {"small": "소형", "mid": "중형", "large": "대형"}
SIGNAL_LABEL = {
    "busan_ratio": "부산",
    "korea_ratio": "한국",
    "caption_lang": "캡션",
    "bio_lang": "바이오",
    "food_ratio": "음식",
    "view_rate": "배율",
    "views_abs": "도달",
}
# 신호값 → 사람이 읽는 라벨(계정별 채점 표의 '신호' 칸에 쓴다).
BUCKET_LABEL = {
    "busan_ratio": {"over30": "30%+", "b15_30": "15~30%", "b5_15": "5~15%", "b1_5": "1~5%", "zero": "없음"},
    "korea_ratio": {"over30": "30%+", "b10_30": "10~30%", "seoul_small": "서울·소량", "none": "없음"},
    "caption_lang": {"foreign": "외국어", "mixed": "혼용", "korean": "한국어"},
    "bio_lang": {"foreign": "외국어", "mixed": "혼용", "korean": "한국어"},
    "food_ratio": {"over50": "50%+", "r30_50": "30~50%", "r15_30": "15~30%", "r5_15": "5~15%", "under5": "5%미만"},
    # 배율 라벨은 구간 경계가 티어마다 달라 숫자를 못 박는다. 대신 티어 안에서의
    # 상대 위치를 말로 쓴다(실제 배율 값은 reason·릴스 패널에 그대로 나온다).
    "view_rate": {
        "excellent": "매우 높음", "good": "높음", "normal": "보통",
        "low": "낮음", "poor": "매우 낮음", "unknown": "미측정",
    },
    "views_abs": {
        "over100k": "10만+", "v30k_100k": "3만~10만", "v10k_30k": "1만~3만",
        "v3k_10k": "3천~1만", "under3k": "3천 미만", "unknown": "미측정",
    },
}
# 채점 표의 행 정의: (표시명, 신호키, 배점표).
# BASE_ROWS는 크롤 텍스트만으로 항상 채점되고, REACH_ROWS는 릴스 조회수를
# 추출한 계정에만 붙는다. 점수·설명·표가 모두 이 정의 하나만 본다.
BASE_ROWS = [
    ("지역 · 부산", "busan_ratio", BUSAN_PTS),
    ("지역 · 한국(부산 외)", "korea_ratio", KOREA_PTS),
    ("외국인 · 캡션", "caption_lang", CAP_LANG_PTS),
    ("외국인 · 바이오", "bio_lang", BIO_LANG_PTS),
    ("음식", "food_ratio", FOOD_PTS),
]
REACH_ROWS = [
    ("도달 · 조회수 배율", "view_rate", VIEW_RATE_PTS),
    ("도달 · 절대 조회수", "views_abs", VIEWS_ABS_PTS),
]
SCORE_ROWS = BASE_ROWS + REACH_ROWS


def _rows_max(rows: list, tier: str) -> int:
    """행 묶음의 티어별 만점. 배점표를 고치면 만점도 따라 움직이도록 계산해서 쓴다."""
    return sum(max(opt.get(tier, 0) for opt in table.values()) for _label, _key, table in rows)


# 티어별 만점. 도달 축이 없으면 나머지 세 축 만점(60/55/50)으로 환산하고,
# 있으면 네 축 만점(100)이라 환산이 곧 항등식이 된다.
TIER_MAX = {tier: _rows_max(BASE_ROWS, tier) for tier in TIER_LABEL}
TIER_MAX_REACH = {tier: _rows_max(SCORE_ROWS, tier) for tier in TIER_LABEL}
# 도달 축 배점(40/45/50)은 네 축 중 가장 무겁다 — 릴스 조회수가 광고 효과와
# 가장 직접 연결되는 지표라서다. 세 축 만점과 합쳐 정확히 100이 되어야
# 등급 임계값(A 70+ 등)이 그대로 의미를 갖는다.
assert set(TIER_MAX_REACH.values()) == {100}, TIER_MAX_REACH


def has_reach(verdict: dict) -> bool:
    """도달 축을 채점할 수 있는 계정인지(릴스 조회수가 충분히 있는지)."""
    return verdict.get("view_rate", "unknown") not in ("", "unknown", None)


def grade_for(verdict: dict, score: int) -> str:
    """점수 → 등급. 조회수 미측정이면 UNMEASURED_GRADE_CAP까지만 올라간다."""
    grade = grade_of(score)
    if has_reach(verdict):
        return grade
    cap = UNMEASURED_GRADE_CAP
    return cap if _GRADE_ORDER.index(grade) > _GRADE_ORDER.index(cap) else grade


def grade_capped(verdict: dict) -> bool:
    """등급이 '조회수 미측정' 때문에 눌렸는지(화면에서 이유를 밝히려고 쓴다)."""
    return not has_reach(verdict) and grade_of(verdict.get("score") or 0) != verdict.get("grade")


def rows_for(verdict: dict) -> list:
    return SCORE_ROWS if has_reach(verdict) else BASE_ROWS


def max_for(verdict: dict) -> int:
    tier = verdict.get("tier") or "small"
    return (TIER_MAX_REACH if has_reach(verdict) else TIER_MAX)[tier]


def _row_points(verdict: dict) -> list[tuple]:
    """(표시명, 신호키, 신호값, 점수, 만점) — 점수·설명·표가 공유하는 단일 계산."""
    tier = verdict.get("tier") or "small"
    out = []
    for label, key, table in rows_for(verdict):
        value = verdict.get(key, "")
        out.append(
            (
                label,
                key,
                value,
                table.get(value, {}).get(tier, 0),
                max(opt.get(tier, 0) for opt in table.values()),
            )
        )
    return out


def explain_score(verdict: dict) -> str:
    """왜 그 점수인지 사람이 읽을 수 있게 항목별 배점을 풀어 쓴다.

    점수는 코드가 배점표로 계산하므로, 각 축이 몇 점을 줬는지 그대로 보여주면
    추적이 된다. 게이트 탈락은 점수가 0인 이유를 따로 밝힌다.
    """
    tier = verdict.get("tier") or "small"
    tier_ko = TIER_LABEL.get(tier, tier)
    if verdict.get("gate") == "fail":
        return f"지역 게이트 탈락({tier_ko}·부산 조건 미달) → 0점 (D)"
    rows = _row_points(verdict)
    raw = sum(pts for _l, _k, _v, pts, _m in rows)
    # 도달 축이 빠진 채점은 만점이 달라 점수의 뜻도 달라진다 — 머리말에 밝혀 둔다.
    head = f"[{tier_ko}]" if has_reach(verdict) else f"[{tier_ko}·조회수 미측정]"
    parts = " + ".join(f"{SIGNAL_LABEL[key]} {pts}" for _l, key, _v, pts, _m in rows)
    # 등급이 눌렸으면 원래 몇 등급이었는지 같이 적는다 — 안 그러면 점수 70+에
    # 등급 B인 행이 계산 실수처럼 보인다.
    grade = verdict.get("grade", "")
    if grade_capped(verdict):
        grade = f"{grade}, 조회수 미측정이라 {grade_of(verdict.get('score') or 0)}→{grade}"
    return (
        f"{head} {parts} = {raw}/{max_for(verdict)}"
        f" → {verdict.get('score', 0)}점 ({grade})"
    )


def score_breakdown(verdict: dict) -> dict:
    """계정별 채점 표에 쓸 구조화된 배점 내역.

    각 축(부산·한국·캡션·바이오·음식·도달)마다 어떤 신호가 몇 점을 줬는지(만점
    대비) 한 행씩 만들고, 합계·환산 점수·게이트 여부를 함께 담는다. 화면에서
    표로 그린다. 조회수를 추출하지 않은 계정은 도달 행 자체가 빠지고,
    reach_measured가 False로 온다(화면이 '아직 안 쟀음'을 구분할 수 있게).
    """
    tier = verdict.get("tier") or "small"
    rows = [
        {
            "dim": label,
            "signal": BUCKET_LABEL.get(key, {}).get(value, value or "-"),
            "points": pts,
            "max": max_pts,
        }
        for label, key, value, pts, max_pts in _row_points(verdict)
    ]
    return {
        "tier": tier,
        "tier_label": TIER_LABEL.get(tier, tier),
        "rows": rows,
        "raw": sum(r["points"] for r in rows),
        "max": max_for(verdict),
        "reach_measured": has_reach(verdict),
        # 등급이 조회수 미측정으로 눌렸으면, 재보면 받을 수 있었던 등급도 같이 준다.
        "grade_capped": grade_capped(verdict),
        "grade_uncapped": grade_of(verdict.get("score") or 0),
        "gate_failed": verdict.get("gate") == "fail",
        "score": verdict.get("score", 0),
        "grade": verdict.get("grade", ""),
    }


# ── 2단계: 스코어링 신호 추출 (LLM 없이 크롤 텍스트로 정량 계산) ──────
# 언어는 스크립트(한글) 비율로, 음식·지역은 다국어 키워드 매칭으로 잰다.
# LLM 추정과 달리 값이 흔들리지 않고, 어떤 키워드가 걸렸는지 추적할 수 있다.

# 부산 지명. 서면·남포동 등 부산 고유 지명 + 'busan/釜山'.
BUSAN_KEYWORDS = [
    "부산", "busan", "釜山", "해운대", "광안리", "광안대교", "서면", "남포동",
    "센텀", "센텀시티", "기장", "송정", "자갈치", "영도", "태종대", "다대포",
    "전포", "부산대", "경성대", "부산역", "국제시장", "감천", "밀락", "오륙도",
    "청사포", "부산불꽃", "부산여행", "부산맛집", "해운대해수욕장",
]
# 부산을 뺀 한국(서울 등) 신호. 일반 '한국' + 다른 도시·명소.
KOREA_KEYWORDS = [
    "한국", "korea", "korean", "韓国", "서울", "seoul", "제주", "jeju", "강남",
    "홍대", "명동", "이태원", "성수", "인천", "대구", "광주", "대전", "경주",
    "강릉", "속초", "전주", "여수", "가평", "수원", "경기", "강원",
]
# 음식 콘텐츠 신호(다국어 + 이모지). 짧고 모호한 토큰(bar 등)은 뺐다.
FOOD_KEYWORDS = [
    # 한국어
    "맛집", "먹방", "음식", "요리", "카페", "커피", "디저트", "브런치", "메뉴",
    "레스토랑", "베이커리", "빵집", "한식", "일식", "중식", "양식", "분식",
    "술집", "와인", "칵테일", "노포", "미식", "존맛", "맛스타그램", "카페투어",
    # 일본어
    "グルメ", "カフェ", "ランチ", "ディナー", "スイーツ", "料理", "美味", "食べ",
    # 영어
    "cafe", "coffee", "brunch", "lunch", "dinner", "restaurant", "dessert",
    "foodie", "yummy", "delicious", "bakery", "noodle", "ramen", "sushi",
    "foodstagram", "eatery",
    # 중국어
    "美食", "咖啡", "餐厅",
    # 이모지
    "🍽", "🍜", "🍣", "🍔", "🍕", "☕", "🍰", "🍚", "🍲", "🥘", "🍛", "🥟",
]


def _has_kw(text: str, keywords: list[str]) -> bool:
    """텍스트에 키워드가 하나라도 들어 있는지(대소문자 무시)."""
    low = text.lower()
    return any(k.lower() in low for k in keywords)


def _lang_bucket(text: str) -> str | None:
    """텍스트의 언어 신호. 한글 비율로 foreign/mixed/korean. 글자 없으면 None.

    한국어 계정은 거의 전부 한글, 일본어·영어·중국어는 한글이 없으므로,
    한글 비율만으로도 '외국어/혼용/한국어'를 충분히 가른다.
    """
    if not any(c.isalpha() for c in text):
        return None
    h = hangul_ratio(text)  # discover.py의 구현 재사용(0~1)
    if h >= 0.7:
        return "korean"
    if h <= 0.2:
        return "foreign"
    return "mixed"


def _busan_bucket(frac: float) -> str:
    if frac >= 0.30:
        return "over30"
    if frac >= 0.15:
        return "b15_30"
    if frac >= 0.05:
        return "b5_15"
    return "b1_5" if frac > 0 else "zero"


def _korea_bucket(frac: float) -> str:
    if frac >= 0.30:
        return "over30"
    if frac >= 0.10:
        return "b10_30"
    return "seoul_small" if frac > 0 else "none"


def _food_bucket(frac: float) -> str:
    if frac > 0.50:
        return "over50"
    if frac >= 0.30:
        return "r30_50"
    if frac >= 0.15:
        return "r15_30"
    if frac >= 0.05:
        return "r5_15"
    return "under5"


def analyze_text_signals(row: dict) -> dict:
    """크롤 텍스트(계정명·소개글·캡션)에서 스코어링 신호를 정량 추출한다.

    - 언어: 소개글/캡션의 한글 비율로 판정
    - 음식: 음식 키워드가 걸린 캡션 비율
    - 지역: 부산/한국 지명이 걸린 캡션 비율(+소개글 거주 언급 보정)
    반환에는 사람이 읽을 근거(reason)도 담는다.
    """
    bio = f"{row.get('full_name', '')} {row.get('biography', '')}".strip()
    caps = [c for c in (row.get("captions") or "").split(CAPTION_SEP) if c.strip()]
    total = len(caps)

    bio_lang = _lang_bucket(bio) or "korean"
    caption_lang = _lang_bucket(" ".join(caps)) or "korean"  # 캡션 없으면 korean(0점)

    if total:
        busan_hits = sum(1 for c in caps if _has_kw(c, BUSAN_KEYWORDS))
        # 부산이 아닌 한국 신호만 센다(부산이 걸린 캡션은 지역②에서 중복 계상 안 함).
        korea_hits = sum(
            1 for c in caps if _has_kw(c, KOREA_KEYWORDS) and not _has_kw(c, BUSAN_KEYWORDS)
        )
        food_hits = sum(1 for c in caps if _has_kw(c, FOOD_KEYWORDS))
        busan_ratio = _busan_bucket(busan_hits / total)
        korea_ratio = _korea_bucket(korea_hits / total)
        food_ratio = _food_bucket(food_hits / total)
    else:
        busan_hits = korea_hits = food_hits = 0
        busan_ratio, korea_ratio, food_ratio = "zero", "none", "under5"

    # 소개글에 부산/한국 거주가 적히면, 캡션에 지명이 없어도 지역 신호로 인정한다.
    # (예: "일본인 in Busan" — 콘텐츠 위치 태그가 없어도 거주가 확실한 경우)
    if _has_kw(bio, BUSAN_KEYWORDS) and busan_ratio == "zero":
        busan_ratio = "b5_15"
    elif korea_ratio == "none" and _has_kw(bio, KOREA_KEYWORDS):
        korea_ratio = "seoul_small"

    lang_ko = {"foreign": "외국어", "mixed": "혼용", "korean": "한국어"}
    if total:
        reason = (
            f"캡션 {total}개 중 부산 {busan_hits}·한국 {korea_hits}·음식 {food_hits} · "
            f"캡션 {lang_ko[caption_lang]}·바이오 {lang_ko[bio_lang]}"
        )
    else:
        reason = f"캡션 없음(소개글만) · 바이오 {lang_ko[bio_lang]}"

    return {
        "caption_lang": caption_lang,
        "bio_lang": bio_lang,
        "busan_ratio": busan_ratio,
        "korea_ratio": korea_ratio,
        "food_ratio": food_ratio,
        "reason": reason,
    }


def _view_rate_bucket(rate: float, tier: str) -> str:
    """조회수 배율 → 구간. 경계가 티어마다 다르므로 티어를 함께 받는다."""
    excellent, good, normal, low = VIEW_RATE_CUTS[tier]
    if rate >= excellent:
        return "excellent"
    if rate >= good:
        return "good"
    if rate >= normal:
        return "normal"
    if rate >= low:
        return "low"
    return "poor"


def _views_abs_bucket(median: int) -> str:
    for cut, name in VIEWS_ABS_CUTS:
        if median >= cut:
            return name
    return "under3k"


def analyze_reels_signals(reel: dict | None, followers: int, tier: str) -> dict:
    """릴스 조회수(reels.csv 한 행) → 도달 축 신호.

    채점하려면 셋 다 있어야 한다: 표본 MIN_REELS_SAMPLE개 이상, 0보다 큰
    조회수 중앙값, 0보다 큰 팔로워 수(배율의 분모). 하나라도 없으면 'unknown'을
    돌려주고, 그러면 도달 축 자체가 채점에서 빠진다(0점이 아니라 미측정).

    reason에는 실제 배율을 숫자로 남긴다 — 구간 이름('높음')만으로는 1.6배인지
    2.9배인지 알 수 없어 계정끼리 비교가 안 된다.
    """
    unmeasured = {"view_rate": "unknown", "views_abs": "unknown", "reels_reason": ""}
    if not reel:
        return unmeasured

    count = reel.get("reels_count") or 0
    median = reel.get("views_median") or 0
    if not isinstance(count, int) or not isinstance(median, int):
        return unmeasured  # CSV에서 빈 칸으로 읽힌 경우("")
    if count < MIN_REELS_SAMPLE or median <= 0:
        note = reel.get("note") or ""
        why = f"릴스 표본 {count}개(최소 {MIN_REELS_SAMPLE}개)" if count else (note or "릴스 없음")
        return {**unmeasured, "reels_reason": f"도달 미측정 — {why}"}
    if followers <= 0:
        return {**unmeasured, "reels_reason": "도달 미측정 — 팔로워 수 없음"}

    rate = median / followers
    return {
        "view_rate": _view_rate_bucket(rate, tier),
        "views_abs": _views_abs_bucket(median),
        "reels_reason": (
            f"릴스 {count}개 중앙값 {median:,}회 · 팔로워 대비 {rate:.2f}배"
        ),
    }

# ── 1단계: 선별(스크리닝) — 유일한 LLM 단계 ──────────────────
# 스코어링은 코드가 키워드/스크립트로 계산하므로, LLM은 여기서만 쓴다:
# '개인 vs 상업', '진짜 외국인 인플루언서인가' 같은 의미 판단은 키워드로 잡기
# 어렵기 때문. 점수는 매기지 않고 통과 여부만 정한다.
SCREEN_SCHEMA = {
    "type": "object",
    "properties": {
        "screen": {
            "type": "string",
            "enum": ["pass", "fail"],
            "description": "스코어링으로 넘길지. 애매하면 pass. fail은 명백히 무관할 때만.",
        },
        "screen_reason": {
            "type": "string",
            "description": "합격/불합격 근거 한 문장(한국어).",
        },
    },
    "required": ["screen", "screen_reason"],
    "additionalProperties": False,
}

# 선별은 점수가 아니라 '이 계정을 채점할 가치가 있는가'만 본다. 관대하게 —
# 애매하면 통과시키고, 명백한 상업·무관 계정만 떨어뜨린다(발굴 단계라 재현율 우선).
SCREEN_RUBRIC = """\
당신은 부산의 외식업체를 위한 인플루언서 발굴에서, 계정을 **채점할 가치가
있는지** 1차로 거릅니다. 점수는 매기지 않습니다 — 통과(pass) 여부만 정하세요.

# 통과 기준
개인이 운영하는 계정이고, 한국(특히 부산) 또는 음식·여행·일상과 조금이라도
접점이 보이면 **pass** 입니다. 확신이 없으면 **pass** 로 두세요.

# 불합격(fail)은 다음처럼 명백할 때만:
- 개인이 아닌 상업/브랜드 계정(쇼핑몰, 매장 공식, 업체 마케팅, 재판매)
- 한국과도 음식·여행·일상과도 접점이 전혀 없는 분야(B2B, 시공, 정치 등)
- 계정 정보가 사실상 비어 있어 아무것도 판단할 수 없는 경우

# 판단 재료
계정명·이름·소개글·최근 게시물 캡션. 캡션이 비어 있는 것은 수집이 안 된
것일 뿐 부정적 신호가 아닙니다 — 그 이유만으로 fail 하지 마세요.

screen_reason에는 왜 그렇게 봤는지 한 문장으로 적으세요.
"""


@dataclass
class JudgeConfig:
    preset: str = DEFAULT_PRESET
    model: str = ""  # 비우면 프리셋 기본 모델 사용
    output: str = "verdicts.csv"
    # 판정은 계정마다 독립이라 병렬로 돌려도 된다. 인스타 크롤링과 달리
    # 차단 걱정이 없으므로, API 레이트리밋만 넘지 않게 적당히 잡는다.
    concurrency: int = 4
    rejudge: bool = False  # True면 이미 판정된 계정도 다시 판정
    max_accounts: int = 0  # 0이면 제한 없음

    @property
    def output_path(self) -> Path:
        return BASE_DIR / self.output

    @property
    def preset_obj(self) -> ModelPreset:
        if self.preset not in PRESETS_BY_KEY:
            raise JudgeError(f"알 수 없는 모델 선택: {self.preset}")
        return PRESETS_BY_KEY[self.preset]

    @property
    def model_id(self) -> str:
        """실제로 호출할 모델 ID. UI에서 직접 지정한 값이 프리셋보다 우선."""
        return (self.model or "").strip() or self.preset_obj.model


class JudgeError(Exception):
    """설정 문제 등 판정을 시작조차 할 수 없는 상황."""


RowFn = Callable[[dict], None]
StopFn = Callable[[], bool]


def compute_score(verdict: dict) -> int:
    """신호값을 0~100 점수로 환산한다(LLM이 아니라 코드가 계산).

    채점된 축의 원점수를 그 조합의 티어별 만점으로 나눠 100점으로 환산한다.
    도달 축까지 있으면 만점이 이미 100이라 환산은 항등식이 된다.
    지역 게이트를 통과하지 못하면 0점(등급 D)으로 둔다.
    """
    tier = verdict.get("tier") or "small"
    # 게이트 탈락은 '지역 조건 미달'이라는 뜻이므로 다른 항목이 좋아도 0으로 내린다.
    if not passes_region_gate(
        tier, verdict.get("busan_ratio", "zero"), verdict.get("korea_ratio", "none")
    ):
        return 0
    raw = sum(pts for _l, _k, _v, pts, _m in _row_points(verdict))
    return round(raw * 100 / max_for(verdict))


def format_account(row: dict) -> str:
    """후보 한 건을 LLM에게 보낼 텍스트로 만든다."""
    captions = [c for c in (row.get("captions") or "").split(CAPTION_SEP) if c.strip()]
    parts = [
        f"계정명: @{row.get('username', '')}",
        f"이름: {row.get('full_name') or '(없음)'}",
        f"팔로워: {row.get('followers') or '?'}",
        f"게시물 수: {row.get('posts') or '?'}",
        f"소개글: {row.get('biography') or '(없음)'}",
        f"외부 링크: {row.get('external_url') or '(없음)'}",
        f"이메일: {row.get('email') or '(없음)'}",
    ]
    if captions:
        parts.append("최근 게시물 캡션:")
        parts.extend(f"  {i + 1}. {c}" for i, c in enumerate(captions))
    else:
        parts.append("최근 게시물 캡션: (수집되지 않음)")
    return "\n".join(parts)


def load_verdicts(config: JudgeConfig | None = None) -> dict[str, dict]:
    """저장된 판정 결과를 {username: verdict}로 읽는다."""
    config = config or JudgeConfig()
    if not config.output_path.exists():
        return {}
    with open(config.output_path, encoding="utf-8-sig", newline="") as f:
        rows = {}
        for row in csv.DictReader(f):
            if not row.get("username"):
                continue
            # CSV는 전부 문자열이라, 정렬에 쓰는 score만 숫자로 되돌린다.
            # 선별 불합격(미채점) 계정은 점수가 비어 있으니 그대로 둔다("-"로 표시).
            sc = str(row.get("score") or "").strip()
            row["score"] = int(sc) if sc.isdigit() else ""
            rows[row["username"]] = row
        return rows


def write_verdicts(verdicts: list[dict], config: JudgeConfig) -> None:
    """판정 CSV를 통째로 다시 쓴다(재채점처럼 기존 행을 고쳐야 할 때).

    run_judging은 새 판정을 덧붙이기만 하므로 append로 충분하지만, 재채점은
    이미 있는 행의 점수를 바꾸는 일이라 이어 쓸 수 없다.
    """
    with open(config.output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=VERDICT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in verdicts:
            writer.writerow({k: row.get(k, "") for k in VERDICT_FIELDS})


def ensure_header(config: JudgeConfig) -> bool:
    """CSV 헤더가 지금의 VERDICT_FIELDS와 다르면 파일을 새 헤더로 다시 쓴다.

    run_judging은 결과를 append로 이어 붙이는데, DictWriter는 파일에 이미 있는
    헤더가 아니라 VERDICT_FIELDS 순서로 값을 쓴다. 그래서 컬럼이 늘어난 뒤
    옛 파일에 그냥 이어 붙이면 새 행만 칸이 밀려 저장된다(헤더는 옛 것, 값은 새
    순서). 판정을 시작하기 전에 한 번 맞춰 두는 편이 안전하다.
    """
    path = config.output_path
    if not path.exists():
        return False
    with open(path, encoding="utf-8-sig", newline="") as f:
        header = next(csv.reader(f), [])
    if header == VERDICT_FIELDS:
        return False
    # 이름으로 읽어 이름으로 다시 쓴다 — 새 컬럼은 빈 칸으로 채워진다.
    write_verdicts(list(load_verdicts(config).values()), config)
    return True


def rescore_all(config: JudgeConfig | None = None, log: LogFn = print) -> dict:
    """저장된 판정의 점수를 **LLM 없이** 다시 매긴다.

    스코어링은 크롤 텍스트와 릴스 조회수에서 코드가 계산하므로
    (analyze_text_signals + analyze_reels_signals + 배점표), 다음 경우에
    재호출 없이 점수만 새로 뽑을 수 있다:

      - 갱신으로 캡션·소개글이 바뀌었을 때 → 지역·음식·언어 신호가 달라진다
      - 릴스 조회수를 새로 추출했을 때 → 도달 축이 붙거나 값이 달라진다
      - 배점표나 등급 임계값을 손봤을 때 → 순위가 달라진다

    LLM이 정한 선별 결과(screen/screen_reason)는 그대로 둔다. 그건 '채점할
    가치가 있는가'라는 별개 판단이고, 다시 물으면 API 비용이 든다. 선별에서
    떨어진 계정은 애초에 점수가 없으므로 건드릴 것도 없다.
    """
    config = config or JudgeConfig()
    stats = {"ok": False, "rescored": 0, "skipped": 0, "changed": 0}

    verdicts = load_verdicts(config)
    if not verdicts:
        log("판정된 계정이 없습니다. 먼저 LLM 판정을 실행하세요.")
        stats["ok"] = True
        return stats

    candidates = {row["username"]: row for row in read_candidates()}
    reels = load_reels()
    updated = []
    for username, verdict in verdicts.items():
        row = candidates.get(username)
        # 선별 불합격이거나 후보 CSV에서 사라진 계정은 채점 대상이 아니다.
        if verdict.get("screen") == "fail" or row is None:
            updated.append(verdict)
            stats["skipped"] += 1
            continue
        before = verdict.get("score")
        fresh = {**verdict, **score_account(row, reels.get(username))}
        updated.append(fresh)
        stats["rescored"] += 1
        if before != fresh["score"]:
            stats["changed"] += 1
            log(
                f"  @{username}: {before}점 → {fresh['score']}점 "
                f"({verdict.get('grade', '?')}→{fresh['grade']})"
            )

    write_verdicts(updated, config)
    log(
        f"재채점 완료. {stats['rescored']}건 채점 "
        f"(점수 변동 {stats['changed']}건 / 미채점 {stats['skipped']}건)"
    )
    stats["ok"] = True
    return stats


def list_local_models(host: str | None = None) -> list[str]:
    """Ollama에 설치된 모델 목록. 안 떠 있으면 빈 목록.

    기본값을 인자에 박지 않고 호출 시점에 읽는다. def 시점에 묶어두면
    OLLAMA_HOST를 바꿔도 반영되지 않아 호스트를 바꿀 수단이 사라진다.
    """
    host = host or OLLAMA_HOST
    try:
        with urllib.request.urlopen(f"{host}/api/tags", timeout=2) as r:
            data = json.loads(r.read())
    except Exception:
        return []
    return sorted(m["name"] for m in data.get("models", []) if m.get("name"))


def _make_client(config: JudgeConfig):
    """선택된 프리셋에 맞는 클라이언트. 준비가 안 됐으면 원인을 분명히 알려준다."""
    preset = config.preset_obj

    # session.py가 import 시점에 .env를 읽어두므로 여기서도 그 값들이 보인다.
    if preset.api_key_env and not os.environ.get(preset.api_key_env, "").strip():
        raise JudgeError(
            f"{preset.api_key_env}가 없습니다. instacrawl/.env에 넣어주세요. "
            f"({preset.label})"
        )

    if preset.provider == "anthropic":
        try:
            import anthropic
        except ImportError as e:
            raise JudgeError("anthropic 패키지가 없습니다. `uv sync`를 실행하세요.") from e
        return anthropic.Anthropic()

    try:
        import openai
    except ImportError as e:
        raise JudgeError("openai 패키지가 없습니다. `uv sync`를 실행하세요.") from e

    if preset.key == "local":
        if not list_local_models():
            raise JudgeError(
                f"Ollama에 연결할 수 없거나 설치된 모델이 없습니다({OLLAMA_HOST}). "
                "`ollama serve`가 떠 있는지, `ollama pull <모델>`을 했는지 확인하세요."
            )
        if not config.model_id:
            raise JudgeError("로컬 모델을 선택하세요.")

    return openai.OpenAI(
        # Ollama는 키를 검사하지 않지만 SDK가 비어 있는 키를 거부하므로 더미를 넣는다.
        api_key=os.environ.get(preset.api_key_env, "").strip() or "not-needed",
        base_url=preset.base_url,
    )


def is_schema_node(value: object) -> bool:
    """값이 아니라 JSON 스키마의 '속성 정의'인지.

        {"type": "string", "description": "..."}   ← 정의(답이 아님)
        "개인 계정으로 부산 접점이 있음"              ← 답

    약한 모델이 답 대신 스키마를 베껴 돌려줄 때를 가려내는 데 쓴다.
    """
    return isinstance(value, dict) and ("type" in value or "description" in value)


def unwrap_schema_envelope(obj: dict) -> dict:
    """모델이 답을 JSON 스키마 껍데기 안에 넣어 돌려줄 때 실제 값만 꺼낸다.

    약한 로컬 모델은 프롬프트에 준 스키마를 그대로 흉내 내, 선별 값을
    최상위가 아니라 properties 안에 넣어 돌려주는 경우가 있다:

        {"type": "object", "properties": {"screen": "...", ...}, "required": [...]}

    이때 최상위에는 우리 신호(screen 등)가 없어 전부 기본값으로 떨어진다.
    properties 안에 실제 답이 들어 있으면 그쪽을 본문으로 쓴다.

    단, properties 안이 답이 아니라 스키마 **정의** 그대로일 수도 있다(모델이
    받은 스키마를 통째로 되돌려준 경우). 그건 벗겨봐야 값이 없으므로 벗기지
    않고 그대로 둔다 — 호출부가 '판정 실패'로 처리하게 하려는 것이다.
    """
    if not isinstance(obj, dict):
        return obj
    props = obj.get("properties")
    # 우리가 원하는 키가 최상위에 없고 properties 안에 있으면, 껍데기로 보고 벗긴다.
    if isinstance(props, dict) and "screen_reason" not in obj and "screen_reason" in props:
        if not is_schema_node(props.get("screen_reason")):
            return props
    return obj


def extract_json(text: str) -> dict:
    """응답 텍스트에서 JSON 객체를 꺼낸다.

    구조화 출력을 지원하는 모델은 순수 JSON만 주지만, 로컬 모델은 ```json 펜스나
    앞뒤 설명을 붙이거나, 답을 스키마 껍데기로 감싸기도 한다. 그런 응답도
    살려내기 위한 관대한 파서.
    """
    text = (text or "").strip()

    def loads(s: str) -> dict:
        return unwrap_schema_envelope(json.loads(s))

    try:
        return loads(text)
    except json.JSONDecodeError:
        pass
    # ```json ... ``` 펜스 제거
    fenced = re.search(r"```(?:json)?\s*(.+?)```", text, re.DOTALL)
    if fenced:
        try:
            return loads(fenced.group(1).strip())
        except json.JSONDecodeError:
            pass
    # 첫 '{' 부터 마지막 '}' 까지를 잘라 시도
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        return loads(text[start : end + 1])
    raise ValueError(f"JSON을 찾지 못했습니다: {text[:200]}")


def _call_anthropic(client, row: dict, config: JudgeConfig, system: str, schema: dict) -> str:
    response = client.messages.create(
        model=config.model_id,
        max_tokens=2000,
        system=[
            {
                "type": "text",
                "text": system,
                # 프롬프트는 모든 요청에서 동일하므로 캐시해 입력 비용을 줄인다.
                "cache_control": {"type": "ephemeral"},
            }
        ],
        # 스키마를 강제해 파싱 실패와 값 흔들림을 없앤다.
        output_config={"format": {"type": "json_schema", "schema": schema}},
        messages=[{"role": "user", "content": format_account(row)}],
    )
    if response.stop_reason == "refusal":
        raise RuntimeError("모델이 응답을 거부했습니다.")
    return next(b.text for b in response.content if b.type == "text")


def _call_openai(client, row: dict, config: JudgeConfig, system: str, schema: dict) -> str:
    """OpenAI 호환 엔드포인트(OpenAI / Gemini / Ollama) 공통 호출."""
    preset = config.preset_obj
    if preset.structured == "json_object":
        # json_schema를 못 쓰는 경우, 원하는 형태를 프롬프트로 알려준다.
        system += (
            "\n\n# 출력 형식\n"
            "설명 없이 아래 JSON 스키마를 따르는 JSON 객체 하나만 출력하세요.\n"
            f"{json.dumps(schema, ensure_ascii=False)}"
        )
        response_format = {"type": "json_object"}
    else:
        response_format = {
            "type": "json_schema",
            "json_schema": {"name": "verdict", "schema": schema, "strict": True},
        }

    response = client.chat.completions.create(
        model=config.model_id,
        max_tokens=2000,
        response_format=response_format,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": format_account(row)},
        ],
    )
    return response.choices[0].message.content or ""


def _call_llm(client, row: dict, config: JudgeConfig, system: str, schema: dict) -> dict:
    """프롬프트·스키마를 받아 LLM을 한 번 호출하고 JSON으로 파싱해 돌려준다."""
    if config.preset_obj.provider == "anthropic":
        text = _call_anthropic(client, row, config, system, schema)
    else:
        text = _call_openai(client, row, config, system, schema)
    return extract_json(text)


def screen_account(client, row: dict, config: JudgeConfig) -> dict:
    """1단계 — LLM 선별. 명백히 무관한 계정을 '불합격'으로 거른다(관대하게)."""
    raw = _call_llm(client, row, config, SCREEN_RUBRIC, SCREEN_SCHEMA)
    screen_raw, reason_raw = raw.get("screen"), raw.get("screen_reason")
    # 답 자리에 스키마 정의가 오거나 screen이 아예 없으면 '판정을 못 한 것'이다.
    # 여기에 아래의 '애매하면 pass' 규칙을 적용하면, 판정하지 못한 계정이 근거도
    # 없이 합격으로 굳고 verdicts.csv에 남아 재판정 대상에서도 빠진다.
    # 그래서 조용히 통과시키지 않고 오류로 올려, 다음 실행에서 다시 시도하게 한다.
    if not isinstance(screen_raw, str) or is_schema_node(reason_raw):
        raise ValueError(
            "모델이 선별 결과 대신 스키마를 되돌려줬습니다: "
            + json.dumps(raw, ensure_ascii=False)[:200]
        )
    # 애매하면 통과 — 이 단계의 목적은 명백한 쓰레기(상업·무관)만 걷어내는 것.
    screen = "fail" if screen_raw.strip().lower().startswith("fail") else "pass"
    reason = str(reason_raw or "").strip() or "(근거 없음)"
    return {"screen": screen, "screen_reason": reason}


def score_account(row: dict, reel: dict | None = None) -> dict:
    """2단계 — 스코어링. LLM 없이 크롤 텍스트로 신호를 뽑아 코드가 점수를 매긴다.

    신호(언어·음식·지역)는 analyze_text_signals가 캡션·소개글에서 정량 추출하고,
    티어·게이트·점수·등급·채점 이유는 배점표로 계산한다. 값이 흔들리지 않고,
    어떤 근거로 그 점수가 나왔는지 전부 추적된다.

    reel(reels.csv의 해당 계정 행)을 주면 도달 축까지 채점한다. 안 주면
    도달 축을 빼고 나머지 축으로만 환산한다 — 조회수는 계정마다 따로 추출하는
    수동 작업이라, 아직 안 뽑았다는 이유로 점수가 깎이면 안 된다.
    """
    verdict = analyze_text_signals(row)
    try:
        followers = int(float(row.get("followers") or 0))
    except (TypeError, ValueError):
        followers = 0
    verdict["tier"] = tier_of(followers)
    reels_signals = analyze_reels_signals(reel, followers, verdict["tier"])
    reels_reason = reels_signals.pop("reels_reason", "")
    verdict.update(reels_signals)
    if reels_reason:
        verdict["reason"] = f"{verdict['reason']} · {reels_reason}"
    verdict["gate"] = (
        "pass"
        if passes_region_gate(verdict["tier"], verdict["busan_ratio"], verdict["korea_ratio"])
        else "fail"
    )
    verdict["score"] = compute_score(verdict)
    verdict["grade"] = grade_for(verdict, verdict["score"])
    verdict["fit"] = GRADE_TO_FIT[verdict["grade"]]
    verdict["score_reason"] = explain_score(verdict)
    # 계정별 채점 표(구조화). CSV에는 저장하지 않고(VERDICT_FIELDS 밖) 화면 전달용.
    verdict["breakdown"] = score_breakdown(verdict)
    return verdict


def judge_account(client, row: dict, config: JudgeConfig, reel: dict | None = None) -> dict:
    """계정 한 건을 2단계로 판정한다: LLM 선별(합격/불합격) → 합격만 스코어링.

    실패(예외)는 그대로 올린다(호출부가 처리). 불합격이면 스코어링을 건너뛰어
    점수 관련 값은 비워 둔다. 스코어링은 LLM을 쓰지 않으므로 합격 계정당
    LLM 호출은 선별 1회뿐이다.
    """
    verdict = {"username": row["username"], **screen_account(client, row, config)}
    if verdict["screen"] == "fail":
        verdict["fit"] = "drop"  # 배지용. 등급·점수는 비워 둔다(미채점).
        return verdict
    verdict.update(score_account(row, reel))
    return verdict


def run_judging(
    config: JudgeConfig | None = None,
    log: LogFn = print,
    on_row: RowFn | None = None,
    should_stop: StopFn | None = None,
) -> dict:
    """candidates.csv의 후보들을 판정해 verdicts.csv에 쌓는다.

    discover.run_discovery와 같은 콜백 규약(log/on_row/should_stop)을 쓴다.
    """
    config = config or JudgeConfig()
    should_stop = should_stop or (lambda: False)
    on_row = on_row or (lambda _row: None)
    stats = {"ok": False, "judged": 0, "screened_out": 0, "failed": 0, "stopped": False}

    try:
        client = _make_client(config)
    except JudgeError as e:
        log(str(e))
        stats["error"] = str(e)
        return stats

    log(f"판정 모델: {config.preset_obj.label} / {config.model_id}")

    candidates = read_candidates()
    if not candidates:
        log("판정할 후보가 없습니다. 먼저 발굴을 실행하세요.")
        stats["ok"] = True
        return stats

    done = set() if config.rejudge else set(load_verdicts(config))
    todo = [c for c in candidates if c["username"] not in done]
    if config.max_accounts > 0 and len(todo) > config.max_accounts:
        log(f"이번 실행은 {config.max_accounts}개만 판정합니다(전체 {len(todo)}개 중).")
        todo = todo[: config.max_accounts]

    log(f"후보 {len(candidates)}개 중 판정 대상 {len(todo)}개 (이미 판정됨 {len(done)}개)")
    if not todo:
        log("모두 판정되어 있습니다. 다시 판정하려면 '재판정'을 켜세요.")
        stats["ok"] = True
        return stats

    no_captions = sum(1 for c in todo if not (c.get("captions") or "").strip())
    if no_captions:
        log(f"주의: {no_captions}개 계정은 캡션이 없어 소개글만으로 판단합니다.")

    # 릴스 조회수는 계정마다 따로(수동으로) 뽑는 값이라, 지금 있는 것만 쓴다.
    # 없는 계정은 도달 축을 빼고 채점하고, 나중에 추출하면 재채점으로 붙는다.
    reels = load_reels()
    with_reels = sum(1 for c in todo if c["username"] in reels)
    if with_reels:
        log(f"릴스 조회수가 있는 계정 {with_reels}개는 도달 축까지 채점합니다.")

    if ensure_header(config):
        log("판정 CSV의 컬럼이 바뀌어 헤더를 새 형식으로 맞췄습니다.")
    write_header = not config.output_path.exists()
    with open(config.output_path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=VERDICT_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()

        def work(row: dict) -> tuple[dict | None, Exception | None]:
            # pool.map은 작업을 한 번에 전부 큐에 넣는다. 중단 시 남은 작업이
            # API를 호출하지 않고 바로 빠져나오도록, 여기서 먼저 확인한다.
            if should_stop():
                return None, None
            try:
                return judge_account(client, row, config, reels.get(row["username"])), None
            except Exception as e:
                error_row = {"username": row.get("username", "?")}
                return error_row, e

        with ThreadPoolExecutor(max_workers=config.concurrency) as pool:
            for result, error in pool.map(work, todo):
                if result is None:  # 중단으로 건너뛴 작업
                    stats["stopped"] = True
                    continue
                if error is not None:
                    log(f"  - {result.get('username')} 판정 실패 [{type(error).__name__}]: {error}")
                    stats["failed"] += 1
                    continue
                writer.writerow(result)
                f.flush()
                stats["judged"] += 1
                on_row(result)
                if result.get("screen") == "fail":  # 1단계에서 걸러진 계정
                    stats["screened_out"] += 1
                    log(f"  [불합격] @{result['username']} — {result['screen_reason']}")
                else:
                    log(
                        f"  [{result['grade']}·{result['tier']}] @{result['username']} "
                        f"({result['score']}점) {result['score_reason']}"
                    )

    if stats["stopped"]:
        log("\n사용자 요청으로 중단했습니다. 지금까지 판정 결과는 저장되어 있습니다.")
    else:
        scored = stats["judged"] - stats["screened_out"]
        log(
            f"\n완료. 처리 {stats['judged']}건 "
            f"(스코어링 {scored}건 / 선별 불합격 {stats['screened_out']}건) "
            f"/ 오류 {stats['failed']}건"
        )
        log(f"결과는 {config.output_path} 에 저장되었습니다.")
    stats["ok"] = True
    return stats


def main() -> None:
    run_judging()


if __name__ == "__main__":
    main()
