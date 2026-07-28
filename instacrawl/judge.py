"""수집된 후보 계정을 LLM으로 1차 선별한다.

    uv run python judge.py        # CLI
    (웹 대시보드의 'LLM 판정' 패널에서도 실행)

이 단계는 **최종 판단이 아니라 발굴 단계**다. 목표는 정밀도가 아니라 재현율 —
사람이 눈으로 볼 후보 목록을 좁혀주되, 애매한 계정을 떨어뜨리지 않는 것이다.
그래서 판정 기준을 일부러 널널하게 잡고, 확신이 없으면 남기는 쪽으로 기울인다.

설계 메모:
- 크롤링(discover.py)과 분리되어 있다. 루브릭을 고쳐도 재크롤 없이 다시 돌린다.
- 결과는 candidates.csv를 건드리지 않고 verdicts.csv에 따로 쌓는다.
- 최종 점수는 LLM이 아니라 코드가 계산한다(SCORE_WEIGHTS). 가중치를 바꿔도
  LLM을 다시 호출할 필요가 없고, 왜 그 점수인지 추적할 수 있다.
"""

import csv
import json
import os
import re
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

from discover import BASE_DIR, CAPTION_SEP, read_candidates
from session import LogFn


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
        base_url="http://localhost:11434/v1",
        structured="json_object",
        note="API 비용이 들지 않습니다. 다국어 판단 품질은 모델 크기에 좌우됩니다.",
    ),
]

PRESETS_BY_KEY = {p.key: p for p in MODEL_PRESETS}
DEFAULT_PRESET = "claude-opus"

OLLAMA_HOST = "http://localhost:11434"

VERDICT_FIELDS = [
    "username",
    "fit",
    "confidence",
    "is_foreigner",
    "nationality_guess",
    "content_theme",
    "korea_relevance",
    "contactable",
    "score",
    "reason",
]

# 판정 결과 → 정렬용 점수. LLM이 아니라 여기서 계산하므로,
# 가중치를 바꿔도 재호출 없이 순위만 다시 매기면 된다.
SCORE_WEIGHTS = {
    "fit": {"keep": 50, "maybe": 25, "drop": 0},
    "is_foreigner": {"likely": 25, "unclear": 10, "unlikely": 0},
    "korea_relevance": {"resident": 15, "visitor": 15, "interested": 8, "none": 0},
    "theme": {"food": 10, "travel": 10, "daily": 3},  # 해당 테마마다 가산(최대 20)
    "contactable": 5,
}

# LLM이 반드시 이 형태로만 답하도록 강제하는 스키마.
# 자유 텍스트로 받으면 파싱이 매번 깨지고, 값 집합이 흔들려 정렬도 불가능하다.
VERDICT_SCHEMA = {
    "type": "object",
    "properties": {
        "fit": {
            "type": "string",
            "enum": ["keep", "maybe", "drop"],
            "description": "후보로 남길지. 확신이 없으면 maybe. drop은 명백히 무관할 때만.",
        },
        "confidence": {
            "type": "string",
            "enum": ["high", "medium", "low"],
            "description": "판단 근거가 얼마나 충분했는지. fit과 별개로 매긴다.",
        },
        "is_foreigner": {"type": "string", "enum": ["likely", "unclear", "unlikely"]},
        "nationality_guess": {
            "type": "string",
            "description": "추정 국적. 모르면 빈 문자열.",
        },
        "content_theme": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": ["food", "travel", "daily", "beauty", "commerce", "other"],
            },
        },
        "korea_relevance": {
            "type": "string",
            "enum": ["resident", "visitor", "interested", "none"],
            "description": "한국 거주 / 방문 / 관심 / 무관",
        },
        "contactable": {
            "type": "boolean",
            "description": "이메일·DM 문의 안내 등 연락 경로가 보이는지",
        },
        "reason": {
            "type": "string",
            "description": "판단 근거 한두 문장(한국어). 사람이 검수할 때 읽는 부분.",
        },
    },
    "required": [
        "fit",
        "confidence",
        "is_foreigner",
        "nationality_guess",
        "content_theme",
        "korea_relevance",
        "contactable",
        "reason",
    ],
    "additionalProperties": False,
}

# 시스템 프롬프트 = 루브릭. 모든 계정 판정에서 동일하므로 프롬프트 캐싱 대상이다.
#
# '널널하게'를 프롬프트에 반영한 방식이 중요하다. 모델은 "보수적으로 판단하라"
# 같은 지시를 문자 그대로 따르기 때문에, 신중하라고 쓰면 애매한 계정을 실제로
# 떨어뜨린다. 그래서 반대로 (1) 남기는 것이 기본값임을 명시하고,
# (2) 불확실성은 fit이 아니라 confidence로 표현하게 분리하고,
# (3) drop 조건을 좁게 열거했다.
RUBRIC = """\
당신은 한국의 외식업체를 위해 인스타그램 인플루언서 후보를 1차 선별합니다.

# 목표
찾는 대상은 **한국을 방문하거나 한국에 거주하는 외국인** 중, 음식·여행·일상
콘텐츠를 올리는 마이크로 인플루언서입니다. 이들에게 매장 방문·협업을 제안하게 됩니다.

# 이 작업의 성격 — 반드시 지킬 것
이것은 **최종 결정이 아니라 발굴 단계의 1차 거르기**입니다. 이후 사람이 직접
프로필을 열어 확인합니다. 따라서 **놓치는 것이 잘못 통과시키는 것보다 훨씬 나쁩니다.**

- 기본값은 남기는 것입니다. 애매하면 `maybe`를 쓰고, 절대 `drop`으로 보내지 마세요.
- 판단 근거가 부족하다는 느낌이 들면 그것은 `fit`을 낮출 이유가 아니라
  `confidence`를 `low`로 둘 이유입니다. 두 값을 섞지 마세요.
- `drop`은 다음처럼 **명백히 무관할 때만** 씁니다:
  · 개인이 아닌 상업 계정(쇼핑몰, 매장 홍보, 브랜드 공식, 업체 마케팅)
  · 한국과도 음식·여행과도 접점이 전혀 없는 분야(예: 지역 인테리어 시공, B2B)
  · 계정 정보가 사실상 비어 있어 아무것도 판단할 수 없는 경우
- 위에 해당하지 않으면 `keep` 또는 `maybe`입니다. 확신이 서지 않아서,
  정보가 적어서, 콘텐츠가 애매해서 떨어뜨리지 마세요.

# 판단 재료
계정명·이름·소개글·최근 게시물 캡션이 주어집니다. 소개글은 짧고 여러 언어와
이모지가 섞여 있는 경우가 많습니다(한국어/일본어/영어/중국어 등).
캡션이 있으면 소개글보다 콘텐츠 성격을 잘 드러내니 함께 보세요.
캡션이 비어 있는 것은 수집이 안 된 것일 뿐, 부정적 신호가 아닙니다.

# 각 항목
- is_foreigner: 한국인이 아닐 가능성. 외국어 사용, 국기 이모지, "한국 거주 중"
  같은 표현이 단서입니다. 한국어를 잘 쓰는 외국인도 많으니 한국어를 쓴다는
  이유만으로 `unlikely`로 두지 마세요.
- korea_relevance: 한국 거주(resident) / 방문·여행(visitor) / 관심은 있으나
  방문 여부 불명(interested) / 접점 없음(none).
- content_theme: 해당하는 것을 모두. 판단이 서지 않으면 `other`.
- contactable: 이메일, "DM 문의", 협업 문의 안내, 링크 등 연락 수단의 존재.
- reason: 한국어 한두 문장. 왜 그렇게 봤는지 구체적 근거를 짚어주세요.
  ("일본어 소개글 + 부산 거주 언급, 음식 캡션 다수" 처럼)
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
    weights: dict = field(default_factory=lambda: dict(SCORE_WEIGHTS))

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


def compute_score(verdict: dict, weights: dict | None = None) -> int:
    """판정 결과를 정렬용 0~100 점수로 환산한다(LLM이 아니라 코드가 계산)."""
    w = weights or SCORE_WEIGHTS
    # drop은 '명백히 무관'이라는 뜻이므로 다른 항목이 아무리 좋아도 0으로 내린다.
    # (이게 없으면 상업 계정인데 외국인·한국거주라 65점을 받아 약한 maybe보다
    #  위로 올라온다 — 검토 목록 맨 위가 버릴 계정으로 채워진다.)
    if verdict.get("fit") == "drop":
        return 0
    score = 0
    score += w["fit"].get(verdict.get("fit", ""), 0)
    score += w["is_foreigner"].get(verdict.get("is_foreigner", ""), 0)
    score += w["korea_relevance"].get(verdict.get("korea_relevance", ""), 0)
    themes = verdict.get("content_theme") or []
    theme_score = sum(w["theme"].get(t, 0) for t in themes)
    score += min(theme_score, 20)  # 테마 가산은 상한을 둬 한쪽으로 쏠리지 않게
    if verdict.get("contactable"):
        score += w["contactable"]
    return min(score, 100)


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
            # CSV는 전부 문자열이라, 코드에서 쓰는 타입으로 되돌린다.
            row["contactable"] = row.get("contactable") == "True"
            row["score"] = int(row.get("score") or 0)
            rows[row["username"]] = row
        return rows


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


def extract_json(text: str) -> dict:
    """응답 텍스트에서 JSON 객체를 꺼낸다.

    구조화 출력을 지원하는 모델은 순수 JSON만 주지만, 로컬 모델은 ```json 펜스나
    앞뒤 설명을 붙이는 경우가 흔하다. 그런 응답도 살려내기 위한 관대한 파서.
    """
    text = (text or "").strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # ```json ... ``` 펜스 제거
    fenced = re.search(r"```(?:json)?\s*(.+?)```", text, re.DOTALL)
    if fenced:
        try:
            return json.loads(fenced.group(1).strip())
        except json.JSONDecodeError:
            pass
    # 첫 '{' 부터 마지막 '}' 까지를 잘라 시도
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        return json.loads(text[start : end + 1])
    raise ValueError(f"JSON을 찾지 못했습니다: {text[:200]}")


def normalize_verdict(raw: dict) -> dict:
    """모델이 스키마를 벗어난 값을 줘도 안전한 값으로 맞춘다.

    로컬 모델은 enum을 지키지 않고 'Keep', '적합', 'unknown' 같은 값을 내기도 한다.
    이때 판정을 버리지 않고 보수적으로 복구한다 — 이 단계의 목적이 재현율이므로,
    알 수 없는 값은 전부 '남기는' 쪽(maybe/unclear)으로 기울인다.
    """

    def pick(value, allowed: list[str], default: str) -> str:
        text = str(value or "").strip().lower()
        if text in allowed:
            return text
        for a in allowed:  # 'Keep', 'KEEP' 같은 변형 흡수
            if text.startswith(a):
                return a
        return default

    themes = raw.get("content_theme")
    if isinstance(themes, str):
        themes = [t.strip() for t in themes.split(",")]
    allowed_themes = ["food", "travel", "daily", "beauty", "commerce", "other"]
    themes = [t for t in (themes or []) if str(t).strip().lower() in allowed_themes]

    return {
        # 알 수 없는 값 → maybe (drop이 아니라). 판단 불가로 후보를 잃지 않는다.
        "fit": pick(raw.get("fit"), ["keep", "maybe", "drop"], "maybe"),
        "confidence": pick(raw.get("confidence"), ["high", "medium", "low"], "low"),
        "is_foreigner": pick(
            raw.get("is_foreigner"), ["likely", "unclear", "unlikely"], "unclear"
        ),
        "nationality_guess": str(raw.get("nationality_guess") or "").strip(),
        "content_theme": [str(t).strip().lower() for t in themes] or ["other"],
        "korea_relevance": pick(
            raw.get("korea_relevance"),
            ["resident", "visitor", "interested", "none"],
            "interested",
        ),
        "contactable": bool(raw.get("contactable")),
        "reason": str(raw.get("reason") or "").strip() or "(근거 없음)",
    }


def _call_anthropic(client, row: dict, config: JudgeConfig) -> str:
    response = client.messages.create(
        model=config.model_id,
        max_tokens=2000,
        system=[
            {
                "type": "text",
                "text": RUBRIC,
                # 루브릭은 모든 요청에서 동일하므로 캐시해 입력 비용을 줄인다.
                "cache_control": {"type": "ephemeral"},
            }
        ],
        # 스키마를 강제해 파싱 실패와 값 흔들림을 없앤다.
        output_config={"format": {"type": "json_schema", "schema": VERDICT_SCHEMA}},
        messages=[{"role": "user", "content": format_account(row)}],
    )
    if response.stop_reason == "refusal":
        raise RuntimeError("모델이 응답을 거부했습니다.")
    return next(b.text for b in response.content if b.type == "text")


def _call_openai(client, row: dict, config: JudgeConfig) -> str:
    """OpenAI 호환 엔드포인트(OpenAI / Gemini / Ollama) 공통 호출."""
    preset = config.preset_obj
    system = RUBRIC
    if preset.structured == "json_object":
        # json_schema를 못 쓰는 경우, 원하는 형태를 프롬프트로 알려준다.
        system += (
            "\n\n# 출력 형식\n"
            "설명 없이 아래 JSON 스키마를 따르는 JSON 객체 하나만 출력하세요.\n"
            f"{json.dumps(VERDICT_SCHEMA, ensure_ascii=False)}"
        )
        response_format = {"type": "json_object"}
    else:
        response_format = {
            "type": "json_schema",
            "json_schema": {
                "name": "verdict",
                "schema": VERDICT_SCHEMA,
                "strict": True,
            },
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


def judge_account(client, row: dict, config: JudgeConfig) -> dict:
    """계정 한 건을 판정한다. 실패하면 예외를 그대로 올린다(호출부가 처리)."""
    if config.preset_obj.provider == "anthropic":
        text = _call_anthropic(client, row, config)
    else:
        text = _call_openai(client, row, config)
    verdict = normalize_verdict(extract_json(text))
    verdict["username"] = row["username"]
    verdict["score"] = compute_score(verdict, config.weights)
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
    stats = {"ok": False, "judged": 0, "failed": 0, "stopped": False}

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
                return judge_account(client, row, config), None
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
                # content_theme은 리스트라 CSV에 넣을 때 문자열로 편다.
                out = dict(result)
                out["content_theme"] = ",".join(result.get("content_theme") or [])
                writer.writerow(out)
                f.flush()
                stats["judged"] += 1
                on_row(result)
                log(
                    f"  [{result['fit']}] @{result['username']} "
                    f"({result['score']}점, 확신 {result['confidence']}) {result['reason']}"
                )

    if stats["stopped"]:
        log("\n사용자 요청으로 중단했습니다. 지금까지 판정 결과는 저장되어 있습니다.")
    else:
        log(f"\n완료. 판정 {stats['judged']}건 / 실패 {stats['failed']}건")
        log(f"결과는 {config.output_path} 에 저장되었습니다.")
    stats["ok"] = True
    return stats


def main() -> None:
    run_judging()


if __name__ == "__main__":
    main()
