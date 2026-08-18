"""협업 제안 DM 초안 생성 — 문장을 만들 뿐, 보내지는 않는다.

첫 DM은 사람이 직접 보낸다(dm-reply-automation.md 참고 — 콜드 DM 자동 발송은
밴 위험이 있어 답장 단계부터 자동화한다). 그래서 이 모듈의 목표는 발송이 아니라
**초안을 만들어 복사하기 좋게 내주는 것**이다.

설계 메모:
- 템플릿은 거주 기준 2종(한국 거주 / 해외 거주)뿐이다. 언어는 영어 하나로 통일.
  거주 구분은 자동 추론하지 않는다 — 사람이 보고 고른다. 크롤 데이터로는
  '부산 콘텐츠가 있다'까지만 알 수 있고 거주 여부는 알 수 없어서, 추론했다가
  틀리면 도쿄 사는 사람에게 "가게에 들러 달라"는 메시지가 나간다.
- 본문은 **고정 문장 + 슬롯**으로만 이루어진다. 화면에서 슬롯만 입력받고 나머지는
  읽기 전용으로 보여주므로, '일부만 수정 가능'이 규칙이 아니라 구조로 보장된다.
  (HTML textarea는 일부만 잠글 수 없다. contenteditable로 흉내 내면 붙여넣기·
  되돌리기에서 쉽게 깨진다.)
- 슬롯은 두 단계다. 공통(가게·담당자·제안)은 한 번 입력해 모든 DM에 쓰고,
  계정별(호칭·개인화 한 줄)만 계정마다 다르게 넣는다.
- 발송 기록을 남긴다. 첫 DM이 수동이라 도구가 기억해 두지 않으면 같은 계정에
  두 번 보내게 된다.
"""

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
DM_CSV = BASE_DIR / "dm.csv"
# 공통 슬롯 값(가게 이름 등)은 매번 다시 칠 이유가 없어 파일에 남긴다.
DM_CONFIG = BASE_DIR / "dm_config.json"

DM_FIELDS = ["username", "template", "sent_at", "body"]


@dataclass(frozen=True)
class Slot:
    """본문에서 사람이 채우는 자리 하나."""

    key: str
    label: str
    placeholder: str = ""
    hint: str = ""
    multiline: bool = False


# 공통 슬롯 — 한 번 입력하면 모든 DM에 그대로 들어간다.
COMMON_SLOTS: list[Slot] = [
    Slot("store", "가게 이름", "1953형제돼지국밥", "본문 도입부와 서명에 함께 들어갑니다."),
    Slot("store_desc", "가게 한 줄 소개", "a Gukbap restaurant in Busan",
         "영어로. 두 템플릿이 같은 문구를 씁니다."),
    Slot("sender", "보내는 사람", "Jiwon", "두 템플릿 모두 'I'm ○○ from ...'에 들어갑니다."),
    Slot("guests", "초대 인원", "2", "'a complimentary meal for N people'의 N."),
    Slot("deliverable", "요청 콘텐츠", "1 feed post + stories"),
]

# 계정별 슬롯 — 계정마다 다르게 채운다.
ACCOUNT_SLOTS: list[Slot] = [
    Slot("name", "호칭", "Aichu", "비우면 계정명이 들어갑니다."),
    Slot("post_ref", "언급할 게시물", "your reel on living in Busan as a Japanese creator",
         "생략 가능 — 비우면 그 문장이 통째로 자연스럽게 줄어듭니다.", multiline=True),
]

ALL_SLOTS = COMMON_SLOTS + ACCOUNT_SLOTS


@dataclass(frozen=True)
class Template:
    key: str
    label: str
    hint: str
    body: str


# 본문의 {{슬롯}}만 치환된다. 나머지 문장은 화면에서 고칠 수 없다.
# 문안을 바꾸려면 여기만 고치면 되고, 슬롯 이름은 위 목록과 맞춰야 한다.
#
# [[키]]...[[/키]]  = 그 슬롯이 채워졌을 때만 남는 구간
# [[^키]]...[[/^키]] = 비었을 때만 남는 구간
# 생략 가능한 슬롯은 값만 지우면 앞뒤 문장부호까지 같이 사라져야 한다.
# 그냥 비우면 "your posts around Busan —  — really stood out." 같은 문장이 남는다.
TEMPLATES: list[Template] = [
    Template(
        key="resident",
        label="거주형",
        hint="한국에 살고 있어 바로 방문할 수 있는 계정. 지점 주소 사진을 함께 첨부하세요.",
        body="""Hi {{name}}! 👋

I'm {{sender}} from {{store}}, {{store_desc}}.

[[post_ref]]I've been following your posts around Busan — {{post_ref}} — really stood out.[[/post_ref]][[^post_ref]]I've been following your posts around Busan.[[/^post_ref]] The way you show the city to a non-Korean audience is exactly the kind of thing we'd love to be part of.

We'd like to invite you in for a complimentary meal for {{guests}} people. No strict script — just come, eat, and share it however feels natural to you (we'd love {{deliverable}}, but happy to discuss).

We have several branches around Busan — I've attached a photo with all the addresses, so feel free to pick whichever one is most convenient for you.

Let me know if you're interested and I'll find a time that works for you.

Thanks for reading! 🙏
{{store}}""",
    ),
    Template(
        key="visitor",
        label="여행형",
        hint="해외에 살아 한국 방문 일정에 맞춰 제안하는 계정. 지점 주소 사진을 함께 첨부하세요.",
        body="""Hi {{name}}! 👋

I'm {{sender}} from {{store}}, {{store_desc}}.

I came across your Korea content[[post_ref]] — {{post_ref}} —[[/post_ref]] and loved how you cover food while traveling. If Busan is on your route again, we'd like to host you.

We'd cover a full meal for {{guests}} people, and we're flexible on dates — just let us know when you're planning to be in the city, even if it's a few months out. Happy to hold the invite open.

We have multiple branches across Busan, so wherever you end up staying, there's likely one nearby. I've attached a photo with all the addresses — feel free to drop by whichever suits your plans.

In exchange we'd love {{deliverable}}, but we're open to whatever format fits your usual content.

Are you planning a Korea trip anytime soon?

{{store}}""",
    ),
]

TEMPLATES_BY_KEY = {t.key: t for t in TEMPLATES}
DEFAULT_TEMPLATE = "resident"


# [[키]]본문[[/키]] / [[^키]]본문[[/^키]] — 조건 구간.
_BLOCK_RE = re.compile(r"\[\[(\^?)(\w+)\]\](.*?)\[\[/\1\2\]\]", re.DOTALL)


def render(template_key: str, values: dict) -> str:
    """템플릿 + 슬롯 값 → 보낼 본문.

    화면에서도 같은 규칙으로 미리보기를 조립한다(입력하는 대로 즉시 보이도록).
    여기서 한 번 더 만드는 이유는 저장할 본문을 서버가 확정하기 위해서다 —
    화면이 보낸 문자열을 그대로 믿으면 기록이 실제 템플릿과 어긋날 수 있다.
    """
    template = TEMPLATES_BY_KEY.get(template_key)
    if template is None:
        raise KeyError(f"알 수 없는 템플릿: {template_key}")

    def filled(key: str) -> bool:
        return bool(str(values.get(key, "") or "").strip())

    def pick(m: re.Match) -> str:
        negate, key, inner = m.group(1), m.group(2), m.group(3)
        keep = (not filled(key)) if negate else filled(key)
        return inner if keep else ""

    # 조건 구간을 먼저 정리해야, 지워질 구간 안의 슬롯을 헛되이 채우지 않는다.
    body = _BLOCK_RE.sub(pick, template.body)
    for slot in ALL_SLOTS:
        body = body.replace("{{" + slot.key + "}}", str(values.get(slot.key, "") or "").strip())
    # 슬롯이 비어 생긴 빈 줄이 세 줄 이상 이어지면 두 줄로 접는다.
    while "\n\n\n" in body:
        body = body.replace("\n\n\n", "\n\n")
    return body.strip()


def load_common() -> dict:
    """저장해 둔 공통 슬롯 값(없으면 빈 dict)."""
    if not DM_CONFIG.exists():
        return {}
    try:
        data = json.loads(DM_CONFIG.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return {k: v for k, v in data.items() if k in {s.key for s in COMMON_SLOTS}}


def save_common(values: dict) -> dict:
    """공통 슬롯 값을 저장한다. 정의된 키만 남긴다."""
    keep = {s.key: str(values.get(s.key, "") or "") for s in COMMON_SLOTS}
    DM_CONFIG.write_text(json.dumps(keep, ensure_ascii=False, indent=2), encoding="utf-8")
    return keep


def load_records() -> dict[str, dict]:
    """발송 기록을 {username: row}로 읽는다."""
    if not DM_CSV.exists():
        return {}
    rows: dict[str, dict] = {}
    with open(DM_CSV, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            username = (row.get("username") or "").strip()
            if username:
                rows[username] = row
    return rows


def mark_sent(username: str, template_key: str, body: str) -> dict:
    """'보냈다'고 기록한다(계정당 한 줄, 다시 보내면 덮어쓴다).

    본문 스냅샷까지 남기는 이유: 나중에 답장을 분류할 때 무엇을 보냈는지
    모르면 맥락 없이 회신만 보게 된다.
    """
    row = {
        "username": username,
        "template": template_key,
        "sent_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "body": body,
    }
    records = load_records()
    records[username] = row
    _write(records)
    return row


def unmark_sent(username: str) -> bool:
    """발송 표시를 되돌린다(잘못 눌렀을 때). 기록이 없으면 False."""
    records = load_records()
    if records.pop(username, None) is None:
        return False
    _write(records)
    return True


def _write(records: dict[str, dict]) -> None:
    with open(DM_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DM_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in records.values():
            writer.writerow({k: row.get(k, "") for k in DM_FIELDS})


def config_payload() -> dict:
    """화면이 필요한 것 한 묶음 — 템플릿 본문·슬롯 정의·저장된 공통 값."""
    as_slot = lambda s: {  # noqa: E731
        "key": s.key, "label": s.label, "placeholder": s.placeholder,
        "hint": s.hint, "multiline": s.multiline,
    }
    return {
        "templates": [
            {"key": t.key, "label": t.label, "hint": t.hint, "body": t.body} for t in TEMPLATES
        ],
        "default_template": DEFAULT_TEMPLATE,
        "common_slots": [as_slot(s) for s in COMMON_SLOTS],
        "account_slots": [as_slot(s) for s in ACCOUNT_SLOTS],
        "common": load_common(),
    }
