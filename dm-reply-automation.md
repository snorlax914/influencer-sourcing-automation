# 인스타 DM 답장 자동 처리 (n8n self-hosted)

**목표**: 첫 DM은 사람이 수동 발송 → 인플루언서가 답장하면 자동으로
① 관심도 분류 → ② 시트/CRM 기록 → ③ 자동 응답 발송 → ④ 담당자 알림.

첫 접촉을 사람이 하므로 콜드 DM 자동화의 밴 위험이 없고, 답장 처리는
인스타그램 **공식 메시징 API**를 쓰므로 약관 위반이 아니다.

---

## 0. 핵심 제약 (먼저 이해할 것)

- **24시간 창(window)**: 상대의 마지막 메시지로부터 24시간 안에만 자유롭게
  자동 응답 가능. 그 이후엔 제한(메시지 태그/인간상담원 태그)이 걸린다.
  → 초기 응대 자동화에는 충분하지만, "며칠 뒤 팔로업"은 제한적.
- **앱 심사(App Review)**: 심사 전 개발모드에선 *내 Meta 앱에 역할이 등록된
  계정*하고만 메시지가 오간다. 실제 인플루언서와 하려면 심사 통과 필요
  (`instagram_business_manage_messages` 등). 며칠~2주 소요.
- **메시징 계정**: 스크래핑에 쓰던 계정 말고 **식당 공식 프로페셔널 계정**을 사용.

---

## 1. 사전 준비 (Meta 쪽)

1. 인스타 계정을 **프로페셔널(비즈니스/크리에이터)** 로 전환.
2. **페이스북 페이지**와 연결(표준 경로).
3. [Meta for Developers](https://developers.facebook.com)에서 **앱 생성**.
4. 제품에 **Instagram** 추가, 권한 요청:
   - `instagram_business_basic`
   - `instagram_business_manage_messages`
5. **액세스 토큰** 발급(장기 토큰으로 교환해 저장).
6. Webhooks에서 **`messages`** 필드 구독 → 콜백 URL = n8n 웹훅 주소.

> Graph API 버전/권한명은 갱신될 수 있으니 세팅 시점의 공식 문서로 확인.

---

## 2. self-hosted n8n을 외부에 노출 (Webhook용)

Meta는 **유효한 인증서를 가진 공개 HTTPS URL**로만 웹훅을 보낸다.
self-hosted라면 아래 중 하나로 노출:

- **Cloudflare Tunnel** (추천, 무료·고정 도메인·인증서 자동)
- Nginx 리버스 프록시 + Let's Encrypt
- ngrok (테스트용, 무료 플랜은 URL이 바뀜)

n8n 환경변수에 외부 URL을 알려줘야 웹훅 주소가 올바르게 생성된다:

```
WEBHOOK_URL=https://n8n.mydomain.com/
N8N_HOST=n8n.mydomain.com
N8N_PROTOCOL=https
```

---

## 3. n8n 워크플로우 A — 웹훅 검증 (GET)

Meta가 웹훅 등록 시 `GET`으로 `hub.challenge`를 보내 검증한다.
그대로 되돌려줘야 등록이 완료된다.

```
[Webhook 노드 (GET, path: /instagram)]
        ↓
[IF] query["hub.verify_token"] == 내가 정한 검증토큰 ?
        ↓ true
[Respond to Webhook] body = {{ $json.query["hub.challenge"] }}  (Content-Type: text/plain)
```

---

## 4. n8n 워크플로우 B — 답장 처리 (POST)

```
[Webhook 노드 (POST, path: /instagram)]
        ↓
[Function] 페이로드 파싱: entry[].messaging[] 에서
           senderId = messaging.sender.id  (IGSID)
           text     = messaging.message.text
           isEcho   = messaging.message.is_echo  (내가 보낸 메시지면 true)
        ↓
[IF] isEcho == true  또는  text 없음  →  중단 (읽음/에코/스티커 무시)
        ↓ (실제 상대 답장만 통과)
[HTTP Request] 관심도 분류 (Claude API, 아래 5번)
        ↓
[Switch] classification 값으로 분기
   ├─ "INTERESTED"     → 자동응답(관심) + 시트기록 + 담당자 알림
   ├─ "NOT_INTERESTED" → (선택)정중한 마무리 응답 + 시트기록
   └─ "UNCLEAR"        → 담당자 알림만(사람이 직접 응대) + 시트기록
        ↓
[HTTP Request] 자동 응답 발송 (아래 6번)
        ↓
[Google Sheets: Append] 로그 기록 (아래 7번)
        ↓
[Slack/Email] 담당자 알림
```

---

## 5. 관심도 분류 — Claude API (HTTP Request 노드)

`POST https://api.anthropic.com/v1/messages`

헤더:
```
x-api-key: <ANTHROPIC_API_KEY>
anthropic-version: 2023-06-01
content-type: application/json
```

바디 (저렴한 Haiku 모델 사용):
```json
{
  "model": "claude-haiku-4-5-20251001",
  "max_tokens": 200,
  "system": "너는 인플루언서 섭외 답장을 분류하는 분류기다. 반드시 JSON만 출력한다. 형식: {\"classification\":\"INTERESTED|NOT_INTERESTED|UNCLEAR\",\"rate_mentioned\":\"<언급된 단가 또는 빈문자열>\",\"summary\":\"<한 줄 요약>\"}",
  "messages": [
    { "role": "user", "content": "인플루언서 답장: \"{{ $json.text }}\"" }
  ]
}
```

응답의 `content[0].text`를 JSON 파싱해 `classification` 사용.
(키워드 규칙보다 오탐이 적고, 단가 언급/톤까지 함께 뽑을 수 있다.)

---

## 6. 자동 응답 발송 (HTTP Request 노드)

`POST https://graph.instagram.com/v21.0/me/messages`
(페이스북 로그인 방식이면 `https://graph.facebook.com/v21.0/{PAGE_ID}/messages`)

쿼리/헤더: `access_token=<장기토큰>`

바디:
```json
{
  "recipient": { "id": "{{ $json.senderId }}" },
  "message":   { "text": "{{ $json.replyText }}" }
}
```

⚠️ 반드시 **24시간 창 안**에서만 성공. 웹훅 수신 즉시 보내면 문제없다.

---

## 7. Google Sheets 로그 스키마

| timestamp | igsid | message | classification | rate_mentioned | action | summary |
|-----------|-------|---------|----------------|----------------|--------|---------|
| 수신시각 | 발신자 ID | 답장 원문 | 관심도 | 언급 단가 | 보낸 자동응답/알림 | 한줄요약 |

`igsid`를 키로 두면 같은 인플루언서의 대화 이력을 추적할 수 있다.
발신자의 username이 필요하면 `GET /{igsid}?fields=name,username` 로 조회(권한 필요).

---

## 8. 메시지 템플릿 (자동 응답 예시)

**관심(INTERESTED) 응답**
```
안녕하세요! 관심 가져주셔서 감사합니다 😊
저희 [식당명]은 [지역]에 위치한 [컨셉] 식당입니다.
협업 관련 상세 안내와 방문 일정 조율을 위해 아래 이메일로
미디어킷(팔로워/조회수 등)을 보내주시면 빠르게 회신드리겠습니다.
📩 [이메일주소]
```

**분류 불명확(UNCLEAR)** → 자동응답 없이 담당자에게 넘김(사람이 판단).

---

## 9. 한계와 주의

- 24시간 창을 넘긴 팔로업은 자동으로 못 보낸다(태그 필요, 제한적).
- 앱 심사 전에는 실제 인플루언서와 테스트 불가(개발모드 제약).
- 자동 응답은 "첫 회신"까지만 자동화하고, 실제 협의(단가/일정)는
  사람이 이어받는 구조를 권장 — 전환율·신뢰도 면에서 유리.
- 토큰/시크릿(Anthropic 키, IG 토큰)은 n8n Credentials로 관리, 코드에 하드코딩 금지.

---

## 요약 흐름도

```
수동 첫 DM ─▶ 상대 답장 ─▶ [Webhook] ─▶ 파싱/에코필터
   ─▶ [Claude 분류] ─▶ 관심? ─┬─ 예 ─▶ 자동응답 + 시트기록 + 알림
                              └─ 애매 ─▶ 시트기록 + 담당자 알림(수동응대)
```
