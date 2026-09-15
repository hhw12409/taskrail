---
name: driftmq-adapter-investigation
description: taskrail의 queue-driftmq adapter를 구현할 때 필요한 driftmq 클라이언트 라이브러리(DriftClient/Producer/Consumer/Header)의 실제 API 계약을 조사할 때 사용. "driftmq adapter API 확인", "queue-driftmq 구현 전 조사", "driftmq 헤더 어떻게 붙이는지", "driftmq DLQ 헤더 뭔지" 같은 요청에 사용. taskrail 전체 MVP를 막는 게이트가 아니라, driftmq adapter 구현 단계에서만 필요하다 — core/memory adapter 작업에는 이 스킬을 쓰지 않는다.
---

# driftmq adapter API 조사

taskrail은 이제 `JobQueue` 인터페이스 뒤에 여러 broker adapter를 꽂는 구조다. driftmq는 그 중
`queue-driftmq`라는 **선택적** adapter일 뿐이며, taskrail 코어(Job 정의, 상태 머신, worker pool)는
driftmq를 몰라도 완전히 동작해야 한다. 이 스킬은 **driftmq adapter를 실제로 구현할 때만** 쓴다 —
memory adapter나 job runtime 코어 작업 중에는 필요 없다.

## 왜 소스를 직접 읽어야 하는가

README 예제는 "가장 흔한 사용법"만 보여준다. adapter가 필요로 하는 것은 그보다 미묘한 질문들이다:
`Producer.publish()`가 언제 완료로 간주되는지(fsync 완료 시점), `Consumer.run(Handler)`가 이미
자체 백오프를 갖고 있어서 taskrail의 retry policy와 중복될 위험은 없는지, DLQ 진입 시점을 taskrail의
DEAD 상태와 어떻게 매핑할지 같은 것들은 소스 코드를 직접 읽어야만 확실히 답할 수 있다.

## 기존 조사 자산 먼저 확인

`_workspace/driftmq_adapter/00_findings.md`에 driftmq Java 클라이언트의 핵심 계약이 이미 소스
기반으로 확인되어 있다: `DriftClient`/`Producer`/`Consumer`의 생성자·메서드 시그니처, 커스텀 헤더
부착 방법(`x-driftmq-` 접두사는 예약어라 사용 금지), `attemptCount`가 토픽(=stage) 전환 시 리셋되는
사실, DLQ 토픽 명명 규칙(`<topic>.dlq`, lazy 생성)과 예약 헤더 5종(`x-driftmq-dlq-*`). **이 내용은
재조사하지 않는다.** 새로 조사가 필요한 것은 이 계약을 `JobQueue` 인터페이스(enqueue/consume/ack/
nack)로 매핑할 때 생기는 adapter 설계 질문뿐이다.

## 조사 절차 (신규 질문에 대해서만)

1. driftmq 소스 위치를 찾는다(기존 findings에 기록된 경로를 우선 확인). 로컬에 소스가 없으면
   사용자에게 위치를 물어본다 — 절대 README 스니펫으로 대체하지 않는다.
2. taskrail-architect가 `docs/failure-model.md`에서 정한 driftmq-transport-retry vs
   taskrail-application-retry 경계를 구현 가능하게 만들 API 사실을 확인한다. 예: driftmq의 max-attempts
   설정을 taskrail이 조회/제어할 수 있는지, DLQ 도달을 taskrail이 어떻게 polling/구독으로 감지하는지.
3. 소스만으로 답이 안 나오는 항목은 최소 실행 테스트로 확인하거나, 그것도 어려우면 "불확실"로 명시한다.
   그럴듯한 기본값을 지어내지 않는다 — taskrail 코드가 그 가정 위에 세워지면 디버깅 비용이 커진다.

## 출력

`_workspace/driftmq_adapter/00_findings.md`에 새 섹션(`## queue-driftmq adapter 조사 결과
({날짜})`)으로 **이어서** 추가한다. 기존 기록을 덮어쓰지 않는다. "확인된 사실"(출처 인용 포함),
"불확실/확인 불가 항목", "adapter 설계에 주는 영향" 세 하위 섹션으로 작성한다. 템플릿은
`driftmq-adapter-investigator` 에이전트 정의를 따른다.
