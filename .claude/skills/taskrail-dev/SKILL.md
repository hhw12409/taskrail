---
name: taskrail-dev
description: taskrail(독립적인 Background Job Runtime) 개발을 조율하는 오케스트레이터. "taskrail 만들어줘", "taskrail 설계 문서 작성", "taskrail 구현 시작", "taskrail MVP 개발", "taskrail 이어서 진행", "taskrail 다시 실행", "taskrail 일부만 다시", "taskrail 검증해줘", "taskrail 리뷰해줘" 등 taskrail 개발/설계/재실행/부분 수정/검증 요청 시 반드시 사용. taskrail-architect, driftmq-adapter-investigator, taskrail-builder, taskrail-qa 네 에이전트로 구성된 팀을 조율한다.
---

# taskrail 개발 오케스트레이터

`taskrail-architect`(설계) → 사용자 리뷰 게이트 → `taskrail-builder` ↔ `taskrail-qa`(구현/QA 루프,
필요 시 `driftmq-adapter-investigator` 합류) 순으로 진행하는 4인 팀을 조율해 taskrail
(독립적인 Background Job Runtime) MVP를 만든다. **실행 모드: 에이전트 팀** (설계 리뷰 결과가 구현에
직접 영향을 주고, QA 피드백이 builder에게 실시간으로 돌아가야 하므로 팀 통신이 필요하다).

## taskrail의 정체성 (2026-09-15 재정의)

taskrail은 더 이상 "driftmq 위의 선형 파이프라인 상태 추적기"가 아니다. **독립적인 Background Job
Runtime**이며, driftmq는 `JobQueue` 인터페이스 뒤의 선택적 adapter(`queue-driftmq`) 중 하나일 뿐이다.
전체 배경은 `docs/reference/ecosystem-architecture-source.md`(DriftMQ=Message Broker,
Taskrail=Job Runtime, Beacon=Realtime Delivery 3-프로젝트 생태계 설계) 2절, 5절, 10절, 14절, 15절을
참고한다. 이 저장소는 그 중 **Taskrail**만 다룬다.

## Phase 0: 컨텍스트 확인

`README.md`/`docs/architecture.md` 존재 여부와 `_workspace/progress.md` 상태로 실행 모드를
판별한다:

- 설계 문서(README/docs/architecture.md 등)가 없음 → **초기 실행**: 설계부터 시작 (Phase 2).
- 설계 문서는 있지만 사용자 승인 기록이 없음 → **승인 대기 재개**: 기존 설계 문서를 사용자에게 다시
  제시하고 승인을 구한다 (재작성하지 않는다).
- 설계 승인됨 + `_workspace/progress.md`에 일부 모듈만 완료 표시 → **구현 이어서 진행**: 완료된
  모듈은 건너뛰고 다음 모듈부터 builder에게 할당한다.
- 모든 모듈 완료 + 사용자가 특정 모듈 수정/버그 수정을 요청 → **부분 재실행**: 해당 모듈만 builder에게
  재할당하고, 관련 QA만 다시 수행한다.
- 모든 모듈 완료 + MVP 완료 기준까지 통과 + 사용자가 "재검증"만 요청 → `taskrail-qa`만 단독으로
  재호출한다 (팀 전체를 재구성할 필요 없음).
- 사용자가 "설계 문서만 다시 검토"를 요청 → `taskrail-architect`만 단독으로 재호출한다.

`_workspace/progress.md`가 없으면 만들어서 Phase A(설계)/Phase B(구현, 모듈별
NOT_STARTED/IN_PROGRESS/BUILT/QA_PASSED)의 상태를 추적한다 — 팀이 재구성되거나 세션이 끊겨도 어디까지
했는지 알 수 있어야 한다.

## Phase 1: 팀 구성 (초기 실행 또는 재개 시)

```
TeamCreate(team_name="taskrail-dev", members=[taskrail-architect, driftmq-adapter-investigator, taskrail-builder, taskrail-qa])
```

설계 문서만 재검토하거나 QA만 단독 재검증하는 경우, 팀 전체를 구성하지 않고 해당 에이전트만 `Agent`
도구로 직접 호출해도 된다 (서브 에이전트 패턴 — Phase 2-1의 하이브리드 예외).

## Phase 2: 설계 → 사용자 승인 게이트

**이 게이트는 반드시 사람(사용자)이 통과시킨다 — 에이전트끼리 자동으로 다음 단계로 넘어가지 않는다.**
생태계 문서 15절 "지금 바로 전체 코드를 작성하지 마라... 설계를 검토받는다"가 명시 요구사항이다.

1. `TaskCreate`로 `taskrail-architect`에게 설계 문서 작성을 할당한다.
2. architect가 README/`docs/architecture.md`/`docs/job-lifecycle.md`/`docs/failure-model.md`/
   Public API 초안을 완성하고 SendMessage로 보고하면, 오케스트레이터(나)가 생태계 문서 15절 Phase 3
   형식(Architecture, Component diagram, Job lifecycle, Retry/backoff/timeout 정책, Failure
   scenarios, Storage/queue 모델, Public API, MVP scope, Non-goals, 기술적 trade-off)으로 정리해
   **사용자에게 직접 제시**한다.
3. 언어 스택 결정과 driftmq adapter 구현 방식(driftmq는 Java 클라이언트로 확인됨,
   `_workspace/driftmq_adapter/00_findings.md`)처럼 트레이드오프가 있는 항목은 사용자에게 명확히
   질문한다 (AskUserQuestion 등). 사용자의 결정을 설계 문서에 반영하도록 architect에게 전달한다.
4. 사용자가 설계를 승인하면 `_workspace/progress.md`에 승인 기록을 남기고 Phase 3으로 진행한다.
   승인 전에는 절대 `taskrail-builder`에게 구현 태스크를 할당하지 않는다.

## Phase 3: 구현 ↔ 점진적 QA 루프

`taskrail-module-builder` 스킬의 구현 순서(`packages/core` → `packages/queue-memory` → job runtime
→ CLI → `packages/queue-driftmq`)대로 진행한다.

각 단계마다:

1. `TaskCreate`로 `taskrail-builder`에게 해당 모듈 구현을 할당한다 (이전 단계 산출물에 의존하므로
   순서를 건너뛰지 않는다).
2. builder가 완료를 SendMessage로 알리면, `TaskCreate`로 `taskrail-qa`에게 해당 모듈의 경계면 검증을
   할당한다.
3. QA가 문제를 발견하면 builder에게 직접 SendMessage로 버그 리포트를 보내 수정을 요청한다
   (오케스트레이터를 거치지 않고 팀원끼리 직접 조율 — 팀 모드를 쓰는 이유다). 수정 후 재검증한다.
4. `packages/queue-driftmq` 단계에서 builder나 architect가 driftmq API 불확실성을 발견하면,
   `driftmq-adapter-investigator`에게 직접 질문하도록 한다 — 결과는
   `_workspace/driftmq_adapter/00_findings.md`에 누적된다.
5. QA 통과 시 `_workspace/progress.md`에 해당 모듈을 QA_PASSED로 기록하고 다음 모듈로 넘어간다.

builder가 스코프 밖 아이디어(Scheduled Job/cron, queue-postgres, DAG, compensation 등)를
`_workspace/deferred_ideas.md`에 기록했다면, 그 파일은 그대로 두고 구현에 반영하지 않는다.

## Phase 4: 최종 완료 기준 검증

모든 모듈이 QA_PASSED가 되면, `taskrail-qa`에게 MVP 완료 기준 8개 항목(job 성공 실행, retry/DEAD
처리, timeout, concurrency, delayed job, 상태/이력 조회, graceful shutdown 복구, memory/driftmq
adapter 간 동작 일치)을 실제 실행으로 검증하도록 `TaskCreate`한다. 결과를 오케스트레이터가 수집한다.

## Phase 5: 종합 보고 및 정리

- MVP 완료 기준 8개 항목의 통과/실패를 사용자에게 보고한다. 실패 항목이 있으면 원인과 재시도 결과를
  함께 보고한다.
- `_workspace/deferred_ideas.md`에 쌓인 스코프 밖 아이디어 메모를 사용자에게 그대로 전달한다 (구현하지
  않았다는 점을 명시).
- `_workspace/progress.md`를 최신 상태로 갱신한다.
- 팀을 정리한다 (`TeamDelete` 또는 유휴 상태로 둠 — 후속 요청이 곧바로 올 가능성이 높으므로 즉시
  해체할 필요는 없다).
- 사용자에게 개선하고 싶은 부분이 있는지 물어본다 (하네스 진화 피드백 수집).

## 데이터 전달 프로토콜

- **파일 기반**: 설계 문서(`README.md`, `docs/architecture.md`, `docs/job-lifecycle.md`,
  `docs/failure-model.md`, Public API 초안), `_workspace/progress.md`(Phase/모듈별 진행 상태),
  `_workspace/driftmq_adapter/00_findings.md`(driftmq adapter API 조사 결과),
  `_workspace/deferred_ideas.md`(스코프 밖 아이디어 메모). 실제 taskrail 소스 코드는 프로젝트의 정식
  경로(`packages/...`)에 작성하며 `_workspace/`에 두지 않는다.
- **태스크 기반**: `TaskCreate`/`TaskUpdate`로 설계·모듈별 구현·QA 작업과 의존 순서를 관리한다.
- **메시지 기반**: builder ↔ qa의 실시간 버그 리포트/재검증 요청, architect에 대한 설계 질문,
  driftmq-adapter-investigator에 대한 API 질문.

## 에러 핸들링

- driftmq adapter API 조사에서 소스를 찾을 수 없거나 확인이 근본적으로 불가능하면, 추측하지 말고
  사용자에게 소스 위치나 판단을 요청한다.
- builder 구현이 실패(컴파일/실행 에러)하면 1회 자체 수정 재시도 후, 여전히 실패하면 원인과 시도한
  내용을 사용자에게 보고하고 다음 지시를 기다린다.
- QA가 발견한 버그는 삭제/무시하지 않고 항상 builder에게 전달해 수정 루프를 돈다. 사용자가 명시적으로
  "이 버그는 나중에"라고 하지 않는 한 다음 모듈로 넘어가지 않는다.
- 설계 문서가 모호해 builder/QA가 판단할 수 없는 상황이 반복되면, 구현을 멈추고
  `taskrail-architect`에게 문서 보강을 요청한다 — 애매한 채로 구현을 진행하지 않는다.
- 상충되는 요구(예: 사용자가 MVP 스코프 밖 기능을 요청)는 승인된 설계 문서의 MVP 경계를 근거로 들어
  확인 질문을 먼저 한다 — 조용히 구현하거나 조용히 무시하지 않는다.

## 테스트 시나리오

**정상 흐름**: 사용자가 "taskrail 만들어줘"라고 요청 → Phase 0에서 초기 실행 판별 → 팀 구성 →
architect가 설계 문서 작성 → 사용자에게 10개 항목 제시 및 승인 → 5단계 구현/QA 루프(core →
queue-memory → job runtime → CLI → queue-driftmq) → 최종 완료 기준 8개 통과 → 종합 보고.

**에러 흐름**: architect가 언어 스택 비교표에서 "driftmq가 Java이므로 taskrail도 Java를 쓰면
adapter 구현이 단순해지지만, TS로 가면 별도 프로세스+IPC가 필요하다"는 트레이드오프를 발견 →
오케스트레이터가 이를 명시적 질문으로 사용자에게 제시 → 사용자 결정에 따라 설계 문서를 갱신하고
Phase 3 구현으로 진행.
