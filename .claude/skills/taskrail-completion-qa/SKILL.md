---
name: taskrail-completion-qa
description: taskrail(Background Job Runtime) 모듈을 완성 직후 검증하거나, MVP 완료 기준(job 성공 실행, retry/DEAD 처리, timeout, concurrency, delayed job, 상태 조회, graceful shutdown, adapter 간 동작 일치)을 실제로 실행해 확인할 때 사용. "이 모듈 검증해줘", "taskrail 테스트", "retry 제대로 도는지 확인", "memory adapter랑 driftmq adapter 동작 같은지 확인", "taskrail 완료 기준 통과하는지" 같은 요청에 사용할 것.
---

# taskrail 완료 기준 검증

taskrail 검증의 핵심은 "파일이 존재한다"가 아니라 "경계면 양쪽이 실제로 맞물린다"이다. taskrail에서
가장 중요한 경계면은 **`JobQueue` 인터페이스와 그 구현체(memory/driftmq) 사이**다 — 인터페이스가
약속한 동작을 adapter가 실제로 지키지 않으면, "adapter를 바꿔도 코드는 그대로"라는 taskrail의 핵심
가치 제안이 깨진다.

## 경계면 교차 비교 체크리스트

- `JobQueue` 인터페이스의 `enqueue`/`consume`/`ack`/`nack` 시그니처와 동작(특히 nack 시 재시도
  여부)이 `packages/queue-memory`와 `packages/queue-driftmq`(구현되었다면) 양쪽에서 문자 그대로
  같은가 — **같은 최소 시나리오를 두 adapter 모두에 돌려서 결과를 비교**한다
- Job 상태 전이(`WAITING → RUNNING → SUCCESS`, `RUNNING → FAILED → RETRY_WAIT → WAITING`,
  `RUNNING → FAILED → DEAD`)가 `docs/job-lifecycle.md`에 적힌 조건과 코드에서 정확히 일치하는가
- `taskrail-architect`가 작성한 Public API 초안의 시그니처와 실제 구현 코드의 시그니처가 갈라지지
  않았는가
- driftmq adapter 단계: DLQ 감지 로직이 참조하는 헤더 키가
  `_workspace/driftmq_adapter/00_findings.md`의 실제 driftmq DLQ 헤더 컨벤션과 일치하는가, 그리고
  driftmq의 transport-level retry와 taskrail의 application-level retry가 `docs/failure-model.md`의
  경계대로 중복 없이 동작하는가

## 점진적 QA — 왜 끝에 몰아서 하지 않는가

`taskrail-module-builder`의 구현 순서(core → queue-memory → job runtime → CLI →
queue-driftmq)를 따라가며, 각 단계가 끝날 때마다 바로 그 모듈을 검증한다. core의 `JobQueue` 인터페이스
설계 결함은 뒤의 모든 adapter로 전파되므로, 1단계(core) 검증을 가장 꼼꼼히 한다. 6단계를 다 만든
뒤에 한 번에 테스트하면 초기 단계의 설계 실수가 늦게 발견되어 되돌릴 범위가 커진다.

## MVP 완료 기준 (전체 구현 완료 후 실제 실행으로 검증)

코드를 읽고 판단하는 것으로 끝내지 않는다 — 실제로 실행해서 확인한다:

1. job을 정의(`taskrail.job()`)하고 enqueue → memory adapter로 성공 실행 → SUCCESS로 기록되는지 확인
2. handler가 계속 실패하도록 만들었을 때, 설정된 retry 횟수만큼 backoff를 거쳐 재시도되고 소진되면
   DEAD로 기록되는지 확인
3. handler가 timeout을 초과했을 때 taskrail이 감지해 실패로 처리하는지 확인
4. worker concurrency를 N으로 설정했을 때 동시 실행 job이 실제로 N개를 넘지 않는지(여러 job을
   동시에 투입해 확인)
5. delayed job이 지정한 지연 시간 이전에는 실행되지 않는지 확인
6. jobId로 상태/이력 조회 시 현재 상태와 전이 이력이 정확히 나오는지 확인
7. taskrail 프로세스를 중간에 재시작해도(graceful shutdown 경로) 진행 중이던 job 상태가 유실되지
   않는지 실제로 프로세스를 재기동해서 확인
8. driftmq adapter가 구현되었다면, 1~7번 시나리오를 driftmq adapter로도 반복해 memory adapter와
   동작이 일치하는지 확인 — `JobQueue` 추상화가 실제로 새지 않는지의 최종 검증

## 버그 리포트

문제를 발견하면 builder에게: 어떤 두 모듈/파일 사이의 경계면인지, 어떤 값이 어떻게 불일치하는지,
파일:줄번호, 그리고 그 불일치가 실제로 어떤 입력에서 어떤 실패로 이어지는지(구체적 시나리오)를
포함해 전달한다. "버그가 있다"는 서술이 아니라 재현 가능한 시나리오로 전달해야 builder가 바로
고칠 수 있다.
