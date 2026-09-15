## 하네스: taskrail 개발

**목표:** taskrail을 driftmq에 종속되지 않는 독립적인 **Background Job Runtime**(`JobQueue` 인터페이스 + memory/driftmq adapter)으로 설계·구현한다. 코드보다 architecture/Public API 문서를 먼저 확정해 사용자 리뷰를 통과시킨 뒤에만 구현을 시작하고, `JobQueue` 인터페이스와 adapter 사이의 경계면을 중심으로 점진적으로 검증한다. 생태계 전체 설계 원문: `docs/reference/ecosystem-architecture-source.md` (DriftMQ=Message Broker / Taskrail=Job Runtime / Beacon=Realtime Delivery 3-프로젝트 생태계, 이 저장소는 Taskrail만 다룸).

**트리거:** taskrail 개발/설계/구현/재실행/부분 수정/검증 관련 요청 시 `taskrail-dev` 스킬을 사용하라. 단순 질문은 직접 응답 가능.

**변경 이력:**
| 날짜 | 변경 내용 | 대상 | 사유 |
|------|----------|------|------|
| 2026-09-15 | 초기 구성 (driftmq-investigator, taskrail-builder, taskrail-qa 3인 팀 + taskrail-dev 오케스트레이터) | 전체 | 그린필드 프로젝트 하네스 구축 |
| 2026-09-15 | 전면 재구성: taskrail 정체성을 "driftmq 위 선형 파이프라인 상태 추적기"에서 "독립 Background Job Runtime"으로 재정의. driftmq-investigator→driftmq-adapter-investigator로 재편(전체 MVP 게이트가 아닌 driftmq adapter 구현 시에만 관여), taskrail-architect 신규 추가(설계 문서 우선 원칙), taskrail-builder/taskrail-qa 재작성(JobQueue 인터페이스+adapter 경계면 중심). 스킬도 전부 재작성하고 taskrail-architecture-design 신규 추가. 구버전 스펙은 `docs/archive/taskrail-mvp-spec-v1-linear-pipeline.md`로 보존, driftmq API 조사 자산은 `_workspace/driftmq_adapter/00_findings.md`로 재배치해 재사용 | 전체 (agents/, skills/, docs/) | 사용자가 공유한 새 생태계 아키텍처 설계 문서(DriftMQ/Taskrail/Beacon 3-프로젝트 분리, taskrail=독립 Job Runtime)에 맞춰 재구성 요청 |
