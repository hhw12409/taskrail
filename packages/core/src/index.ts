/**
 * 여기서 export되는 것이 사용자가 보는 표면 전부다.
 * core 내부 부품은 `@taskrail/core/internal`에 있으며 Public API 계약이 아니다.
 */

export { Taskrail } from "./runtime/taskrail.js";
export type { TaskrailOptions } from "./runtime/taskrail.js";

export type {
  JobHandler,
  JobDefinition,
  JobHandle,
  JobOptions,
  EnqueueOptions,
  JobContext,
} from "./job/definition.js";

export type { Jitter, BackoffPolicy } from "./job/backoff.js";

export type { Worker, WorkerOptions } from "./runtime/worker.js";

export { FatalJobError, TimeoutError, TaskrailError } from "./support/errors.js";

export type { JobStatus } from "./job/status.js";
export type { JobSnapshot, JobStateStore } from "./job/snapshot.js";
export { MemoryStateStore } from "./job/snapshot.js";

export type { TaskrailEvents } from "./events/types.js";

export type { Logger } from "./support/logger.js";

export type {
  JobQueue,
  QueueCapabilities,
  ConsumeOptions,
  Subscription,
  Delivery,
  DeliveryReceipt,
  DeadLetterReason,
} from "./queue/queue.js";

export type { JobEnvelope } from "./queue/envelope.js";
export { ENVELOPE_VERSION } from "./queue/envelope.js";

export { WIRE_HEADERS, toWireHeaders } from "./queue/headers.js";
export type { WireHeaderKey } from "./queue/headers.js";
