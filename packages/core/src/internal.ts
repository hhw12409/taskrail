/**
 * Public API가 아니다. 같은 모노레포의 다른 `@taskrail/*` 패키지가 재사용하는 부품만 모은다.
 * semver 계약 없이 바뀔 수 있다.
 */

export { JobRegistry } from "./job/registry.js";
export type { RegisteredJob } from "./job/registry.js";

export {
  DEFAULT_QUEUE,
  DEFAULT_MAX_ATTEMPTS,
  DEFAULT_TIMEOUT_MS,
} from "./job/definition.js";

export {
  DEFAULT_BACKOFF,
  computeBackoffDelay,
  isSerializableBackoff,
  assertSerializableBackoff,
} from "./job/backoff.js";
export type { SerializableBackoffPolicy } from "./job/backoff.js";

export {
  JOB_STATUSES,
  TERMINAL_STATUSES,
  isTerminal,
  canTransition,
} from "./job/status.js";

export { isJobEnvelope, nextAttemptEnvelope } from "./queue/envelope.js";

export {
  buildEnvelope,
  resolveJobOptions,
  resolveNotBefore,
  assertJsonSerializable,
} from "./runtime/enqueuer.js";
export type { ResolvedJobOptions, BuildEnvelopeInput } from "./runtime/enqueuer.js";

export {
  WorkerImpl,
  resolveWorkerOptions,
  DEFAULT_CONCURRENCY,
  DEFAULT_INLINE_DELAY_MS,
  DEFAULT_REQUEUE_INTERVAL_MS,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
} from "./runtime/worker.js";
export type { ResolvedWorkerOptions, WorkerDeps } from "./runtime/worker.js";

export { EventBus } from "./events/bus.js";
export type { TaskrailEventName, TaskrailEventPayload } from "./events/types.js";

export { describeError, isFatalJobError } from "./support/errors.js";
export { noopLogger, withContext } from "./support/logger.js";
export { ulid } from "./support/ulid.js";
