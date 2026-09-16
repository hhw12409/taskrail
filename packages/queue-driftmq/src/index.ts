export { DriftmqQueue } from "./queue/driftmq-queue.js";
export type { DriftmqQueueOptions } from "./queue/options.js";
export {
  DEFAULT_CONNECT_TIMEOUT_MS,
  DEFAULT_POLL_INTERVAL_MS,
  DEFAULT_REQUEST_TIMEOUT_MS,
  DEFAULT_TOPIC_PREFIX,
} from "./queue/options.js";

export { brokerDlqTopic, deadTopic, jobTopic } from "./queue/topics.js";
export { DEAD_HEADERS } from "./dead/dead-letter.js";

export {
  DRIFTMQ_HEADERS,
  RESERVED_HEADER_PREFIX,
  readAttemptCount,
  readBrokerDeadLetterInfo,
} from "./message/reserved.js";
export type { BrokerDeadLetterInfo } from "./message/reserved.js";

export {
  BrokerError,
  ConnectionError,
  DriftmqError,
  ProtocolError,
  isBrokerError,
} from "./support/errors.js";
export { ErrorCode } from "./wire/protocol.js";
export { isValidTopic } from "./wire/topic.js";
