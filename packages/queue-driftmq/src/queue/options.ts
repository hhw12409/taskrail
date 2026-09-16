import { TaskrailError } from "@taskrail/core";
import type { Logger } from "@taskrail/core";
import { noopLogger } from "@taskrail/core/internal";

import { assertValidTopic } from "../wire/topic.js";

export const DEFAULT_TOPIC_PREFIX = "taskrail";
export const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
export const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
export const DEFAULT_POLL_INTERVAL_MS = 250;

export interface DriftmqQueueOptions {
  host: string;
  port: number;
  connectTimeoutMs?: number;
  /** 토픽 접두사. 기본 "taskrail" → 큐 "default"는 토픽 "taskrail.default". */
  topicPrefix?: string;
  /** consumer group id. 기본 "taskrail-<queue>". */
  consumerGroup?: string;
  /** 시작 시 토픽이 없으면 생성. 끄면 토픽이 없을 때 UNKNOWN_TOPIC이 그대로 올라온다. */
  autoCreateTopics?: boolean;
  /** 요청 하나가 응답을 기다리는 한계. 넘으면 커넥션을 버리고 재연결한다. */
  requestTimeoutMs?: number;
  /** FETCH가 빈 결과를 돌려줬을 때 다음 폴링까지의 간격. */
  pollIntervalMs?: number;
  logger?: Logger;
}

export interface ResolvedDriftmqOptions {
  readonly host: string;
  readonly port: number;
  readonly connectTimeoutMs: number;
  readonly topicPrefix: string;
  readonly consumerGroup: string | undefined;
  readonly autoCreateTopics: boolean;
  readonly requestTimeoutMs: number;
  readonly pollIntervalMs: number;
  readonly logger: Logger;
}

export function resolveOptions(options: DriftmqQueueOptions): ResolvedDriftmqOptions {
  if (typeof options?.host !== "string" || options.host.length === 0) {
    throw new TaskrailError("host는 비어 있지 않은 문자열이어야 한다");
  }
  if (!Number.isInteger(options.port) || options.port < 1 || options.port > 65535) {
    throw new TaskrailError("port는 1~65535 범위의 정수여야 한다");
  }

  const topicPrefix = options.topicPrefix ?? DEFAULT_TOPIC_PREFIX;
  assertValidTopic(topicPrefix, "topicPrefix");

  return {
    host: options.host,
    port: options.port,
    connectTimeoutMs: positive(options.connectTimeoutMs, DEFAULT_CONNECT_TIMEOUT_MS, "connectTimeoutMs"),
    topicPrefix,
    consumerGroup: options.consumerGroup,
    autoCreateTopics: options.autoCreateTopics ?? true,
    requestTimeoutMs: positive(options.requestTimeoutMs, DEFAULT_REQUEST_TIMEOUT_MS, "requestTimeoutMs"),
    pollIntervalMs: positive(options.pollIntervalMs, DEFAULT_POLL_INTERVAL_MS, "pollIntervalMs"),
    logger: options.logger ?? noopLogger,
  };
}

function positive(value: number | undefined, fallback: number, name: string): number {
  const resolved = value ?? fallback;
  if (!Number.isFinite(resolved) || resolved <= 0) {
    throw new TaskrailError(`${name}는 0보다 큰 유한한 숫자여야 한다`);
  }
  return resolved;
}
