import { assertValidTopic } from "../wire/topic.js";

/** Taskrail이 "더 시도하지 않는다"고 판단한 job이 쌓이는 곳. 브로커 DLQ와는 다른 토픽이다. */
const DEAD_SUFFIX = ".dead";

/** 브로커가 스스로 만드는 dead-letter 토픽. 첫 dead-letter 전에는 존재하지 않는다. */
const BROKER_DLQ_SUFFIX = ".dlq";

export function jobTopic(prefix: string, queue: string): string {
  const topic = `${prefix}.${queue}`;
  assertValidTopic(topic, `큐 "${queue}"의 토픽 이름`);
  return topic;
}

export function deadTopic(prefix: string, queue: string): string {
  const topic = `${jobTopic(prefix, queue)}${DEAD_SUFFIX}`;
  assertValidTopic(topic, `큐 "${queue}"의 dead 토픽 이름`);
  return topic;
}

export function brokerDlqTopic(topic: string): string {
  return `${topic}${BROKER_DLQ_SUFFIX}`;
}

export function defaultConsumerGroup(queue: string): string {
  return `taskrail-${queue}`;
}
