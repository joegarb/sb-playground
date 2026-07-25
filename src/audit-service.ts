import { ServiceBusReceivedMessage } from "@azure/service-bus";
import { runConsumer } from "./consumer.js";

// Idempotent, append-only audit log. Never throws, so it never dead-letters.
// At-least-once delivery means the same event can arrive twice, so dedupe on messageId.
const processed = new Set<string>();

async function handleAuditLog(msg: ServiceBusReceivedMessage) {
  const key = msg.messageId as string;
  if (processed.has(key)) {
    console.log(`[audit] duplicate ${key} ignored (idempotent)`);
    return;
  }
  processed.add(key);
  const body = msg.body as { file: string; revision: number; summary: string };
  console.log(`[audit] appended ${key}: rev${body.revision} ${body.file} — ${body.summary}`);
}

runConsumer("audit-log", handleAuditLog);
