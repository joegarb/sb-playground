import {
  ServiceBusClient,
  ServiceBusReceivedMessage,
  ServiceBusReceiver,
} from "@azure/service-bus";
import { CONNECTION_STRING, TOPIC } from "./config.js";

// Peek / replay / drain the dead-letter queue (the system sub-queue at
// <entity>/$DeadLetterQueue). Run as:
//   npm run dlq                                   # peek (default, non-destructive)
//   npm run dlq -- audit-log                      # peek another subscription's DLQ
//   npm run dlq -- downstream-checks --replay     # re-publish to the topic, then remove
//   npm run dlq -- downstream-checks --drain      # archive to log, then remove
const subscription = process.argv[2] ?? "downstream-checks";
const replay = process.argv.includes("--replay");
const drain = process.argv.includes("--drain");

if (replay && drain) {
  console.error("pass --replay OR --drain, not both");
  process.exit(1);
}

function describe(msg: ServiceBusReceivedMessage): string {
  const body = msg.body as { file?: string; revision?: number } | undefined;
  return [
    `  ${msg.messageId}`,
    body?.revision !== undefined ? `rev${body.revision}` : "",
    body?.file ?? "",
    `— reason=${msg.deadLetterReason ?? "n/a"}`,
    `deliveryCount=${msg.deliveryCount}`,
    msg.deadLetterErrorDescription ? `(${msg.deadLetterErrorDescription})` : "",
  ]
    .filter(Boolean)
    .join("  ");
}

// Snapshot the DLQ up front and process only that fixed set. If a consumer is running and
// a message is still poison, replay re-dead-letters it — a "loop until empty" would chase
// it forever. Bounding to the snapshot guarantees termination.
async function drainSnapshot(
  receiver: ServiceBusReceiver,
  handle: (msg: ServiceBusReceivedMessage) => Promise<void>
): Promise<number> {
  const snapshot = await receiver.peekMessages(2000);
  const targets = new Set(snapshot.map((m) => m.sequenceNumber?.toString()));
  console.log(`processing ${targets.size} message(s) currently in the DLQ ...`);

  let done = 0;
  while (targets.size > 0) {
    const batch = await receiver.receiveMessages(Math.min(10, targets.size), {
      maxWaitTimeInMs: 5000,
    });
    if (batch.length === 0) break;
    for (const m of batch) {
      const sn = m.sequenceNumber?.toString();
      if (sn && targets.has(sn)) {
        await handle(m);
        await receiver.completeMessage(m);
        targets.delete(sn);
        done++;
      } else {
        await receiver.abandonMessage(m); // arrived after the snapshot — leave it
      }
    }
  }
  return done;
}

async function main() {
  const client = new ServiceBusClient(CONNECTION_STRING);
  const dlqReceiver = client.createReceiver(TOPIC, subscription, {
    subQueueType: "deadLetter",
  });

  try {
    if (!replay && !drain) {
      const msgs = await dlqReceiver.peekMessages(50); // peek: does not lock or remove
      console.log(`${TOPIC}/${subscription}/$DeadLetterQueue — ${msgs.length} message(s):`);
      if (msgs.length === 0) console.log("  (empty)");
      for (const m of msgs) console.log(describe(m));
      return;
    }

    if (replay) {
      const sender = client.createSender(TOPIC);
      const replayed = await drainSnapshot(dlqReceiver, async (m) => {
        await sender.sendMessages({ body: m.body, messageId: m.messageId as string });
        console.log(`  replayed ${m.messageId} back onto ${TOPIC}`);
      });
      await sender.close();
      console.log(`replayed ${replayed} message(s) from ${subscription} DLQ`);
      if (replayed > 0) {
        console.warn(
          `note: if the downstream consumer is still failing these, they'll re-dead-letter` +
            ` — re-check with: npm run dlq -- ${subscription}`
        );
      }
      return;
    }

    // --drain: give up on the messages. Log them as a record, then remove — nothing is
    // re-published, so there is no bounce-back.
    const drained = await drainSnapshot(dlqReceiver, async (m) => {
      console.log(`  archived + removed:${describe(m)}`);
    });
    console.log(`drained ${drained} message(s) from ${subscription} DLQ`);
  } finally {
    await dlqReceiver.close();
    await client.close();
  }
}

main().catch((err) => {
  console.error("dlq reader failed:", err);
  process.exit(1);
});
