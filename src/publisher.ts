import { ServiceBusClient, ServiceBusMessage } from "@azure/service-bus";
import { CONNECTION_STRING, TOPIC } from "./config.js";

interface DesignChangeEvent {
  changeId: string; // used as the idempotency / dedupe key
  project: string;
  file: string;
  revision: number;
  summary: string;
  poison?: boolean; // makes the downstream-checks handler throw
}

const events: DesignChangeEvent[] = [
  { changeId: "chg-001", project: "apollo", file: "layout-v46.json", revision: 46, summary: "Header spacing updated" },
  { changeId: "chg-002", project: "apollo", file: "layout-v47.json", revision: 47, summary: "Nav component resized" },
  { changeId: "chg-003", project: "orion", file: "schema-v12.json", revision: 12, summary: "Renamed field user_id" },
  { changeId: "chg-BAD", project: "orion", file: "config-v13.json", revision: 13, summary: "Recompute derived values", poison: true },
  { changeId: "chg-004", project: "orion", file: "tokens-v14.json", revision: 14, summary: "Updated color tokens" },
];

async function main() {
  const client = new ServiceBusClient(CONNECTION_STRING);
  const sender = client.createSender(TOPIC);

  try {
    const messages: ServiceBusMessage[] = events.map((e) => ({
      body: e,
      messageId: e.changeId, // lets Service Bus and handlers recognise duplicates
      // sessionId: e.project, // would give ordered, per-key processing
      applicationProperties: { project: e.project, revision: e.revision },
    }));

    await sender.sendMessages(messages);
    console.log(`published ${messages.length} events to topic "${TOPIC}"`);
    for (const e of events) {
      console.log(`  ${e.changeId}  rev${e.revision}  ${e.file}${e.poison ? "  <- poison" : ""}`);
    }
  } finally {
    await sender.close();
    await client.close();
  }
}

main().catch((err) => {
  console.error("publisher failed:", err);
  process.exit(1);
});
