import { ServiceBusReceivedMessage } from "@azure/service-bus";
import { runConsumer } from "../consumer.js";

// Recomputes checks on a design change. Can fail: throwing abandons the message, which
// drives the retry -> dead-letter path (this subscription's MaxDeliveryCount is 3).
async function handleDownstreamChecks(msg: ServiceBusReceivedMessage) {
  const body = msg.body as { file: string; poison?: boolean };
  if (body.poison) {
    throw new Error(`cannot recompute checks for ${body.file}`);
  }
  console.log(`[checks] recomputed checks for ${body.file}`);
}

runConsumer("downstream-checks", handleDownstreamChecks);
