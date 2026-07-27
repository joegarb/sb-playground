import { ServiceBusReceivedMessage } from "@azure/service-bus";
import { runConsumer } from "./consumer.js";

// Only receives messages the broker routed here via the subscription's SQL filter
// (project = 'apollo' in config.json), which matches on applicationProperties — not
// the body. Reads the routing metadata straight off applicationProperties to show it.
async function handleApolloChanges(msg: ServiceBusReceivedMessage) {
  const { project, revision } = msg.applicationProperties ?? {};
  console.log(`[apollo] ${msg.messageId}  project=${project}  rev${revision}`);
}

runConsumer("apollo-changes", handleApolloChanges);
