import { ServiceBusClient, ServiceBusReceivedMessage } from "@azure/service-bus";
import { CONNECTION_STRING, TOPIC } from "./config.js";

// Shared receive loop. Settlement policy lives here: peekLock, complete on success,
// abandon on throw (-> redeliver -> auto-DLQ once deliveryCount exceeds MaxDeliveryCount).
export type MessageHandler = (msg: ServiceBusReceivedMessage) => Promise<void>;

export function runConsumer(subscription: string, handle: MessageHandler): void {
  const client = new ServiceBusClient(CONNECTION_STRING);
  const receiver = client.createReceiver(TOPIC, subscription, { receiveMode: "peekLock" });

  console.log(`listening on ${TOPIC}/${subscription} ... (Ctrl+C to stop)`);

  receiver.subscribe(
    {
      processMessage: async (msg) => {
        try {
          await handle(msg);
          await receiver.completeMessage(msg);
        } catch (err) {
          const attempt = msg.deliveryCount ?? 0;
          console.warn(
            `[${subscription}] attempt ${attempt} failed for ${msg.messageId}: ${(err as Error).message}`
          );
          await receiver.abandonMessage(msg);
        }
      },
      processError: async (args) => {
        console.error(`[${subscription}] receiver error:`, args.error.message);
      },
    },
    { autoCompleteMessages: false } // settle explicitly, not via the SDK
  );

  process.on("SIGINT", async () => {
    console.log("\nclosing...");
    await receiver.close();
    await client.close();
    process.exit(0);
  });
}
