# Azure Service Bus playground

A small, runnable demo of the Azure Service Bus **topic → subscriptions** model, using
the official Service Bus emulator. One event is published; multiple independent
subscriptions react. It demonstrates fan-out, at-least-once delivery + idempotency,
poison-message dead-lettering, and a DLQ replay/drain tool.

The example domain: a *design change* is published once; an **audit-log** subscription
records it, and a **downstream-checks** subscription recomputes checks (and fails on a
poison message).

```
                          ┌─────────────────────────┐
  publisher ──publish──▶  │  topic: design-changes  │
                          └───────────┬─────────────┘
                                      │ (fan-out — each subscription gets its own copy)
                    ┌─────────────────┴──────────────────┐
                    ▼                                     ▼
          subscription: audit-log            subscription: downstream-checks
          (idempotent append,                (recomputes checks; poison msg
           always completes)                  fails 3× → dead-letter queue)
```

## Prerequisites

- Docker Desktop running
- Node 20+

## Setup

```bash
npm install
cp .env.example .env
docker compose up         # starts the emulator; topic + subscriptions come from config.json
```

Leave the emulator running (Ctrl+C to stop). Its storage is in-memory — restarting
resets all messages.

## Run

In separate terminals:

```bash
npm run audit     # consumer on the audit-log subscription
npm run checks    # consumer on the downstream-checks subscription
npm run publish   # publishes 5 events, one of them a poison message
```

### What to observe

- **Fan-out** — one `publish`, and *both* `audit` and `checks` react. That's a topic vs. a
  plain queue.
- **Poison → DLQ** — `chg-BAD` fails in `checks` only. `deliveryCount` climbs 1→2→3, then
  Service Bus auto-moves it to the dead-letter queue. `audit` handles the same message
  fine — subscriptions are independent.
- **Idempotency** — run `npm run publish` twice. `audit` logs the second batch as
  duplicates and ignores them. At-least-once delivery means handlers must tolerate re-runs.

## Dead-letter queue tool

```bash
npm run dlq                                  # peek the DLQ (non-destructive)
npm run dlq -- audit-log                     # peek a different subscription's DLQ
npm run dlq -- downstream-checks --replay    # re-publish DLQ messages to the topic, then remove
npm run dlq -- downstream-checks --drain     # archive (log) DLQ messages, then remove
```

`--replay` and `--drain` snapshot the DLQ up front and process only that fixed set, so they
always terminate — even if a running consumer keeps re-dead-lettering a still-poison message
(otherwise you'd get an infinite replay loop).

## References

- [Azure Service Bus emulator](https://github.com/Azure/azure-service-bus-emulator-installer) — installer, Docker Compose templates, and the `config.json` schema.
- [Azure SDK for JS — Service Bus TypeScript samples](https://github.com/Azure/azure-sdk-for-js/tree/main/sdk/servicebus/service-bus/samples/v7/typescript)
- [Get started with Azure Service Bus topics (TypeScript)](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-typescript-how-to-use-topics-subscriptions) — Microsoft Learn
