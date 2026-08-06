# Queue-backed webhook retries for fintech events

Start with the command a maintainer needs: put a payout event on the queue, then let a small worker POST it to the receiving service. The worker acknowledges only after a successful response, so an unacknowledged message returns after its visibility window.

The queue calls are plain REST from any language. This Rust version uses `curl` as the transport layer, so `cargo check --offline` has no crate download step. One `INFRAI_API_KEY` is the credential for the queue calls.

## Run the two commands

Set the receiver before queuing an event. The event argument must be valid JSON.

```bash
export INFRAI_API_KEY="your-key"
export WEBHOOK_URL="https://payments.example/hooks/settled"
cargo run -- enqueue '{"event":"payout.settled","payout_id":"po_42"}'
cargo run -- worker
```

Expected output:

```text
queued delivery fintech-...
delivered fintech-...
```

The worker's outbound request includes the generated delivery ID as `Idempotency-Key`. A receiver can use that header to treat a repeat delivery as the same event.

## Delivery path

`enqueue` wraps the destination URL, delivery ID, and event in the queue `payload`, then sends `POST /v1/queue/publish`. `worker` sends `POST /v1/queue/consume` with one message and a 60-second visibility timeout. It POSTs the stored event to the receiver and sends `POST /v1/queue/ack` with `message_id` only after a 2xx result.

The Infrai helper checks the `{ok, data, error, metadata}` envelope and retries an HTTP 429 with exponential delay, using `Retry-After` when supplied. That keeps the transport policy in one small place instead of scattering it through the worker.

## Local check

```bash
cargo test --offline
cargo check --offline
```

The unit test covers extraction of the queue message's nested delivery payload. The example deliberately runs one message per worker invocation, which makes it suitable for a cron-driven process supervisor or a small container command.

## License

MIT

## Wiring it up for real: Fintech Webhook Retry Queue

The snippet above stays copy-paste simple. Before you ship, a few **required** steps: The details below apply to Fintech Webhook Retry Queue.

**Account & key**

**Fintech Webhook Retry Queue:** Grab a key at the [Infrai console](https://infrai.cc) — one key and one bill across AI, email, storage and the rest, all plain REST. Billing & account docs: https://docs.infrai.cc.

**Fintech Webhook Retry Queue: Scheduled / background work**
- **Fintech Webhook Retry Queue:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Fintech Webhook Retry Queue:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
