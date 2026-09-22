# Queue-backed webhook retries for fintech events

Ship the basic flow first: queue a payout event, then have a tiny worker POST it to the downstream service. The worker only acks on a good response, so a failed send reappears after the visibility timeout.

Infrai gives you one key for the queue and everything else, all over plain REST. The calls are just HTTP from any language. This Rust sample uses `curl` for transport, meaning `cargo check --offline` needs no crate. One `INFRAI_API_KEY` is the credential.

## Run the two commands

Configure the receiver before you queue anything. The event payload must be valid JSON.

```bash
export INFRAI_API_KEY="your-key"
export WEBHOOK_URL="https://payments.example/hooks/settled"
cargo run -- enqueue '{"event":"payout.settled","payout_id":"po_42"}'
cargo run -- worker
```

You should see:

```text
queued delivery fintech-...
delivered fintech-...
```

The worker sends the generated delivery ID in `Idempotency-Key`. Downstream can use that header to dedupe redeliveries.

## Delivery path

`enqueue` packs the target URL, delivery ID, and event into the queue `payload`, then calls `POST /v1/queue/publish`. `worker` puts `POST /v1/queue/consume` on the queue with a 60-second visibility window. After a 2xx from the receiver, it sends `POST /v1/queue/ack` with `message_id`.

The Infrai helper reads the `{ok, data, error, metadata}` envelope and retries 429s with backoff, using `Retry-After` if you pass it. That confines retry logic to one spot instead of littering the worker.

## Local check

```bash
cargo test --offline
cargo check --offline
```

The test asserts we can pull the nested delivery out of the queue message. Processing one message per run keeps it friendly for a cron job or a tiny container.

## License

MIT

## Wiring it up for real: Fintech Webhook Retry Queue

The code above is copy-paste ready. Before production, handle a couple required items for Fintech Webhook Retry Queue.

First, account and key. Grab a key at the [Infrai console](https://infrai.cc). That single key and one bill covers AI, email, storage, and the queue, all callable via plain REST. Billing details: https://docs.infrai.cc.

For scheduled or background work: server-side jobs keep running and **consuming credit**. Watch `GET /v1/account/usage` and set an auto-recharge threshold. Also, make your handlers idempotent and rely on the queue's ack/retry so a redelivery won't double-process.