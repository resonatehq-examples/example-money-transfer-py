<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/banner-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="./assets/banner-light.png">
    <img alt="Money Transfer — Resonate example" src="./assets/banner-dark.png">
  </picture>
</p>

# Money Transfer

**Resonate Python SDK**

Move funds between two accounts as a saga — debit, credit, and on a credit-side failure, run a compensating debit-reversal. Each step is durable and idempotent, so a worker crash mid-transfer never leaves the ledger in a half-applied state.

## What this example demonstrates

- **Saga pattern.** A multi-step business operation written as straight-line Python, with compensation triggered when a step fails.
- **Durable steps.** Each `ctx.run(...)` call is a checkpoint. If the worker crashes after the debit but before the credit, Resonate replays from the last successful checkpoint.
- **Idempotency.** Ledger entries are keyed by a deterministic operation id (`{transfer_id}-debit`, `{transfer_id}-credit`, `{transfer_id}-reversal`) and inserted with `INSERT OR IGNORE`. Replays apply the entry once and only once.
- **Explicit compensation.** When the credit step raises, the workflow catches the exception and runs the reversal — also durable, also idempotent.

## How the saga is wired

```text
                 transfer_money workflow
                        │
                        ▼
        ┌──────────────────────────────────┐
        │ 1. apply_entry  source  -amount  │   debit (checkpoint)
        └────────────────┬─────────────────┘
                         │
                         ▼
        ┌──────────────────────────────────┐
        │ 2. credit_target   target  amount│   credit (checkpoint)
        └────────────────┬─────────────────┘
                         │ on raise
                         ▼
        ┌──────────────────────────────────┐
        │ 3. apply_entry  source  +amount  │   compensating reversal
        └──────────────────────────────────┘
```

Each numbered box is a durable checkpoint. Steps 1 and 3 reuse the same `apply_entry` function — the difference is just the sign of the amount and the operation id.

## How to run

This example uses [uv](https://docs.astral.sh/uv/) for the Python environment.

Install dependencies:

```shell
uv sync
```

Run the demo:

```shell
uv run main.py
```

You'll see two transfers: one happy path that commits, and one where the credit is configured to fail so the saga compensates. The closing balances show that `alice` is left correctly debited only by the committed transfer.

Sample output:

```text
opening balances: alice=200.0 bob=0.0

[saga] transfer transfer-001: alice -> bob  $50.0
  [ledger] transfer-001-debit:  alice -50.0  // debit
  [ledger] transfer-001-credit: bob +50.0    // credit
[saga] transfer transfer-001 committed

[saga] transfer transfer-002: alice -> bob  $75.0
  [ledger] transfer-002-debit: alice -75.0   // debit
[saga] credit failed: target account 'bob' rejected the credit. Compensating...
  [ledger] transfer-002-reversal: alice +75.0 // reversal

closing balances: alice=150.0 bob=50.0
```

## Running against a Resonate Server

The demo runs in **local mode** (`Resonate.local()`) — no server required, useful for iterating on the workflow itself.

For a server-backed deployment that survives process restarts, swap the constructor:

```python
# Replace this:
resonate = Resonate.local()
# With this:
resonate = Resonate()  # auto-detects RESONATE_HOST / RESONATE_URL
```

Then start the legacy Resonate server in a separate terminal:

```shell
resonate serve --aio-store-sqlite-path ./resonate.db
```

The Python SDK currently speaks the legacy server protocol, so use `resonate serve` rather than `resonate dev`.

## Files

- [`main.py`](./main.py) — the saga workflow, the SQLite ledger helpers, and a small demo driver.
- [`pyproject.toml`](./pyproject.toml) — pins `resonate-sdk>=0.6.3`.

## How it works

### Idempotent ledger entries

```python
def apply_entry(ctx, op_id, account, amount, note=""):
    db = ctx.get_dependency("db")
    cursor = db.execute(
        "INSERT OR IGNORE INTO transfers (uuid, account, amount, note) "
        "VALUES (?, ?, ?, ?)",
        (op_id, account, amount, note),
    )
    return op_id
```

The `uuid` column is the table's primary key. `INSERT OR IGNORE` makes a replay of the same step a no-op — the row is already there, the balance already reflects it.

### The saga, written as straight-line code

```python
def transfer_money(ctx, transfer_id, source, target, amount, *, simulate_credit_failure=False):
    debit_id    = f"{transfer_id}-debit"
    credit_id   = f"{transfer_id}-credit"
    reversal_id = f"{transfer_id}-reversal"

    yield ctx.run(apply_entry, debit_id, source, -amount, "debit")

    try:
        yield ctx.run(
            credit_target, credit_id, target, amount,
            fail=simulate_credit_failure,
        ).options(retry_policy=Never())
    except Exception as err:
        yield ctx.run(apply_entry, reversal_id, source, amount, "reversal")
        return {"status": "compensated", "error": str(err)}

    return {"status": "committed", ...}
```

`retry_policy=Never()` is intentional on the credit step: this saga's compensation IS the response to a credit-side failure. In production you'd typically allow a few retries first (network blips happen) and only compensate after the upstream has clearly rejected the credit.

### Account balances

Balances aren't stored — they're computed by summing the ledger:

```sql
SELECT COALESCE(SUM(amount), 0) FROM transfers WHERE account = ?
```

This makes the ledger the single source of truth. A reversal entry undoes a debit by adding the equivalent positive amount; the saga's correctness reduces to "the ledger reflects every committed step."

## Related

- [example-money-transfer-application-ts](https://github.com/resonatehq-examples/example-money-transfer-application-ts) — the TypeScript port (HTTP API + same saga shape).
- [Resonate Python SDK](https://github.com/resonatehq/resonate-sdk-py)
- [Resonate docs](https://docs.resonatehq.io)
