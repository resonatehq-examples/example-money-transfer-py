"""Money Transfer — saga pattern with durable compensation.

A workflow that moves funds between two accounts in a SQLite ledger.

The transfer runs as two checkpointed steps: debit the source, credit the
target. If the credit fails, a compensating debit-reversal runs to restore
the source balance — the saga pattern, but written as straight-line code
because Resonate makes the steps durable.

Each step is idempotent: it inserts a ledger row keyed by a deterministic
operation id, so replaying the workflow after a crash never double-applies
an entry.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Generator

from resonate import Context, Resonate
from resonate.retry_policies import Never

DB_PATH = "./transfers.db"


# --- Ledger setup -----------------------------------------------------------


def setup_database(path: str = DB_PATH) -> sqlite3.Connection:
    """Open the SQLite ledger and create the transfers table if needed."""
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS transfers (
            uuid TEXT PRIMARY KEY,
            account TEXT NOT NULL,
            amount REAL NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    return conn


# --- Ledger operations (blocking steps) -------------------------------------


def apply_entry(
    ctx: Context,
    op_id: str,
    account: str,
    amount: float,
    note: str = "",
) -> str:
    """Apply a single ledger entry. Idempotent on `op_id`.

    The `INSERT OR IGNORE` clause means a replay of this step after a crash
    is a no-op — the row is already there, the balance is already correct.
    """
    db: sqlite3.Connection = ctx.get_dependency("db")
    cursor = db.execute(
        "INSERT OR IGNORE INTO transfers (uuid, account, amount, note) VALUES (?, ?, ?, ?)",
        (op_id, account, amount, note),
    )
    if cursor.rowcount == 0:
        print(f"  [ledger] {op_id} already applied (idempotent no-op)")
    else:
        sign = "+" if amount >= 0 else ""
        print(f"  [ledger] {op_id}: {account} {sign}{amount}  // {note}")
    return op_id


def get_balance(ctx: Context, account: str) -> float:
    """Compute an account's balance by summing its ledger entries."""
    db: sqlite3.Connection = ctx.get_dependency("db")
    row = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM transfers WHERE account = ?",
        (account,),
    ).fetchone()
    return float(row[0]) if row else 0.0


# --- The saga ---------------------------------------------------------------


class TransferRejected(Exception):
    """Raised when the credit leg fails and the saga must compensate."""


def credit_target(
    ctx: Context,
    op_id: str,
    target: str,
    amount: float,
    *,
    fail: bool = False,
) -> str:
    """Credit the target account. Pass `fail=True` to simulate a failure."""
    if fail:
        raise TransferRejected(f"target account {target!r} rejected the credit")
    return apply_entry(ctx, op_id, target, amount, note="credit")


def transfer_money(
    ctx: Context,
    transfer_id: str,
    source: str,
    target: str,
    amount: float,
    *,
    simulate_credit_failure: bool = False,
) -> Generator[Any, Any, dict]:
    """Move `amount` from `source` to `target` as a saga.

    Steps:
      1. Debit the source account.
      2. Credit the target account.
      3. If (2) fails, run a compensating debit-reversal on the source.

    Each step is durable. If the worker crashes after step (1) but before
    step (2), Resonate replays the workflow, sees the source debit is
    already in the ledger (idempotent insert), and continues from there.
    """
    print(f"\n[saga] transfer {transfer_id}: {source} -> {target}  ${amount}")

    debit_id = f"{transfer_id}-debit"
    credit_id = f"{transfer_id}-credit"
    reversal_id = f"{transfer_id}-reversal"

    # Step 1 — debit the source (durable checkpoint).
    yield ctx.run(apply_entry, debit_id, source, -amount, "debit")

    # Step 2 — credit the target (durable checkpoint). On failure,
    # compensate by reversing the debit, then re-raise so the caller
    # sees the saga aborted.
    #
    # `retry_policy=Never()` is intentional here: this saga's compensation
    # IS the response to a credit-side failure. In production you might
    # use a few retries first (network blips happen) and only compensate
    # once the upstream has clearly rejected the credit.
    try:
        yield ctx.run(
            credit_target,
            credit_id,
            target,
            amount,
            fail=simulate_credit_failure,
        ).options(retry_policy=Never())
    except Exception as err:
        print(f"[saga] credit failed: {err}. Compensating...")
        # Compensating action — also durable + idempotent.
        yield ctx.run(apply_entry, reversal_id, source, amount, "reversal")
        return {
            "transfer_id": transfer_id,
            "status": "compensated",
            "error": str(err),
        }

    print(f"[saga] transfer {transfer_id} committed")
    return {
        "transfer_id": transfer_id,
        "status": "committed",
        "source": source,
        "target": target,
        "amount": amount,
    }


# --- Demo -------------------------------------------------------------------


def main() -> None:
    db = setup_database()

    # Local mode — no Resonate Server required to run the demo.
    # For a server-backed deployment, replace `Resonate.local()` with
    # `Resonate()` (auto-detects RESONATE_HOST_STORE) and run
    # `resonate serve` alongside this process.
    resonate = Resonate.local()
    resonate.set_dependency("db", db)

    transfer = resonate.register(transfer_money)

    # Seed the source account so it has something to send.
    db.execute(
        "INSERT OR IGNORE INTO transfers (uuid, account, amount, note) VALUES (?, ?, ?, ?)",
        ("seed-alice", "alice", 200.0, "seed"),
    )

    print(f"opening balances: alice={get_balance_direct(db, 'alice')} bob={get_balance_direct(db, 'bob')}")

    # --- happy path ---------------------------------------------------------
    result = transfer.run("transfer-001", "transfer-001", "alice", "bob", 50.0)
    print(f"result: {result}")

    # --- failure path: credit rejected, saga compensates --------------------
    result = transfer.run(
        "transfer-002",
        "transfer-002",
        "alice",
        "bob",
        75.0,
        simulate_credit_failure=True,
    )
    print(f"result: {result}")

    print(
        f"\nclosing balances: alice={get_balance_direct(db, 'alice')} bob={get_balance_direct(db, 'bob')}"
    )
    print("(transfer-002 was compensated, so alice ends at 200 - 50 = 150)")


def get_balance_direct(db: sqlite3.Connection, account: str) -> float:
    """Read a balance outside the workflow (for demo logging only)."""
    row = db.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM transfers WHERE account = ?",
        (account,),
    ).fetchone()
    return float(row[0]) if row else 0.0


if __name__ == "__main__":
    main()
