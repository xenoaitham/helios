"""(lsn, pk) upsert semantics against a real scratch database (cdc_test).

These are the replay-safety guarantees the DoD names: any re-delivery or
reordering of the same event stream must converge to the same raw state.
"""

from __future__ import annotations

import json

import pytest

from cdc.events import Event
from cdc.upsert import apply_events

TOPIC = "helios.public.users"


def _event(lsn: int, op: str, after=None, before=None, table: str = "users", user_id: int = 1, ts_ms: int = 1000) -> Event:
    return Event(
        topic=f"helios.public.{table}", table=table, pk={"user_id": user_id},
        lsn=lsn, op=op, ts_ms=ts_ms, before=before, after=after,
    )


def _fetch(tconn, table: str, user_id: int = 1):
    pk = json.dumps({"user_id": user_id})
    if table == "users":
        return tconn.execute("SELECT op, lsn, ts_ms, after FROM raw.cdc_users WHERE pk = %s::jsonb", (pk,)).fetchone()
    if table == "orders":
        return tconn.execute("SELECT op, lsn, ts_ms, after FROM raw.cdc_orders WHERE pk = %s::jsonb", (pk,)).fetchone()
    if table == "order_items":
        return tconn.execute("SELECT op, lsn, ts_ms, after FROM raw.cdc_order_items WHERE pk = %s::jsonb", (pk,)).fetchone()
    if table == "payments":
        return tconn.execute("SELECT op, lsn, ts_ms, after FROM raw.cdc_payments WHERE pk = %s::jsonb", (pk,)).fetchone()
    raise AssertionError(f"unknown table {table}")


def test_insert_then_row_lands(tconn):
    applied, coalesced, rejected = apply_events(tconn, [_event(100, "r", after={"user_id": 1, "email": "v1@x.io"})])
    assert (applied, coalesced, rejected) == (1, 0, 0)
    row = _fetch(tconn, "users")
    assert row[0] == "r"
    assert row[1] == 100
    assert row[3] == {"user_id": 1, "email": "v1@x.io"}


def test_replay_same_event_is_idempotent(tconn):
    events = [_event(100, "r", after={"user_id": 1, "email": "v1@x.io"})]
    apply_events(tconn, events)
    before = _fetch(tconn, "users")
    applied, _, _ = apply_events(tconn, events)  # exact same event re-delivered
    assert applied == 1  # equal-lsn update executes but converges to same state
    row = _fetch(tconn, "users")
    assert (row[0], row[1], row[3]) == (before[0], before[1], before[3])


def test_newer_lsn_replaces(tconn):
    apply_events(tconn, [_event(100, "r", after={"user_id": 1, "email": "v1@x.io"})])
    applied, _, _ = apply_events(tconn, [_event(200, "u", before={"user_id": 1}, after={"user_id": 1, "email": "v2@x.io"})])
    assert applied == 1
    row = _fetch(tconn, "users")
    assert row[0] == "u" and row[1] == 200 and row[3]["email"] == "v2@x.io"


def test_stale_lsn_never_clobbers(tconn):
    apply_events(tconn, [_event(200, "u", after={"user_id": 1, "email": "v2@x.io"})])
    applied, coalesced, rejected = apply_events(tconn, [_event(100, "r", after={"user_id": 1, "email": "v1@x.io"})])
    assert (applied, coalesced, rejected) == (0, 0, 1)
    row = _fetch(tconn, "users")
    assert row[1] == 200 and row[3]["email"] == "v2@x.io"


def test_delete_lands_tombstone_and_stale_insert_rejected(tconn):
    apply_events(tconn, [_event(300, "d", before={"user_id": 1})])
    row = _fetch(tconn, "users")
    assert row[0] == "d" and row[3] is None
    # an older INSERT replaying after the delete must not resurrect the row
    applied, _, _ = apply_events(tconn, [_event(100, "r", after={"user_id": 1, "email": "v1@x.io"})])
    assert applied == 0
    assert _fetch(tconn, "users")[0] == "d"
    # but a genuinely newer event (e.g. re-issue) still applies
    apply_events(tconn, [_event(400, "c", after={"user_id": 1, "email": "new@x.io"})])
    assert _fetch(tconn, "users")[0] == "c"


def test_shuffled_replay_converges(tconn):
    stream = [
        _event(100, "r", after={"user_id": 1, "email": "v1@x.io"}),
        _event(200, "u", after={"user_id": 1, "email": "v2@x.io"}),
        _event(300, "u", after={"user_id": 1, "email": "v3@x.io"}),
        _event(400, "d", before={"user_id": 1, "email": "v3@x.io"}),
    ]
    apply_events(tconn, list(reversed(stream)))  # worst case: reverse order
    apply_events(tconn, stream)  # then the true order again
    apply_events(tconn, [stream[1], stream[3], stream[0], stream[2]])
    row = _fetch(tconn, "users")
    assert row[0] == "d" and row[1] == 400 and row[3] is None


def test_snapshot_low_lsn_does_not_override_streaming(tconn):
    # snapshot 'r' rows carry the snapshot-start LSN; a streaming event that
    # arrives (or replays) with a higher LSN always wins
    apply_events(tconn, [_event(500, "u", after={"user_id": 1, "email": "live@x.io"})])
    apply_events(tconn, [_event(410, "r", after={"user_id": 1, "email": "snapshot@x.io"})])
    assert _fetch(tconn, "users")[3]["email"] == "live@x.io"


def test_distinct_pks_do_not_interfere(tconn):
    apply_events(tconn, [
        _event(100, "r", after={"user_id": 1, "email": "a@x.io"}),
        _event(100, "r", after={"user_id": 2, "email": "b@x.io"}, user_id=2),
        _event(150, "u", after={"user_id": 2, "email": "b2@x.io"}, user_id=2),
    ])
    assert _fetch(tconn, "users", 1)[3]["email"] == "a@x.io"
    assert _fetch(tconn, "users", 2)[1] == 150


def test_batch_mixed_tables(tconn):
    events = [
        _event(100, "r", after={"user_id": 1, "email": "a@x.io"}),
        _event(100, "r", after={"order_id": 10, "total_amount": "42.10"}, table="orders"),
        _event(100, "r", after={"payment_id": 20, "amount": "9.99"}, table="payments"),
    ]
    applied, coalesced, rejected = apply_events(tconn, events)
    assert (applied, coalesced, rejected) == (3, 0, 0)
    assert tconn.execute("SELECT count(*) FROM raw.cdc_orders").fetchone()[0] == 1
    assert tconn.execute("SELECT count(*) FROM raw.cdc_payments").fetchone()[0] == 1


def test_batch_atomicity_on_error(tconn):
    # one oversized numeric in a bad event poisons the batch; the transaction
    # rolls back so the good event in the same batch is NOT partially applied
    good = _event(100, "r", after={"user_id": 1, "email": "a@x.io"})
    bad = Event(
        topic=TOPIC, table="users", pk={"user_id": 2}, lsn="not-a-number",
        op="r", ts_ms=1, before=None, after={"user_id": 2},
    )
    with pytest.raises(Exception):
        with tconn.transaction():
            apply_events(tconn, [good, bad])
    assert tconn.execute("SELECT count(*) FROM raw.cdc_users").fetchone()[0] == 0


def test_transaction_block_is_a_real_commit(tconn):
    # Session-3 CRITIC lesson: on an autocommit connection, transaction() must
    # COMMIT (not become a savepoint inside an implicit tx) — the row must
    # survive the connection being closed and re-opened.
    with tconn.transaction():
        apply_events(tconn, [_event(100, "r", after={"user_id": 1, "email": "persist@x.io"})])
    tconn.rollback()  # would erase a savepoint-only write; a real commit survives
    assert tconn.execute("SELECT count(*) FROM raw.cdc_users").fetchone()[0] == 1


def test_same_pk_twice_in_one_batch_coalesces(tconn):
    # A single poll can carry several events for one PK (snapshot chunk +
    # concurrent update, or back-to-back mutator ticks). Postgres refuses an
    # INSERT ... ON CONFLICT DO UPDATE that hits the same row twice, so the
    # batch must be coalesced keeping the LAST event in stream order.
    applied, coalesced, rejected = apply_events(tconn, [
        _event(100, "r", after={"user_id": 1, "email": "v1@x.io"}),
        _event(200, "u", after={"user_id": 1, "email": "v2@x.io"}),
    ])
    assert (applied, coalesced, rejected) == (1, 1, 0)
    row = _fetch(tconn, "users")
    assert row[0] == "u" and row[1] == 200 and row[3]["email"] == "v2@x.io"


def test_delete_as_last_event_in_batch_wins(tconn):
    applied, coalesced, _ = apply_events(tconn, [
        _event(100, "u", after={"user_id": 1, "email": "v1@x.io"}),
        _event(200, "d", before={"user_id": 1, "email": "v1@x.io"}),
    ])
    assert (applied, coalesced) == (1, 1)
    row = _fetch(tconn, "users")
    assert row[0] == "d" and row[3] is None
