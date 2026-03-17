from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
import redis
import redis.asyncio as redis_async
from bub_tapestore_redis import AsyncRedisTapeStore, RedisTapeStore
from redis.crc import key_slot
from redis.exceptions import RedisError
from republic.core.errors import ErrorKind
from republic.core.results import ErrorPayload
from republic.tape.context import LAST_ANCHOR, TapeContext
from republic.tape.entries import TapeEntry
from republic.tape.manager import AsyncTapeManager, TapeManager
from republic.tape.query import TapeQuery


def _redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6379/15")


def _unique_prefix() -> str:
    return f"test:bub-tapestore-redis:{uuid.uuid4().hex}"


def _seed_entries() -> list[TapeEntry]:
    return [
        TapeEntry.message({"role": "user", "content": "before"}),
        TapeEntry.anchor("a1"),
        TapeEntry.message({"role": "user", "content": "task 1"}),
        TapeEntry.message({"role": "assistant", "content": "answer 1"}),
        TapeEntry.anchor("a2"),
        TapeEntry.message({"role": "user", "content": "task 2"}),
    ]


def _connect_sync() -> redis.Redis:
    client = redis.Redis.from_url(_redis_url())
    client.ping()
    return client


async def _connect_async() -> redis_async.Redis:
    client = redis_async.Redis.from_url(_redis_url())
    await client.ping()
    return client


def _cleanup_sync(client: redis.Redis, prefix: str) -> None:
    keys = list(client.scan_iter(f"{prefix}:*"))
    keys.extend(client.scan_iter(f"{{{prefix}:*}}:*"))
    if keys:
        client.delete(*keys)


async def _cleanup_async(client: redis_async.Redis, prefix: str) -> None:
    keys = [key async for key in client.scan_iter(f"{prefix}:*")]
    keys.extend([key async for key in client.scan_iter(f"{{{prefix}:*}}:*")])
    if keys:
        await client.delete(*keys)


@pytest.fixture
def sync_store() -> RedisTapeStore:
    try:
        client = _connect_sync()
    except RedisError as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"Redis is unavailable: {exc}")

    prefix = _unique_prefix()
    store = RedisTapeStore(client, key_prefix=prefix)
    try:
        yield store
    finally:
        _cleanup_sync(client, prefix)
        client.close()


@pytest.fixture
async def async_store() -> AsyncRedisTapeStore:
    try:
        client = await _connect_async()
    except RedisError as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"Redis is unavailable: {exc}")

    prefix = _unique_prefix()
    store = AsyncRedisTapeStore(client, key_prefix=prefix)
    try:
        yield store
    finally:
        await _cleanup_async(client, prefix)
        await client.aclose()


def test_list_tapes_returns_sorted_names(sync_store: RedisTapeStore) -> None:
    sync_store.append("beta", TapeEntry.message({"role": "user", "content": "hello"}))
    sync_store.append("alpha", TapeEntry.message({"role": "assistant", "content": "world"}))

    assert sync_store.list_tapes() == ["alpha", "beta"]


def test_tape_local_keys_share_one_cluster_hash_slot(sync_store: RedisTapeStore) -> None:
    keys = [
        sync_store._keys.entries("session"),
        sync_store._keys.next_id("session"),
        sync_store._keys.anchors("session"),
    ]

    assert len({key_slot(key.encode("utf-8")) for key in keys}) == 1


def test_append_assigns_incrementing_ids_and_reset_restarts(sync_store: RedisTapeStore) -> None:
    sync_store.append("session", TapeEntry.message({"role": "user", "content": "first"}))
    sync_store.append("session", TapeEntry.message({"role": "assistant", "content": "second"}))

    first_pass = list(TapeQuery(tape="session", store=sync_store).all())
    assert [entry.id for entry in first_pass] == [1, 2]

    sync_store.reset("session")
    sync_store.append("session", TapeEntry.message({"role": "user", "content": "third"}))

    second_pass = list(TapeQuery(tape="session", store=sync_store).all())
    assert [entry.id for entry in second_pass] == [1]
    assert second_pass[0].payload["content"] == "third"


def test_fetch_all_matches_republic_query_contract(sync_store: RedisTapeStore) -> None:
    tape = "contract"
    for entry in _seed_entries():
        sync_store.append(tape, entry)

    entries = list(TapeQuery(tape=tape, store=sync_store).between_anchors("a1", "a2").kinds("message").limit(1).all())
    assert len(entries) == 1
    assert entries[0].payload["content"] == "task 1"


def test_query_text_matches_payload_and_meta(sync_store: RedisTapeStore) -> None:
    tape = "searchable"

    sync_store.append(tape, TapeEntry.message({"role": "user", "content": "Database timeout on checkout"}, scope="db"))
    sync_store.append(tape, TapeEntry.event("run", {"status": "ok"}, scope="system"))

    entries = list(TapeQuery(tape=tape, store=sync_store).query("timeout").all())
    assert [entry.kind for entry in entries] == ["message"]

    meta_entries = list(TapeQuery(tape=tape, store=sync_store).query("system").all())
    assert [entry.kind for entry in meta_entries] == ["event"]


def test_query_between_dates_is_inclusive(sync_store: RedisTapeStore) -> None:
    tape = "dated"

    sync_store.append(
        tape,
        TapeEntry(
            id=0,
            kind="message",
            payload={"role": "user", "content": "before"},
            date="2026-03-01T08:00:00+00:00",
        ),
    )
    sync_store.append(
        tape,
        TapeEntry(
            id=0,
            kind="message",
            payload={"role": "user", "content": "during"},
            date="2026-03-02T09:30:00+00:00",
        ),
    )
    sync_store.append(
        tape,
        TapeEntry(
            id=0,
            kind="message",
            payload={"role": "user", "content": "after"},
            date="2026-03-04T18:45:00+00:00",
        ),
    )

    entries = list(TapeQuery(tape=tape, store=sync_store).between_dates(date(2026, 3, 2), "2026-03-03").all())
    assert [entry.payload["content"] for entry in entries] == ["during"]


def test_query_combines_anchor_date_and_text_filters(sync_store: RedisTapeStore) -> None:
    tape = "combined"

    sync_store.append(
        tape,
        TapeEntry(id=0, kind="anchor", payload={"name": "a1"}, date="2026-03-01T00:00:00+00:00"),
    )
    sync_store.append(
        tape,
        TapeEntry(
            id=0,
            kind="message",
            payload={"role": "user", "content": "old timeout"},
            date="2026-03-01T12:00:00+00:00",
        ),
    )
    sync_store.append(
        tape,
        TapeEntry(id=0, kind="anchor", payload={"name": "a2"}, date="2026-03-02T00:00:00+00:00"),
    )
    sync_store.append(
        tape,
        TapeEntry(
            id=0,
            kind="message",
            payload={"role": "user", "content": "new timeout"},
            meta={"source": "ops"},
            date="2026-03-02T12:00:00+00:00",
        ),
    )
    sync_store.append(
        tape,
        TapeEntry(
            id=0,
            kind="message",
            payload={"role": "user", "content": "new success"},
            meta={"source": "ops"},
            date="2026-03-03T12:00:00+00:00",
        ),
    )

    entries = list(
        TapeQuery(tape=tape, store=sync_store)
        .after_anchor("a2")
        .between_dates("2026-03-02", "2026-03-02")
        .query("timeout")
        .all()
    )
    assert [entry.payload["content"] for entry in entries] == ["new timeout"]


def test_missing_anchor_and_invalid_date_raise_errorpayload(sync_store: RedisTapeStore) -> None:
    with pytest.raises(ErrorPayload) as missing_anchor:
        list(TapeQuery(tape="empty", store=sync_store).after_anchor("missing").all())
    assert missing_anchor.value.kind == ErrorKind.NOT_FOUND

    sync_store.append(
        "dated",
        TapeEntry(id=0, kind="message", payload={"role": "user", "content": "entry"}, date="2026-03-03T00:00:00+00:00"),
    )
    with pytest.raises(ErrorPayload) as invalid_range:
        list(TapeQuery(tape="dated", store=sync_store).between_dates("2026-03-05", "2026-03-01").all())
    assert invalid_range.value.kind == ErrorKind.INVALID_INPUT


def test_duplicate_anchor_names_use_latest_start_and_first_following_end(sync_store: RedisTapeStore) -> None:
    tape = "duplicate-anchors"
    entries = [
        TapeEntry.anchor("start"),
        TapeEntry.message({"role": "user", "content": "old block"}),
        TapeEntry.anchor("start"),
        TapeEntry.message({"role": "user", "content": "kept"}),
        TapeEntry.anchor("end"),
        TapeEntry.message({"role": "user", "content": "after"}),
        TapeEntry.anchor("end"),
    ]

    for entry in entries:
        sync_store.append(tape, entry)

    after_start = list(TapeQuery(tape=tape, store=sync_store).after_anchor("start").all())
    assert [entry.payload["content"] for entry in after_start if entry.kind == "message"] == ["kept", "after"]

    between = list(TapeQuery(tape=tape, store=sync_store).between_anchors("start", "end").all())
    assert [entry.payload["content"] for entry in between if entry.kind == "message"] == ["kept"]


def test_reset_clears_anchor_indexes(sync_store: RedisTapeStore) -> None:
    sync_store.append("session", TapeEntry.anchor("a1"))
    sync_store.reset("session")
    sync_store.append("session", TapeEntry.message({"role": "user", "content": "fresh"}))

    with pytest.raises(ErrorPayload) as exc_info:
        list(TapeQuery(tape="session", store=sync_store).after_anchor("a1").all())
    assert exc_info.value.kind == ErrorKind.NOT_FOUND


def test_empty_string_anchor_is_indexed(sync_store: RedisTapeStore) -> None:
    tape = "empty-anchor"
    sync_store.append(tape, TapeEntry.anchor(""))
    sync_store.append(tape, TapeEntry.message({"role": "user", "content": "after empty"}))

    entries = list(TapeQuery(tape=tape, store=sync_store).after_anchor("").all())
    assert [entry.payload["content"] for entry in entries if entry.kind == "message"] == ["after empty"]

    manager = TapeManager(store=sync_store)
    messages = manager.read_messages(tape, context=TapeContext(anchor=LAST_ANCHOR))
    assert [message["content"] for message in messages] == ["after empty"]


def test_tapemanager_reads_last_anchor_slice(sync_store: RedisTapeStore) -> None:
    for entry in _seed_entries():
        sync_store.append("test_tape", entry)

    manager = TapeManager(store=sync_store)
    messages = manager.read_messages("test_tape", context=TapeContext(anchor=LAST_ANCHOR))
    assert [message["content"] for message in messages] == ["task 2"]


@pytest.mark.asyncio
async def test_async_store_matches_sync_contract(async_store: AsyncRedisTapeStore) -> None:
    tape = "session"
    for entry in _seed_entries():
        await async_store.append(tape, entry)

    entries = list(await TapeQuery(tape=tape, store=async_store).between_anchors("a1", "a2").kinds("message").all())
    assert [entry.payload["content"] for entry in entries] == ["task 1", "answer 1"]

    names = await async_store.list_tapes()
    assert names == ["session"]


@pytest.mark.asyncio
async def test_async_store_reset_and_id_restart(async_store: AsyncRedisTapeStore) -> None:
    await async_store.append("session", TapeEntry.message({"role": "user", "content": "first"}))
    await async_store.append("session", TapeEntry.message({"role": "assistant", "content": "second"}))

    first_pass = list(await TapeQuery(tape="session", store=async_store).all())
    assert [entry.id for entry in first_pass] == [1, 2]

    await async_store.reset("session")
    await async_store.append("session", TapeEntry.message({"role": "user", "content": "third"}))

    second_pass = list(await TapeQuery(tape="session", store=async_store).all())
    assert [entry.id for entry in second_pass] == [1]


@pytest.mark.asyncio
async def test_async_duplicate_anchor_names_follow_same_semantics(async_store: AsyncRedisTapeStore) -> None:
    tape = "async-duplicate-anchors"
    entries = [
        TapeEntry.anchor("start"),
        TapeEntry.message({"role": "user", "content": "old block"}),
        TapeEntry.anchor("start"),
        TapeEntry.message({"role": "user", "content": "kept"}),
        TapeEntry.anchor("end"),
        TapeEntry.message({"role": "user", "content": "after"}),
        TapeEntry.anchor("end"),
    ]

    for entry in entries:
        await async_store.append(tape, entry)

    between = list(await TapeQuery(tape=tape, store=async_store).between_anchors("start", "end").all())
    assert [entry.payload["content"] for entry in between if entry.kind == "message"] == ["kept"]


@pytest.mark.asyncio
async def test_async_empty_string_anchor_is_indexed(async_store: AsyncRedisTapeStore) -> None:
    tape = "async-empty-anchor"
    await async_store.append(tape, TapeEntry.anchor(""))
    await async_store.append(tape, TapeEntry.message({"role": "user", "content": "after empty"}))

    entries = list(await TapeQuery(tape=tape, store=async_store).after_anchor("").all())
    assert [entry.payload["content"] for entry in entries if entry.kind == "message"] == ["after empty"]


@pytest.mark.asyncio
async def test_async_tapemanager_reads_last_anchor_slice(async_store: AsyncRedisTapeStore) -> None:
    for entry in _seed_entries():
        await async_store.append("test_tape", entry)

    manager = AsyncTapeManager(store=async_store)
    messages = await manager.read_messages("test_tape", context=TapeContext(anchor=LAST_ANCHOR))
    assert [message["content"] for message in messages] == ["task 2"]
