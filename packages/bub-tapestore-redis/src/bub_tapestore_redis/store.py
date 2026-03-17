"""Redis-backed TapeStore implementations for Republic."""

from __future__ import annotations

import base64
import json
from collections.abc import Iterable, Sequence
from contextlib import suppress
from datetime import UTC, datetime, time
from datetime import date as date_type
from typing import Any

import redis
import redis.asyncio as redis_async
from republic.core.errors import ErrorKind
from republic.core.results import ErrorPayload
from republic.tape.entries import TapeEntry
from republic.tape.query import TapeQuery

DEFAULT_KEY_PREFIX = "republic:tape"
_ANCHOR_SEPARATOR = ":"
_ANCHOR_ID_WIDTH = 20

# Each tape uses a tape-local Redis hash tag so multi-key writes stay inside one
# cluster slot while different tapes can still spread across the cluster.
# - {prefix}:tapes tracks all known tape names as an eventually consistent set
# - {prefix:tape}:entries stores append-only JSON entries in order
# - {prefix:tape}:next_id allocates monotonically increasing entry ids
# - {prefix:tape}:anchors stores all anchors in one zset with score=id
#   and member="{encoded-name}:{zero-padded-id}" so repeated names remain distinct

_APPEND_ENTRY_SCRIPT = """
local entry = cjson.decode(ARGV[1])
local next_id = redis.call('INCR', KEYS[1])
entry['id'] = next_id
local encoded = cjson.encode(entry)
redis.call('RPUSH', KEYS[2], encoded)
if ARGV[2] ~= '' then
  redis.call('ZADD', KEYS[3], next_id, ARGV[2] .. string.format('%020d', next_id))
end
return encoded
"""


def _normalize_prefix(value: str) -> str:
    stripped = value.strip(":")
    return stripped or DEFAULT_KEY_PREFIX


def _hash_tag(value: str) -> str:
    return value.replace("{", "_").replace("}", "_")


def _decode_text(value: Any) -> str:
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8")
    return str(value)


def _serialize_entry(entry: TapeEntry) -> str:
    return json.dumps(
        {
            "id": entry.id,
            "kind": entry.kind,
            "payload": entry.payload,
            "meta": entry.meta,
            "date": entry.date,
        },
        sort_keys=True,
    )


def _deserialize_entry(value: Any) -> TapeEntry:
    raw = json.loads(_decode_text(value))
    return TapeEntry(
        id=int(raw["id"]),
        kind=str(raw["kind"]),
        payload=dict(raw.get("payload", {})),
        meta=dict(raw.get("meta", {})),
        date=str(raw["date"]),
    )


def _parse_datetime_boundary(value: str, *, is_end: bool) -> datetime:
    if "T" not in value and " " not in value:
        try:
            parsed_date = date_type.fromisoformat(value)
        except ValueError:
            pass
        else:
            boundary_time = time.max if is_end else time.min
            return datetime.combine(parsed_date, boundary_time, tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        try:
            parsed_date = date_type.fromisoformat(value)
        except ValueError as exc:
            raise ErrorPayload(ErrorKind.INVALID_INPUT, f"Invalid ISO date or datetime: '{value}'.") from exc
        boundary_time = time.max if is_end else time.min
        parsed = datetime.combine(parsed_date, boundary_time, tzinfo=UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _entry_in_datetime_range(entry: TapeEntry, start_dt: datetime, end_dt: datetime) -> bool:
    entry_dt = _parse_datetime_boundary(entry.date, is_end=False)
    return start_dt <= entry_dt <= end_dt


def _entry_matches_query(entry: TapeEntry, query: str) -> bool:
    needle = query.casefold()
    haystack = json.dumps(
        {
            "kind": entry.kind,
            "date": entry.date,
            "payload": entry.payload,
            "meta": entry.meta,
        },
        sort_keys=True,
        default=str,
    ).casefold()
    return needle in haystack


def _encode_anchor_name(name: str) -> str:
    return base64.urlsafe_b64encode(name.encode("utf-8")).decode("ascii")


def _anchor_match_pattern(name: str) -> str:
    return f"{_encode_anchor_name(name)}{_ANCHOR_SEPARATOR}*"


def _parse_anchor_id(value: Any) -> int:
    member = _decode_text(value)
    _, entry_id = member.rsplit(_ANCHOR_SEPARATOR, 1)
    return int(entry_id)


def _anchor_arg(entry: TapeEntry) -> str:
    if entry.kind != "anchor":
        return ""
    name = entry.payload.get("name")
    if name is None:
        return ""
    return f"{_encode_anchor_name(str(name))}{_ANCHOR_SEPARATOR}"


def _scan_sync_anchor_ids(client: redis.Redis, key: str, pattern: str) -> list[int]:
    cursor = 0
    ids: list[int] = []
    while True:
        cursor, values = client.zscan(key, cursor=cursor, match=pattern)
        ids.extend(_parse_anchor_id(member) for member, _score in values)
        if cursor == 0:
            return ids


async def _scan_async_anchor_ids(client: redis_async.Redis, key: str, pattern: str) -> list[int]:
    cursor = 0
    ids: list[int] = []
    while True:
        cursor, values = await client.zscan(key, cursor=cursor, match=pattern)
        ids.extend(_parse_anchor_id(member) for member, _score in values)
        if cursor == 0:
            return ids


def _apply_query(entries: Sequence[TapeEntry], query: TapeQuery[Any]) -> list[TapeEntry]:
    # Anchor boundaries are resolved via the single Redis anchor zset before loading
    # entries. The remaining query contract still runs in Python to preserve republic
    # behavior for date/text/kind/limit filters.
    sliced = list(entries)
    if query._between_dates is not None:
        start_date, end_date = query._between_dates
        start_dt = _parse_datetime_boundary(start_date, is_end=False)
        end_dt = _parse_datetime_boundary(end_date, is_end=True)
        if start_dt > end_dt:
            raise ErrorPayload(ErrorKind.INVALID_INPUT, "Start date must be earlier than or equal to end date.")
        sliced = [entry for entry in sliced if _entry_in_datetime_range(entry, start_dt, end_dt)]
    if query._query:
        sliced = [entry for entry in sliced if _entry_matches_query(entry, query._query)]
    if query._kinds:
        sliced = [entry for entry in sliced if entry.kind in query._kinds]
    if query._limit is not None:
        sliced = sliced[: query._limit]
    return sliced


class _RedisKeyspace:
    def __init__(self, prefix: str) -> None:
        self._prefix = _normalize_prefix(prefix)

    @property
    def tapes(self) -> str:
        return f"{self._prefix}:tapes"

    def _tape_key(self, tape: str, suffix: str) -> str:
        tag = _hash_tag(f"{self._prefix}:{tape}")
        return f"{{{tag}}}:{suffix}"

    def entries(self, tape: str) -> str:
        return self._tape_key(tape, "entries")

    def next_id(self, tape: str) -> str:
        return self._tape_key(tape, "next_id")

    def anchors(self, tape: str) -> str:
        return self._tape_key(tape, "anchors")


def _last_anchor_id(client: redis.Redis, key: str) -> int:
    values = client.zrevrange(key, 0, 0)
    return _parse_anchor_id(values[0]) if values else -1


async def _async_last_anchor_id(client: redis_async.Redis, key: str) -> int:
    values = await client.zrevrange(key, 0, 0)
    return _parse_anchor_id(values[0]) if values else -1


def _resolve_sync_slice_bounds(client: redis.Redis, keys: _RedisKeyspace, query: TapeQuery[Any]) -> tuple[int, int]:
    if query._between_anchors is not None:
        start_name, end_name = query._between_anchors
        start_ids = _scan_sync_anchor_ids(client, keys.anchors(query.tape), _anchor_match_pattern(start_name))
        start_id = max(start_ids, default=-1)
        if start_id < 0:
            raise ErrorPayload(ErrorKind.NOT_FOUND, f"Anchor '{start_name}' was not found.")
        end_ids = _scan_sync_anchor_ids(client, keys.anchors(query.tape), _anchor_match_pattern(end_name))
        end_candidates = [entry_id for entry_id in end_ids if entry_id > start_id]
        end_id = min(end_candidates, default=-1)
        if end_id < 0:
            raise ErrorPayload(ErrorKind.NOT_FOUND, f"Anchor '{end_name}' was not found.")
        return start_id, end_id - 2
    if query._after_last:
        anchor_id = _last_anchor_id(client, keys.anchors(query.tape))
        if anchor_id < 0:
            raise ErrorPayload(ErrorKind.NOT_FOUND, "No anchors found in tape.")
        return anchor_id, -1
    if query._after_anchor is not None:
        anchor_id = max(
            _scan_sync_anchor_ids(client, keys.anchors(query.tape), _anchor_match_pattern(query._after_anchor)),
            default=-1,
        )
        if anchor_id < 0:
            raise ErrorPayload(ErrorKind.NOT_FOUND, f"Anchor '{query._after_anchor}' was not found.")
        return anchor_id, -1
    return 0, -1


async def _resolve_async_slice_bounds(
    client: redis_async.Redis,
    keys: _RedisKeyspace,
    query: TapeQuery[Any],
) -> tuple[int, int]:
    if query._between_anchors is not None:
        start_name, end_name = query._between_anchors
        start_ids = await _scan_async_anchor_ids(client, keys.anchors(query.tape), _anchor_match_pattern(start_name))
        start_id = max(start_ids, default=-1)
        if start_id < 0:
            raise ErrorPayload(ErrorKind.NOT_FOUND, f"Anchor '{start_name}' was not found.")
        end_ids = await _scan_async_anchor_ids(client, keys.anchors(query.tape), _anchor_match_pattern(end_name))
        end_candidates = [entry_id for entry_id in end_ids if entry_id > start_id]
        end_id = min(end_candidates, default=-1)
        if end_id < 0:
            raise ErrorPayload(ErrorKind.NOT_FOUND, f"Anchor '{end_name}' was not found.")
        return start_id, end_id - 2
    if query._after_last:
        anchor_id = await _async_last_anchor_id(client, keys.anchors(query.tape))
        if anchor_id < 0:
            raise ErrorPayload(ErrorKind.NOT_FOUND, "No anchors found in tape.")
        return anchor_id, -1
    if query._after_anchor is not None:
        anchor_id = max(
            await _scan_async_anchor_ids(client, keys.anchors(query.tape), _anchor_match_pattern(query._after_anchor)),
            default=-1,
        )
        if anchor_id < 0:
            raise ErrorPayload(ErrorKind.NOT_FOUND, f"Anchor '{query._after_anchor}' was not found.")
        return anchor_id, -1
    return 0, -1


class RedisTapeStore:
    """Sync TapeStore implementation backed by Redis."""

    def __init__(self, client: redis.Redis, *, key_prefix: str = DEFAULT_KEY_PREFIX) -> None:
        self._client = client
        self._keys = _RedisKeyspace(key_prefix)

    def list_tapes(self) -> list[str]:
        values = self._client.smembers(self._keys.tapes)
        return sorted(_decode_text(value) for value in values)

    def reset(self, tape: str) -> None:
        with self._client.pipeline(transaction=True) as pipe:
            pipe.delete(self._keys.entries(tape), self._keys.next_id(tape), self._keys.anchors(tape))
            pipe.execute()
        # Reset clears the tape atomically; registry cleanup is only eventual.
        with suppress(Exception):
            self._client.srem(self._keys.tapes, tape)

    def fetch_all(self, query: TapeQuery[Any]) -> Iterable[TapeEntry]:
        start_index, end_index = _resolve_sync_slice_bounds(self._client, self._keys, query)
        if end_index >= 0 and end_index < start_index:
            return []
        raw_entries = self._client.lrange(self._keys.entries(query.tape), start_index, end_index)
        entries = [_deserialize_entry(value) for value in raw_entries]
        return _apply_query(entries, query)

    def append(self, tape: str, entry: TapeEntry) -> None:
        # Lua keeps id allocation and anchor zset maintenance atomic so concurrent
        # writers cannot assign duplicate ids or leave the anchor index behind.
        self._client.eval(
            _APPEND_ENTRY_SCRIPT,
            3,
            self._keys.next_id(tape),
            self._keys.entries(tape),
            self._keys.anchors(tape),
            _serialize_entry(entry),
            _anchor_arg(entry),
        )
        # Tape registry is best-effort. The tape data itself is already durable if
        # this update fails, so append should not raise after commit.
        with suppress(Exception):
            self._client.sadd(self._keys.tapes, tape)


class AsyncRedisTapeStore:
    """Async TapeStore implementation backed by Redis."""

    def __init__(self, client: redis_async.Redis, *, key_prefix: str = DEFAULT_KEY_PREFIX) -> None:
        self._client = client
        self._keys = _RedisKeyspace(key_prefix)

    async def list_tapes(self) -> list[str]:
        values = await self._client.smembers(self._keys.tapes)
        return sorted(_decode_text(value) for value in values)

    async def reset(self, tape: str) -> None:
        pipe = self._client.pipeline(transaction=True)
        pipe.delete(self._keys.entries(tape), self._keys.next_id(tape), self._keys.anchors(tape))
        await pipe.execute()
        with suppress(Exception):
            await self._client.srem(self._keys.tapes, tape)

    async def fetch_all(self, query: TapeQuery[Any]) -> Iterable[TapeEntry]:
        start_index, end_index = await _resolve_async_slice_bounds(self._client, self._keys, query)
        if end_index >= 0 and end_index < start_index:
            return []
        raw_entries = await self._client.lrange(self._keys.entries(query.tape), start_index, end_index)
        entries = [_deserialize_entry(value) for value in raw_entries]
        return _apply_query(entries, query)

    async def append(self, tape: str, entry: TapeEntry) -> None:
        await self._client.eval(
            _APPEND_ENTRY_SCRIPT,
            3,
            self._keys.next_id(tape),
            self._keys.entries(tape),
            self._keys.anchors(tape),
            _serialize_entry(entry),
            _anchor_arg(entry),
        )
        with suppress(Exception):
            await self._client.sadd(self._keys.tapes, tape)
