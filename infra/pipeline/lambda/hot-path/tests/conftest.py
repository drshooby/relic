"""A dict-backed fake DynamoDB resource.

Not moto: this keeps tests fast and dependency-free, and mirrors how the Go
operator drives Kinesis retry paths through a narrow interface.

It mimics the boto3 *resource* surface the handler actually uses:
    resource.Table(name).batch_writer(overwrite_by_pkeys=...) -> context
        manager with put_item, buffering and flushing at a 25-item boundary
        or at __exit__ -- just like the real one.
    resource.Table(name).update_item(**kwargs)

Two contracts the earlier version of this fake did NOT model, which is how
two Critical bugs (float values, duplicate keys in one batch) shipped behind
35 green tests:

  - boto3's TypeSerializer raises TypeError on any float in an item. DynamoDB
    has no native float type; callers must convert to decimal.Decimal.
  - BatchWriteItem raises ValidationException if a single batch contains two
    operations on the same key, UNLESS the caller passed
    overwrite_by_pkeys=[...] to batch_writer(), in which case boto3 collapses
    duplicates client-side before ever calling the API.
"""

import pytest


def _reject_floats(item, table_name):
    """Walk an item and raise the way boto3's TypeSerializer does.

    Real boto3: TypeError("Float types are not supported. Use Decimal
    types instead."). Recurses into dicts/lists since attrs is a nested
    dict that could theoretically carry a float too.
    """
    if isinstance(item, float):
        raise TypeError(
            "Float types are not supported. Use Decimal types instead."
        )
    if isinstance(item, dict):
        for v in item.values():
            _reject_floats(v, table_name)
    elif isinstance(item, (list, tuple)):
        for v in item:
            _reject_floats(v, table_name)


class FakeBatchWriter:
    FLUSH_AT = 25

    def __init__(self, table, overwrite_by_pkeys=None):
        self.table = table
        self.overwrite_by_pkeys = overwrite_by_pkeys
        self._buffer = []
        self._seen_keys = set()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *exc_info):
        # Real batch_writer flushes remaining buffered items at __exit__,
        # via `while self._items_buffer: self._flush()`. Propagate normally
        # if the with-block itself already raised.
        if exc_type is None:
            self._flush_all()
        return False

    def _key_of(self, item):
        return (item["session_id"], item["seq"])

    def put_item(self, Item):
        key = self._key_of(Item)

        if self.overwrite_by_pkeys:
            # Real boto3 collapses duplicates client-side when
            # overwrite_by_pkeys is set: last write for a given key wins,
            # and no ValidationException is raised.
            self._buffer = [i for i in self._buffer if self._key_of(i) != key]
            self._buffer.append(Item)
        else:
            if key in self._seen_keys:
                raise ValueError(
                    "ValidationException: Provided list of item keys "
                    "contains duplicates"
                )
            self._seen_keys.add(key)
            self._buffer.append(Item)

        if len(self._buffer) >= self.FLUSH_AT:
            self._flush()

    def _flush(self):
        chunk, self._buffer = self._buffer[: self.FLUSH_AT], self._buffer[self.FLUSH_AT :]
        self._write_chunk(chunk)

    def _flush_all(self):
        while self._buffer:
            self._flush()

    def _write_chunk(self, chunk):
        if self.table.fail_at_exit:
            # Simulates a failure surfacing at flush/__exit__ time rather
            # than on the first put_item -- the realistic failure point,
            # since real boto3 buffers and only talks to DynamoDB at the
            # 25-item boundary or at __exit__.
            raise self.table.fail_with
        if self.table.fail_with is not None:
            raise self.table.fail_with
        for item in chunk:
            _reject_floats(item, self.table.name)
            self.table.writes += 1
            self.table.items[self._key_of(item)] = item


class FakeTable:
    def __init__(self, name, *, fail_with=None, fail_at_exit=False):
        self.name = name
        self.items = {}
        self.updates = []
        self.writes = 0
        self.fail_with = fail_with
        self.fail_at_exit = fail_at_exit

    def batch_writer(self, overwrite_by_pkeys=None):
        return FakeBatchWriter(self, overwrite_by_pkeys=overwrite_by_pkeys)

    def update_item(self, **kwargs):
        _reject_floats(kwargs.get("ExpressionAttributeValues", {}), self.name)
        self.updates.append(kwargs)
        return {}


class FakeDynamoResource:
    def __init__(self, *, fail_with=None, fail_at_exit=False):
        self._tables = {}
        self.fail_with = fail_with
        self.fail_at_exit = fail_at_exit

    def Table(self, name):
        if name not in self._tables:
            self._tables[name] = FakeTable(
                name, fail_with=self.fail_with, fail_at_exit=self.fail_at_exit
            )
        return self._tables[name]


@pytest.fixture
def fake_ddb():
    return FakeDynamoResource()
