"""A dict-backed fake DynamoDB resource.

Not moto: this keeps tests fast and dependency-free, and mirrors how the Go
operator drives Kinesis retry paths through a narrow interface.

It mimics the boto3 *resource* surface the handler actually uses:
    resource.Table(name).batch_writer() -> context manager with put_item
    resource.Table(name).update_item(**kwargs)
"""

import pytest


class FakeBatchWriter:
    def __init__(self, table):
        self.table = table

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        # Real batch_writer flushes here; nothing buffered in the fake.
        return False

    def put_item(self, Item):
        if self.table.fail_with is not None:
            raise self.table.fail_with
        self.table.writes += 1
        self.table.items[(Item["session_id"], Item["seq"])] = Item


class FakeTable:
    def __init__(self, name, *, fail_with=None):
        self.name = name
        self.items = {}
        self.updates = []
        self.writes = 0
        self.fail_with = fail_with

    def batch_writer(self):
        return FakeBatchWriter(self)

    def update_item(self, **kwargs):
        self.updates.append(kwargs)
        return {}


class FakeDynamoResource:
    def __init__(self, *, fail_with=None):
        self._tables = {}
        self.fail_with = fail_with

    def Table(self, name):
        if name not in self._tables:
            self._tables[name] = FakeTable(name, fail_with=self.fail_with)
        return self._tables[name]


@pytest.fixture
def fake_ddb():
    return FakeDynamoResource()
