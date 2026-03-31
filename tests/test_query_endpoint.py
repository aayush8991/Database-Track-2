import pytest
from fastapi.testclient import TestClient
import web.dashboard as dashboard

client = TestClient(dashboard.app)

class FakeCursor:
    def __init__(self):
        self._rows = []
        self.description = []
        self.rowcount = 0
    def execute(self, q, params=None):
        # store last query for assertions
        self.last_q = q
        self.last_params = params
        # set rows according to a simple heuristic
        if 'SELECT' in q.upper():
            self._rows = [("alice", 1)]
            self.description = [("username",), ("val",)]
        elif 'UPDATE' in q.upper() or 'DELETE' in q.upper():
            self.rowcount = 1
    def fetchall(self):
        return self._rows
    def fetchone(self):
        return self._rows[0] if self._rows else (0,)

class DummyConn:
    def commit(self):
        pass

class DummyResult:
    def __init__(self, matched=0, modified=0, deleted=0):
        self.matched_count = matched
        self.modified_count = modified
        self.deleted_count = deleted


class FakeFindCursor:
    def __init__(self, docs):
        self._docs = list(docs)
    def limit(self, _n):
        return self
    def max_time_ms(self, _ms):
        return self
    def __iter__(self):
        return iter(self._docs)

@pytest.fixture(autouse=True)
def isolate(monkeypatch):
    # Provide isolated handlers and metadata for each test
    fake_cursor = FakeCursor()
    dummy_conn = DummyConn()
    monkeypatch.setattr(dashboard, 'sql_handler', dashboard.sql_handler)
    dashboard.sql_handler.cursor = fake_cursor
    dashboard.sql_handler.conn = dummy_conn

    # Simple metadata manager replacement
    class FakeMeta:
        def __init__(self):
            self.global_schema = {'relational_structure': {'tables': {'structured_data': {'columns': ['username', 'val']}}},
                                  'unstructured_collections': {'unstructured_data': {}}}
        def get_field_route(self, f):
            if f in ('username', 'val'):
                return 'SQL'
            return 'MONGO'
    fm = FakeMeta()
    monkeypatch.setattr(dashboard, 'metadata_manager', fm)

    # Fake mongo collection/DB
    class FakeColl:
        def __init__(self):
            self._docs = [{"_id": "1", "username": "alice", "mval": 9}]
        def find(self, q):
            return FakeFindCursor(self._docs)
        def insert_one(self, doc, session=None):
            self._docs.append(doc)
        def update_many(self, filt, update):
            return DummyResult(matched=1, modified=1)
        def delete_many(self, filt):
            return DummyResult(deleted=1)
        def delete_one(self, filt, session=None):
            return
    class FakeDB:
        def __init__(self):
            self._coll = FakeColl()
        def __getitem__(self, name):
            return self._coll
        def get_collection(self, name):
            return self._coll
    monkeypatch.setattr(dashboard, 'mongo_handler', dashboard.mongo_handler)
    dashboard.mongo_handler.db = FakeDB()

    # bypass auth for tests
    monkeypatch.setattr(dashboard, '_get_user_from_request', lambda req: {'username': 'test', 'role': 'admin'})
    monkeypatch.setattr(dashboard, 'rate_limit', lambda request, cost=1.0: None)
    dashboard.query_trace.clear()

    yield


def test_read_sql_only():
    payload = {
        "operation": "read",
        "entity": "structured_data",
        "fields": ["username", "val"],
        "conditions": [{"field": "username", "op": "eq", "value": "alice"}]
    }
    r = client.post('/query', json=payload)
    assert r.status_code == 200
    data = r.json()
    assert 'results' in data
    assert isinstance(data['results'], list)
    assert data['results'][0]['username'] == 'alice'


def test_insert_uses_transaction(monkeypatch):
    # Provide a fake transaction context that executes forwards immediately
    class FakeTxCtx:
        def __enter__(self):
            return self
        def add_sql(self, fwd, comp):
            fwd(None)
        def add_mongo(self, fwd, comp):
            fwd(None, None)
        def __exit__(self, exc_type, exc, tb):
            return False
    monkeypatch.setattr(dashboard.tc, 'transaction', lambda : FakeTxCtx())

    payload = {
        "operation": "insert",
        "entity": "unstructured_data",
        "data": {"username": "bob", "mval": 5}
    }
    r = client.post('/query', json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j.get('status') == 'committed'
    assert 'uuid' in j


def test_update_sql():
    payload = {
        "operation": "update",
        "entity": "structured_data",
        "data": {"val": 42},
        "conditions": [{"field": "username", "op": "eq", "value": "alice"}]
    }
    r = client.post('/query', json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j.get('status') == 'ok'
    assert 'affected' in j


def test_delete_mongo():
    # Ensure metadata treats this entity as unstructured -> MONGO
    payload = {
        "operation": "delete",
        "entity": "unstructured_data",
        "conditions": [{"field": "username", "op": "eq", "value": "alice"}]
    }
    r = client.post('/query', json=payload)
    assert r.status_code == 200
    j = r.json()
    assert j.get('status') == 'ok'
    assert 'deleted' in j


def test_query_trace_records_logical_read():
    payload = {
        "operation": "read",
        "entity": "structured_data",
        "fields": ["username", "val"],
    }
    r = client.post('/query', json=payload)
    assert r.status_code == 200

    t = client.get('/api/query-trace?limit=5')
    assert t.status_code == 200
    jt = t.json()
    assert 'traces' in jt
    assert len(jt['traces']) >= 1
    assert jt['traces'][0]['endpoint'] == '/query'
    assert jt['traces'][0]['operation'] == 'read'


def test_query_trace_records_raw_json_query():
    r = client.post('/api/tools/json-query', json={
        'collection': 'unstructured_data',
        'query': {'username': 'alice'},
        'limit': 5,
    })
    assert r.status_code == 200

    t = client.get('/api/query-trace?limit=10')
    assert t.status_code == 200
    traces = t.json().get('traces', [])
    assert any(x.get('endpoint') == '/api/tools/json-query' for x in traces)
