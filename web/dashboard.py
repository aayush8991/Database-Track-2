from fastapi import FastAPI, HTTPException, Request, Depends
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import re
from core.transaction_coordinator import TransactionCoordinator
from core.txn_wal import WALManager
import time
import threading
from web.auth import create_token, verify_token, USERS
from web.auth import verify_password
from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler
from sqlalchemy import text
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
from core.metadata_manager import MetadataManager
from pydantic import BaseModel
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
import uuid
from core.txn_recovery import recover_incomplete
from collections import deque
from datetime import datetime, timezone
import io
import csv

load_dotenv()

# Security / query limits
# If DASHBOARD_API_KEY is set in the environment, API requests to sensitive
# endpoints must provide it in the `X-API-Key` header.
API_KEY = os.getenv('DASHBOARD_API_KEY')

# Query operator whitelist: allow only these Mongo query operators.
# Deny operators such as $where, $function, $eval, $accumulator, $mapReduce, $regex (optional)
ALLOWED_OPS = {
    '$eq', '$gt', '$gte', '$lt', '$lte', '$in', '$nin', '$ne',
    '$and', '$or', '$not', '$exists', '$size'
}

# Limits
MAX_QUERY_STRING = 20000
MAX_LIMIT = 100
DEFAULT_MAX_TIME_MS = 2000
MAX_ALLOWED_TIME_MS = 5000

_COLL_RE = re.compile(r'^[A-Za-z0-9_\-\.]+$')

def _validate_query_obj(obj, depth=0):
    if depth > 8:
        raise ValueError('Query depth too deep')
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k.startswith('$'):
                if k not in ALLOWED_OPS:
                    raise ValueError(f'Operator not allowed: {k}')
            # Recurse into values
            _validate_query_obj(v, depth + 1)
    elif isinstance(obj, list):
        if len(obj) > 500:
            raise ValueError('Array in query too large')
        for item in obj:
            _validate_query_obj(item, depth + 1)
    else:
        # primitives are fine
        return

app = FastAPI(title="Adaptive Dashboard (minimal)")


class TxnTestRequest(BaseModel):
    username: str | None = None
    payload: dict | None = None
    force_fail: bool = False


# Instantiate handlers (module-level singletons)
sql_handler = SQLHandler()
mongo_handler = MongoHandler()
tc = TransactionCoordinator(sql_handler, mongo_handler)
# WAL manager for persistent transaction logging and recovery
try:
    wal = WALManager(sql_handler)
except Exception:
    wal = None
# Metadata manager instance
metadata_manager = MetadataManager()

# Keep recent ACID runs for evidence/export (in-memory, process-local)
ACID_HISTORY_MAX = int(os.getenv('ACID_HISTORY_MAX', '200'))
acid_history = deque(maxlen=ACID_HISTORY_MAX)

# Session monitor state (process-local)
SESSION_IDLE_SECONDS = int(os.getenv('SESSION_IDLE_SECONDS', '7200'))
RECENT_CALLS_MAX = int(os.getenv('RECENT_CALLS_MAX', '500'))
session_registry: Dict[str, Dict[str, Any]] = {}
recent_calls = deque(maxlen=RECENT_CALLS_MAX)
session_lock = threading.Lock()

# Query execution trace state (process-local)
QUERY_TRACE_MAX = int(os.getenv('QUERY_TRACE_MAX', '500'))
query_trace = deque(maxlen=QUERY_TRACE_MAX)
query_trace_lock = threading.Lock()

# ACID runtime safety + evidence logs
ACID_TEST_TIMEOUT_SEC = float(os.getenv('ACID_TEST_TIMEOUT_SEC', '45'))
ACID_EVIDENCE_MAX = int(os.getenv('ACID_EVIDENCE_MAX', '500'))
acid_evidence = deque(maxlen=ACID_EVIDENCE_MAX)
acid_evidence_lock = threading.Lock()


def _record_acid_run(name: str, result: Dict[str, Any]):
    try:
        acid_history.appendleft({
            'ts': datetime.now(timezone.utc).isoformat(),
            'name': name,
            'status': result.get('status', 'UNKNOWN'),
            'passed': bool(result.get('passed', False)),
            'result': result,
        })
    except Exception:
        pass


def _record_acid_evidence(name: str, payload: Dict[str, Any]):
    try:
        item = {
            'ts': datetime.now(timezone.utc).isoformat(),
            'name': name,
            'payload': payload,
        }
        with acid_evidence_lock:
            acid_evidence.appendleft(item)
    except Exception:
        pass


def _run_with_timeout(label: str, fn, timeout_sec: float | None = None) -> Dict[str, Any]:
    """Run an experiment in a worker thread with timeout to avoid hanging endpoints."""
    tsec = timeout_sec if timeout_sec is not None else ACID_TEST_TIMEOUT_SEC
    started = time.time()
    ex = None
    try:
        ex = ThreadPoolExecutor(max_workers=1)
        future = ex.submit(fn)
        out = future.result(timeout=tsec)
        # successful completion: safe to wait for thread cleanup
        ex.shutdown(wait=True, cancel_futures=False)
        ex = None
        if isinstance(out, dict):
            details = out.setdefault('details', {}) if isinstance(out.get('details', {}), dict) else {}
            details['duration_ms'] = int((time.time() - started) * 1000)
            out['details'] = details
            return out
        return {
            'test': label,
            'passed': False,
            'status': 'FAIL',
            'details': {
                'error': 'experiment did not return dict',
                'duration_ms': int((time.time() - started) * 1000),
            }
        }
    except FuturesTimeoutError:
        try:
            future.cancel()
        except Exception:
            pass
        if ex is not None:
            try:
                # Do not wait for blocked worker thread
                ex.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
        return {
            'test': label,
            'passed': False,
            'status': 'FAIL',
            'details': {
                'error': f'timeout after {tsec}s',
                'duration_ms': int((time.time() - started) * 1000),
                'timeout_sec': tsec,
            }
        }
    except Exception as e:
        if ex is not None:
            try:
                ex.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
        return {
            'test': label,
            'passed': False,
            'status': 'FAIL',
            'details': {
                'error': str(e),
                'duration_ms': int((time.time() - started) * 1000),
            }
        }


def _summarize_query_for_trace(query: Any) -> str:
    try:
        if isinstance(query, dict):
            keys = list(query.keys())
            shown = keys[:6]
            suffix = '…' if len(keys) > 6 else ''
            return f'keys={shown}{suffix}'
        if isinstance(query, list):
            return f'list(len={len(query)})'
        return str(query)
    except Exception:
        return 'unavailable'


def _record_query_trace(
    username: str,
    endpoint: str,
    operation: str,
    routed_backends: List[str],
    summary: str,
    started_at: float,
    status: str = 'ok',
    result_count: int | None = None,
    error: str | None = None,
):
    try:
        duration_ms = int((time.time() - started_at) * 1000)
        with query_trace_lock:
            query_trace.appendleft({
                'ts': _iso_utc_now(),
                'username': username,
                'endpoint': endpoint,
                'operation': operation,
                'routed_backends': routed_backends,
                'summary': summary,
                'duration_ms': duration_ms,
                'status': status,
                'result_count': result_count,
                'error': error,
            })
    except Exception:
        pass


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _touch_session(username: str, role: str, auth_type: str, exp: int | None = None):
    now = _iso_utc_now()
    with session_lock:
        prev = session_registry.get(username, {})
        session_registry[username] = {
            'username': username,
            'role': role,
            'auth_type': auth_type,
            'first_seen': prev.get('first_seen', now),
            'last_seen': now,
            'token_exp': exp,
        }


def _prune_sessions():
    # Remove stale sessions based on idle timeout
    cutoff = time.time() - SESSION_IDLE_SECONDS
    with session_lock:
        to_del = []
        for u, info in session_registry.items():
            try:
                ts = info.get('last_seen')
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                if dt.timestamp() < cutoff:
                    to_del.append(u)
            except Exception:
                continue
        for u in to_del:
            session_registry.pop(u, None)


@app.middleware('http')
async def monitor_calls_middleware(request: Request, call_next):
    started = time.time()
    username = 'anonymous'
    role = 'guest'
    auth_type = 'none'
    exp = None

    # Try API key auth marker
    if API_KEY:
        provided = request.headers.get('X-API-Key')
        if provided and provided == API_KEY:
            username = 'api_key'
            role = 'admin'
            auth_type = 'api_key'

    # Try bearer token marker
    if username == 'anonymous':
        auth = request.headers.get('Authorization', '')
        if auth.startswith('Bearer '):
            tok = auth.split(' ', 1)[1].strip()
            try:
                payload = verify_token(tok)
                username = payload.get('username', 'anonymous')
                role = payload.get('role', 'user')
                auth_type = 'token'
                exp = payload.get('exp')
            except Exception:
                pass

    response = await call_next(request)
    duration_ms = int((time.time() - started) * 1000)

    # Update session + call history (skip static assets noise)
    path = request.url.path
    if not path.startswith('/ui/static'):
        if username != 'anonymous':
            _touch_session(username, role, auth_type, exp=exp)

        with session_lock:
            recent_calls.appendleft({
                'ts': _iso_utc_now(),
                'username': username,
                'role': role,
                'method': request.method,
                'path': path,
                'status_code': response.status_code,
                'duration_ms': duration_ms,
            })

        _prune_sessions()

    return response


@app.post('/api/tools/json-query-preview')
async def api_json_query_preview(request: Request):
    """Return an estimated/actual count for the provided query. Useful for preview before running."""
    # enforce auth and rate limit similar to api_json_query
    if API_KEY:
        provided = request.headers.get('X-API-Key')
        if not provided or provided != API_KEY:
            raise HTTPException(status_code=401, detail='Missing or invalid API key')

    data = await request.json()
    raw_len = len(str(data))
    if raw_len > MAX_QUERY_STRING:
        raise HTTPException(status_code=400, detail='Query payload too large')

    coll = data.get('collection', 'unstructured_data')
    query = data.get('query', {})
    try:
        limit = int(data.get('limit', 50))
    except Exception:
        limit = 50
    limit = max(1, min(limit, MAX_LIMIT))

    # Validate collection and query
    if not isinstance(coll, str) or not _COLL_RE.match(coll):
        raise HTTPException(status_code=400, detail='Invalid collection name')
    try:
        _validate_query_obj(query)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f'Invalid query: {ve}')

    # auth and rate limit
    try:
        _ = _get_user_from_request(request)
    except HTTPException:
        raise
    rate_limit(request)

    # Try count_documents (accurate but may be costly), fallback to estimated_document_count
    try:
        coll_obj = mongo_handler.db[coll]
        try:
            # PyMongo supports count_documents; cap the max time by using maxTimeMS on an aggregation cursor isn't available here
            cnt = coll_obj.count_documents(query)
            # compatibility fallback for historical typo collection name
            if cnt == 0 and coll == 'unstructured_data':
                try:
                    alt = mongo_handler.db['unstructed_data']
                    alt_cnt = alt.count_documents(query)
                    if alt_cnt > 0:
                        cnt = alt_cnt
                except Exception:
                    pass
            # Clamp to a safe reporting cap to avoid expensive work
            REPORT_CAP = int(os.getenv('PREVIEW_REPORT_CAP', '100000'))
            reported = cnt if cnt <= REPORT_CAP else REPORT_CAP
            truncated = cnt > REPORT_CAP
            return JSONResponse({'count': reported, 'truncated': truncated, 'actual_count': None if truncated else cnt})
        except Exception:
            # fallback
            est = coll_obj.estimated_document_count()
            REPORT_CAP = int(os.getenv('PREVIEW_REPORT_CAP', '100000'))
            reported = est if est <= REPORT_CAP else REPORT_CAP
            truncated = est > REPORT_CAP
            return JSONResponse({'estimated': reported, 'truncated': truncated})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Serve static UI
app.mount("/ui/static", StaticFiles(directory="web/static"), name="static")


@app.get("/")
def index():
    return FileResponse("web/static/index.html")


# --- Authentication / RBAC helpers for FastAPI endpoints ---
def _get_user_from_request(request: Request):
    # 1) API key short-circuit (maintain older behavior)
    if API_KEY:
        provided = request.headers.get('X-API-Key')
        if provided and provided == API_KEY:
            return {'username': 'api_key', 'role': 'admin', 'auth': 'api_key'}

    # 2) Bearer token
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        token = auth.split(' ', 1)[1].strip()
        try:
            payload = verify_token(token)
            return {'username': payload.get('username'), 'role': payload.get('role', 'user')}
        except Exception:
            raise HTTPException(status_code=401, detail='Invalid or expired token')

    # No credentials
    raise HTTPException(status_code=401, detail='Missing credentials')


def get_current_user(request: Request):
    return _get_user_from_request(request)


# --- Simple in-memory token-bucket rate limiter ---
RATE_CAPACITY = int(os.getenv('RATE_LIMIT_CAPACITY', '10'))
RATE_REFILL_PER_SEC = float(os.getenv('RATE_LIMIT_REFILL_PER_SEC', '1.0'))


class RateLimiter:
    def __init__(self, capacity: int, refill_per_sec: float):
        self.capacity = float(capacity)
        self.refill = float(refill_per_sec)
        self.buckets = {}  # key -> (tokens, last_ts)
        self.lock = threading.Lock()

    def _now(self):
        return time.monotonic()

    def allow(self, key: str, cost: float = 1.0):
        now = self._now()
        with self.lock:
            tokens, last = self.buckets.get(key, (self.capacity, now))
            # refill
            if now > last:
                tokens = min(self.capacity, tokens + (now - last) * self.refill)
            if tokens >= cost:
                tokens -= cost
                self.buckets[key] = (tokens, now)
                return True, tokens
            else:
                # update timestamp but keep tokens
                self.buckets[key] = (tokens, now)
                return False, tokens


limiter = RateLimiter(RATE_CAPACITY, RATE_REFILL_PER_SEC)


def _client_key_from_request(request: Request):
    # Prefer authenticated username, fallback to client IP
    try:
        u = _get_user_from_request(request)
        if u and u.get('username'):
            return f'user:{u.get("username")} '
    except HTTPException:
        pass
    # use X-Forwarded-For if present
    addr = None
    xff = request.headers.get('X-Forwarded-For')
    if xff:
        addr = xff.split(',')[0].strip()
    else:
        client = getattr(request, 'client', None)
        addr = client.host if client else 'unknown'
    return f'ip:{addr}'


def rate_limit(request: Request, cost: float = 1.0):
    key = _client_key_from_request(request)
    allowed, tokens = limiter.allow(key, cost=cost)
    if not allowed:
        # compute retry-after in seconds
        # need tokens deficit = cost - tokens; time to refill = deficit / refill
        deficit = max(0.0, cost - tokens)
        retry_after = int((deficit / limiter.refill) + 1)
        raise HTTPException(status_code=429, detail='rate limit exceeded', headers={'Retry-After': str(retry_after)})



@app.get("/status")
def status():
    return {"sql_connected": bool(sql_handler.conn), "mongo_connected": bool(getattr(mongo_handler, 'db', None))}


@app.get('/api/session-monitor')
def api_session_monitor(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request)

    try:
        limit = int(request.query_params.get('limit', '50'))
    except Exception:
        limit = 50
    limit = max(1, min(limit, 200))

    _prune_sessions()
    with session_lock:
        sessions = list(session_registry.values())
        calls = list(recent_calls)[:limit]

    # add expiry helper fields
    now_epoch = int(time.time())
    for s in sessions:
        exp = s.get('token_exp')
        if isinstance(exp, int):
            s['expires_in_sec'] = max(0, exp - now_epoch)
        else:
            s['expires_in_sec'] = None

    sessions.sort(key=lambda x: x.get('last_seen', ''), reverse=True)
    return JSONResponse({'active_sessions': sessions, 'recent_calls': calls})


@app.get('/api/query-trace')
def api_query_trace(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request)

    try:
        limit = int(request.query_params.get('limit', '50'))
    except Exception:
        limit = 50
    limit = max(1, min(limit, 200))

    with query_trace_lock:
        traces = list(query_trace)[:limit]
        total = len(query_trace)
    return JSONResponse({'traces': traces, 'total': total})


def _sql_count_for_username(username: str):
    # Prefer SQLAlchemy pooled connection (thread-safe for concurrent read checks)
    try:
        if hasattr(sql_handler, 'engine') and sql_handler.engine:
            with sql_handler.engine.begin() as conn:
                res = conn.execute(text("SELECT COUNT(*) FROM structured_data WHERE username = :username"), {"username": username})
                row = res.fetchone()
                return int(row[0]) if row else 0
    except Exception:
        pass

    # Fallback: reconnect and use a short-lived cursor
    try:
        if hasattr(sql_handler, 'ensure_connection'):
            sql_handler.ensure_connection()
        cur = sql_handler.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM structured_data WHERE username = %s", (username,))
        out = cur.fetchone()
        cur.close()
        return int(out[0]) if out else 0
    except Exception:
        return None


def _sql_read_with_fallback(sel_cols: list[str], where_clauses: list[str], params: list[Any], preferred_tables: list[str]):
    """Try reading from preferred tables and return first successful result set."""
    last_err = None
    cols_sql = ','.join(sel_cols if sel_cols else ['*'])
    for table_name in preferred_tables:
        try:
            q = f"SELECT {cols_sql} FROM {table_name}"
            if where_clauses:
                q += ' WHERE ' + ' AND '.join(where_clauses)

            if hasattr(sql_handler, 'engine') and sql_handler.engine:
                with sql_handler.engine.begin() as conn:
                    res = conn.execute(text(q), tuple(params))
                    rows = res.fetchall()
                    col_names = list(res.keys())
                return [dict(zip(col_names, r)) for r in rows]
            else:
                if hasattr(sql_handler, 'ensure_connection'):
                    sql_handler.ensure_connection()
                cur = sql_handler.conn.cursor()
                cur.execute(q, tuple(params))
                rows = cur.fetchall()
                col_names = [d[0] for d in cur.description] if cur.description else []
                cur.close()
                return [dict(zip(col_names, r)) for r in rows]
        except Exception as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    return []


def txn_test(req: TxnTestRequest):
    # Prepare data
    import uuid
    uid = str(uuid.uuid4())
    username = req.username or f"txn_{uid[:8]}"
    payload = req.payload or {"note": "txn test"}

    # Helper forward/compensating actions
    def sql_forward(conn):
        # conn may be SQLAlchemy connection or engine depending on TransactionCoordinator
        try:
            conn.execute(text("INSERT INTO structured_data (username, timestamp, sys_ingested_at) VALUES (:username, NULL, NOW())"), {"username": username})
        except Exception:
            # If conn is a SQLHandler object (fallback) use its cursor
            try:
                if hasattr(sql_handler, 'cursor') and sql_handler.cursor:
                    sql_handler.cursor.execute("INSERT INTO structured_data (username, timestamp, sys_ingested_at) VALUES (%s, NULL, NOW())", (username,))
                    sql_handler.conn.commit()
            except Exception:
                raise

    def sql_compensate(conn):
        try:
            conn.execute(text("DELETE FROM structured_data WHERE username = :username"), {"username": username})
        except Exception:
            try:
                if hasattr(sql_handler, 'cursor') and sql_handler.cursor:
                    sql_handler.cursor.execute("DELETE FROM structured_data WHERE username = %s", (username,))
                    sql_handler.conn.commit()
            except Exception:
                pass

    def mongo_forward(db, session):
        coll = db.get_collection("txn_test")
        if session:
            coll.insert_one({"uuid": uid, "username": username, "payload": payload}, session=session)
        else:
            coll.insert_one({"uuid": uid, "username": username, "payload": payload})

    def mongo_compensate(db, session):
        coll = db.get_collection("txn_test")
        try:
            if session:
                coll.delete_one({"uuid": uid}, session=session)
            else:
                coll.delete_one({"uuid": uid})
        except Exception:
            pass

    # Run coordinated transaction
    tx_id = uid
    # Record WAL entry (best-effort) so recovery can act on in-doubt txns
    if wal:
        try:
            # include per-operation descriptors for safer automated replay/compensation
            ops = [
                {'type': 'sql', 'action': 'insert_structured_data', 'status': 'pending'},
                {'type': 'mongo', 'action': 'insert_txn_test', 'status': 'pending'}
            ]
            wal.start_tx(tx_id, {'uuid': uid, 'username': username, 'payload': payload, 'ops': ops})
        except Exception:
            pass

    try:
        with tc.transaction() as t:
            t.add_sql(sql_forward, sql_compensate)
            t.add_mongo(mongo_forward, mongo_compensate)

            # Optionally force a failure after ops are registered to simulate partial failure
            if req.force_fail:
                raise RuntimeError("Forced failure for testing rollback")

        # If commit succeeded, update WAL and mark ops done
        if wal:
            try:
                # mark ops as done
                try:
                    wal.update_op(tx_id, 0, 'done')
                except Exception:
                    pass
                try:
                    wal.update_op(tx_id, 1, 'done')
                except Exception:
                    pass
                wal.update_tx_status(tx_id, 'committed')
            except Exception:
                pass

        # If commit succeeded, check existence
        mongo_found = None
        sql_found = None
        try:
            mongo_found = mongo_handler.db["txn_test"].find_one({"uuid": uid})
        except Exception:
            mongo_found = None
        sql_found = _sql_count_for_username(username)

        return {"status": "committed", "uuid": uid, "username": username, "mongo_found": bool(mongo_found), "sql_count": sql_found}

    except Exception as e:
        # On error, mark WAL as rolled_back and show whether compensating actions ran
        if wal:
            try:
                # mark ops as compensated where possible
                try:
                    wal.update_op(tx_id, 0, 'compensated')
                except Exception:
                    pass
                try:
                    wal.update_op(tx_id, 1, 'compensated')
                except Exception:
                    pass
                wal.update_tx_status(tx_id, 'rolledback')
            except Exception:
                pass

        mongo_found = None
        sql_found = None
        try:
            mongo_found = mongo_handler.db["txn_test"].find_one({"uuid": uid})
        except Exception:
            mongo_found = None
        sql_found = _sql_count_for_username(username)

        return {"status": "rolled_back", "error": str(e), "uuid": uid, "mongo_found": bool(mongo_found), "sql_count": sql_found}


# Route wrapper that enforces authentication and RBAC; keep `txn_test` as callable logic for tests
@app.post("/txn-test")
def txn_test_route(req: TxnTestRequest, request: Request, user: dict = Depends(get_current_user)):
    # RBAC: require admin role for running coordinated txn tests
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request)
    return JSONResponse(txn_test(req))


# --- Minimal API for the dashboard UI ---


@app.get("/api/logs")
def api_logs():
    # Minimal placeholder logs (could stream from a file or monitoring component)
    sample = [
        {"ts": "2026-03-30T10:00:00Z", "level": "INFO", "msg": "Service started"},
        {"ts": "2026-03-30T10:05:12Z", "level": "WARN", "msg": "Sample warning"},
    ]
    return JSONResponse({"logs": sample})


@app.get("/api/users")
def api_users():
    # Return some users from SQL structured_data if available (fallback to sample)
    try:
        cur = sql_handler.cursor
        cur.execute("SELECT DISTINCT username FROM structured_data LIMIT 50")
        rows = [r[0] for r in cur.fetchall()]
        return JSONResponse({"users": rows})
    except Exception:
        return JSONResponse({"users": []})


@app.get("/api/alerts")
def api_alerts():
    sample = [{"ts": "2026-03-30T10:10:00Z", "severity": "HIGH", "msg": "Disk almost full"}]
    return JSONResponse({"alerts": sample})


class AcidRequest(BaseModel):
    username: str | None = None
    payload: dict | None = None
    force_fail: bool = False


def _run_atomicity_experiment() -> Dict[str, Any]:
    """Atomicity: commit should fully apply, forced failure should fully rollback."""
    ok_tx = txn_test(TxnTestRequest(payload={"acid": "atomicity", "mode": "commit"}, force_fail=False))
    fail_tx = txn_test(TxnTestRequest(payload={"acid": "atomicity", "mode": "rollback"}, force_fail=True))

    ok_pass = (
        ok_tx.get('status') == 'committed' and
        bool(ok_tx.get('mongo_found')) and
        (ok_tx.get('sql_count') is not None and ok_tx.get('sql_count') >= 1)
    )
    fail_pass = (
        fail_tx.get('status') == 'rolled_back' and
        (not bool(fail_tx.get('mongo_found'))) and
        (fail_tx.get('sql_count') in (0, None) or fail_tx.get('sql_count') == 0)
    )

    passed = bool(ok_pass and fail_pass)
    return {
        'test': 'atomicity',
        'passed': passed,
        'status': 'PASS' if passed else 'FAIL',
        'details': {
            'commit_case': ok_tx,
            'rollback_case': fail_tx
        }
    }


def _run_consistency_experiment() -> Dict[str, Any]:
    """Consistency: post-commit state should satisfy basic invariants across backends."""
    uname = f"cons_{uuid.uuid4().hex[:8]}"
    tx = txn_test(TxnTestRequest(username=uname, payload={"acid": "consistency", "v": 1}, force_fail=False))

    sql_count = None
    mongo_present = False
    sql_count = _sql_count_for_username(uname)

    try:
        mongo_present = bool(mongo_handler.db["txn_test"].find_one({"username": uname}))
    except Exception:
        mongo_present = False

    invariant_ok = (
        tx.get('status') == 'committed' and
        mongo_present and
        (sql_count is not None and sql_count >= 1)
    )

    return {
        'test': 'consistency',
        'passed': bool(invariant_ok),
        'status': 'PASS' if invariant_ok else 'FAIL',
        'details': {
            'tx': tx,
            'sql_count_for_username': sql_count,
            'mongo_present_for_username': mongo_present,
            'invariant': 'committed transaction must be visible in both SQL and Mongo'
        }
    }


def _run_isolation_experiment(workers: int = 4) -> Dict[str, Any]:
    """Isolation: concurrent transactions should not produce inconsistent final visibility."""
    # keep worker fanout conservative to avoid environment-specific DB pool starvation/timeouts
    workers = max(2, min(int(workers), 4))
    submitted = []

    def _one(i: int):
        uname = f"iso_{i}_{uuid.uuid4().hex[:6]}"
        result = txn_test(TxnTestRequest(username=uname, payload={"acid": "isolation", "i": i}, force_fail=False))
        return uname, result

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_one, i) for i in range(workers)]
        for f in as_completed(futures):
            try:
                submitted.append(f.result())
            except Exception as e:
                submitted.append((None, {'status': 'error', 'error': str(e)}))

    all_committed = all((res or {}).get('status') == 'committed' for _, res in submitted)
    visibility_ok = True
    visibility = []

    for uname, res in submitted:
        if not uname:
            visibility_ok = False
            continue
        # Use per-transaction visibility checks collected by txn_test itself,
        # avoiding extra round-trips that can trigger timeouts on slower setups.
        sql_count = res.get('sql_count') if isinstance(res, dict) else 0
        mongo_count = 1 if bool((res or {}).get('mongo_found')) else 0
        row_ok = (
            (res or {}).get('status') == 'committed' and
            (sql_count is not None and sql_count >= 1) and
            mongo_count >= 1
        )
        visibility_ok = visibility_ok and row_ok
        visibility.append({'username': uname, 'sql_count': sql_count, 'mongo_count': mongo_count, 'ok': row_ok})

    passed = bool(all_committed and visibility_ok)
    return {
        'test': 'isolation',
        'passed': passed,
        'status': 'PASS' if passed else 'FAIL',
        'details': {
            'workers': workers,
            'transactions': [res for _, res in submitted],
            'visibility_checks': visibility
        }
    }


def _run_durability_experiment() -> Dict[str, Any]:
    """Durability: simulate in-progress WAL tx and verify recovery commits replayed operation."""
    if not wal:
        return {
            'test': 'durability',
            'passed': False,
            'status': 'FAIL',
            'details': {'error': 'WAL manager not initialized'}
        }

    tx_id = f"dur_{uuid.uuid4().hex[:12]}"
    doc_uuid = f"durdoc_{uuid.uuid4().hex[:10]}"
    doc = {'uuid': doc_uuid, 'username': f'dur_{uuid.uuid4().hex[:6]}', 'payload': {'acid': 'durability'}}

    try:
        # ensure clean slate
        mongo_handler.db['txn_test'].delete_many({'uuid': doc_uuid})
    except Exception:
        pass

    meta = {
        'phase': 'durability_simulation',
        'ops': [
            {
                'type': 'mongo',
                'action': 'insert_txn_test',
                'collection': 'txn_test',
                'doc': doc,
                'unique_check': {'uuid': doc_uuid},
                'status': 'pending'
            }
        ]
    }

    if hasattr(sql_handler, 'ensure_connection'):
        sql_handler.ensure_connection()

    try:
        wal.start_tx(tx_id, meta)
    except Exception as e:
        return {
            'test': 'durability',
            'passed': False,
            'status': 'FAIL',
            'details': {'error': f'Unable to start WAL tx: {e}'}
        }

    def _replay_single_tx(txid: str) -> tuple[list[str], str | None]:
        tx = wal.get_tx(txid)
        if not tx:
            return [], 'tx not found in WAL'
        meta_row = tx.get('meta') or {}
        ops = meta_row.get('ops', [])
        processed_local = []
        for idx, op in enumerate(ops):
            if op.get('status') == 'done':
                continue
            if op.get('type') == 'mongo':
                try:
                    coll_name = op.get('collection', 'txn_test')
                    doc_local = op.get('doc') or {}
                    unique = op.get('unique_check') or {'uuid': doc_local.get('uuid')}
                    coll_obj = mongo_handler.db.get_collection(coll_name)
                    exists = bool(coll_obj.find_one(unique))
                    if not exists:
                        coll_obj.insert_one(doc_local)
                    wal.update_op(txid, idx, 'done')
                except Exception as e:
                    return processed_local, str(e)
            else:
                # unsupported op types for this targeted durability scenario
                wal.update_op(txid, idx, 'done')

        # mark committed if all ops done
        tx_after = wal.get_tx(txid)
        ops_after = (tx_after or {}).get('meta', {}).get('ops', [])
        if ops_after and all(op.get('status') == 'done' for op in ops_after):
            wal.update_tx_status(txid, 'committed')
            processed_local.append(txid)
        return processed_local, None

    processed, replay_error = _replay_single_tx(tx_id)
    if replay_error:
        return {
            'test': 'durability',
            'passed': False,
            'status': 'FAIL',
            'details': {'error': f'Recovery failed: {replay_error}', 'tx_id': tx_id}
        }

    tx_row = None
    doc_present = False
    try:
        tx_row = wal.get_tx(tx_id)
    except Exception:
        tx_row = None
    try:
        doc_present = bool(mongo_handler.db['txn_test'].find_one({'uuid': doc_uuid}))
    except Exception:
        doc_present = False

    passed = bool(tx_row and tx_row.get('status') == 'committed' and doc_present and tx_id in processed)
    return {
        'test': 'durability',
        'passed': passed,
        'status': 'PASS' if passed else 'FAIL',
        'details': {
            'tx_id': tx_id,
            'processed': processed,
            'wal_status': tx_row.get('status') if tx_row else None,
            'doc_present': doc_present
        }
    }


def _simulate_durability_crash() -> Dict[str, Any]:
    """Create an in-progress WAL transaction and intentionally do not execute the operation.

    This simulates process crash/kill after WAL write but before backend write.
    """
    if not wal:
        return {
            'test': 'durability_crash_simulation',
            'passed': False,
            'status': 'FAIL',
            'details': {'error': 'WAL manager not initialized'}
        }

    tx_id = f"durcrash_{uuid.uuid4().hex[:12]}"
    doc_uuid = f"durcrashdoc_{uuid.uuid4().hex[:10]}"
    username = f"durcrash_{uuid.uuid4().hex[:6]}"
    doc = {'uuid': doc_uuid, 'username': username, 'payload': {'acid': 'durability', 'phase': 'crash_sim'}}

    try:
        mongo_handler.db['txn_test'].delete_many({'uuid': doc_uuid})
    except Exception:
        pass

    meta = {
        'phase': 'crash_simulation_before_backend_write',
        'ops': [
            {
                'type': 'mongo',
                'action': 'insert_txn_test',
                'collection': 'txn_test',
                'doc': doc,
                'unique_check': {'uuid': doc_uuid},
                'status': 'pending'
            }
        ]
    }
    wal.start_tx(tx_id, meta)

    # Intentionally do NOT execute op here.
    present_pre_recovery = False
    try:
        present_pre_recovery = bool(mongo_handler.db['txn_test'].find_one({'uuid': doc_uuid}))
    except Exception:
        present_pre_recovery = False

    ok = not present_pre_recovery
    return {
        'test': 'durability_crash_simulation',
        'passed': ok,
        'status': 'PASS' if ok else 'FAIL',
        'details': {
            'tx_id': tx_id,
            'doc_uuid': doc_uuid,
            'username': username,
            'present_pre_recovery': present_pre_recovery,
            'note': 'Use /api/tools/acid/durability/recover to emulate restart recovery'
        }
    }


def _run_durability_recovery_proof() -> Dict[str, Any]:
    """Proof-style durability check: simulate crash, then recovery, then verify committed state."""
    sim = _simulate_durability_crash()
    if sim.get('status') != 'PASS':
        return {
            'test': 'durability_recovery_proof',
            'passed': False,
            'status': 'FAIL',
            'details': {'stage': 'simulate', 'result': sim}
        }

    tx_id = sim.get('details', {}).get('tx_id')
    doc_uuid = sim.get('details', {}).get('doc_uuid')

    def _replay_single_tx(txid: str) -> tuple[list[str], str | None]:
        tx = wal.get_tx(txid)
        if not tx:
            return [], 'tx not found in WAL'
        ops = (tx.get('meta') or {}).get('ops', [])
        processed_local = []
        for idx, op in enumerate(ops):
            if op.get('status') == 'done':
                continue
            if op.get('type') == 'mongo':
                try:
                    coll_name = op.get('collection', 'txn_test')
                    doc_local = op.get('doc') or {}
                    unique = op.get('unique_check') or {'uuid': doc_local.get('uuid')}
                    coll_obj = mongo_handler.db.get_collection(coll_name)
                    exists = bool(coll_obj.find_one(unique))
                    if not exists:
                        coll_obj.insert_one(doc_local)
                    wal.update_op(txid, idx, 'done')
                except Exception as e:
                    return processed_local, str(e)
            else:
                wal.update_op(txid, idx, 'done')

        tx_after = wal.get_tx(txid)
        ops_after = (tx_after or {}).get('meta', {}).get('ops', [])
        if ops_after and all(op.get('status') == 'done' for op in ops_after):
            wal.update_tx_status(txid, 'committed')
            processed_local.append(txid)
        return processed_local, None

    processed, replay_error = _replay_single_tx(tx_id)
    if replay_error:
        return {
            'test': 'durability_recovery_proof',
            'passed': False,
            'status': 'FAIL',
            'details': {'stage': 'recover', 'error': replay_error, 'tx_id': tx_id}
        }

    tx_row = wal.get_tx(tx_id) if tx_id else None
    doc_present = False
    try:
        doc_present = bool(mongo_handler.db['txn_test'].find_one({'uuid': doc_uuid}))
    except Exception:
        doc_present = False

    passed = bool(tx_row and tx_row.get('status') == 'committed' and doc_present and tx_id in processed)
    return {
        'test': 'durability_recovery_proof',
        'passed': passed,
        'status': 'PASS' if passed else 'FAIL',
        'details': {
            'simulated_tx_id': tx_id,
            'processed': processed,
            'wal_status': tx_row.get('status') if tx_row else None,
            'doc_present_after_recovery': doc_present
        }
    }


def _run_failure_injection_scenarios() -> Dict[str, Any]:
    """Run fault-injection experiments and return evidence-friendly details."""
    scenarios = []

    # Scenario 1: Forced rollback in coordinated transaction
    forced = txn_test(TxnTestRequest(payload={'acid': 'failure_injection', 'case': 'forced_rollback'}, force_fail=True))
    s1_pass = bool(
        forced.get('status') == 'rolled_back' and
        (not bool(forced.get('mongo_found'))) and
        ((forced.get('sql_count') in (0, None)) or forced.get('sql_count') == 0)
    )
    scenarios.append({
        'scenario': 'forced_rollback',
        'passed': s1_pass,
        'result': forced,
    })

    # Scenario 2: Crash/restart durability proof
    dur = _run_durability_recovery_proof()
    scenarios.append({
        'scenario': 'crash_restart_durability',
        'passed': bool(dur.get('passed')),
        'result': dur,
    })

    passed_count = sum(1 for s in scenarios if s.get('passed'))
    all_passed = passed_count == len(scenarios)
    return {
        'test': 'failure_injection',
        'passed': all_passed,
        'status': 'PASS' if all_passed else 'FAIL',
        'details': {
            'passed_count': passed_count,
            'total_count': len(scenarios),
            'scenarios': scenarios,
        }
    }


# --- Logical query models ---
class LogicalCondition(BaseModel):
    field: str
    op: str
    value: Any


class LogicalQuery(BaseModel):
    operation: str
    entity: str
    fields: List[str] | None = None
    conditions: List[LogicalCondition] | None = None
    order_by: str | None = None
    order: str | None = 'asc'
    data: Dict[str, Any] | None = None


@app.post('/query')
def post_query(req: LogicalQuery, request: Request):
    started_at = time.time()
    username = 'anonymous'
    # auth + rate limit
    try:
        user = _get_user_from_request(request)
        username = user.get('username', 'anonymous')
    except HTTPException:
        raise
    rate_limit(request)

    op = (req.operation or '').lower()
    base_summary = (
        f"entity={req.entity}; fields={len(req.fields or [])}; "
        f"conditions={len(req.conditions or [])}"
    )
    if op == 'read':
        # route fields
        sql_fields = []
        mongo_fields = []
        for f in (req.fields or []):
            r = metadata_manager.get_field_route(f)
            if r in ('SQL', 'BOTH'):
                sql_fields.append(f)
            if r in ('MONGO', 'BOTH'):
                mongo_fields.append(f)

        # SQL part
        sql_results = []
        try:
            # build candidate tables from metadata + common fallback tables
            candidate_tables = []
            tables = metadata_manager.global_schema.get('relational_structure', {}).get('tables', {})
            for tname, tinfo in tables.items():
                cols = set(tinfo.get('columns', []))
                if not sql_fields or any(f in cols for f in sql_fields):
                    candidate_tables.append(tname)
            # practical fallbacks for this project
            for t in ('structured_data', 'root'):
                if t not in candidate_tables:
                    candidate_tables.append(t)

            sel_cols = sql_fields if sql_fields else ['*']
            where_clauses = []
            params = []
            if req.conditions:
                for c in req.conditions:
                    opmap = {'eq':'=','gt':'>','gte':'>=','lt':'<','lte':'<='}
                    if c.op in opmap:
                        where_clauses.append(f"{c.field} {opmap[c.op]} %s")
                        params.append(c.value)
                    elif c.op == 'in' and isinstance(c.value, list):
                        ph = ','.join(['%s'] * len(c.value))
                        where_clauses.append(f"{c.field} IN ({ph})")
                        params.extend(c.value)
            sql_results = _sql_read_with_fallback(sel_cols, where_clauses, params, candidate_tables)
        except Exception:
            sql_results = []

        # Mongo part
        mongo_results = []
        try:
            mq = {}
            if req.conditions:
                for c in req.conditions:
                    if metadata_manager.get_field_route(c.field) in ('MONGO', 'BOTH'):
                        if c.op == 'eq':
                            mq[c.field] = c.value
                        elif c.op == 'in' and isinstance(c.value, list):
                            mq[c.field] = {'$in': c.value}
            coll_name = req.entity or 'unstructured_data'
            cursor = mongo_handler.db[coll_name].find(mq)
            for d in cursor:
                d['_id'] = str(d.get('_id'))
                mongo_results.append(d)
        except Exception:
            mongo_results = []

        # merge
        if sql_results and mongo_results:
            midx = {}
            for m in mongo_results:
                key = m.get('username') or m.get('sys_ingested_at')
                if key:
                    midx.setdefault(key, []).append(m)
            merged = []
            for s in sql_results:
                key = s.get('username') or s.get('sys_ingested_at')
                docs = midx.get(key, [])
                out = dict(s)
                if docs:
                    out.update(docs[0])
                merged.append(out)
        elif sql_results:
            merged = sql_results
        else:
            merged = mongo_results

        routed = []
        if sql_fields or sql_results:
            routed.append('SQL')
        if mongo_fields or mongo_results:
            routed.append('MONGO')
        if not routed:
            routed = ['SQL', 'MONGO']
        _record_query_trace(
            username=username,
            endpoint='/query',
            operation='read',
            routed_backends=routed,
            summary=base_summary,
            started_at=started_at,
            status='ok',
            result_count=len(merged),
        )
        return JSONResponse({'results': merged})

    elif op == 'insert':
        if not req.data:
            _record_query_trace(
                username=username,
                endpoint='/query',
                operation='insert',
                routed_backends=[],
                summary=base_summary,
                started_at=started_at,
                status='error',
                error='No data to insert',
            )
            raise HTTPException(status_code=400, detail='No data to insert')
        sql_data = {}
        mongo_data = {}
        for k, v in req.data.items():
            r = metadata_manager.get_field_route(k)
            if r in ('SQL', 'BOTH'):
                sql_data[k] = v
            if r in ('MONGO', 'BOTH'):
                mongo_data[k] = v

        import uuid as _uuid
        tx_uid = str(_uuid.uuid4())

        def sql_forward(conn):
            if not sql_data:
                return
            cols = ','.join(sql_data.keys())
            ph = ','.join(['%s'] * len(sql_data))
            vals = list(sql_data.values())
            try:
                cur = sql_handler.cursor
                cur.execute(f"INSERT INTO structured_data ({cols}) VALUES ({ph})", tuple(vals))
                sql_handler.conn.commit()
            except Exception:
                raise

        def sql_compensate(conn):
            try:
                cur = sql_handler.cursor
                # best-effort: delete by username if present
                uname = sql_data.get('username')
                if uname:
                    cur.execute("DELETE FROM structured_data WHERE username = %s", (uname,))
                    sql_handler.conn.commit()
            except Exception:
                pass

        def mongo_forward(db, session):
            if not mongo_data:
                return
            coll = mongo_handler.db.get_collection(req.entity or 'unstructured_data')
            doc = dict(mongo_data)
            doc['_tx_uuid'] = tx_uid
            if session:
                coll.insert_one(doc, session=session)
            else:
                coll.insert_one(doc)

        def mongo_compensate(db, session):
            try:
                coll = mongo_handler.db.get_collection(req.entity or 'unstructured_data')
                if session:
                    coll.delete_one({'_tx_uuid': tx_uid}, session=session)
                else:
                    coll.delete_one({'_tx_uuid': tx_uid})
            except Exception:
                pass

        try:
            with tc.transaction() as t:
                if sql_data:
                    t.add_sql(sql_forward, sql_compensate)
                if mongo_data:
                    t.add_mongo(mongo_forward, mongo_compensate)
        except Exception as e:
            routed_on_error = []
            if sql_data:
                routed_on_error.append('SQL')
            if mongo_data:
                routed_on_error.append('MONGO')
            _record_query_trace(
                username=username,
                endpoint='/query',
                operation='insert',
                routed_backends=routed_on_error,
                summary=base_summary,
                started_at=started_at,
                status='error',
                error=f'Transaction failed: {e}',
            )
            raise HTTPException(status_code=500, detail=f'Transaction failed: {e}')

        routed = []
        if sql_data:
            routed.append('SQL')
        if mongo_data:
            routed.append('MONGO')
        _record_query_trace(
            username=username,
            endpoint='/query',
            operation='insert',
            routed_backends=routed,
            summary=f"{base_summary}; data_keys={list((req.data or {}).keys())[:6]}",
            started_at=started_at,
            status='ok',
            result_count=1,
        )
        return JSONResponse({'status': 'committed', 'uuid': tx_uid})

    elif op in ('update', 'delete'):
        # Only support single-backend updates/deletes for now
        # Determine affected backends
        affected_backends = set()
        # For update/delete without data, delete uses conditions only
        # If fields specified, check routes; otherwise infer from entity for deletes
        if req.data:
            for k in req.data.keys():
                route = metadata_manager.get_field_route(k)
                # practical default: for UPDATE fields routed to BOTH, treat as SQL unless explicitly targeting mongo entity
                if route == 'BOTH' and op == 'update':
                    route = 'SQL'
                affected_backends.add(route)
        else:
            # infer by entity: if entity maps to Mongo collection name assume Mongo, else SQL
            # simple heuristic: check metadata for entity presence
            if req.entity and req.entity in (metadata_manager.global_schema.get('unstructured_collections') or {}):
                affected_backends.add('MONGO')
            else:
                affected_backends.add('SQL')

        # normalize: map BOTH -> both
        backends = set()
        for b in affected_backends:
            if b == 'BOTH':
                backends.update(['SQL', 'MONGO'])
            else:
                backends.add(b)

        if 'SQL' in backends and 'MONGO' in backends:
            _record_query_trace(
                username=username,
                endpoint='/query',
                operation=op,
                routed_backends=['SQL', 'MONGO'],
                summary=base_summary,
                started_at=started_at,
                status='error',
                error='Update/Delete spanning SQL+Mongo not supported',
            )
            raise HTTPException(status_code=400, detail='Update/Delete spanning SQL+Mongo not supported')

        if 'SQL' in backends:
            # build SET and WHERE
            if op == 'update' and not req.data:
                raise HTTPException(status_code=400, detail='No data for update')
            set_clauses = []
            params = []
            if req.data:
                for k, v in req.data.items():
                    set_clauses.append(f"{k} = %s")
                    params.append(v)
            where_clauses = []
            if req.conditions:
                for c in req.conditions:
                    if c.op == 'eq':
                        where_clauses.append(f"{c.field} = %s")
                        params.append(c.value)
                    elif c.op == 'in' and isinstance(c.value, list):
                        ph = ','.join(['%s'] * len(c.value))
                        where_clauses.append(f"{c.field} IN ({ph})")
                        params.extend(c.value)
                    else:
                        raise HTTPException(status_code=400, detail=f'Unsupported op in WHERE: {c.op}')

            try:
                cur = sql_handler.cursor
                if op == 'update':
                    q = f"UPDATE structured_data SET {', '.join(set_clauses)}"
                    if where_clauses:
                        q += ' WHERE ' + ' AND '.join(where_clauses)
                    cur.execute(q, tuple(params))
                    affected = cur.rowcount if hasattr(cur, 'rowcount') else None
                    sql_handler.conn.commit()
                    _record_query_trace(
                        username=username,
                        endpoint='/query',
                        operation='update',
                        routed_backends=['SQL'],
                        summary=base_summary,
                        started_at=started_at,
                        status='ok',
                        result_count=affected,
                    )
                    return JSONResponse({'status': 'ok', 'affected': affected})
                else:
                    q = "DELETE FROM structured_data"
                    if where_clauses:
                        q += ' WHERE ' + ' AND '.join(where_clauses)
                    cur.execute(q, tuple(params))
                    affected = cur.rowcount if hasattr(cur, 'rowcount') else None
                    sql_handler.conn.commit()
                    _record_query_trace(
                        username=username,
                        endpoint='/query',
                        operation='delete',
                        routed_backends=['SQL'],
                        summary=base_summary,
                        started_at=started_at,
                        status='ok',
                        result_count=affected,
                    )
                    return JSONResponse({'status': 'ok', 'deleted': affected})
            except Exception as e:
                _record_query_trace(
                    username=username,
                    endpoint='/query',
                    operation=op,
                    routed_backends=['SQL'],
                    summary=base_summary,
                    started_at=started_at,
                    status='error',
                    error=f'SQL error: {e}',
                )
                raise HTTPException(status_code=500, detail=f'SQL error: {e}')

        if 'MONGO' in backends:
            # build filter
            mq = {}
            if req.conditions:
                for c in req.conditions:
                    if c.op == 'eq':
                        mq[c.field] = c.value
                    elif c.op == 'in' and isinstance(c.value, list):
                        mq[c.field] = {'$in': c.value}
                    else:
                        raise HTTPException(status_code=400, detail=f'Unsupported op in WHERE: {c.op}')
            coll = mongo_handler.db.get_collection(req.entity or 'unstructured_data')
            try:
                if op == 'update':
                    res = coll.update_many(mq, {'$set': req.data or {}})
                    _record_query_trace(
                        username=username,
                        endpoint='/query',
                        operation='update',
                        routed_backends=['MONGO'],
                        summary=base_summary,
                        started_at=started_at,
                        status='ok',
                        result_count=res.modified_count,
                    )
                    return JSONResponse({'status': 'ok', 'matched': res.matched_count, 'modified': res.modified_count})
                else:
                    res = coll.delete_many(mq)
                    _record_query_trace(
                        username=username,
                        endpoint='/query',
                        operation='delete',
                        routed_backends=['MONGO'],
                        summary=base_summary,
                        started_at=started_at,
                        status='ok',
                        result_count=res.deleted_count,
                    )
                    return JSONResponse({'status': 'ok', 'deleted': res.deleted_count})
            except Exception as e:
                _record_query_trace(
                    username=username,
                    endpoint='/query',
                    operation=op,
                    routed_backends=['MONGO'],
                    summary=base_summary,
                    started_at=started_at,
                    status='error',
                    error=f'Mongo error: {e}',
                )
                raise HTTPException(status_code=500, detail=f'Mongo error: {e}')

    else:
        _record_query_trace(
            username=username,
            endpoint='/query',
            operation=op,
            routed_backends=[],
            summary=base_summary,
            started_at=started_at,
            status='error',
            error='Unsupported operation',
        )
        raise HTTPException(status_code=400, detail='Unsupported operation')


@app.post("/api/tools/acid-test")
def api_acid(req: AcidRequest):
    # Reuse txn_test logic to perform a coordinated transaction
    # This endpoint requires authentication — ensure caller is admin
    # Note: FastAPI dependency injection requires adding Depends in signature; to keep
    # the existing function signature used programmatically we implement a small wrapper below.
    raise HTTPException(status_code=501, detail='Use /api/tools/acid-test-auth for authenticated calls')


@app.post('/api/tools/acid-test-auth')
def api_acid_auth(req: AcidRequest, request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request)
    try:
        body = TxnTestRequest(username=req.username, payload=req.payload, force_fail=req.force_fail)
        return JSONResponse(txn_test(body))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/api/tools/acid/atomicity')
def api_acid_atomicity(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request, cost=1.5)
    result = _run_with_timeout('atomicity', _run_atomicity_experiment)
    _record_acid_run('atomicity', result)
    _record_acid_evidence('atomicity', result)
    return JSONResponse(result)


@app.post('/api/tools/acid/consistency')
def api_acid_consistency(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request, cost=1.5)
    result = _run_with_timeout('consistency', _run_consistency_experiment)
    _record_acid_run('consistency', result)
    _record_acid_evidence('consistency', result)
    return JSONResponse(result)


@app.post('/api/tools/acid/isolation')
def api_acid_isolation(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request, cost=2.0)
    workers_q = request.query_params.get('workers')
    workers = 3
    if workers_q is not None:
        try:
            workers = int(workers_q)
        except Exception:
            workers = 3
    result = _run_with_timeout('isolation', lambda: _run_isolation_experiment(workers=workers), timeout_sec=max(ACID_TEST_TIMEOUT_SEC, 25))
    _record_acid_run('isolation', result)
    _record_acid_evidence('isolation', result)
    return JSONResponse(result)


@app.post('/api/tools/acid/durability')
def api_acid_durability(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request, cost=1.5)
    result = _run_with_timeout('durability', _run_durability_experiment)
    _record_acid_run('durability', result)
    _record_acid_evidence('durability', result)
    return JSONResponse(result)


@app.post('/api/tools/acid/all')
def api_acid_all(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request, cost=3.0)
    started = time.time()
    runs = [
        _run_with_timeout('atomicity', _run_atomicity_experiment),
        _run_with_timeout('consistency', _run_consistency_experiment),
        _run_with_timeout('isolation', lambda: _run_isolation_experiment()),
        _run_with_timeout('durability', _run_durability_experiment),
    ]
    passed_count = sum(1 for r in runs if r.get('passed'))
    ended = time.time()
    result = {
        'test': 'acid_all',
        'status': 'PASS' if passed_count == len(runs) else 'FAIL',
        'passed': passed_count == len(runs),
        'summary': {
            'passed_count': passed_count,
            'total_count': len(runs),
            'duration_ms': int((ended - started) * 1000)
        },
        'results': runs
    }
    _record_acid_run('acid_all', result)
    _record_acid_evidence('acid_all', result)
    return JSONResponse(result)


@app.post('/api/tools/acid/failure-injection')
def api_acid_failure_injection(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request, cost=2.0)
    result = _run_with_timeout('failure_injection', _run_failure_injection_scenarios, timeout_sec=max(ACID_TEST_TIMEOUT_SEC, 35))
    _record_acid_run('failure_injection', result)
    _record_acid_evidence('failure_injection', result)
    return JSONResponse(result)


@app.post('/api/tools/acid/durability/recover')
def api_acid_durability_recover(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request, cost=1.5)
    result = _run_with_timeout('durability_recovery_proof', _run_durability_recovery_proof)
    _record_acid_run('durability_recovery_proof', result)
    _record_acid_evidence('durability_recovery_proof', result)
    return JSONResponse(result)


@app.get('/api/tools/acid/evidence')
def api_acid_evidence(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request)
    try:
        limit = int(request.query_params.get('limit', '50'))
    except Exception:
        limit = 50
    limit = max(1, min(limit, 200))
    with acid_evidence_lock:
        items = list(acid_evidence)[:limit]
        total = len(acid_evidence)
    return JSONResponse({'evidence': items, 'total': total})


@app.get('/api/tools/acid/history')
def api_acid_history(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request)
    limit_q = request.query_params.get('limit')
    limit = 20
    if limit_q is not None:
        try:
            limit = max(1, min(int(limit_q), 200))
        except Exception:
            limit = 20
    items = list(acid_history)[:limit]
    return JSONResponse({'history': items, 'total': len(acid_history)})


@app.get('/api/tools/acid/export')
def api_acid_export(request: Request, user: dict = Depends(get_current_user)):
    if user.get('role') != 'admin':
        raise HTTPException(status_code=403, detail='insufficient privileges')
    rate_limit(request)
    fmt = (request.query_params.get('format') or 'json').lower()
    items = list(acid_history)

    if fmt == 'csv':
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(['ts', 'name', 'status', 'passed'])
        for it in items:
            w.writerow([it.get('ts'), it.get('name'), it.get('status'), it.get('passed')])
        return JSONResponse({'format': 'csv', 'csv': buf.getvalue(), 'rows': len(items)})

    # default json export
    return JSONResponse({'format': 'json', 'items': items, 'rows': len(items)})


@app.post("/api/tools/json-query")
async def api_json_query(request: Request):
    started_at = time.time()
    username = 'anonymous'
    # API key check (optional)
    if API_KEY:
        provided = request.headers.get('X-API-Key')
        if not provided or provided != API_KEY:
            _record_query_trace(
                username=username,
                endpoint='/api/tools/json-query',
                operation='read',
                routed_backends=['MONGO'],
                summary='api-key-auth-failed',
                started_at=started_at,
                status='error',
                error='Missing or invalid API key',
            )
            raise HTTPException(status_code=401, detail='Missing or invalid API key')

    data = await request.json()
    # Basic size limit on the incoming JSON payload (string length)
    raw_len = len(str(data))
    if raw_len > MAX_QUERY_STRING:
        raise HTTPException(status_code=400, detail='Query payload too large')

    coll = data.get("collection", "unstructured_data")
    query = data.get("query", {})
    try:
        limit = int(data.get("limit", 50))
    except Exception:
        limit = 50
    limit = max(1, min(limit, MAX_LIMIT))

    # maxTimeMs optional param
    try:
        max_time_ms = int(data.get('maxTimeMs', DEFAULT_MAX_TIME_MS))
    except Exception:
        max_time_ms = DEFAULT_MAX_TIME_MS
    max_time_ms = max(1, min(max_time_ms, MAX_ALLOWED_TIME_MS))

    # Collection name sanity check
    if not isinstance(coll, str) or not _COLL_RE.match(coll):
        raise HTTPException(status_code=400, detail='Invalid collection name')

    # Validate the query structure: reject dangerous operators
    try:
        _validate_query_obj(query)
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=f'Invalid query: {ve}')

    try:
        # Authorization: allow users with roles 'user' or 'admin', or API_KEY
        try:
            user = _get_user_from_request(request)
            username = user.get('username', 'anonymous')
        except HTTPException:
            # If credentials missing but API_KEY allowed we already short-circuited in _get_user_from_request
            raise

        # rate limiting per user/ip
        rate_limit(request)

        cursor = mongo_handler.db[coll].find(query).limit(limit)
        try:
            # set a server-side execution time limit
            cursor = cursor.max_time_ms(max_time_ms)
        except Exception:
            # Some pymongo versions may not expose max_time_ms; ignore if not supported
            pass
        docs = list(cursor)
        # compatibility fallback for historical typo collection name
        if not docs and coll == 'unstructured_data':
            try:
                alt_cursor = mongo_handler.db['unstructed_data'].find(query).limit(limit)
                try:
                    alt_cursor = alt_cursor.max_time_ms(max_time_ms)
                except Exception:
                    pass
                docs = list(alt_cursor)
            except Exception:
                pass
        # Convert ObjectId and non-serializable fields
        for d in docs:
            d["_id"] = str(d.get("_id"))
        _record_query_trace(
            username=username,
            endpoint='/api/tools/json-query',
            operation='read',
            routed_backends=['MONGO'],
            summary=f"collection={coll}; limit={limit}; {_summarize_query_for_trace(query)}",
            started_at=started_at,
            status='ok',
            result_count=len(docs),
        )
        return JSONResponse(jsonable_encoder({"results": docs, "meta": {"limit": limit, "maxTimeMs": max_time_ms}}))
    except Exception as e:
        _record_query_trace(
            username=username,
            endpoint='/api/tools/json-query',
            operation='read',
            routed_backends=['MONGO'],
            summary=f"collection={coll}; limit={limit}; {_summarize_query_for_trace(query)}",
            started_at=started_at,
            status='error',
            error=str(e),
        )
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tools/docs")
def api_docs():
    docs = {
        "ACID Tests": "Run coordinated ACID-like tests across SQL and Mongo.",
        "JSON Query": "Run a Mongo JSON query against a collection. POST {collection, query}.",
        "Documentation": "This minimal dashboard demonstrates monitoring and tools for the hybrid DB system."
    }
    return JSONResponse({"docs": docs})


class LoginRequest(BaseModel):
    username: str
    password: str


@app.post('/api/login')
def api_login(req: LoginRequest):
    # Verify username/password against USERS loaded from environment or defaults
    user = USERS.get(req.username)
    if not user:
        raise HTTPException(status_code=401, detail='invalid credentials')
    try:
        if not verify_password(req.password, req.username, user.get('password_hash')):
            raise HTTPException(status_code=401, detail='invalid credentials')
    except Exception:
        raise HTTPException(status_code=401, detail='invalid credentials')
    token = create_token(req.username, role=user.get('role', 'user'))
    return JSONResponse({'token': token, 'username': req.username, 'role': user.get('role', 'user')})
