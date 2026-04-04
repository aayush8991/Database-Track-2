from __future__ import annotations
import json
import datetime
from typing import Any, Dict, List, Optional
import hashlib
import json as _json
import uuid


class WALManager:
    """Simple persistent WAL manager using the project's SQLHandler.

    It stores minimal transaction metadata to allow a recovery worker to
    detect in-doubt transactions and run compensating cleanup.
    """
    def __init__(self, sql_handler):
        import threading
        self.lock = threading.Lock()
        self.sql = sql_handler
        self.table = 'transaction_wal'
        try:
            self._ensure_table()
        except Exception:
            # best-effort; SQLHandler may be unavailable in some test environments
            pass

    def _ensure_table(self):
        with self.lock:
            if hasattr(self.sql, 'ensure_connection'):
                self.sql.ensure_connection()
            cur = self.sql.cursor
            # Use JSON column when available; fallback to TEXT
            create = f"""
        CREATE TABLE IF NOT EXISTS `{self.table}` (
            tx_id VARCHAR(64) PRIMARY KEY,
            status VARCHAR(32) NOT NULL,
            meta JSON,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        try:
            cur.execute(create)
            self.sql.conn.commit()
        except Exception:
            # Try fallback without JSON type (TEXT)
            create2 = f"""
            CREATE TABLE IF NOT EXISTS `{self.table}` (
                tx_id VARCHAR(64) PRIMARY KEY,
                status VARCHAR(32) NOT NULL,
                meta TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            cur.execute(create2)
            self.sql.conn.commit()

    def start_tx(self, tx_id: str, meta: Dict[str, Any]):
        # Ensure ops have checksums and idempotency keys
        ops = meta.get('ops', [])
        for i, op in enumerate(ops):
            if 'idempotency_key' not in op:
                op['idempotency_key'] = str(uuid.uuid4())
            if 'checksum' not in op:
                op['checksum'] = self._compute_op_checksum(op)
            if 'retries' not in op:
                op['retries'] = 0
            if 'status' not in op:
                op['status'] = 'pending'
            ops[i] = op
        meta['ops'] = ops

        with self.lock:
            if hasattr(self.sql, 'ensure_connection'):
                self.sql.ensure_connection()
            cur = self.sql.cursor
            meta_s = _json.dumps(meta, default=str)
            try:
                cur.execute(f"REPLACE INTO `{self.table}` (tx_id, status, meta) VALUES (%s, %s, %s)", (tx_id, 'in_progress', meta_s))
                self.sql.conn.commit()
            except Exception:
                # retry once after reconnect
                if hasattr(self.sql, 'ensure_connection'):
                    self.sql.ensure_connection()
                cur = self.sql.cursor
                cur.execute(f"REPLACE INTO `{self.table}` (tx_id, status, meta) VALUES (%s, %s, %s)", (tx_id, 'in_progress', meta_s))
                self.sql.conn.commit()

    def append_op(self, tx_id: str, op: Dict[str, Any]):
        """Append an operation descriptor to the WAL meta `ops` array."""
        with self.lock:
            if hasattr(self.sql, 'ensure_connection'):
                self.sql.ensure_connection()
            cur = self.sql.cursor
            cur.execute(f"SELECT meta FROM `{self.table}` WHERE tx_id=%s", (tx_id,))
            row = cur.fetchone()
            if not row:
                return
            try:
                meta = _json.loads(row[0]) if row[0] else {}
            except Exception:
                meta = {}
            ops = meta.get('ops', [])
            # annotate op
            if 'idempotency_key' not in op:
                op['idempotency_key'] = str(uuid.uuid4())
            if 'checksum' not in op:
                op['checksum'] = self._compute_op_checksum(op)
            if 'retries' not in op:
                op['retries'] = 0
            if 'status' not in op:
                op['status'] = 'pending'
            ops.append(op)
            meta['ops'] = ops
            meta_s = _json.dumps(meta, default=str)
            cur.execute(f"UPDATE `{self.table}` SET meta=%s, updated_at=CURRENT_TIMESTAMP WHERE tx_id=%s", (meta_s, tx_id))
            self.sql.conn.commit()

    def update_op(self, tx_id: str, op_index: int, new_status: str, error: Optional[Dict[str, Any]] = None, inc_retry: bool = False):
        """Update the status of an operation and optionally attach error info or increment retries."""
        with self.lock:
            if hasattr(self.sql, 'ensure_connection'):
                self.sql.ensure_connection()
            cur = self.sql.cursor
            cur.execute(f"SELECT meta FROM `{self.table}` WHERE tx_id=%s", (tx_id,))
            row = cur.fetchone()
            if not row:
                return
            try:
                meta = _json.loads(row[0]) if row[0] else {}
            except Exception:
                meta = {}
            ops = meta.get('ops', [])
            if 0 <= op_index < len(ops):
                if inc_retry:
                    ops[op_index]['retries'] = ops[op_index].get('retries', 0) + 1
                ops[op_index]['status'] = new_status
                if error:
                    ops[op_index]['last_error'] = error
                meta['ops'] = ops
                meta_s = _json.dumps(meta, default=str)
                cur.execute(f"UPDATE `{self.table}` SET meta=%s, updated_at=CURRENT_TIMESTAMP WHERE tx_id=%s", (meta_s, tx_id))
            self.sql.conn.commit()

    def update_tx_status(self, tx_id: str, status: str):
        with self.lock:
            if hasattr(self.sql, 'ensure_connection'):
                self.sql.ensure_connection()
            cur = self.sql.cursor
            cur.execute(f"UPDATE `{self.table}` SET status=%s, updated_at=CURRENT_TIMESTAMP WHERE tx_id=%s", (status, tx_id))
            self.sql.conn.commit()

    def get_incomplete(self) -> List[Dict[str, Any]]:
        with self.lock:
            if hasattr(self.sql, 'ensure_connection'):
                self.sql.ensure_connection()
            cur = self.sql.cursor
            cur.execute(f"SELECT tx_id, status, meta, created_at, updated_at FROM `{self.table}` WHERE status='in_progress' OR status='pending'")
            rows = cur.fetchall()
            out = []
            for r in rows:
                try:
                    meta = json.loads(r[2]) if r[2] else None
                except Exception:
                    meta = None
                out.append({'tx_id': r[0], 'status': r[1], 'meta': meta, 'created_at': r[3], 'updated_at': r[4]})
            return out

    def get_tx(self, tx_id: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            if hasattr(self.sql, 'ensure_connection'):
                self.sql.ensure_connection()
            cur = self.sql.cursor
            cur.execute(f"SELECT tx_id, status, meta, created_at, updated_at FROM `{self.table}` WHERE tx_id=%s", (tx_id,))
            r = cur.fetchone()
            if not r:
                return None
            try:
                meta = _json.loads(r[2]) if r[2] else None
            except Exception:
                meta = None
            return {'tx_id': r[0], 'status': r[1], 'meta': meta, 'created_at': r[3], 'updated_at': r[4]}

    def update_meta(self, tx_id: str, meta: Dict[str, Any]):
        with self.lock:
            if hasattr(self.sql, 'ensure_connection'):
                self.sql.ensure_connection()
            cur = self.sql.cursor
            meta_s = _json.dumps(meta, default=str)
            cur.execute(f"UPDATE `{self.table}` SET meta=%s, updated_at=CURRENT_TIMESTAMP WHERE tx_id=%s", (meta_s, tx_id))
            self.sql.conn.commit()

    def _compute_op_checksum(self, op: Dict[str, Any]) -> str:
        try:
            # compute canonical JSON then sha256
            j = _json.dumps(op, sort_keys=True, default=str)
            return hashlib.sha256(j.encode()).hexdigest()
        except Exception:
            return ''
