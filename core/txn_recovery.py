"""Recovery worker for WAL-based transactions.

This module provides a simple recovery routine that looks for WAL entries
left `in_progress` and attempts to run compensating cleanup actions using
conservative, idempotent operations (delete by uuid/username).
"""
from typing import Any
import time


def recover_incomplete(wal, sql_handler, mongo_handler, limit: int = 100):
    """Scan WAL for incomplete transactions and attempt compensating cleanup.

    Returns a list of processed tx_ids.
    """
    processed = []
    rows = wal.get_incomplete()
    MAX_RETRIES = 3
    for r in rows[:limit]:
        tx_id = r.get('tx_id')
        meta = r.get('meta') or {}
        ops = meta.get('ops', [])
        any_changed = False
        try:
            # Replay forward operations when safe and mark op status
            for idx, op in enumerate(ops):
                status = op.get('status')
                if status == 'done':
                    continue
                # SQL replay
                if op.get('type') == 'sql':
                    # expect op to include 'sql' and 'params' and optional 'unique_check'
                    unique = op.get('unique_check')
                    should_insert = True
                    try:
                        if unique and sql_handler and getattr(sql_handler, 'cursor', None):
                            cur = sql_handler.cursor
                            # Build where clause from unique_check dict
                            where_clauses = []
                            vals = []
                            for k, v in unique.items():
                                where_clauses.append(f"{k} = %s")
                                vals.append(v)
                            q = f"SELECT COUNT(*) FROM structured_data WHERE {' AND '.join(where_clauses)}"
                            cur.execute(q, tuple(vals))
                            cnt = cur.fetchone()[0]
                            if cnt > 0:
                                should_insert = False
                    except Exception:
                        pass

                    if should_insert and 'sql' in op:
                        # attempt with retry/backoff and error categorization
                        retries = op.get('retries', 0)
                        try:
                            if hasattr(sql_handler, 'engine') and sql_handler.engine:
                                with sql_handler.engine.begin() as conn:
                                    conn.execute(op['sql'], tuple(op.get('params', [])))
                            else:
                                cur = sql_handler.cursor
                                cur.execute(op['sql'], tuple(op.get('params', [])))
                                sql_handler.conn.commit()
                            wal.update_op(tx_id, idx, 'done')
                            any_changed = True
                        except Exception as e:
                            msg = str(e)
                            low = msg.lower()
                            # permanent if duplicate/unique constraint
                            if 'duplicate' in low or 'unique' in low or 'integrity' in low:
                                # treat as already present
                                wal.update_op(tx_id, idx, 'done', error={'msg': msg, 'type': 'permanent'})
                                any_changed = True
                                continue
                            # transient
                            if retries + 1 >= MAX_RETRIES:
                                wal.update_op(tx_id, idx, 'failed', error={'msg': msg, 'type': 'permanent'}, inc_retry=True)
                                continue
                            else:
                                wal.update_op(tx_id, idx, 'retrying', error={'msg': msg, 'type': 'transient'}, inc_retry=True)
                                # backoff
                                try:
                                    time.sleep(0.2 * (2 ** retries))
                                except Exception:
                                    pass
                                # leave for next pass
                                continue

                # Mongo replay
                elif op.get('type') == 'mongo':
                    unique = op.get('unique_check')
                    coll_name = op.get('collection')
                    doc = op.get('doc')
                    try:
                        coll = mongo_handler.db.get_collection(coll_name)
                        exists = False
                        if unique and isinstance(unique, dict):
                            exists = coll.count_documents(unique, limit=1) > 0
                        elif doc and 'uuid' in doc:
                            exists = coll.count_documents({'uuid': doc['uuid']}, limit=1) > 0

                        if not exists and doc:
                            retries = op.get('retries', 0)
                            try:
                                coll.insert_one(doc)
                                wal.update_op(tx_id, idx, 'done')
                                any_changed = True
                            except Exception as e:
                                msg = str(e)
                                low = msg.lower()
                                if 'duplicate' in low or 'unique' in low:
                                    wal.update_op(tx_id, idx, 'done', error={'msg': msg, 'type': 'permanent'})
                                    any_changed = True
                                    continue
                                if retries + 1 >= MAX_RETRIES:
                                    wal.update_op(tx_id, idx, 'failed', error={'msg': msg, 'type': 'permanent'}, inc_retry=True)
                                    continue
                                else:
                                    wal.update_op(tx_id, idx, 'retrying', error={'msg': msg, 'type': 'transient'}, inc_retry=True)
                                    try:
                                        time.sleep(0.2 * (2 ** retries))
                                    except Exception:
                                        pass
                                    continue
                        else:
                            # already present
                            wal.update_op(tx_id, idx, 'done')
                            any_changed = True
                    except Exception as e:
                        # If mongo unavailable, mark retry
                        msg = str(e)
                        retries = op.get('retries', 0)
                        if retries + 1 >= MAX_RETRIES:
                            wal.update_op(tx_id, idx, 'failed', error={'msg': msg, 'type': 'permanent'}, inc_retry=True)
                            continue
                        else:
                            wal.update_op(tx_id, idx, 'retrying', error={'msg': msg, 'type': 'transient'}, inc_retry=True)
                            continue

            # If all ops are done, mark transaction committed
            tx = wal.get_tx(tx_id)
            tx_meta = tx.get('meta') if tx else meta
            all_done = True
            for op in tx_meta.get('ops', []):
                if op.get('status') != 'done':
                    all_done = False
                    break

            if all_done:
                wal.update_tx_status(tx_id, 'committed')
                processed.append(tx_id)
            elif any_changed:
                # persisted some progress; keep tx in progress
                processed.append(tx_id)

        except Exception:
            # leave it for later
            continue
    return processed


if __name__ == '__main__':
    print('This module is a library. Use recover_incomplete() from a supervisor script.')
