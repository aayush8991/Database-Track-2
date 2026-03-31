"""Transaction coordination helpers for multi-backend operations.

This module provides a non-invasive TransactionCoordinator implementing a
SAGA-style coordination with compensating actions as a fallback. It's safe to
use when MongoDB is not running as a replica set (i.e., multi-doc transactions
are unavailable). It also supports using real transactions when both backends
support them (SQLAlchemy transaction + PyMongo session).

Usage (SAGA-style):
    tc = TransactionCoordinator(sql_handler, mongo_handler)
    with tc.transaction() as tx:
        tx.add_sql(lambda conn: conn.execute(...), compensating=lambda conn: conn.execute(...))
        tx.add_mongo(lambda db, session: db.coll.insert_one(...), compensating=lambda db, session: db.coll.delete_one(...))

On commit the forward actions run; on error the registered compensating actions
are executed in reverse order.
"""
from contextlib import contextmanager
import threading
import traceback
from typing import Callable, Any, List, Tuple


class TransactionCoordinator:
    def __init__(self, sql_handler, mongo_handler):
        self.sql = sql_handler
        self.mongo = mongo_handler
        self.lock = threading.Lock()

    @contextmanager
    def transaction(self):
        """Context manager returning a Transaction object.

        The Transaction object exposes `add_sql` and `add_mongo` methods to
        register forward operations and compensating rollback actions.
        """
        tx = Transaction(self.sql, self.mongo)
        try:
            yield tx
            tx._commit()
        except Exception as e:
            try:
                tx._rollback()
            except Exception:
                traceback.print_exc()
            raise


class Transaction:
    def __init__(self, sql_handler, mongo_handler):
        self.sql = sql_handler
        self.mongo = mongo_handler
        # Each operation is (forward_callable, compensating_callable)
        self._ops_sql: List[Tuple[Callable[[Any], Any], Callable[[Any], Any]]] = []
        self._ops_mongo: List[Tuple[Callable[[Any, Any], Any], Callable[[Any, Any], Any]]] = []
        self._committed = False

    def add_sql(self, forward: Callable[[Any], Any], compensating: Callable[[Any], Any] = None):
        """Register a SQL forward op and optional compensating op.

        forward: callable receiving a DB connection (or engine) and performing the write.
        compensating: callable that reverses the forward action if needed.
        """
        self._ops_sql.append((forward, compensating))

    def add_mongo(self, forward: Callable[[Any, Any], Any], compensating: Callable[[Any, Any], Any] = None):
        """Register a Mongo forward op and optional compensating op.

        forward: callable receiving (db, session_or_none) and performing the write.
        compensating: callable that reverses the forward action if needed.
        """
        self._ops_mongo.append((forward, compensating))

    def _commit(self):
        """Execute all forward operations. If any fail, run compensating actions."""
        # Try to use true transactions if available: SQLAlchemy transaction + PyMongo session
        using_mongo_tx = False
        mongo_session = None

        try:
            # Start Mongo session if replica set / transactions are supported
            try:
                if self.mongo and getattr(self.mongo, 'client', None):
                    # Only attempt sessions/transactions if server is a replica set
                    try:
                        info = self.mongo.client.admin.command('ismaster')
                        if info.get('setName'):
                            mongo_session = self.mongo.client.start_session()
                            try:
                                mongo_session.start_transaction()
                                using_mongo_tx = True
                            except Exception:
                                using_mongo_tx = False
                        else:
                            # Not a replica set member; avoid using sessions
                            mongo_session = None
                            using_mongo_tx = False
                    except Exception:
                        mongo_session = None
                        using_mongo_tx = False
            except Exception:
                mongo_session = None
                using_mongo_tx = False

            # Execute SQL forwards inside a SQLAlchemy transaction if available
            if hasattr(self.sql, 'engine') and self.sql.engine:
                with self.sql.engine.begin() as conn:
                    # execute SQL forwards
                    for forward, _ in self._ops_sql:
                        forward(conn)

                    # execute Mongo forwards while SQL transaction open to reduce race window
                    if self._ops_mongo:
                        if using_mongo_tx and mongo_session:
                            for forward, _ in self._ops_mongo:
                                forward(self.mongo.db, mongo_session)
                        else:
                            # Run mongo ops without transaction; rely on compensating actions
                            for forward, _ in self._ops_mongo:
                                forward(self.mongo.db, None)
            else:
                # No SQL engine available: run SQL forwards directly (best-effort)
                for forward, _ in self._ops_sql:
                    forward(self.sql)

                if self._ops_mongo:
                    if using_mongo_tx and mongo_session:
                        for forward, _ in self._ops_mongo:
                            forward(self.mongo.db, mongo_session)
                    else:
                        for forward, _ in self._ops_mongo:
                            forward(self.mongo.db, None)

            # If we started a mongo transaction, commit it
            if using_mongo_tx and mongo_session:
                try:
                    mongo_session.commit_transaction()
                except Exception:
                    # If commit fails, raise to trigger compensating
                    raise

            self._committed = True

        except Exception as e:
            # On any error, perform compensating actions
            traceback.print_exc()
            try:
                self._run_compensating_actions()
            except Exception:
                traceback.print_exc()
            raise
        finally:
            if mongo_session:
                try:
                    mongo_session.end_session()
                except Exception:
                    pass

    def _rollback(self):
        """Explicit rollback: run compensating actions in reverse order."""
        if self._committed:
            # Nothing to rollback if already committed successfully
            return
        self._run_compensating_actions()

    def _run_compensating_actions(self):
        # Run registered compensating actions in reverse order: mongo then sql
        # 1. Mongo compensations (reverse order)
        for forward, compensating in reversed(self._ops_mongo):
            if compensating:
                try:
                    compensating(self.mongo.db, None)
                except Exception:
                    traceback.print_exc()

        # 2. SQL compensations (reverse order)
        for forward, compensating in reversed(self._ops_sql):
            if compensating:
                try:
                    # If we have an engine, obtain a connection for compensation
                    if hasattr(self.sql, 'engine') and self.sql.engine:
                        with self.sql.engine.begin() as conn:
                            compensating(conn)
                    else:
                        compensating(self.sql)
                except Exception:
                    traceback.print_exc()
