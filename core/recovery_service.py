#!/usr/bin/env python3
"""Recovery service CLI

Run periodic recovery passes to process incomplete WAL transactions and
perform idempotent compensating cleanup.

Examples:
  # single run
  python core/recovery_service.py --once

  # run as a simple daemon every 60s
  python core/recovery_service.py --interval 60
"""
from __future__ import annotations
import argparse
import time
import logging
from dotenv import load_dotenv
load_dotenv()

import os
import sys
# Ensure project root is on path so local packages can be imported when running as a script
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler
from core.txn_wal import WALManager
from core.txn_recovery import recover_incomplete


def setup_logging(logfile: str | None = None):
    fmt = "%(asctime)s %(levelname)s %(message)s"
    if logfile:
        logging.basicConfig(filename=logfile, level=logging.INFO, format=fmt)
    else:
        logging.basicConfig(level=logging.INFO, format=fmt)


def run_once(limit: int = 100):
    logging.info("Recovery pass starting (limit=%s)", limit)
    sql = SQLHandler()
    mongo = MongoHandler()
    wal = WALManager(sql)
    processed = recover_incomplete(wal, sql, mongo, limit=limit)
    logging.info("Recovery pass completed, processed %d txns", len(processed))
    if processed:
        logging.info("Processed tx_ids: %s", processed)
    return processed


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--once", action="store_true", help="Run a single recovery pass and exit")
    p.add_argument("--interval", type=int, default=60, help="Seconds between recovery passes (daemon mode)")
    p.add_argument("--limit", type=int, default=100, help="Max transactions to process per pass")
    p.add_argument("--log", type=str, default=None, help="Optional log file path")
    args = p.parse_args()

    setup_logging(args.log)
    logging.info("Starting recovery service (once=%s, interval=%s)", args.once, args.interval)

    if args.once:
        run_once(limit=args.limit)
        return

    try:
        while True:
            run_once(limit=args.limit)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logging.info("Recovery service stopped by user")


if __name__ == '__main__':
    main()
