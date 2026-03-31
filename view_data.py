#!/usr/bin/env python3
import argparse
import json
from typing import Any

from dotenv import load_dotenv

load_dotenv()

from db.sql_handler import SQLHandler
from db.mongo_handler import MongoHandler


def _fmt(v: Any, max_len: int = 80) -> str:
    if isinstance(v, (dict, list, tuple)):
        s = json.dumps(v, default=str)
    else:
        s = str(v)
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


def print_table(headers: list[str], rows: list[list[Any]], title: str = "") -> None:
    if title:
        print(f"\n=== {title} ===")

    if not headers:
        print("(no columns)")
        return

    str_rows = [[_fmt(c) for c in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header_line = "| " + " | ".join(headers[i].ljust(widths[i]) for i in range(len(headers))) + " |"

    print(sep)
    print(header_line)
    print(sep)
    for row in str_rows:
        print("| " + " | ".join(row[i].ljust(widths[i]) for i in range(len(headers))) + " |")
    print(sep)
    print(f"rows shown: {len(rows)}")


def show_sql(limit: int):
    s = SQLHandler()
    c = s.cursor
    c.execute("SHOW TABLES")
    tables = [t[0] for t in c.fetchall()]

    if not tables:
        print("No SQL tables found.")
        return

    print("\nSQL tables:", ", ".join(tables))

    for t in tables:
        try:
            c.execute(f"SELECT * FROM `{t}` LIMIT %s", (limit,))
            rows = c.fetchall()
            headers = [d[0] for d in c.description] if c.description else []
            print_table(headers, rows, title=f"SQL table: {t}")
        except Exception as e:
            print(f"\n=== SQL table: {t} ===")
            print(f"error reading table: {e}")


def show_mongo(limit: int, only_collection: str | None = None):
    m = MongoHandler()
    db = m.db
    cols = db.list_collection_names()
    if only_collection:
        cols = [c for c in cols if c == only_collection]

    if not cols:
        print("No Mongo collections found.")
        return

    print("\nMongo collections:", ", ".join(cols))

    for col in cols:
        docs = list(db[col].find().limit(limit))
        for d in docs:
            if "_id" in d:
                d["_id"] = str(d["_id"])

        keys = []
        seen = set()
        for d in docs:
            for k in d.keys():
                if k not in seen:
                    seen.add(k)
                    keys.append(k)

        rows = []
        for d in docs:
            rows.append([d.get(k, "") for k in keys])

        print_table(keys, rows, title=f"Mongo collection: {col}")


def main():
    p = argparse.ArgumentParser(description="Human-readable tabular data viewer for SQL + Mongo")
    p.add_argument("--limit", type=int, default=10, help="rows/docs per table/collection")
    p.add_argument("--sql", action="store_true", help="show SQL tables only")
    p.add_argument("--mongo", action="store_true", help="show Mongo collections only")
    p.add_argument("--collection", type=str, default=None, help="show only one Mongo collection")
    args = p.parse_args()

    if not args.sql and not args.mongo:
        args.sql = True
        args.mongo = True

    if args.sql:
        show_sql(args.limit)
    if args.mongo:
        show_mongo(args.limit, args.collection)


if __name__ == "__main__":
    main()
