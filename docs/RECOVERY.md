## Recovery Service (WAL) — Deployment Notes

This project includes a small WAL manager and a recovery worker to process
in-doubt coordinated transactions created by the `TransactionCoordinator`.

Files added:

- `core/txn_wal.py` — simple SQL-backed WAL manager (table `transaction_wal`).
- `core/txn_recovery.py` — `recover_incomplete()` routine that performs idempotent compensations.
- `core/recovery_service.py` — CLI/daemon that runs recovery passes once or periodically.
- `deploy/recovery.service` — example systemd unit (edit placeholders before use).
- `deploy/supervisord_recovery.conf` — example supervisord program config (edit placeholders before use).

Quick start (development)

1. Activate your virtualenv in the repo root:

```bash
source .venv/bin/activate
```

2. Run a single recovery pass:

```bash
python core/recovery_service.py --once
```

3. Run as a simple loop (every 60s):

```bash
python core/recovery_service.py --interval 60
```

Systemd installation (example)

1. Copy `deploy/recovery.service` to `/etc/systemd/system/recovery.service` and edit `User`, `WorkingDirectory`, and `ExecStart` to point to your environment and project path.

```bash
sudo cp deploy/recovery.service /etc/systemd/system/recovery.service
sudo vim /etc/systemd/system/recovery.service  # update placeholders
sudo systemctl daemon-reload
sudo systemctl enable --now recovery.service
sudo journalctl -u recovery.service -f
```

Supervisord installation (example)

1. Edit `deploy/supervisord_recovery.conf` to point to your venv and project. Then include it in your supervisord config or load dynamically.

```ini
[program:recovery_service]
command=/path/to/venv/bin/python /path/to/Database-Track-2/core/recovery_service.py --interval 60
directory=/path/to/Database-Track-2
user=your_user
autostart=true
autorestart=true
```

macOS (launchd) installation
---------------------------------
Since macOS does not use systemd, you can run the recovery service using `launchd`.

1. Copy the provided plist to your user LaunchAgents folder:

```bash
mkdir -p ~/Library/LaunchAgents
cp deploy/com.dbtrack2.recovery.plist ~/Library/LaunchAgents/
```

2. Ensure the log directory exists and is writable:

```bash
sudo mkdir -p /var/log/db_track2
sudo chown destructor:staff /var/log/db_track2
```

3. Load the LaunchAgent (user-level):

```bash
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.dbtrack2.recovery.plist
# or for older macOS versions
launchctl load ~/Library/LaunchAgents/com.dbtrack2.recovery.plist
```

4. Check logs:

```bash
tail -f /var/log/db_track2/recovery_service.out
```

To unload:

```bash
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.dbtrack2.recovery.plist
```

Notes & recommendations

- Update the `User` and path placeholders in the provided service files before enabling in production.
- The WAL table created by `WALManager` is `transaction_wal` — back up or manage retention as needed.
- For production, consider running the recovery worker under a process supervisor and centralizing logs (e.g., syslog, ELK).
- To harden recovery, extend `txn_wal` to include per-operation records and add stronger verification before compensations.
