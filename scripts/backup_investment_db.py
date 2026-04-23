from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "investment.db"
BACKUP_DIR = PROJECT_ROOT / "backups"
LOG_PATH = BACKUP_DIR / "backup.log"
BACKUP_PATTERN = re.compile(r"^investment_(\d{8}_\d{6})\.db$")


@dataclass
class BackupFile:
    path: Path
    timestamp: datetime


def _month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _add_months(dt: datetime, months: int) -> datetime:
    year = dt.year + (dt.month - 1 + months) // 12
    month = (dt.month - 1 + months) % 12 + 1
    return dt.replace(year=year, month=month)


def _log(message: str) -> None:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"[{stamp}] {message}\n")


def _parse_backup_files() -> list[BackupFile]:
    files: list[BackupFile] = []
    for path in BACKUP_DIR.glob("investment_*.db"):
        m = BACKUP_PATTERN.match(path.name)
        if not m:
            continue
        try:
            ts = datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        files.append(BackupFile(path=path, timestamp=ts))
    files.sort(key=lambda x: x.timestamp)
    return files


def _should_keep_monthly(ts: datetime, monthly_start: datetime) -> bool:
    return ts.day == 1 and _month_start(ts) >= monthly_start


def _prune_backups(now: datetime) -> tuple[int, list[str]]:
    deleted = 0
    deleted_files: list[str] = []

    daily_threshold = now - timedelta(days=7)
    monthly_start = _add_months(_month_start(now), -11)

    for backup in _parse_backup_files():
        keep_monthly = _should_keep_monthly(backup.timestamp, monthly_start)
        keep_daily = backup.timestamp >= daily_threshold
        if keep_monthly or keep_daily:
            continue
        backup.path.unlink(missing_ok=True)
        deleted += 1
        deleted_files.append(backup.path.name)
    return deleted, deleted_files


def _sqlite_backup(source_path: Path, backup_path: Path) -> None:
    with sqlite3.connect(source_path) as src_conn, sqlite3.connect(backup_path) as dst_conn:
        src_conn.backup(dst_conn)
        dst_conn.commit()


def _integrity_check(db_path: Path) -> bool:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("PRAGMA integrity_check;").fetchone()
    result = str(row[0]) if row else ""
    return result.lower() == "ok"


def main() -> int:
    now = datetime.now()
    if not DB_PATH.exists():
        _log(f"ERROR DB file not found: {DB_PATH}")
        print(f"ERROR: DB file not found: {DB_PATH}")
        return 1

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backup_name = f"investment_{now.strftime('%Y%m%d_%H%M%S')}.db"
    backup_path = BACKUP_DIR / backup_name

    try:
        _sqlite_backup(DB_PATH, backup_path)
        integrity_ok = _integrity_check(backup_path)
        deleted_count, deleted_files = _prune_backups(now)

        if integrity_ok:
            _log(
                f"SUCCESS backup={backup_name} integrity=ok "
                f"deleted={deleted_count} deleted_files={','.join(deleted_files) if deleted_files else '-'}"
            )
            print(f"Backup created: {backup_path}")
            print("Integrity check: OK")
            print(f"Deleted old backups: {deleted_count}")
            return 0

        _log(
            f"ERROR backup={backup_name} integrity=failed "
            f"deleted={deleted_count} deleted_files={','.join(deleted_files) if deleted_files else '-'}"
        )
        print(f"Backup created: {backup_path}")
        print("Integrity check: FAILED")
        return 2

    except Exception as exc:
        _log(f"ERROR backup failed: {exc}")
        print(f"ERROR: backup failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
