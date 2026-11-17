"""SQLite 持久化：基准备份 / 核对点 / 恢复任务的状态全部落盘。

API 重启后据此续跑或按原点重试，不会丢状态、不会把半成品当成功。
"""
import sqlite3
import threading
from datetime import datetime, timezone

from . import config

_LOCK = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS backups (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT NOT NULL,
    status      TEXT NOT NULL,          -- running | done | failed
    start_lsn   TEXT,                   -- 备份起始 WAL 位点（恢复可用性的下界）
    timeline    INTEGER,
    size_bytes  INTEGER,
    error       TEXT,
    created_at  TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS checkpoints (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    restore_lsn TEXT NOT NULL,          -- pg_create_restore_point 返回的 LSN
    wal_file    TEXT NOT NULL,          -- 该 LSN 所在的 WAL 段文件名
    status      TEXT NOT NULL,          -- waiting_wal | ready | expired
    created_at  TEXT NOT NULL,
    ready_at    TEXT
);
CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_name  TEXT NOT NULL,
    status           TEXT NOT NULL,     -- queued | waiting_wal | preparing | recovering
                                        -- | verifying | ready | failed
                                        -- | superseded | stopped | cancelled
    detail           TEXT,
    error            TEXT,
    port             INTEGER,
    data_dir         TEXT,
    log_tail         TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    ready_at         TEXT
);
"""

# DB 级兜底：同一时刻至多一条进行中任务。
# 接单本身由 try_admit_job 在单个事务里原子完成；该唯一部分索引用于
# 防住任何绕过接单路径的写入（如旧版本遗留代码），并在重启去重后重建。
GUARD_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_single_active
ON jobs ((1))
WHERE status IN ('queued','waiting_wal','preparing','recovering','verifying')
"""

# 进行中（未终结）的任务状态：一次只允许一个
ACTIVE_JOB_STATES = ("queued", "waiting_wal", "preparing", "recovering", "verifying")
# 允许发起重试的终结状态
RETRYABLE_JOB_STATES = ("failed", "stopped", "cancelled", "superseded")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(config.STATE_DB, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def init_db() -> None:
    with _LOCK:
        con = _connect()
        try:
            con.executescript(SCHEMA)
            _enforce_single_active(con)  # 重启去重，须在唯一索引之前
            con.execute(GUARD_INDEX)
            con.commit()
        finally:
            con.close()


def _enforce_single_active(con: sqlite3.Connection) -> None:
    """重启去重：历史遗留多条进行中任务时，仅继续最早接单的一条。

    其余标记 cancelled 并写明未执行原因（属于可重试状态，可按原核对点重试）。
    被保留任务的核对点不变，续跑就绪后查询照常对应。
    """
    active = ",".join(f"'{s}'" for s in ACTIVE_JOB_STATES)
    rows = con.execute(
        f"SELECT id FROM jobs WHERE status IN ({active}) ORDER BY id"
    ).fetchall()
    if len(rows) <= 1:
        return
    keep = rows[0]["id"]
    losers = [r["id"] for r in rows[1:]]
    marks = ",".join("?" for _ in losers)
    con.execute(
        f"UPDATE jobs SET status='cancelled', cancel_requested=0, updated_at=?,"
        f" detail=? WHERE id IN ({marks})",
        (now(),
         f"未执行：重启时发现 {len(rows)} 条进行中任务，同一时刻只允许一条，"
         f"仅保留最早接单的 #{keep}；可按原核对点重试",
         *losers),
    )


def try_admit_job(checkpoint_name: str) -> tuple[int | None, dict | None]:
    """原子接单：同一时刻只确认一条进行中任务。

    "查进行中 + 插入 queued" 放在同一个 BEGIN IMMEDIATE 事务里，
    并发新建 / 按原点重试只会放行一条；其余返回冲突（调用方给 409），
    不会留下稍后抢占恢复实例的待执行队列。
    返回 (新任务 id, None)；已有进行中任务时返回 (None, 该任务)。
    """
    active = ",".join(f"'{s}'" for s in ACTIVE_JOB_STATES)
    with _LOCK:
        con = sqlite3.connect(config.STATE_DB, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA busy_timeout=30000")
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                f"SELECT id, checkpoint_name, status FROM jobs"
                f" WHERE status IN ({active}) ORDER BY id LIMIT 1"
            ).fetchone()
            if row:
                con.execute("ROLLBACK")
                return None, dict(row)
            cur = con.execute(
                "INSERT INTO jobs (checkpoint_name, status, created_at, updated_at)"
                " VALUES (?,?,?,?)",
                (checkpoint_name, "queued", now(), now()),
            )
            con.execute("COMMIT")
            return cur.lastrowid, None
        finally:
            con.close()


def execute(sql: str, params: tuple = ()) -> int:
    with _LOCK:
        con = _connect()
        try:
            cur = con.execute(sql, params)
            con.commit()
            return cur.lastrowid
        finally:
            con.close()


def all_rows(sql: str, params: tuple = ()) -> list[dict]:
    with _LOCK:
        con = _connect()
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()


def one_row(sql: str, params: tuple = ()) -> dict | None:
    rows = all_rows(sql, params)
    return rows[0] if rows else None


def update_job(job_id: int, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = now()
    cols = ", ".join(f"{k}=?" for k in fields)
    execute(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), job_id))
