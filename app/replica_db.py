"""副本库（独立 SQLite）。

副本只由同步器写入，结构刻意与源库解耦：
  * specimens        — 标本数据副本；
  * sync_meta        — 单行表，保存阶段、已应用日志序号等持久进度；
  * _applied_changes — 已应用变更的序号流水（附带便于排查的审计信息）。

馆藏计数（按 locality 精确筛选、按 taxon 分组，持久化、查询不扫标本表）：
  * taxon_counts       — 已发布的计数（页面查询只走这张表）；
  * count_meta         — 单行表，published_build_id 指向当前生效的构建；
  * count_builds       — 每次首次构建/重建的状态、进度与错误（持久化）；
  * count_build_rows   — 某轮构建的全量影像（磁盘上的暂存，保证有界内存）；
  * count_build_counts — 某轮构建聚合计数的暂存，完成后整体替换 taxon_counts。

同步器保证：数据行的修改与 sync_meta 进度更新在 *同一个事务* 中提交，
因此“副本数据与进度共同提交”，崩溃后要么整批生效、要么整批回滚。
计数维护与数据变更、审计流水、进度也在同一个事务内完成。
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

# 同步阶段
PHASE_INIT = "INIT"          # 尚未开始 / 初次复制未完成
PHASE_BOOTSTRAP = "BOOTSTRAP"  # 正在做首次后台复制
PHASE_RUNNING = "RUNNING"    # 已就绪，持续增量同步
PHASE_STOPPED = "STOPPED"    # 被用户停止
PHASE_ERROR = "ERROR"        # 同步出错

# 计数构建阶段（持久化在 count_builds.phase）
BUILD_RUNNING = "RUNNING"    # 构建进行中
BUILD_PUBLISHED = "PUBLISHED"  # 完整新计数已发布
BUILD_FAILED = "FAILED"      # 构建失败（含进程重启导致的未完成构建）

# 计数分页扫描/聚合的每页行数（可由环境变量覆盖，便于测试调小）
COUNT_SCAN_PAGE_SIZE = int(os.environ.get("COUNT_SCAN_PAGE_SIZE", "500"))
COUNT_AGG_PAGE_SIZE = int(os.environ.get("COUNT_AGG_PAGE_SIZE", "500"))


class _ReplicaConnection(sqlite3.Connection):
    """带身份标记的连接，便于与源库连接区分（例如测试中注入故障时）。"""

    _is_replica = True


def connect(path: str, *, timeout: float = 30.0) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(
        path, timeout=timeout, isolation_level=None, factory=_ReplicaConnection
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={int(timeout * 1000)}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    # 外键关闭：副本是独立的派生库，不依赖源库对象
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


@contextmanager
def open_replica(path: str):
    """打开副本连接，退出时回收可能残留的未完成事务。

    正常路径的写操作都由调用方用显式 BEGIN IMMEDIATE/COMMIT 管理为短事务；
    这里处理异常路径：若某事务在 COMMIT 前抛错（如提交点故障注入），连接
    仍持有该事务（WAL 下是写锁），必须在关闭前 ROLLBACK 释放，否则后续写者
    会忙等。只读连接不做任何额外语句（其隐式读事务在关闭时自动结束，且 WAL
    读锁不阻塞写者），因此不会产生多余 COMMIT 干扰写事务的故障注入。
    """
    conn = connect(path)
    try:
        yield conn
    finally:
        try:
            # in_transaction 为真表示存在未完成事务（含崩溃后未回滚的写事务）
            if conn.in_transaction:
                conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        conn.close()


def init_replica(path: str) -> None:
    """创建副本结构（幂等）。"""
    with open_replica(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS specimens (
                id       INTEGER PRIMARY KEY,
                taxon    TEXT NOT NULL,
                locality TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_meta (
                id               INTEGER PRIMARY KEY CHECK (id = 1),
                phase            TEXT NOT NULL,
                last_applied_seq INTEGER NOT NULL DEFAULT 0,
                source_max_seq   INTEGER NOT NULL DEFAULT 0,
                bootstrap_copied INTEGER NOT NULL DEFAULT 0,
                last_error       TEXT,
                updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS _applied_changes (
                seq        INTEGER PRIMARY KEY,
                op         TEXT NOT NULL,
                specimen_id INTEGER NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        # ------------------------------------------------------------------ #
        # 馆藏计数（持久化）：页面只查 taxon_counts，绝不临时扫描 specimens
        # ------------------------------------------------------------------ #
        # 已发布计数：(locality, taxon) 两字段按原文本精确分组，无零计数组
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS taxon_counts (
                locality TEXT NOT NULL,
                taxon    TEXT NOT NULL,
                cnt      INTEGER NOT NULL CHECK (cnt > 0),
                PRIMARY KEY (locality, taxon)
            ) WITHOUT ROWID
            """
        )
        # 按查询模式建索引：locality 精确筛选 + taxon 分页（覆盖索引，不回表）
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_taxon_counts_loc_taxon
                ON taxon_counts(locality, taxon)
            """
        )
        # 单行表：当前已发布构建的 id 与水位；build_id 为 NULL 表示首次构建
        # 尚未完成（不返回统计）。published_seq 是计数已覆盖到的 CDC 序号，
        # 同步器据此跳过已包含在已发布计数中的变更，避免发布窗口重复计数。
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS count_meta (
                id                INTEGER PRIMARY KEY CHECK (id = 1),
                published_build_id INTEGER,
                published_seq     INTEGER NOT NULL DEFAULT 0,
                updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        # 轻量迁移：旧库没有 published_seq 列时补上（默认 0）
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(count_meta)")}
        if "published_seq" not in cols:
            conn.execute(
                "ALTER TABLE count_meta ADD COLUMN published_seq "
                "INTEGER NOT NULL DEFAULT 0"
            )
        # 构建登记表：每轮首次构建/重建一行，进度与错误均持久化
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS count_builds (
                build_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                phase         TEXT NOT NULL CHECK (
                                  phase IN ('RUNNING', 'PUBLISHED', 'FAILED')),
                stage         TEXT NOT NULL DEFAULT '',
                total_rows    INTEGER NOT NULL DEFAULT 0,
                scanned_rows  INTEGER NOT NULL DEFAULT 0,
                applied_seq   INTEGER NOT NULL DEFAULT 0,
                target_seq    INTEGER NOT NULL DEFAULT 0,
                last_error    TEXT,
                created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        # 构建暂存影像：全量行落在磁盘表中（而非内存），保证有界内存
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS count_build_rows (
                build_id INTEGER NOT NULL,
                id       INTEGER NOT NULL,
                taxon    TEXT NOT NULL,
                locality TEXT NOT NULL,
                PRIMARY KEY (build_id, id)
            ) WITHOUT ROWID
            """
        )
        # 构建暂存聚合计数：发布时整体替换 taxon_counts（普通 rowid 表，
        # 便于按 rowid 分批清理；(build_id, locality, taxon) 唯一约束保证 upsert）
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS count_build_counts (
                build_id INTEGER NOT NULL,
                locality TEXT NOT NULL,
                taxon    TEXT NOT NULL,
                cnt      INTEGER NOT NULL CHECK (cnt > 0),
                PRIMARY KEY (build_id, locality, taxon)
            )
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO sync_meta(id, phase) VALUES (1, ?)",
            (PHASE_INIT,),
        )
        conn.execute(
            "INSERT OR IGNORE INTO count_meta(id, published_build_id) VALUES (1, NULL)"
        )


def get_meta(path: str) -> dict:
    fallback = {
        "phase": PHASE_INIT,
        "last_applied_seq": 0,
        "source_max_seq": 0,
        "bootstrap_copied": 0,
        "last_error": None,
    }
    if not os.path.exists(path):
        return fallback
    with open_replica(path) as conn:
        try:
            row = conn.execute("SELECT * FROM sync_meta WHERE id = 1").fetchone()
        except sqlite3.OperationalError:
            # 副本文件存在但表结构尚未初始化
            return fallback
        if row is None:
            return fallback
        return dict(row)


def is_ready(path: str) -> bool:
    """就绪前不供查询：只有 RUNNING 阶段才接受标本查询。"""
    return os.path.exists(path) and get_meta(path)["phase"] == PHASE_RUNNING


def get_specimen(path: str, specimen_id: int):
    with open_replica(path) as conn:
        row = conn.execute(
            "SELECT id, taxon, locality FROM specimens WHERE id = ?",
            (specimen_id,),
        ).fetchone()
        return dict(row) if row is not None else None


def reset_for_bootstrap(path: str) -> None:
    """初次复制（重新）开始：清空一切派生数据，回到 INIT 进度。

    初次复制中断后重做时调用，保证半成品快照不会残留。计数是 specimens 的
    派生物，随快照一并清空，待副本就绪后重新首次构建。
    """
    with open_replica(path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM specimens")
        conn.execute("DELETE FROM _applied_changes")
        conn.execute("DELETE FROM taxon_counts")
        conn.execute("DELETE FROM count_build_rows")
        conn.execute("DELETE FROM count_build_counts")
        conn.execute("DELETE FROM count_builds")
        conn.execute(
            "UPDATE count_meta SET published_build_id = NULL, "
            "published_seq = 0, updated_at = datetime('now') WHERE id = 1"
        )
        conn.execute(
            """
            UPDATE sync_meta
               SET phase = ?, last_applied_seq = 0, source_max_seq = 0,
                   bootstrap_copied = 0, last_error = NULL,
                   updated_at = datetime('now')
             WHERE id = 1
            """,
            (PHASE_BOOTSTRAP,),
        )
        conn.execute("COMMIT")


# --------------------------------------------------------------------------- #
# 馆藏计数：查询（只走持久化的 taxon_counts，不扫描 specimens）
# --------------------------------------------------------------------------- #
def get_published_build_id(path: str):
    """当前生效计数所属构建；首次构建完成前为 None（此时不返回统计）。"""
    with open_replica(path) as conn:
        row = conn.execute(
            "SELECT published_build_id FROM count_meta WHERE id = 1"
        ).fetchone()
        return None if row is None else row["published_build_id"]


def count_is_available(path: str) -> bool:
    return get_published_build_id(path) is not None


def count_localities(path: str) -> list[str]:
    """已发布计数中出现过的全部采集地（DISTINCT，供页面下拉，走索引）。"""
    with open_replica(path) as conn:
        rows = conn.execute(
            "SELECT DISTINCT locality FROM taxon_counts ORDER BY locality"
        ).fetchall()
        return [r["locality"] for r in rows]


def count_taxa_for_locality(
    path: str, locality: str, page: int, page_size: int
) -> dict:
    """按 locality 原文精确筛选，分页返回 taxon 及标本数。

    仅查询 taxon_counts（覆盖索引 idx_taxon_counts_loc_taxon），
    不接触 specimens 表；零计数组本就不存在（CHECK cnt > 0），天然不显示。
    """
    page = max(1, int(page))
    page_size = max(1, int(page_size))
    offset = (page - 1) * page_size
    with open_replica(path) as conn:
        total = int(
            conn.execute(
                "SELECT COUNT(*) AS c FROM taxon_counts WHERE locality = ?",
                (locality,),
            ).fetchone()["c"]
        )
        rows = conn.execute(
            """
            SELECT taxon, cnt
              FROM taxon_counts
             WHERE locality = ?
             ORDER BY taxon ASC
             LIMIT ? OFFSET ?
            """,
            (locality, page_size, offset),
        ).fetchall()
        total_specimens = int(
            conn.execute(
                "SELECT COALESCE(SUM(cnt), 0) AS s FROM taxon_counts "
                "WHERE locality = ?",
                (locality,),
            ).fetchone()["s"]
        )
    return {
        "locality": locality,
        "page": page,
        "page_size": page_size,
        "total_groups": total,
        "total_pages": (total + page_size - 1) // page_size,
        "total_specimens": total_specimens,
        "items": [{"taxon": r["taxon"], "count": r["cnt"]} for r in rows],
    }


def get_build(path: str, build_id: int):
    with open_replica(path) as conn:
        row = conn.execute(
            "SELECT * FROM count_builds WHERE build_id = ?", (build_id,)
        ).fetchone()
        return dict(row) if row is not None else None


def get_latest_build(path: str):
    with open_replica(path) as conn:
        row = conn.execute(
            "SELECT * FROM count_builds ORDER BY build_id DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row is not None else None


def list_localities_with_counts(path: str) -> list[dict]:
    """各采集地的分组数与标本总数（页面概览，走 taxon_counts 索引）。"""
    with open_replica(path) as conn:
        rows = conn.execute(
            """
            SELECT locality,
                   COUNT(*)        AS taxon_groups,
                   SUM(cnt)        AS specimen_total
              FROM taxon_counts
             GROUP BY locality
             ORDER BY locality
            """
        ).fetchall()
        return [
            {
                "locality": r["locality"],
                "taxon_groups": r["taxon_groups"],
                "specimen_total": r["specimen_total"],
            }
            for r in rows
        ]
