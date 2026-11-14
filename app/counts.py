"""馆藏计数：事务内增量维护 + 后台首次构建/重建。

设计要点（与需求逐条对应）
==========================

1. 持久化、查询不扫标本表
   计数落在副本的 ``taxon_counts(locality, taxon, cnt)`` 中，(locality, taxon)
   按原文本精确分组、``CHECK (cnt > 0)`` 使零计数组天然不存在。页面查询只走
   覆盖索引 ``idx_taxon_counts_loc_taxon``，绝不临时扫描 specimens。

2. 随副本记录、审计、进度同事务更新
   :func:`apply_count_delta` 由同步器在 *其已有的* 应用事务内调用：插入加一、
   删除减一（降为 0 即删组）、换组更正在同一事务内扣旧组加新组。与标本行、
   ``_applied_changes`` 审计流水、``sync_meta`` 进度一起 COMMIT / 一起 ROLLBACK。

3. 单一写者，避免构建与同步争抢副本写锁
   SQLite 同一时刻只允许一个写事务。同步器是副本的唯一写者；构建器的扫描以
   只读短事务分页读取 specimens，把全量影像写入 *独立的* 暂存表。构建期间的
   增量不在构建器里重放，而是由同步器在它自己的应用事务内通过
   :func:`apply_staging_delta` 顺带更新活动构建的影像——没有第二个写线程，
   因而不会锁饥饿，同步、标本查询、源更正在构建全程都继续可用。

4. 后台构建/重建，不持副本写事务等待全量扫描，全程有界内存
   扫描按固定页大小分页（每页独立短事务，扫描页只读），影像落在磁盘暂存表
   ``count_build_rows``；聚合按页把影像并入 ``count_build_counts``。
   任何事务都不跨全量扫描，内存占用以页大小为界。

5. 发布时纳入构建期间的全部增删改（权威收敛，不依赖内存时序）
   构建登记同步水位 ``start_seq``。收尾时从源端变更日志把区间
   ``(start_seq, w]`` 幂等重放进影像（I/U upsert、D 删除），该区间确定地
   覆盖扫描游标竞争、连续构建注册窗口等一切时序缺口；再等同步器
   ``last_applied_seq`` 追平采样水位 w。发布在持写锁短事务内进行，若同步器
   已越过 w 则锁内把 CDC 补到 sync_seq 并重算聚合，随后整体替换 taxon_counts、
   把 published_build_id 与 published_seq 一起翻转。同步器对序号
   <= published_seq 的批次跳过计数维护（已含于全量影像），杜绝发布窗口重复。

6. 重建期间旧计数继续更新并供查询；完整新计数才可替换
   重建只写 build_id 隔离的暂存表，taxon_counts 始终是上一版完整计数并照常
   增量维护。失败/重启：旧计数原样保留，未完成构建标记 FAILED；首次完成前
   published_build_id 为 NULL，接口不返回统计。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from typing import Optional

from . import replica_db
from .source_db import CHANGE_BATCH_SIZE, connect as source_connect

# 构建子阶段（页面进度展示用）
STAGE_SCAN = "SCAN"
STAGE_CATCH_UP = "CATCH_UP"
STAGE_AGGREGATE = "AGGREGATE"
STAGE_PUBLISH = "PUBLISH"

# 发布前等待同步器追平水位的轮询间隔
BUILD_WAIT_INTERVAL = float(os.environ.get("BUILD_WAIT_INTERVAL", "0.2"))
# 监督线程的检查间隔（自动首次构建）
SUPERVISOR_INTERVAL = float(os.environ.get("SUPERVISOR_INTERVAL", "0.5"))
# 暂存表清理每批删除行数
CLEANUP_BATCH = 500


# --------------------------------------------------------------------------- #
# 同步事务内的增量维护（由 sync._apply_batch 在同一事务内调用）
# --------------------------------------------------------------------------- #
def apply_count_delta(
    conn: sqlite3.Connection,
    op: str,
    specimen_id: int,
    new_taxon: Optional[str],
    new_locality: Optional[str],
    seq: int,
) -> bool:
    """随一条标本变更，在调用方事务内维护已发布计数。

    返回 True 表示执行了计数维护；False 表示该序号已包含在已发布计数中而跳过。

    调用顺序约定（见 sync.SyncEngine._apply_batch）：在标本行被
    INSERT OR REPLACE / DELETE 之前调用本函数，因此 SELECT 读到的是旧行。
    注意不能仅凭日志 op 判断：INSERT OR REPLACE 对已存在的行会先删后插，
    这里以“副本中当前是否存在该 id”为准区分真实插入与更新。

    发布窗口去重：计数发布是把“截至 published_seq 的全量影像”整体换入。
    一个同步批次若其序号 <= published_seq，说明它已被该全量影像包含，
    再对新表做增量就会重复，因此直接跳过（标本行/审计仍由同步器正常写入）。
    首次计数尚未发布（published_build_id IS NULL）时同样不维护，
    发布时以完整构建影像为准。
    """
    meta = conn.execute(
        "SELECT published_build_id, published_seq FROM count_meta WHERE id = 1"
    ).fetchone()
    if meta is None or meta["published_build_id"] is None:
        return False
    if seq is not None and int(seq) <= int(meta["published_seq"]):
        # 已包含在已发布的全量计数中，跳过增量避免重复
        return False

    row = conn.execute(
        "SELECT taxon, locality FROM specimens WHERE id = ?", (specimen_id,)
    ).fetchone()

    if op == "D":
        if row is not None:
            _bump(conn, row["locality"], row["taxon"], -1)
        return True

    # I/U 统一处理：无旧行 -> 新增组；有旧行且分组变化 -> 同一事务扣旧加新
    if row is None:
        _bump(conn, new_locality, new_taxon, +1)
    elif (row["taxon"], row["locality"]) != (new_taxon, new_locality):
        _bump(conn, row["locality"], row["taxon"], -1)
        _bump(conn, new_locality, new_taxon, +1)
    return True


def apply_staging_delta(
    conn: sqlite3.Connection,
    build_id: int,
    op: str,
    specimen_id: int,
    new_taxon: Optional[str],
    new_locality: Optional[str],
) -> None:
    """在同步事务内顺带维护活动构建的暂存影像（单一写者，无额外锁竞争）。

    与标本行的写入在同一个事务：I/U 幂等 upsert 影像行，D 删除影像行。
    这是扫描期的增量快路径；构建收尾时还会从源端 CDC 对 (start_seq, w]
    做一次权威重放（幂等），因此这里即使与扫描顺序交错也不影响最终正确性。
    """
    if op == "D":
        conn.execute(
            "DELETE FROM count_build_rows WHERE build_id = ? AND id = ?",
            (build_id, specimen_id),
        )
    else:
        conn.execute(
            """
            INSERT OR REPLACE INTO count_build_rows
                (build_id, id, taxon, locality)
            VALUES (?, ?, ?, ?)
            """,
            (build_id, specimen_id, new_taxon, new_locality),
        )


def _bump(conn: sqlite3.Connection, locality: str, taxon: str, delta: int) -> None:
    if delta > 0:
        conn.execute(
            """
            INSERT INTO taxon_counts(locality, taxon, cnt)
            VALUES (?, ?, 1)
            ON CONFLICT(locality, taxon)
            DO UPDATE SET cnt = cnt + 1
            """,
            (locality, taxon),
        )
        return
    # 递减：cnt=1 直接删组（零计数组不显示），cnt>1 才减一。
    # 不能先 UPDATE 成 0 再 DELETE——会违反 CHECK(cnt > 0)。
    row = conn.execute(
        "SELECT cnt FROM taxon_counts WHERE locality = ? AND taxon = ?",
        (locality, taxon),
    ).fetchone()
    if row is None:
        return
    if int(row["cnt"]) <= 1:
        conn.execute(
            "DELETE FROM taxon_counts WHERE locality = ? AND taxon = ?",
            (locality, taxon),
        )
    else:
        conn.execute(
            "UPDATE taxon_counts SET cnt = cnt - 1 "
            "WHERE locality = ? AND taxon = ?",
            (locality, taxon),
        )


# --------------------------------------------------------------------------- #
# 后台构建/重建管理器
# --------------------------------------------------------------------------- #
class CountBuildManager:
    """计数构建线程的生命周期、持久进度与自动首次构建。

    活动构建 id（``active_build_id``）只在内存中登记，同步器每批据此把变更
    顺带写入对应暂存影像；进程重启后活动构建被标记 FAILED，不再有增量汇入。
    """

    def __init__(self, source_path: str, replica_path: str):
        self.source_path = source_path
        self.replica_path = replica_path
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._supervisor: Optional[threading.Thread] = None
        self._stop = threading.Event()
        # 当前活动构建 id；同步器通过它把增量并入暂存影像
        self.active_build_id: Optional[int] = None
        # 每轮构建完成（成功或失败）时置位，便于测试确定性等待
        self.build_finished = threading.Event()
        # 仅供 UI/测试观察的瞬时状态（权威进度在 count_builds 表中）
        self._last_error: Optional[str] = None

    # ----------------------------- 生命周期 ----------------------------- #
    def start_supervisor(self) -> None:
        """启动守护监督线程：重启善后 + 副本就绪后自动首次构建。"""
        with self._lock:
            if self._supervisor is not None and self._supervisor.is_alive():
                return
            self._stop.clear()
            t = threading.Thread(
                target=self._supervise, name="count-supervisor", daemon=True
            )
            self._supervisor = t
            t.start()

    def shutdown(self) -> None:
        self._stop.set()

    def is_build_running(self) -> bool:
        with self._lock:
            return self._thread is not None and self._thread.is_alive()

    def start_build(self) -> Optional[int]:
        """开始一轮首次构建/重建。已有构建在跑时返回 None。"""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return None
            build_id = self._insert_build_row()
            self.active_build_id = build_id
            self.build_finished.clear()
            t = threading.Thread(
                target=self._run_safely,
                args=(build_id,),
                name=f"count-build-{build_id}",
                daemon=True,
            )
            self._thread = t
            t.start()
            return build_id

    def wait_until_idle(self, timeout: Optional[float] = None) -> None:
        t = self._thread
        if t is not None:
            t.join(timeout)

    def wait_build_finished(self, timeout: Optional[float] = None) -> bool:
        """等待当前构建结束（成功/失败均可），返回是否已结束。"""
        return self.build_finished.wait(timeout)

    def get_active_build_id(self) -> Optional[int]:
        return self.active_build_id if self.is_build_running() else None

    def status(self) -> dict:
        latest = replica_db.get_latest_build(self.replica_path)
        return {
            "available": replica_db.count_is_available(self.replica_path),
            "published_build_id": replica_db.get_published_build_id(
                self.replica_path
            ),
            "build_running": self.is_build_running(),
            "active_build_id": self.get_active_build_id(),
            "latest_build": latest,
            "last_error": self._last_error,
        }

    # ----------------------------- 监督线程 ----------------------------- #
    def _supervise(self) -> None:
        # 进程重启善后：上次未完成的构建一律标失败，旧发布计数保持不变
        self._mark_stale_builds_failed()
        while not self._stop.is_set():
            try:
                if (
                    not self.is_build_running()
                    and replica_db.is_ready(self.replica_path)
                    and replica_db.get_published_build_id(self.replica_path) is None
                    and self._no_running_build_row()
                ):
                    self.start_build()
            except Exception:
                # 监督逻辑自身不应退出；下轮重试
                pass
            self._stop.wait(SUPERVISOR_INTERVAL)

    def _no_running_build_row(self) -> bool:
        with replica_db.open_replica(self.replica_path) as conn:
            row = conn.execute(
                "SELECT 1 FROM count_builds WHERE phase = ? LIMIT 1",
                (replica_db.BUILD_RUNNING,),
            ).fetchone()
            return row is None

    def _mark_stale_builds_failed(self) -> None:
        try:
            with replica_db.open_replica(self.replica_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    UPDATE count_builds
                       SET phase = ?,
                           stage = '',
                           last_error = COALESCE(
                               last_error, '进程重启：未完成构建标记失败'),
                           updated_at = datetime('now')
                     WHERE phase = ?
                    """,
                    (replica_db.BUILD_FAILED, replica_db.BUILD_RUNNING),
                )
                conn.execute("COMMIT")
        except sqlite3.OperationalError:
            # 副本尚不可写时忽略：下一轮监督再次尝试
            pass

    def _insert_build_row(self) -> int:
        with replica_db.open_replica(self.replica_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            # 构建水位：同步器从此序号之后的变更都会汇入本构建影像
            meta = conn.execute(
                "SELECT last_applied_seq FROM sync_meta WHERE id = 1"
            ).fetchone()
            start_seq = int(meta["last_applied_seq"]) if meta else 0
            total = int(
                conn.execute("SELECT COUNT(*) AS c FROM specimens").fetchone()["c"]
            )
            cur = conn.execute(
                """
                INSERT INTO count_builds(phase, stage, total_rows, scanned_rows,
                                         applied_seq, target_seq)
                VALUES (?, ?, ?, 0, ?, 0)
                """,
                (replica_db.BUILD_RUNNING, STAGE_SCAN, total, start_seq),
            )
            build_id = int(cur.lastrowid)
            conn.execute("COMMIT")
        return build_id

    # ----------------------------- 构建主体 ----------------------------- #
    def _run_safely(self, build_id: int) -> None:
        try:
            self._build(build_id)
        except Exception as exc:  # 构建失败：记录持久错误，旧计数保留
            self._fail_build(build_id, exc)
        finally:
            with self._lock:
                if self.active_build_id == build_id:
                    self.active_build_id = None
            self.build_finished.set()

    def _fail_build(self, build_id: int, exc: Exception) -> None:
        msg = f"{type(exc).__name__}: {exc}"
        self._last_error = msg
        try:
            with replica_db.open_replica(self.replica_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    """
                    UPDATE count_builds
                       SET phase = ?, last_error = ?,
                           updated_at = datetime('now')
                     WHERE build_id = ? AND phase = ?
                    """,
                    (replica_db.BUILD_FAILED, msg, build_id,
                     replica_db.BUILD_RUNNING),
                )
                conn.execute("COMMIT")
        except sqlite3.OperationalError:
            pass
        self._cleanup_staging(build_id)

    def _build(self, build_id: int) -> None:
        build = replica_db.get_build(self.replica_path, build_id)
        start_seq = int(build["applied_seq"])

        # 1) 分页只读扫描副本 specimens -> 磁盘暂存影像（短事务、有界内存）。
        #    扫描期间的增量由同步器经 apply_staging_delta 汇入同一影像。
        self._scan_into_staging(build_id)

        # 2) 等同步器把副本追平到某个采样水位 w
        w = self._wait_sync_caught_up(build_id, start_seq)

        # 3) 权威校正：从源端 CDC 重放 (start_seq, w] 到影像。
        #    日志区间是确定的，覆盖“扫描游标竞争”与“连续重建注册窗口”等
        #    一切时序缺口，使影像严格等于水位 w 的标本全集。
        self._reconcile_from_cdc(build_id, start_seq, w)

        # 4) 分页聚合暂存影像 -> 暂存计数（短事务、有界内存）
        self._aggregate(build_id)

        # 5) 锁内复核水位后单事务整体交换发布
        self._publish(build_id, w)

        # 6) 分批清理暂存表（短事务，不拉长发布事务）
        self._cleanup_staging(build_id)

    def _reconcile_from_cdc(
        self, build_id: int, start_seq: int, w: int
    ) -> None:
        """从源端变更日志重放 (start_seq, w]，把暂存影像校正到水位 w。

        与同步器是否已通过 apply_staging_delta 汇入无关：I/U 幂等 upsert、
        D 删除，重复应用结果相同（幂等）。按批读取、每批一个副本短事务，
        内存有界；只读源库，不与同步器争副本写锁（各自短事务轮转）。
        """
        applied = start_seq
        while applied < w:
            src = source_connect(self.source_path, readonly=True)
            try:
                changes = src.execute(
                    """
                    SELECT seq, op, id, taxon, locality
                      FROM _change_log
                     WHERE seq > ? AND seq <= ?
                     ORDER BY seq ASC
                     LIMIT ?
                    """,
                    (applied, w, CHANGE_BATCH_SIZE),
                ).fetchall()
            finally:
                src.close()
            if not changes:
                break
            with replica_db.open_replica(self.replica_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                for ch in changes:
                    if ch["op"] == "D":
                        conn.execute(
                            "DELETE FROM count_build_rows "
                            "WHERE build_id = ? AND id = ?",
                            (build_id, ch["id"]),
                        )
                    else:
                        conn.execute(
                            """
                            INSERT OR REPLACE INTO count_build_rows
                                (build_id, id, taxon, locality)
                            VALUES (?, ?, ?, ?)
                            """,
                            (build_id, ch["id"], ch["taxon"], ch["locality"]),
                        )
                applied = int(changes[-1]["seq"])
                conn.execute(
                    """
                    UPDATE count_builds
                       SET stage = ?, applied_seq = ?, target_seq = ?,
                           updated_at = datetime('now')
                     WHERE build_id = ?
                    """,
                    (STAGE_CATCH_UP, applied, w, build_id),
                )
                conn.execute("COMMIT")

    def _scan_into_staging(self, build_id: int) -> None:
        last_id = 0
        scanned = 0
        page = replica_db.COUNT_SCAN_PAGE_SIZE
        while True:
            # 只读短事务：不持有副本写锁，同步写入全程不受阻
            with replica_db.open_replica(self.replica_path) as conn:
                rows = conn.execute(
                    """
                    SELECT id, taxon, locality
                      FROM specimens
                     WHERE id > ?
                     ORDER BY id ASC
                     LIMIT ?
                    """,
                    (last_id, page),
                ).fetchall()
            if not rows:
                self._touch_scan(build_id, scanned)
                break
            with replica_db.open_replica(self.replica_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                # 快照行直接写入；与同步器增量、移动游标扫描的竞争，
                # 由收尾阶段对 (start_seq, w] 的 CDC 权威重放统一收敛
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO count_build_rows
                        (build_id, id, taxon, locality)
                    VALUES (?, ?, ?, ?)
                    """,
                    [(build_id, r["id"], r["taxon"], r["locality"]) for r in rows],
                )
                scanned += len(rows)
                conn.execute(
                    """
                    UPDATE count_builds
                       SET stage = ?, scanned_rows = ?,
                           updated_at = datetime('now')
                     WHERE build_id = ?
                    """,
                    (STAGE_SCAN, scanned, build_id),
                )
                conn.execute("COMMIT")
            last_id = int(rows[-1]["id"])
            if len(rows) < page:
                break

    def _touch_scan(self, build_id: int, scanned: int) -> None:
        with replica_db.open_replica(self.replica_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE count_builds
                   SET stage = ?, scanned_rows = ?,
                       updated_at = datetime('now')
                 WHERE build_id = ?
                """,
                (STAGE_SCAN, scanned, build_id),
            )
            conn.execute("COMMIT")

    def _source_max_seq(self) -> int:
        src = source_connect(self.source_path, readonly=True)
        try:
            return int(
                src.execute(
                    "SELECT COALESCE(MAX(seq), 0) AS m FROM _change_log"
                ).fetchone()["m"]
            )
        finally:
            src.close()

    def _wait_sync_caught_up(self, build_id: int, start_seq: int) -> int:
        """等同步器追平某个已采样的源端水位 w，返回该水位。

        高频写入下源端水位会持续前移，不能追逐“当前最大水位”（永远差一点）。
        做法：采样一个水位 w 并固定下来，轮询等待 last_applied_seq >= w。
        同步器每批可推进数百条，而采样后到下一次轮询之间新增的日志有限，
        因此有限几轮即可满足；满足时影像已覆盖到 w（w 之后的残留增量在
        发布锁内二次复核处理）。
        """
        w = self._source_max_seq()
        while True:
            with replica_db.open_replica(self.replica_path) as conn:
                sync_seq = int(
                    conn.execute(
                        "SELECT last_applied_seq FROM sync_meta WHERE id = 1"
                    ).fetchone()["last_applied_seq"]
                )
            self._touch_build(build_id, STAGE_CATCH_UP, min(sync_seq, w), w)
            if sync_seq >= w:
                return w
            if self._stop.is_set():
                raise RuntimeError("构建被停止")
            # 让出写者角色，等待同步器推进到固定水位 w（构建器不与之争锁）
            if self._stop.wait(BUILD_WAIT_INTERVAL):
                raise RuntimeError("构建被停止")

    def _touch_build(self, build_id: int, stage: str,
                     applied: int, target: int) -> None:
        with replica_db.open_replica(self.replica_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE count_builds
                   SET stage = ?, applied_seq = ?, target_seq = ?,
                       updated_at = datetime('now')
                 WHERE build_id = ?
                """,
                (stage, applied, target, build_id),
            )
            conn.execute("COMMIT")

    def _aggregate(self, build_id: int) -> None:
        """按主键分页把暂存影像聚合成暂存计数（每页短事务，内存有界）。"""
        last_id = 0
        aggregated = 0
        page = replica_db.COUNT_AGG_PAGE_SIZE
        while True:
            with replica_db.open_replica(self.replica_path) as conn:
                rows = conn.execute(
                    """
                    SELECT id, taxon, locality
                      FROM count_build_rows
                     WHERE build_id = ? AND id > ?
                     ORDER BY id ASC
                     LIMIT ?
                    """,
                    (build_id, last_id, page),
                ).fetchall()
            if not rows:
                with replica_db.open_replica(self.replica_path) as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        """
                        UPDATE count_builds SET stage = ?,
                               updated_at = datetime('now')
                         WHERE build_id = ?
                        """,
                        (STAGE_AGGREGATE, build_id),
                    )
                    conn.execute("COMMIT")
                break
            with replica_db.open_replica(self.replica_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                for r in rows:
                    conn.execute(
                        """
                        INSERT INTO count_build_counts(build_id, locality, taxon, cnt)
                        VALUES (?, ?, ?, 1)
                        ON CONFLICT(build_id, locality, taxon)
                        DO UPDATE SET cnt = cnt + 1
                        """,
                        (build_id, r["locality"], r["taxon"]),
                    )
                aggregated += len(rows)
                last_id = int(rows[-1]["id"])
                conn.execute(
                    """
                    UPDATE count_builds
                       SET stage = ?, scanned_rows = ?,
                           updated_at = datetime('now')
                     WHERE build_id = ?
                    """,
                    (STAGE_AGGREGATE, aggregated, build_id),
                )
                conn.execute("COMMIT")
            if len(rows) < page:
                break

    def _replay_cdc_into(
        self, conn: sqlite3.Connection, build_id: int,
        lo_seq: int, hi_seq: int,
    ) -> int:
        """在调用方已持有的事务内，把源端 (lo_seq, hi_seq] 重放进影像。

        I/U 幂等 upsert、D 删除；返回实际重放到的最大序号。
        """
        if hi_seq <= lo_seq:
            return lo_seq
        src = source_connect(self.source_path, readonly=True)
        try:
            changes = src.execute(
                """
                SELECT seq, op, id, taxon, locality
                  FROM _change_log
                 WHERE seq > ? AND seq <= ?
                 ORDER BY seq ASC
                 LIMIT ?
                """,
                (lo_seq, hi_seq, CHANGE_BATCH_SIZE),
            ).fetchall()
        finally:
            src.close()
        for ch in changes:
            if ch["op"] == "D":
                conn.execute(
                    "DELETE FROM count_build_rows "
                    "WHERE build_id = ? AND id = ?",
                    (build_id, ch["id"]),
                )
            else:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO count_build_rows
                        (build_id, id, taxon, locality)
                    VALUES (?, ?, ?, ?)
                    """,
                    (build_id, ch["id"], ch["taxon"], ch["locality"]),
                )
        return int(changes[-1]["seq"]) if changes else lo_seq

    def _publish(self, build_id: int, w: int) -> int:
        """单事务原子交换发布完整新计数，返回最终覆盖水位。

        进入这里时已确认同步器 last_applied_seq >= 采样水位 w，影像经 CDC
        权威重放覆盖到 w。关键事实：交换在 BEGIN IMMEDIATE 持写锁事务内进行，
        而 SQLite 同一时刻只有一个写者——同步器在本事务提交前无法提交新批次。
        若获取写锁前同步器已越过 w，则在同一持锁事务内把 CDC 区间
        (w, sync_seq] 权威重放进影像、重算聚合并把发布水位提升到 sync_seq，
        使影像严格等于 sync_seq 的标本全集；同时把 published_seq 置为该水位，
        同步器随后对 <= published_seq 的批次会跳过计数维护，杜绝重复。
        """
        lock_retries = 0
        while True:
            conn = replica_db.connect(self.replica_path)
            try:
                conn.execute("BEGIN IMMEDIATE")
                sync_seq = int(
                    conn.execute(
                        "SELECT last_applied_seq FROM sync_meta WHERE id = 1"
                    ).fetchone()["last_applied_seq"]
                )
                if sync_seq > w:
                    # 锁内权威补齐 (w, sync_seq]：循环重放（每批有上限）
                    cursor = w
                    while cursor < sync_seq:
                        advanced = self._replay_cdc_into(
                            conn, build_id, cursor, sync_seq
                        )
                        if advanced <= cursor:
                            break
                        cursor = advanced
                    # 从已追平到 sync_seq 的完整影像重算暂存计数
                    conn.execute(
                        "DELETE FROM count_build_counts WHERE build_id = ?",
                        (build_id,),
                    )
                    conn.execute(
                        """
                        INSERT INTO count_build_counts
                            (build_id, locality, taxon, cnt)
                        SELECT build_id, locality, taxon, COUNT(*)
                          FROM count_build_rows
                         WHERE build_id = ?
                         GROUP BY locality, taxon
                        """,
                        (build_id,),
                    )
                    w = sync_seq
                conn.execute(
                    "UPDATE count_builds SET stage = ?, "
                    "applied_seq = ?, target_seq = ?, "
                    "updated_at = datetime('now') WHERE build_id = ?",
                    (STAGE_PUBLISH, w, w, build_id),
                )
                # 交换只做 DELETE+INSERT，不含任何全表扫描
                conn.execute("DELETE FROM taxon_counts")
                conn.execute(
                    """
                    INSERT INTO taxon_counts(locality, taxon, cnt)
                    SELECT locality, taxon, cnt
                      FROM count_build_counts
                     WHERE build_id = ?
                    """,
                    (build_id,),
                )
                conn.execute(
                    "UPDATE count_meta SET published_build_id = ?, "
                    "published_seq = ?, updated_at = datetime('now') WHERE id = 1",
                    (build_id, w),
                )
                conn.execute(
                    """
                    UPDATE count_builds
                       SET phase = ?, stage = ?, last_error = NULL,
                           updated_at = datetime('now')
                     WHERE build_id = ?
                    """,
                    (replica_db.BUILD_PUBLISHED, STAGE_PUBLISH, build_id),
                )
                conn.execute("COMMIT")
                conn.close()
                return w
            except sqlite3.OperationalError as exc:
                # 与同步写事务竞争锁：回滚退避后持续重试。锁竞争是暂时的，
                # 不应因人为次数上限而判失败；退避封顶 0.5s 避免忙等。
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                conn.close()
                # 仅“数据库被锁”类错误才重试，其余 OperationalError 上抛
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                lock_retries += 1
                if self._stop.wait(min(0.05 * lock_retries, 0.5)):
                    raise RuntimeError("构建被停止")

    def _cleanup_staging(self, build_id: int) -> None:
        """分批删除暂存数据，每批一个短事务（避免大事务长持写锁）。"""
        with replica_db.open_replica(self.replica_path) as conn:
            while True:
                conn.execute("BEGIN IMMEDIATE")
                cur = conn.execute(
                    """
                    DELETE FROM count_build_rows
                     WHERE build_id = ?
                       AND id IN (
                           SELECT id FROM count_build_rows
                            WHERE build_id = ? ORDER BY id LIMIT ?
                       )
                    """,
                    (build_id, build_id, CLEANUP_BATCH),
                )
                removed = cur.rowcount
                conn.execute("COMMIT")
                if removed < CLEANUP_BATCH:
                    break
            while True:
                conn.execute("BEGIN IMMEDIATE")
                cur = conn.execute(
                    """
                    DELETE FROM count_build_counts
                     WHERE build_id = ?
                       AND rowid IN (
                           SELECT rowid FROM count_build_counts
                            WHERE build_id = ? LIMIT ?
                       )
                    """,
                    (build_id, build_id, CLEANUP_BATCH),
                )
                removed = cur.rowcount
                conn.execute("COMMIT")
                if removed < CLEANUP_BATCH:
                    break
