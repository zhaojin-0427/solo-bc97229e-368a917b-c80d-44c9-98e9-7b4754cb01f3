"""馆藏计数系统正确性测试。

运行：
    python -m pytest tests/test_counts.py -v

覆盖：
  * 首次构建正确性（与直接 GROUP BY 一致、零计数组不显示、原文本分组）
  * 查询走持久化索引而非扫描 specimens
  * 增/删/换组更正随同步同事务维护（扣旧加新、归零删组）
  * 计数与数据、审计、进度同事务提交/回滚
  * 重建期间旧计数继续可用，完整新计数才替换；失败保留旧计数
  * 重启：未完成构建标失败、旧发布保留、首次完成前不返回统计
  * 构建期间持续并发增删改，发布计数最终正确（无重复/遗漏）
  * 有界内存：扫描/聚合按固定页大小分批、每批独立短事务
  * Web API：503/400/409 语义、分页、重建入口、进度与错误
"""
import os
import threading
import time

import pytest

# 必须在导入 app.web 前指向临时目录，避免模块级 app 使用默认 /data 路径
os.environ.setdefault("SOURCE_DB", "/tmp/museum-test-counts/source.db")
os.environ.setdefault("REPLICA_DB", "/tmp/museum-test-counts/replica.db")

from app import counts, replica_db, source_db  # noqa: E402
from app.sync import SyncEngine  # noqa: E402


# --------------------------------------------------------------------------- #
# 辅助
# --------------------------------------------------------------------------- #
@pytest.fixture()
def paths(tmp_path):
    src = str(tmp_path / "source.db")
    rep = str(tmp_path / "replica.db")
    rows = [
        (1, "T. rex", "Montana"),
        (2, "T. rex", "Montana"),
        (3, "Apatosaurus", "Colorado"),
        (4, "T. rex", "Colorado"),
        (5, "Apatosaurus", "Colorado"),
        # 同文字不同大小写/空格必须视为不同分组（按原文本，不做规范化）
        (6, "T. rex", "montana"),
    ]
    source_db.init_demo(src, rows=rows)
    return src, rep


def start_sync(src, rep):
    engine = SyncEngine(src, rep, poll_interval=0.02)
    engine.start()
    deadline = time.time() + 15
    while time.time() < deadline:
        m = replica_db.get_meta(rep)
        if m["phase"] == replica_db.PHASE_RUNNING:
            break
        time.sleep(0.02)
    assert replica_db.get_meta(rep)["phase"] == replica_db.PHASE_RUNNING
    return engine


def wait_caught_up(src, rep, timeout=15):
    target = source_db.max_change_seq(src)
    deadline = time.time() + timeout
    while time.time() < deadline:
        m = replica_db.get_meta(rep)
        if m["phase"] == replica_db.PHASE_RUNNING and m["last_applied_seq"] >= target:
            return
        time.sleep(0.02)
    raise AssertionError("副本未能在时限内追平源端水位")


def build_counts(src, rep, engine=None):
    mgr = counts.CountBuildManager(src, rep)
    # 关键装配：同步器作为副本唯一写者，需要知道活动构建才能把增量并入影像
    if engine is not None:
        engine.count_build_manager = mgr
    build_id = mgr.start_build()
    assert build_id is not None
    mgr.wait_until_idle(20)
    return mgr, build_id


def direct_group_by(rep):
    with replica_db.open_replica(rep) as conn:
        return {
            (r[0], r[1]): r[2]
            for r in conn.execute(
                "SELECT locality, taxon, COUNT(*) FROM specimens GROUP BY 1, 2"
            )
        }


def stored_counts(rep):
    with replica_db.open_replica(rep) as conn:
        return {
            (r[0], r[1]): r[2]
            for r in conn.execute(
                "SELECT locality, taxon, cnt FROM taxon_counts"
            )
        }


# --------------------------------------------------------------------------- #
# 1. 首次构建与查询语义
# --------------------------------------------------------------------------- #
class TestInitialBuild:
    def test_no_stats_before_first_build(self, paths):
        src, rep = paths
        start_sync(src, rep)
        # 首次构建完成前：不返回统计
        assert replica_db.count_is_available(rep) is False
        assert replica_db.get_published_build_id(rep) is None
        assert replica_db.count_localities(rep) == []

    def test_first_build_matches_group_by(self, paths):
        src, rep = paths
        engine = start_sync(src, rep)
        _, build_id = build_counts(src, rep, engine)
        build = replica_db.get_build(rep, build_id)
        assert build["phase"] == replica_db.BUILD_PUBLISHED
        assert replica_db.get_published_build_id(rep) == build_id
        assert stored_counts(rep) == direct_group_by(rep)
        # Montana 与 montana 是两个不同分组（原文本精确）
        assert ("Montana", "T. rex") in stored_counts(rep)
        assert ("montana", "T. rex") in stored_counts(rep)

    def test_zero_count_groups_absent(self, paths):
        src, rep = paths
        engine = start_sync(src, rep)
        build_counts(src, rep, engine)
        # 撤回某采集地最后一个标本 -> 对应组被删除而非保留 0
        source_db.delete_specimen(src, 6)  # montana 唯一标本
        wait_caught_up(src, rep)
        assert ("montana", "T. rex") not in stored_counts(rep)
        with replica_db.open_replica(rep) as conn:
            zero = conn.execute(
                "SELECT COUNT(*) c FROM taxon_counts WHERE cnt <= 0"
            ).fetchone()["c"]
        assert zero == 0
        engine.stop()
        engine.wait_until_idle(5)

    def test_pagination_and_exact_locality_filter(self, paths):
        src, rep = paths
        # 制造同采集地多个 taxon 以验证分页
        for i in range(7, 13):
            source_db.insert_specimen(src, i, f"taxon-{i}", "Montana")
        engine = start_sync(src, rep)
        build_counts(src, rep, engine)

        page1 = replica_db.count_taxa_for_locality(rep, "Montana", 1, 3)
        page2 = replica_db.count_taxa_for_locality(rep, "Montana", 2, 3)
        assert page1["total_groups"] == 7  # T. rex + taxon-7..12
        assert page1["total_pages"] == 3
        assert len(page1["items"]) == 3 and len(page2["items"]) == 3
        taxa_p1 = [it["taxon"] for it in page1["items"]]
        taxa_p2 = [it["taxon"] for it in page2["items"]]
        assert taxa_p1 == sorted(taxa_p1) and taxa_p2 == sorted(taxa_p2)
        assert set(taxa_p1).isdisjoint(taxa_p2)
        # 精确筛选：montana（小写）不含 Montana 的数据
        lower = replica_db.count_taxa_for_locality(rep, "montana", 1, 20)
        assert lower["total_groups"] == 1
        engine.stop()
        engine.wait_until_idle(5)

    def test_query_uses_index_not_specimen_scan(self, paths):
        src, rep = paths
        engine = start_sync(src, rep)
        build_counts(src, rep, engine)
        with replica_db.open_replica(rep) as conn:
            plan = conn.execute(
                "EXPLAIN QUERY PLAN "
                "SELECT taxon, cnt FROM taxon_counts "
                "WHERE locality = ? ORDER BY taxon LIMIT ? OFFSET ?",
                ("Montana", 20, 0),
            ).fetchall()
        detail = " ".join(row[3] for row in plan)
        assert "specimens" not in detail
        assert "taxon_counts" in detail


# --------------------------------------------------------------------------- #
# 2. 增量维护：与同步同事务、扣旧加新
# --------------------------------------------------------------------------- #
class TestIncrementalMaintenance:
    def test_insert_update_delete_maintained(self, paths):
        src, rep = paths
        engine = start_sync(src, rep)
        build_counts(src, rep, engine)

        source_db.insert_specimen(src, 20, "Stegosaurus", "Colorado")
        wait_caught_up(src, rep)
        co = replica_db.count_taxa_for_locality(rep, "Colorado", 1, 50)
        assert next(i for i in co["items"] if i["taxon"] == "Stegosaurus")[
            "count"
        ] == 1

        # 换组更正：Colorado -> Montana，同一同步事务扣旧加新
        source_db.update_specimen(src, 20, "Stegosaurus", "Montana")
        wait_caught_up(src, rep)
        co = replica_db.count_taxa_for_locality(rep, "Colorado", 1, 50)
        mo = replica_db.count_taxa_for_locality(rep, "Montana", 1, 50)
        assert all(i["taxon"] != "Stegosaurus" for i in co["items"])
        assert next(i for i in mo["items"] if i["taxon"] == "Stegosaurus")[
            "count"
        ] == 1

        # 删除：减一，归零删组
        source_db.delete_specimen(src, 20)
        wait_caught_up(src, rep)
        mo = replica_db.count_taxa_for_locality(rep, "Montana", 1, 50)
        assert all(i["taxon"] != "Stegosaurus" for i in mo["items"])

        # 与直接 GROUP BY 始终一致
        assert stored_counts(rep) == direct_group_by(rep)
        engine.stop()
        engine.wait_until_idle(5)

    def test_count_and_data_commit_together(self, paths, monkeypatch):
        """提交点故障：标本行、审计、进度、计数要么全生效要么全回滚。"""
        src, rep = paths
        engine = start_sync(src, rep)
        build_counts(src, rep, engine)
        source_db.insert_specimen(src, 30, "Ankylosaurus", "Montana")
        target = source_db.max_change_seq(src)

        real_factory = replica_db._ReplicaConnection  # noqa: SLF001
        crash = {"done": False}

        class CrashingConnection(real_factory):
            def execute(self, sql, *args):
                if (
                    not crash["done"]
                    and isinstance(sql, str)
                    and sql.strip().upper() == "COMMIT"
                ):
                    crash["done"] = True
                    raise __import__("sqlite3").OperationalError(
                        "simulated crash at commit"
                    )
                return super().execute(sql, *args)

        monkeypatch.setattr(replica_db, "_ReplicaConnection", CrashingConnection)
        victim = SyncEngine(src, rep, poll_interval=0.02)
        victim.start()
        # 等到故障注入触发（崩溃的那批整体回滚），随后的轮次即可正常提交
        deadline = time.time() + 10
        while time.time() < deadline and not crash["done"]:
            time.sleep(0.02)
        assert crash["done"]
        # 崩溃批次回滚后，引擎按持久进度重试并最终追平（故障只注入一次）
        deadline = time.time() + 10
        while time.time() < deadline:
            if replica_db.get_meta(rep)["last_applied_seq"] >= target:
                break
            time.sleep(0.02)
        victim.stop()
        victim.wait_until_idle(10)
        monkeypatch.undo()

        # 故障批次没有半生效：最终计数与实际标本完全一致（无重复/丢失）
        assert replica_db.get_meta(rep)["last_applied_seq"] >= target
        mo = replica_db.count_taxa_for_locality(rep, "Montana", 1, 50)
        ank = [i for i in mo["items"] if i["taxon"] == "Ankylosaurus"]
        assert len(ank) == 1 and ank[0]["count"] == 1
        assert stored_counts(rep) == direct_group_by(rep)
        engine.stop()
        engine.wait_until_idle(5)

    def test_maintenance_skipped_before_first_publish(self, paths):
        """首次发布前的同步不写计数；发布以完整构建影像为准。"""
        src, rep = paths
        engine = start_sync(src, rep)
        source_db.insert_specimen(src, 40, "Pteranodon", "Kansas")
        wait_caught_up(src, rep)
        with replica_db.open_replica(rep) as conn:
            assert conn.execute("SELECT COUNT(*) c FROM taxon_counts").fetchone()[
                "c"
            ] == 0
        build_counts(src, rep, engine)
        assert stored_counts(rep) == direct_group_by(rep)
        engine.stop()
        engine.wait_until_idle(5)


# --------------------------------------------------------------------------- #
# 3. 重建：旧计数可用、完整才替换、失败保留
# --------------------------------------------------------------------------- #
class TestRebuild:
    def test_rebuild_keeps_old_counts_until_published(self, paths, monkeypatch):
        src, rep = paths
        # 调小分页，让重建扫描持续一段时间，便于观察中间态
        monkeypatch.setattr(replica_db, "COUNT_SCAN_PAGE_SIZE", 2)
        monkeypatch.setattr(replica_db, "COUNT_AGG_PAGE_SIZE", 2)
        engine = start_sync(src, rep)
        mgr, first_id = build_counts(src, rep, engine)
        snapshot = stored_counts(rep)

        # 注入栅栏：扫描处理完第一页后阻塞，确定性地停在“构建进行中”
        reached = threading.Event()
        release = threading.Event()
        real_scan = counts.CountBuildManager._scan_into_staging

        def gated_scan(self, build_id):
            reached.set()
            assert release.wait(10), "构建未在栅栏处停留"
            return real_scan(self, build_id)

        monkeypatch.setattr(counts.CountBuildManager, "_scan_into_staging",
                            gated_scan)
        second_id = mgr.start_build()
        assert reached.wait(10)
        try:
            # 构建进行中：已发布构建仍是旧版，旧计数照常可查、内容不变
            assert mgr.is_build_running()
            assert replica_db.get_build(rep, second_id)["phase"] == \
                replica_db.BUILD_RUNNING
            assert replica_db.get_published_build_id(rep) == first_id
            assert stored_counts(rep) == snapshot
            # 构建期间同步与源更正仍可用，旧计数继续增量更新
            source_db.insert_specimen(src, 80, "Gated", "Montana")
            wait_caught_up(src, rep)
            assert replica_db.get_published_build_id(rep) == first_id
            assert ("Montana", "Gated") in stored_counts(rep)
        finally:
            release.set()
        assert mgr.wait_build_finished(15)
        assert replica_db.get_build(rep, second_id)["phase"] == \
            replica_db.BUILD_PUBLISHED
        assert replica_db.get_published_build_id(rep) == second_id
        assert stored_counts(rep) == direct_group_by(rep)
        engine.stop()
        engine.wait_until_idle(5)

    def test_failed_rebuild_preserves_old_counts(self, paths, monkeypatch):
        src, rep = paths
        engine = start_sync(src, rep)
        _, first_id = build_counts(src, rep, engine)
        before = stored_counts(rep)

        # 让聚合阶段必失败
        real_agg = counts.CountBuildManager._aggregate

        def boom(self, build_id):
            raise RuntimeError("simulated aggregate failure")

        monkeypatch.setattr(counts.CountBuildManager, "_aggregate", boom)
        mgr = counts.CountBuildManager(src, rep)
        bad_id = mgr.start_build()
        mgr.wait_until_idle(10)
        monkeypatch.undo()

        bad = replica_db.get_build(rep, bad_id)
        assert bad["phase"] == replica_db.BUILD_FAILED
        assert "simulated aggregate failure" in bad["last_error"]
        # 旧计数完整保留且仍是已发布版本
        assert replica_db.get_published_build_id(rep) == first_id
        assert stored_counts(rep) == before

        # 之后可以重新成功构建
        mgr2, ok_id = build_counts(src, rep, engine)
        assert replica_db.get_published_build_id(rep) == ok_id
        assert stored_counts(rep) == direct_group_by(rep)
        engine.stop()
        engine.wait_until_idle(5)

    def test_concurrent_changes_during_build_are_included(self, paths, monkeypatch):
        """构建全程并发增删改：发布计数必须纳入全部变更，无重复无遗漏。"""
        src, rep = paths
        monkeypatch.setattr(replica_db, "COUNT_SCAN_PAGE_SIZE", 1)
        monkeypatch.setattr(replica_db, "COUNT_AGG_PAGE_SIZE", 1)
        engine = start_sync(src, rep)
        build_counts(src, rep, engine)  # 先有一版发布计数

        stop_flag = threading.Event()
        next_id = [1000]

        def writer():
            while not stop_flag.is_set():
                next_id[0] += 1
                nid = next_id[0]
                try:
                    source_db.insert_specimen(src, nid, f"dyn-{nid % 7}", "Montana")
                    if nid % 2 == 0:
                        source_db.update_specimen(
                            src, nid, f"dyn-{nid % 7}", "Colorado"
                        )
                    if nid % 5 == 0:
                        source_db.delete_specimen(src, nid)
                except Exception:
                    pass
                time.sleep(0.001)

        t = threading.Thread(target=writer)
        t.start()
        try:
            mgr = counts.CountBuildManager(src, rep)
            engine.count_build_manager = mgr  # 让同步器把增量并入本构建影像
            build_id = mgr.start_build()
            mgr.wait_until_idle(30)
        finally:
            stop_flag.set()
            t.join()
        wait_caught_up(src, rep)

        assert replica_db.get_build(rep, build_id)["phase"] == \
            replica_db.BUILD_PUBLISHED
        assert stored_counts(rep) == direct_group_by(rep)
        engine.stop()
        engine.wait_until_idle(5)

    def test_bounded_memory_short_transactions(self, paths, monkeypatch):
        """扫描/聚合按固定页大小分批，每批独立事务，不持写事务跨全量扫描。"""
        src, rep = paths
        for i in range(50, 70):
            source_db.insert_specimen(src, i, f"bulk-{i}", "Wyoming")
        engine = start_sync(src, rep)
        monkeypatch.setattr(replica_db, "COUNT_SCAN_PAGE_SIZE", 5)
        monkeypatch.setattr(replica_db, "COUNT_AGG_PAGE_SIZE", 5)

        open_count = {"n": 0}
        real_open = replica_db.open_replica

        def counting_open(path):
            open_count["n"] += 1
            return real_open(path)

        monkeypatch.setattr(counts.replica_db, "open_replica", counting_open)
        mgr = counts.CountBuildManager(src, rep)
        engine.count_build_manager = mgr
        build_id = mgr.start_build()
        mgr.wait_until_idle(20)
        # 20+ 行、页大小 5：必然产生多个独立短事务连接，而非一个长事务
        assert open_count["n"] > 4
        assert replica_db.get_build(rep, build_id)["phase"] == \
            replica_db.BUILD_PUBLISHED
        assert stored_counts(rep) == direct_group_by(rep)
        engine.stop()
        engine.wait_until_idle(5)


# --------------------------------------------------------------------------- #
# 4. 重启语义
# --------------------------------------------------------------------------- #
class TestRestartSemantics:
    def test_interrupted_build_marked_failed_on_restart(self, paths, monkeypatch):
        src, rep = paths
        engine = start_sync(src, rep)
        # 手工制造一个“停在 RUNNING”的未完成构建（模拟进程被 kill）
        with replica_db.open_replica(rep) as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                "INSERT INTO count_builds(phase, stage, total_rows) "
                "VALUES (?, ?, 10)",
                (replica_db.BUILD_RUNNING, counts.STAGE_SCAN),
            )
            stuck_id = cur.lastrowid
            conn.execute("COMMIT")

        mgr = counts.CountBuildManager(src, rep)
        mgr._mark_stale_builds_failed()  # 模拟新进程启动时的善后
        b = replica_db.get_build(rep, stuck_id)
        assert b["phase"] == replica_db.BUILD_FAILED
        assert b["last_error"]
        # 从未发布过 -> 仍不返回统计
        assert replica_db.count_is_available(rep) is False

        # 监督线程能自动触发首次构建
        mgr.start_supervisor()
        deadline = time.time() + 15
        while time.time() < deadline and not replica_db.count_is_available(rep):
            time.sleep(0.05)
        mgr.shutdown()
        assert replica_db.count_is_available(rep) is True
        assert stored_counts(rep) == direct_group_by(rep)
        engine.stop()
        engine.wait_until_idle(5)

    def test_restart_keeps_published_counts(self, paths):
        src, rep = paths
        engine = start_sync(src, rep)
        _, build_id = build_counts(src, rep, engine)
        published = stored_counts(rep)
        # 新管理器实例 = 新进程：旧发布计数原样可查
        mgr = counts.CountBuildManager(src, rep)
        mgr._mark_stale_builds_failed()
        assert replica_db.get_published_build_id(rep) == build_id
        assert stored_counts(rep) == published
        engine.stop()
        engine.wait_until_idle(5)


# --------------------------------------------------------------------------- #
# 5. Web API
# --------------------------------------------------------------------------- #
class TestCountsWebAPI:
    @pytest.fixture()
    def client(self, paths, monkeypatch):
        # 测试使用极小分页，验证有界分批路径
        monkeypatch.setattr(replica_db, "COUNT_SCAN_PAGE_SIZE", 3)
        monkeypatch.setattr(replica_db, "COUNT_AGG_PAGE_SIZE", 3)
        from app.web import create_app

        src, rep = paths
        app = create_app(src, rep, poll_interval=0.02, auto_start=False)
        app.config["TESTING"] = True
        with app.test_client() as c:
            yield c, app.config["SYNC_ENGINE"], app.config["COUNT_BUILD_MANAGER"]

    def _wait_running(self, engine):
        deadline = time.time() + 15
        while time.time() < deadline:
            if replica_db.get_meta(engine.replica_path)["phase"] == \
                    replica_db.PHASE_RUNNING:
                return
            time.sleep(0.02)
        raise AssertionError("副本未就绪")

    def _wait_published(self, mgr):
        deadline = time.time() + 20
        while time.time() < deadline:
            if replica_db.count_is_available(mgr.replica_path):
                return
            time.sleep(0.05)
        raise AssertionError("计数未发布")

    def test_503_before_first_build(self, client):
        c, engine, mgr = client
        engine.start()
        self._wait_running(engine)
        r = c.get("/api/counts?locality=Montana")
        assert r.status_code == 503
        assert r.get_json()["error"] == "COUNTS_NOT_AVAILABLE"
        r2 = c.get("/api/counts/localities")
        assert r2.status_code == 503
        engine.stop()
        engine.wait_until_idle(5)

    def test_missing_locality_is_400(self, client):
        c, engine, mgr = client
        engine.start()
        self._wait_running(engine)
        c.post("/api/counts/rebuild")
        self._wait_published(mgr)
        r = c.get("/api/counts")
        assert r.status_code == 400
        assert r.get_json()["error"] == "BAD_REQUEST"
        engine.stop()
        engine.wait_until_idle(5)

    def test_rebuild_query_status_flow(self, client):
        c, engine, mgr = client
        # 副本未就绪时拒绝构建
        assert c.post("/api/counts/rebuild").status_code == 503
        engine.start()
        self._wait_running(engine)

        r = c.post("/api/counts/rebuild")
        assert r.status_code == 200 and r.get_json()["build_id"] >= 1
        # 构建中再次请求 -> 409
        r2 = c.post("/api/counts/rebuild")
        assert r2.status_code == 409
        self._wait_published(mgr)

        r = c.get("/api/counts?locality=Montana&page=1")
        assert r.status_code == 200
        body = r.get_json()
        assert body["locality"] == "Montana"
        assert body["page_size"] == 20
        assert body["total_specimens"] == 2  # id 1,2
        assert body["items"] == [{"taxon": "T. rex", "count": 2}]

        r = c.get("/api/counts/localities")
        locs = {x["locality"]: x for x in r.get_json()["localities"]}
        assert "Montana" in locs and "montana" in locs  # 原文精确，大小写分组

        st = c.get("/api/counts/status").get_json()
        assert st["available"] is True
        assert st["latest_build"]["phase"] == replica_db.BUILD_PUBLISHED

        # 源库增改撤后，计数随同步更新
        c.post("/api/demo/insert",
               json={"id": 50, "taxon": "X", "locality": "Montana"})
        deadline = time.time() + 10
        while time.time() < deadline:
            body = c.get("/api/counts?locality=Montana").get_json()
            if any(i["taxon"] == "X" for i in body["items"]):
                break
            time.sleep(0.05)
        else:
            pytest.fail("新增未反映到计数")
        c.post("/api/demo/update",
               json={"id": 50, "taxon": "X", "locality": "Colorado"})
        deadline = time.time() + 10
        while time.time() < deadline:
            body = c.get("/api/counts?locality=Montana").get_json()
            if all(i["taxon"] != "X" for i in body["items"]):
                break
            time.sleep(0.05)
        else:
            pytest.fail("换组更正未扣旧组")
        engine.stop()
        engine.wait_until_idle(5)

    def test_status_reports_error_and_progress(self, client, monkeypatch):
        c, engine, mgr = client
        engine.start()
        self._wait_running(engine)

        def boom(self, build_id):
            raise RuntimeError("web demo failure")

        monkeypatch.setattr(counts.CountBuildManager, "_scan_into_staging", boom)
        r = c.post("/api/counts/rebuild")
        assert r.status_code == 200
        mgr.wait_until_idle(10)
        st = c.get("/api/counts/status").get_json()
        assert st["available"] is False
        assert st["latest_build"]["phase"] == replica_db.BUILD_FAILED
        assert "web demo failure" in st["latest_build"]["last_error"]
        engine.stop()
        engine.wait_until_idle(5)
