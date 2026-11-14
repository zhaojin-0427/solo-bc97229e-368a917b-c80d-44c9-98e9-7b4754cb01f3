"""自然博物馆标本登记库同步 Web 控制台（Flask）。

页面与 API：
  GET  /                 控制台页面（同步进度、错误、操作按钮、查询框、馆藏计数）
  POST /api/sync/start   启动后台同步
  POST /api/sync/stop    请求停止后台同步
  GET  /api/status       同步进度与错误（JSON）
  GET  /api/specimen/<id>  按标本号查询副本（就绪前返回 503）

  # 馆藏计数（持久化，按 locality 精确筛选、分页列 taxon 与标本数）
  GET  /api/counts/localities          可选采集地与各地汇总（仅已发布计数）
  GET  /api/counts?locality=...&page=  分页计数（首次构建完成前 503）
  GET  /api/counts/status              构建进度、阶段与错误（JSON）
  POST /api/counts/rebuild             后台重建（构建中再次请求返回 409）

  # 演示用：直接对源库提交增 / 改 / 撤（鉴定员的日常操作）
  POST /api/demo/insert
  POST /api/demo/update
  POST /api/demo/delete
  GET  /api/source/specimens  查看源库当前标本（仅演示辅助）
"""
from __future__ import annotations

import os

from flask import Flask, jsonify, render_template, request

from . import counts as counts_mod
from . import replica_db, source_db
from .sync import SyncEngine

SOURCE_DB = os.environ.get("SOURCE_DB", "/data/source/specimens.db")
REPLICA_DB = os.environ.get("REPLICA_DB", "/data/replica/specimens_replica.db")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1.0"))
# 容器启动时是否自动开始同步（1/0）
AUTO_START = os.environ.get("AUTO_START", "1") == "1"
# 计数分页每页组数
COUNTS_PAGE_SIZE = int(os.environ.get("COUNTS_PAGE_SIZE", "20"))


def create_app(
    source_db_path: str | None = None,
    replica_db_path: str | None = None,
    poll_interval: float | None = None,
    auto_start: bool | None = None,
) -> Flask:
    app = Flask(__name__)
    source_path = source_db_path or SOURCE_DB
    replica_path = replica_db_path or REPLICA_DB
    interval = POLL_INTERVAL if poll_interval is None else poll_interval
    do_auto_start = AUTO_START if auto_start is None else auto_start

    engine = SyncEngine(source_path, replica_path, poll_interval=interval)
    app.config["SYNC_ENGINE"] = engine

    # 计数构建管理器：重启善后、副本就绪后自动首次构建、手动重建。
    # 注册到同步引擎：同步器作为副本唯一写者，把变更顺带汇入活动构建影像。
    build_manager = counts_mod.CountBuildManager(source_path, replica_path)
    engine.count_build_manager = build_manager
    app.config["COUNT_BUILD_MANAGER"] = build_manager

    # 确保示例源库与副本结构存在（源库不存在则播种演示数据）
    source_db.init_demo(source_path)
    replica_db.init_replica(replica_path)
    if do_auto_start:
        engine.start()
    build_manager.start_supervisor()

    # ----------------------------- 页面 ----------------------------- #
    @app.get("/")
    def index():
        return render_template("index.html")

    # --------------------------- 同步控制 --------------------------- #
    @app.post("/api/sync/start")
    def sync_start():
        # 每次启动前确保源库 CDC 对象齐全（兼容外部替换的库文件）
        source_db.init_source(source_path)
        started = engine.start()
        return jsonify({"started": started, "status": engine.status()})

    @app.post("/api/sync/stop")
    def sync_stop():
        stopped = engine.stop()
        return jsonify({"stop_requested": stopped, "status": engine.status()})

    @app.get("/api/status")
    def status():
        st = engine.status()
        st["source"] = {
            "path": source_path,
            "specimen_count": _safe(source_db.count_specimens, source_path),
            "max_change_seq": _safe(source_db.max_change_seq, source_path),
        }
        return jsonify(st)

    # --------------------------- 副本查询 --------------------------- #
    @app.get("/api/specimen/<int:specimen_id>")
    def get_specimen(specimen_id: int):
        # 就绪前不供查询
        if not replica_db.is_ready(replica_path):
            meta = replica_db.get_meta(replica_path)
            return (
                jsonify(
                    {
                        "error": "REPLICA_NOT_READY",
                        "message": "副本尚未完成首次复制，暂不提供查询",
                        "phase": meta["phase"],
                    }
                ),
                503,
            )
        row = replica_db.get_specimen(replica_path, specimen_id)
        if row is None:
            return (
                jsonify(
                    {
                        "error": "NOT_FOUND",
                        "message": f"副本中不存在标本 {specimen_id}"
                        "（可能已被鉴定员撤回）",
                        "id": specimen_id,
                    }
                ),
                404,
            )
        return jsonify(row)

    # --------------------- 演示：源库增 / 改 / 撤 --------------------- #
    @app.post("/api/demo/insert")
    def demo_insert():
        data = request.get_json(force=True)
        try:
            specimen_id = int(data["id"])
            source_db.insert_specimen(
                source_path, specimen_id, str(data["taxon"]), str(data["locality"])
            )
        except KeyError as exc:
            return jsonify({"error": "BAD_REQUEST", "message": str(exc)}), 400
        except Exception as exc:  # 主键冲突等
            return jsonify({"error": "SOURCE_ERROR", "message": str(exc)}), 409
        return jsonify({"ok": True})

    @app.post("/api/demo/update")
    def demo_update():
        data = request.get_json(force=True)
        try:
            specimen_id = int(data["id"])
            source_db.update_specimen(
                source_path, specimen_id, str(data["taxon"]), str(data["locality"])
            )
        except KeyError as exc:
            return jsonify({"error": "NOT_FOUND", "message": str(exc)}), 404
        except Exception as exc:
            return jsonify({"error": "SOURCE_ERROR", "message": str(exc)}), 409
        return jsonify({"ok": True})

    @app.post("/api/demo/delete")
    def demo_delete():
        data = request.get_json(force=True)
        try:
            specimen_id = int(data["id"])
            source_db.delete_specimen(source_path, specimen_id)
        except KeyError as exc:
            return jsonify({"error": "NOT_FOUND", "message": str(exc)}), 404
        return jsonify({"ok": True})

    @app.get("/api/source/specimens")
    def source_specimens():
        conn = source_db.connect(source_path, readonly=True)
        try:
            rows = conn.execute(
                "SELECT id, taxon, locality FROM specimens ORDER BY id"
            ).fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])

    # --------------------------- 馆藏计数 --------------------------- #
    @app.get("/api/counts/localities")
    def counts_localities():
        if not replica_db.count_is_available(replica_path):
            return _counts_unavailable()
        return jsonify(
            {
                "published_build_id":
                    replica_db.get_published_build_id(replica_path),
                "localities":
                    replica_db.list_localities_with_counts(replica_path),
            }
        )

    @app.get("/api/counts")
    def counts_query():
        locality = request.args.get("locality", "")
        if not locality:
            return (
                jsonify(
                    {
                        "error": "BAD_REQUEST",
                        "message": "必须提供 locality 参数（按采集地原文精确筛选）",
                    }
                ),
                400,
            )
        # 首次构建完成前不返回统计
        if not replica_db.count_is_available(replica_path):
            return _counts_unavailable()
        try:
            page = int(request.args.get("page", "1"))
        except ValueError:
            page = 1
        result = replica_db.count_taxa_for_locality(
            replica_path, locality, page, COUNTS_PAGE_SIZE
        )
        result["published_build_id"] = replica_db.get_published_build_id(
            replica_path
        )
        return jsonify(result)

    @app.get("/api/counts/status")
    def counts_status():
        return jsonify(build_manager.status())

    @app.post("/api/counts/rebuild")
    def counts_rebuild():
        # 重建以 RUNNING 副本为前提；副本未就绪时明确拒绝
        if not replica_db.is_ready(replica_path):
            meta = replica_db.get_meta(replica_path)
            return (
                jsonify(
                    {
                        "error": "REPLICA_NOT_READY",
                        "message": "副本尚未完成首次复制，暂不能构建计数",
                        "phase": meta["phase"],
                    }
                ),
                503,
            )
        build_id = build_manager.start_build()
        if build_id is None:
            return (
                jsonify(
                    {
                        "error": "BUILD_IN_PROGRESS",
                        "message": "已有计数构建在运行，请等待其完成",
                        "status": build_manager.status(),
                    }
                ),
                409,
            )
        return jsonify({"started": True, "build_id": build_id})

    def _counts_unavailable():
        latest = replica_db.get_latest_build(replica_path)
        return (
            jsonify(
                {
                    "error": "COUNTS_NOT_AVAILABLE",
                    "message": "馆藏计数首次构建尚未完成，暂不提供统计",
                    "latest_build": latest,
                }
            ),
            503,
        )

    @app.get("/healthz")
    def healthz():
        return jsonify({"ok": True})

    return app


def _safe(fn, *args):
    """源库可能被外部占用/锁定，状态接口不应因此 500。"""
    try:
        return fn(*args)
    except Exception as exc:  # pragma: no cover - 防御性
        return f"error: {type(exc).__name__}: {exc}"


app = create_app()
