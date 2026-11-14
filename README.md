# 自然博物馆标本登记库同步系统

把本地 SQLite(WAL) 标本登记库持续复制到一个**独立的** SQLite 副本，供分布研究
查询；鉴定员仍直接在源库登记、更正、撤回。提供 Flask 网页启动/停止同步、查看
进度与错误、按标本号查询副本，并附带可持续修改的示例源库。

此外提供**馆藏计数**：博物馆研究员按采集地（locality 原文精确筛选）分页查看各
分类（taxon）的标本数。计数持久化在副本中，查询只走索引、绝不临时扫描标本表；
支持后台首次构建与重建，构建期间同步、标本查询、源更正全程可用。

## 馆藏计数：需求与实现对照

| 需求 | 实现方式 |
| --- | --- |
| 按 locality 精确筛选，分页列 taxon 与标本数 | `GET /api/counts?locality=...&page=...`，两字段按原文本分组（大小写/空格不同即不同组），覆盖索引 `idx_taxon_counts_loc_taxon` 分页 |
| 零计数组不显示 | `taxon_counts.cnt` 带 `CHECK (cnt > 0)`，计数降为 0 的组在同事务内直接删除 |
| 计数持久化、查询不临时扫描标本表 | 计数落在副本表 `taxon_counts`；页面查询的执行计划只命中该表索引，不接触 `specimens` |
| 可后台首次构建或重建 | `CountBuildManager` 守护线程：副本就绪后自动首次构建；`POST /api/counts/rebuild` 手动重建 |
| 扫描期间同步/标本查询/源更正继续可用，不持副本写事务等全量扫描 | 扫描按固定页大小分页、每页独立短事务（扫描页为只读）；SQLite 单一写者，构建器不与同步器争锁，增量由同步器在其应用事务内顺带汇入 |
| 新计数发布须纳入构建期间的增删改 | 构建登记水位 `start_seq`；同步器经 `apply_staging_delta` 把后续变更写入构建影像；收尾再从源端 CDC 对 `(start_seq, w]` 做幂等权威重放，确定覆盖一切时序缺口；发布水位写入 `count_meta.published_seq` |
| 计数随副本记录、审计、进度同事务更新；换组更正扣旧组加新组 | `apply_count_delta` 在同步器 `_apply_batch` 的同一事务内调用，与标本行、`_applied_changes`、`sync_meta` 一起 COMMIT/ROLLBACK；更新先读旧组，扣旧加新；序号 ≤ `published_seq` 的变更跳过，杜绝发布窗口重复 |
| 重建期间旧计数继续更新并供查询，完整新计数才可替换 | 重建只写 `build_id` 隔离的暂存表，`taxon_counts` 始终是上一版完整计数；只有追平水位后才在单事务内 DELETE+INSERT 交换并翻转 `published_build_id`/`published_seq` |
| 失败或重启保留旧计数，未完成构建标失败，首次完成前不返回统计 | 失败/重启把停在 RUNNING 的构建标 FAILED 并清理暂存，旧发布计数不动；`published_build_id IS NULL` 时计数接口返回 503 |
| 构建全程有界内存 | 全量影像落在磁盘暂存表，扫描/CDC 重放/聚合都按固定页大小分批处理，内存以页大小为界 |
| 页面显示进度及错误 | 计数卡片展示阶段/子阶段、扫描行数、日志追平水位、进度条与 `last_error`，定时刷新 |

### 计数构建为什么正确（单一写者 + CDC 权威重放 + 发布水位去重）

SQLite 同一时刻只允许一个写事务，因此让**同步器始终是副本的唯一写者**，构建器
不自己长期占用写锁：

1. 构建登记当前同步水位 `start_seq`，分页只读扫描 `specimens` 写入磁盘影像
   `count_build_rows`（每页短事务，有界内存）；扫描期间同步器在它本来的应用
   事务内通过 `apply_staging_delta` 顺带把每条变更 upsert/delete 进同一影像；
2. 等同步器追平某个采样水位 w 后，从源端变更日志把区间 `(start_seq, w]`
   **幂等重放**进影像（I/U upsert、D 删除）。日志区间是确定的，覆盖移动游标
   扫描竞争、连续构建注册窗口等一切时序缺口，使影像严格等于水位 w 的标本全集；
3. 分页聚合影像为 `count_build_counts`；
4. 在只做交换的短事务内发布：若获取写锁前同步器已越过 w，则在同一事务内把
   CDC `(w, sync_seq]` 补进影像并重算聚合，然后整体替换 `taxon_counts`，把
   `published_build_id` 与 `published_seq=sync_seq` 一起翻转。同步器之后对
   序号 ≤ `published_seq` 的批次会跳过计数维护（已包含在全量影像中），序号
   更大的批次才增量维护，因此发布窗口既不遗漏也不重复。


## 需求与实现对照

| 需求 | 实现方式 |
| --- | --- |
| 源库可并发提交增删改，首次复制期间也不停写 | 同步器只用只读短事务打开源库；WAL 允许读写并发，源写入不等待任何复制/同步完成 |
| 不得漏更 | 源库 `_change_log` 触发器记录全部行变更；首次复制 = 分页快照 + 重放快照窗口内全部日志；增量按序号续读 |
| 不得恢复已删标本 | 删除事件 `D` 按序号重放为 `DELETE`；删除后不会再被任何快照/重放路径插回 |
| 不得以旧鉴定覆盖新值 | 日志按 `seq` 升序应用，`INSERT OR REPLACE` 永远以更大序号携带的最新值为准 |
| 一次源事务的改动一起可见，回滚不传播 | 触发器与 DML 同事务：COMMIT 则行与日志原子可见，ROLLBACK 则一起消失，同步器永远看不到回滚内容 |
| 就绪前不供查询 | 副本 `phase != RUNNING` 时查询接口返回 HTTP 503 |
| 增量同步不反复扫描全表 | 只执行 `WHERE seq > ? AND seq <= ?` 的主键索引区间查询（`EXPL QUERY PLAN` 测试断言为 SEARCH 而非 SCAN） |
| 副本数据与进度共同提交 | 每批数据变更、审计流水、`sync_meta` 进度在副本的**同一个事务**中 COMMIT |
| 初次复制中断可重做 | 未完成时 `last_applied_seq=0`，重启清空副本重做 bootstrap（幂等） |
| 就绪后重启按持久进度接续 | 只要 `last_applied_seq > 0`，无论上次 RUNNING/STOPPED/ERROR，都从该序号续跑，不重做快照 |
| 源为本地 SQLite WAL | 源库 `PRAGMA journal_mode=WAL`；副本为独立 SQLite 文件，独立挂载卷 |
| 允许增加辅助表及触发器 | 源库增加 `_change_log` 表与 3 个触发器；副本增加 `sync_meta`、`_applied_changes` |

## 架构

```
鉴定员/演示写入器 ──写──> 源库 specimens.db (SQLite WAL)
                            │ specimens 表
                            │ + _change_log (AFTER I/U/D 触发器, AUTOINCREMENT seq)
                            │
              只读短事务 SELECT（不阻塞写入）
                            ▼
                    SyncEngine 后台线程
              ┌─────────────────────────────┐
              │ bootstrap: 主键分页快照       │
              │   → 重放日志 [1, w1] → RUNNING│
              │ incremental: seq > 已应用序号 │
              └─────────────────────────────┘
                            │ 每批一个事务：数据 + 进度一起 COMMIT
                            ▼
                    副本 specimens_replica.db (独立 SQLite)
              specimens / sync_meta(持久进度) / _applied_changes(审计)
              taxon_counts(已发布计数) / count_meta(当前发布构建)
              count_builds(构建状态/进度/错误)
              count_build_rows / count_build_counts(构建暂存影像与聚合)
                            ▲
              Flask 网页（标本仅 RUNNING 可查；计数首次发布前返回 503）
```

### 首次复制为什么正确（并发窗口）

1. 记录源端当前最大序号 `w0`；
2. 按主键分页把 `specimens` 快照写入副本（每页独立短事务，无长读锁）；
3. 快照结束再读一次最大序号 `w1`；
4. 重放日志区间 `[1, w1]`（从 1 开始对快照行是幂等覆盖）。

快照期间发生的任何并发提交，其日志序号都落在 `(w0, w1]`，必然被第 4 步覆盖：
插入被补进、删除被删掉（不复活）、更正以新值覆盖旧值。

## 目录结构

```
app/
  source_db.py     源库：WAL、specimens、CDC 表与触发器、演示写入 API
  replica_db.py    副本：表结构（含计数表/暂存表）、持久进度、就绪判定、查询
  counts.py        馆藏计数：事务内增量维护 + 后台首次构建/重建管理器
  sync.py          同步引擎：bootstrap + 增量循环，后台线程生命周期
  web.py           Flask 应用与 REST API
  templates/index.html  控制台页面（同步控制、标本查询、馆藏计数）
scripts/
  seed_source.py   播种示例源库（容器入口调用）
  demo_mutator.py  持续演示写入器（周期增/改/撤）
tests/             46 个 pytest 测试（CDC 正确性、并发、崩溃、续跑、计数构建/重建、Web E2E）
Dockerfile, docker-compose.yml, entrypoint.sh, wsgi.py
```

## 用 Docker Compose 启动（推荐）

```bash
# 构建并启动 Web + 后台同步（源库自动播种 8 条示例标本）
docker compose up -d --build

# 浏览器打开
#   http://localhost:8000/
```

打开网页后可以：

* 查看同步阶段（INIT/BOOTSTRAP/RUNNING/STOPPED/ERROR）、已应用序号、源端水位、错误；
* 启动/停止同步；按标本号查询副本（未就绪时返回 503）；
* 在页面上直接对**源库**新增 / 更正 / 撤回，观察副本自动追上；
* 在“馆藏计数”卡片查看构建进度与错误、选择采集地分页浏览各分类标本数，
  或点击“后台（重新）构建计数”。副本就绪后系统也会自动完成首次构建。

附带一个持续写入器，模拟鉴定员不停操作（可选 profile，不影响主服务）：

```bash
docker compose --profile mutator up -d
docker compose logs -f mutator
```

数据持久化在两个独立命名卷 `source_data`、`replica_data`。删除容器再
`docker compose up -d` 会自动按副本里的持久进度接续；清空 `replica_data` 卷
则会重新做首次复制。

## 本地（无 Docker）运行

```bash
pip install -r requirements.txt
export SOURCE_DB=./data/source/specimens.db
export REPLICA_DB=./data/replica/specimens_replica.db
python -m scripts.seed_source          # 播种示例源库（可选，Web 启动也会自动播种）
python -m flask --app wsgi:app run --port 8000
# 或：gunicorn --bind 0.0.0.0:8000 --workers 1 --threads 8 wsgi:app
```

## REST API

| 方法 路径 | 说明 |
| --- | --- |
| `POST /api/sync/start` | 启动后台同步（已在运行则返回 `started:false`） |
| `POST /api/sync/stop` | 请求停止（当前批次完成后退出，phase=STOPPED） |
| `GET  /api/status` | 阶段、`last_applied_seq`、源端水位、已拷贝行数、错误、线程状态 |
| `GET  /api/specimen/<id>` | 按标本号查副本；未就绪 503，不存在/已撤回 404 |
| `GET  /api/counts?locality=...&page=n` | 按 locality 原文精确筛选，分页返回 taxon 与标本数；首次发布前 503，缺 locality 400 |
| `GET  /api/counts/localities` | 已发布计数中的采集地及各地分组数/标本合计；首次发布前 503 |
| `GET  /api/counts/status` | 计数是否可用、当前发布构建、最新构建阶段/进度/错误、是否构建中 |
| `POST /api/counts/rebuild` | 后台（重新）构建计数；副本未就绪 503，已有构建在跑 409 |
| `POST /api/demo/insert` | 对源库新增 `{id,taxon,locality}` |
| `POST /api/demo/update` | 对源库更正 `{id,taxon,locality}` |
| `POST /api/demo/delete` | 撤回源记录 `{id}` |
| `GET  /api/source/specimens` | 源库当前标本（演示辅助） |
| `GET  /healthz` | 健康检查 |

## 测试

```bash
pip install pytest
pytest tests/ -v
```

覆盖要点：触发器捕获全部 DML、回滚不传播、已提交序号单调、单事务多行原子可见、
首次复制期间并发增删改的最终一致性、删除不复活、旧值不覆盖新值、中断重做、
重启按持久进度接续且不重做快照、提交点崩溃后数据与进度均不半生效、增量走索引
不全表扫描、同步期间源写入不被阻塞、就绪前 503、Web 端到端增改撤链路；计数首次
构建与 GROUP BY 一致、原文本精确分组、零计数组不显示、查询走索引不扫标本表、
增删/换组更正同事务扣旧加新、提交点共同回滚、重建期间旧计数可用、失败保留旧计数、
重启善后与自动首次构建、高并发增删改下发布计数严格正确、有界内存短事务、
计数 Web API 的 503/400/409 与分页语义。

## 关键环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SOURCE_DB` | `/data/source/specimens.db` | 源库文件路径 |
| `REPLICA_DB` | `/data/replica/specimens_replica.db` | 副本文件路径 |
| `AUTO_START` | `1` | 容器/应用启动时是否自动开始同步 |
| `POLL_INTERVAL` | `1.0` | 无新变更时的轮询秒数（stop 可立即唤醒） |
| `SNAPSHOT_PAGE_SIZE` | `500` | 首次复制快照每页行数 |
| `CHANGE_BATCH_SIZE` | `500` | 增量每批应用的日志条数 |
| `COUNT_SCAN_PAGE_SIZE` | `500` | 计数构建扫描/CDC 重放每页行数（有界内存） |
| `COUNT_AGG_PAGE_SIZE` | `500` | 计数构建聚合每页行数 |
| `BUILD_WAIT_INTERVAL` | `0.2` | 发布前等待同步器追平采样水位的轮询秒数 |
| `SUPERVISOR_INTERVAL` | `0.5` | 计数监督线程（重启善后/自动首次构建）轮询秒数 |
| `COUNTS_PAGE_SIZE` | `20` | 馆藏计数页面每页展示的 taxon 组数 |
| `MUTATOR_INTERVAL` | `3` | 演示写入器操作间隔秒数 |
