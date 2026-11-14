# 夜间门店日结 · 取数负载回放 + 销售补正（FastAPI + psycopg + PostgreSQL）

重现夜间门店日结取数高峰，判断哪些取数请求需要错峰。上传由取数日志整理的
CSV（最多 50 行，含相对到达毫秒、查询模板、门店号），系统严格按记录的间隔对
PostgreSQL 发起**只读**查询，用一个上限为 **2 条连接**的连接池制造排队，
并对每个请求执行 **3 秒硬性截止（含排队时间）**。

数据工程师发现已入库销售金额有误时，可在网页上提交**销售补正**：上传最多
20 行含 `sale_id、revision、amount` 的 CSV，系统按修订版本规则整批应用或
整批拒绝，补正完成后即可重新回放查询昨日汇总。

## 快速开始

```bash
docker compose up --build
# 首次启动会初始化约 168 万行虚构销售数据，约需 30-90 秒
```

浏览器打开 <http://localhost:8000>：

**回放**

1. 直接“下载样例 CSV”，或自备 CSV；
2. 选择文件后点“开始回放”；
3. 表格逐条刷新排队时长、执行时长、总耗时与状态（成功/超时/错误/已取消）；
4. 运行中可随时点“取消整轮”；
5. 整轮结束（完成或已取消）后才能启动下一轮。

**销售补正**

1. 点“下载补正样例 CSV”（`samples/correction.csv`，sale_id 200–219，
   均为**昨日**销售，提交后门店 1–20 的昨日单日汇总即发生变化）；
2. 选择补正 CSV 后点“提交补正”；
3. 页面展示处理结果：成功时逐行列出“已更新/幂等跳过”及原值；
   整批拒绝时逐行列出冲突行、冲突原因与库中当前修订/金额；
4. 补正与回放互斥：回放进行中提交补正（或补正进行中启动回放）会直接
   提示冲突，不进入排队。

健康检查：`GET http://localhost:8000/api/health`

## 回放 CSV 格式

表头必须包含三列（顺序不限，允许 `arrival`/`query_template`/`store_id` 等别名）：

```csv
arrival_ms,template,store
0,q2_settlement_audit,1
0,q2_settlement_audit,2
100,q1_daily_summary,4
```

- `arrival_ms`：相对本轮起点的毫秒数，非负整数；
- `template`：`q1_daily_summary`（快，走索引）或 `q2_settlement_audit`（慢，
  全表窗口重算 + `pg_sleep(1.5)`，约 2–3 秒）；
- `store`：门店号；
- 最多 **50** 行数据，超出或字段非法会返回 400 并说明行号。

## 补正 CSV 格式

表头必须包含三列（顺序不限，允许 `sale`/`rev` 别名）：

```csv
sale_id,revision,amount
200,1,199.90
201,1,88.00
```

- `sale_id`：正整数，必须对应**已有**销售行（补正只修改已有行）；
- `revision`：正整数修订号；未补正过的行一律按修订 **0** 处理；
- `amount`：非负、最多两位小数（如 `88`、`88.5`、`88.55` 合法，
  `88.555`、`-1`、`1e3` 非法），且不超过 NUMERIC(12,2) 上限；
- **金额与修订必须一起提交**，任一字段为空整行即非法；
- 最多 **20** 行数据，同一批次内 `sale_id` 不得重复。

## 补正版本语义（与需求逐条对应）

| 需求 | 实现位置 / 做法 |
| --- | --- |
| 只修改已有销售行 | `UPDATE sales ... WHERE sale_id = %s`；行不存在 → 冲突 `not_found` |
| 未补正的行按修订 0 处理 | `sales.revision INTEGER NOT NULL DEFAULT 0`（见 `db/init/01_schema_data.sql`） |
| 更高修订才能覆盖 | `new_revision > stored_revision` 才执行 UPDATE |
| 同版同金额重试返回成功 | 同修订且金额相等 → 幂等跳过，批次仍算成功（安全重试） |
| 旧版 / 同版异值 / 找不到行 → 整批拒绝 | 逐行评估收集全部冲突行，发现任一冲突即回滚整个事务，HTTP 409 返回所有冲突行 |
| 金额与修订须一起提交 | CSV 校验阶段强制两列同现，缺一直接 400 |
| 并发上传不得让新版回退 | 单事务内 `SELECT ... FOR UPDATE` 行锁 + 仅允许更高修订覆盖；应用层互斥门同时只允许一批补正 |
| 不留半批数据 | 整批一个事务，冲突即 ROLLBACK，全部更新原子提交或全部撤销 |
| 补正与回放互斥 | `RunManager` 内的全站互斥门：回放运行中拒补正、补正进行中拒回放，冲突返回 409 并直接提示 |
| 复用现有最多两条连接 | 补正与回放共用同一个 `ConnectionPool(min=max=2)`，不新建任何连接池 |
| 回放仍执行只读模板 | 池连接会话级 `default_transaction_read_only = on` 保持不变；补正仅在自身事务内 `SET TRANSACTION READ WRITE` 临时放开，事务结束即失效 |

## 回放引擎关键语义

| 需求 | 实现位置 / 做法 |
| --- | --- |
| 按记录间隔真实查询 | `app/engine.py` 调度线程按 `arrival_ms` 用单调时钟派发 |
| 单进程、最多两条连接、可复用 | `uvicorn --workers 1`；`ConnectionPool(min=max=2)` |
| 全站一次只跑一轮 | `RunManager` 持有唯一 `Run`，重复启动返回 409 |
| 每请求从计划到达起限 3 秒（含排队） | `deadline = planned_at + 3s`，排队与执行共用同一截止 |
| 排队过期不得执行 | 获取连接后二次校验截止/取消，过期直接归还并标记，从不执行 SQL |
| 已执行则主动停止 | 看门狗到点经独立连接执行 `pg_cancel_backend(<pid>)` 停止在途 SQL |
| 取消整轮后不再启动请求 | 取消事件置位，调度器与排队 worker 均停止派发 |
| 报错/超时/取消后先停 SQL、清事务再还连接 | worker `finally` 等待看门狗结束后 `rollback()`，再 `putconn()` |
| 迟到取消不误伤下一请求 | 取消按连接 PID 精确作用于当前在途请求；连接复用时重新初始化 |
| 全部收尾后才报完成/已取消 | finalizer 等待调度器 + 线程池全部任务结束才置终态 |

### 状态含义

- `success`：3 秒内返回；
- `timeout`：排队中过期（从未执行）或执行中被看门狗主动停止（`sqlstate=57014`）；
- `error`：SQL 本身报错，连接经 rollback 后回收复用；
- `canceled`：整轮取消时仍在排队或在途被停止。

## 本地测试（无需 Docker / 第三方包）

引擎并发语义用假连接池覆盖（排队超时不执行、运行超时主动停止、取消、连接
复用不串扰、错误恢复、单轮互斥）：

```bash
python3 -I tests/test_engine_fake.py
```

补正语义用内存销售表覆盖（新版覆盖、幂等重试、旧版/同版异值/行不存在整批
回滚、冲突行汇总、与回放互斥、并发补正互斥、CSV 校验）：

```bash
python3 -I tests/test_correction_fake.py
```

## 目录结构

```
.
├── docker-compose.yml
├── .dockerignore               # 构建上下文为仓库根（样例 CSV 需打进镜像）
├── app/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py            # FastAPI 路由（回放 + 补正）
│   ├── engine.py          # 调度/排队/3秒截止/取消/连接回收/补正互斥门
│   ├── correction.py      # 补正批次：版本规则、单事务、整批拒绝
│   ├── db.py              # 两条连接的只读连接池 + 模板加载
│   ├── csvutil.py         # 回放/补正 CSV 解析与校验（≤50 / ≤20 行）
│   └── static/index.html  # 单页前端（回放 + 补正入口与结果展示）
├── db/init/01_schema_data.sql   # 表（含 revision 列）、168 万行虚构数据、两个查询模板
├── samples/replay.csv           # 回放样例
├── samples/correction.csv       # 补正样例（sale_id 200-219，影响昨日汇总）
└── tests/
    ├── test_engine_fake.py      # 引擎并发测试（标准库即可运行）
    └── test_correction_fake.py  # 补正语义测试（标准库即可运行）
```

## 重置数据

```bash
docker compose down -v   # 删除数据库卷（补正记录一并清空）
docker compose up --build
```
