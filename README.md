# 夜间门店日结 · 取数负载回放（FastAPI + psycopg + PostgreSQL）

重现夜间门店日结取数高峰，判断哪些取数请求需要错峰。上传由取数日志整理的
CSV（最多 50 行，含相对到达毫秒、查询模板、门店号），系统严格按记录的间隔对
PostgreSQL 发起**只读**查询，用一个上限为 **2 条连接**的连接池制造排队，
并对每个请求执行 **3 秒硬性截止（含排队时间）**。

## 快速开始

```bash
docker compose up --build
# 首次启动会初始化约 168 万行虚构销售数据，约需 30-90 秒
```

浏览器打开 <http://localhost:8000>：

1. 直接“下载样例 CSV”，或自备 CSV；
2. 选择文件后点“开始回放”；
3. 表格逐条刷新排队时长、执行时长、总耗时与状态（成功/超时/错误/已取消）；
4. 运行中可随时点“取消整轮”；
5. 整轮结束（完成或已取消）后才能启动下一轮。

健康检查：`GET http://localhost:8000/api/health`

## CSV 格式

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

## 关键语义（与需求逐条对应）

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

## 目录结构

```
.
├── docker-compose.yml
├── app/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py            # FastAPI 路由
│   ├── engine.py          # 调度/排队/3秒截止/取消/连接回收
│   ├── db.py              # 两条连接的只读连接池 + 模板加载
│   ├── csvutil.py         # CSV 解析与校验（≤50 行）
│   └── static/index.html  # 单页前端
├── db/init/01_schema_data.sql   # 表、168 万行虚构数据、两个查询模板
├── samples/replay.csv          # 可直接上传的样例
└── tests/test_engine_fake.py   # 引擎并发测试（标准库即可运行）
```

## 重置数据

```bash
docker compose down -v   # 删除数据库卷
docker compose up --build
```
