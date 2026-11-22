# 流变工坊 · 实时CDC与流式数据同步平台

面向数据库CDC事件捕获、变更日志解析、流式数据转换、维表关联补全、水位线管理、乱序容忍、结果落湖归档与端到端延监控的实时CDC与流式同步平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

### 映射交接工作台

解析映射改版期间，同一源端补交旧批会带来重复接纳和进度不一致。工作台把
"转换批次交付数据"作为交接凭据，分别记录维表关联、去重窗口实际接纳的事件，
再由日志解析按连续前缀核定可提交位点。

- 页面：`http://127.0.0.1:5173/handoff-workbench`（侧栏「映射交接」）
- 账本：`GET /api/handoff/state`、`/workbench`、`/versions`、`/batches`
- 凭据：`GET /api/handoff/acceptances?side=&version=&position=`、`/queues`、`/consistency`
- 动作：`POST /api/handoff/actions/{arrive_new_batch|pause_old_ack|resend_both|receive_old_ack|recover|interrupt|commit|run_scenario|reset}`

规则：重叠位点沿用旧批归属与旧映射，每侧每条只接纳一次；新批独有段用新映射并
按订单键保序；提交位点只取从 101 起两侧都按所属版本确认过的连续前缀。模块服务
也暴露了对应入口：`EventTransService.deliver_batches_to_sides`、
`DimJoinService.acceptance_ledger`、`DedupStreamService.acceptance_ledger`、
`BinlogParseService.resolve_committable_position`。

回归测试：`cd backend && .venv/bin/python -m unittest discover -s tests`

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| CDC数据源 | `cdc_source` | 链路源端 | 源端编号、数据库类型、连接地址 |
| 日志解析 | `binlog_parse` | 解析规则 | 规则编号、源端、日志格式 |
| 事件转换 | `event_trans` | 转换管道 | 管道编号、源端、事件类型 |
| 维表关联 | `dim_join` | 关联配置 | 配置编号、事实表、维度表 |
| 水位管理 | `watermark_mgr` | 水位线 | 水位编号、数据流、当前位点 |
| 乱序处理 | `out_of_order` | 乱序策略 | 策略编号、数据流、时间字段 |
| 结果落盘 | `sink_writer` | 落盘管道 | 管道编号、数据流、目标载体 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标编号、数据流、源端位点 |
| DDL处理 | `ddl_handle` | DDL适配 | 适配编号、源端、DDL类型 |
| 断点恢复 | `checkpoint` | 检查点 | 检查点编号、数据流、保存位点 |
| Schema演进 | `schema_evo` | 演进记录 | 记录编号、数据流、变更列 |
| 流去重 | `dedup_stream` | 去重窗口 | 窗口编号、数据流、去重键 |
| 湖仓提交 | `iceberg_commit` | 提交快照 | 快照编号、目标表、数据文件 |
| 小文件合并 | `compaction` | 合并任务 | 任务编号、目标表、文件数量 |
| 快照过期 | `expire_snap` | 过期策略 | 策略编号、目标表、保留数量 |
| 流聚合 | `metric_aggr` | 聚合窗口 | 窗口编号、数据流、聚合键 |
| 增量回填 | `backfill_run` | 回填任务 | 任务编号、目标表、起始位点 |
| 告警规则 | `alert_rule` | 告警策略 | 策略编号、监控指标、告警条件 |
| 数据比对 | `data_diff` | 比对任务 | 任务编号、源端、目标端 |
| 编排调度 | `task_schedule` | 调度计划 | 计划编号、关联链路、Cron表达式 |
| 流审计 | `stream_audit` | 审计记录 | 记录编号、数据流、操作类型 |
| 健康报告 | `stream_health` | 健康评估 | 评估编号、数据流、健康分数 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
