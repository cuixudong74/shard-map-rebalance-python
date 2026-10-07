# shard-map-rebalance-python

一个本地可复现的分片映射管理与在线再平衡服务（FastAPI）。不依赖任何外部
服务或真实凭据：数据面用进程内 `DataPlane` 模拟，状态持久化在本地目录，
开箱即可运行与验证。

## 运行

```bash
uv sync
uv run python main.py          # 监听 127.0.0.1:8000
uv run pytest                  # 全部自动化测试
```

## 映射版本语义

- 键空间是固定的 2^32 哈希环，分片为连续半开区间 `[start, end)`，全体
  分片无缝无重叠地铺满整个环（每次提交都会校验该不变量）。
- 每次映射变更（再平衡单步、分裂、合并）都会提交一个**单调递增、不可变**
  的完整版本。读取方在任意时刻只能拿到某个已提交的完整版本，写读并发
  时不会出现新旧归属混合。
- 读取方通过 `GET /map` 拿到 `version`，之后用
  `GET /map/refresh?version=N` 判断自己是否需要刷新。
- 长时读取方可通过 `acquire/release`（HTTP 层在请求内自动配对）钉住版本：
  被钉住的版本不会被回收；历史版本总数受 `max_versions` 约束，超出后
  最旧且未被钉住的版本被回收，内存占用有界。

## 再平衡策略

- **确定性**：规划器所有遍历都基于排序后的分片 ID 与节点 ID，同一输入
  同一目标必然得到同一方案，与字典/集合遍历顺序无关。
- **最少迁移**：目标节点 i（排序后）的容量为 `floor(N/K)` 或
  `floor(N/K)+1`；已在幸存节点上且未超容量的分片保持不动，迁移数 =
  各节点超出容量部分 + 孤儿分片（被移除节点上的分片），即理论最小值。
- **在线执行**：每个分片的迁移是独立的状态机
  （PREPARE → COPYING → SWAPPING → DONE / ABORTED），由并发信号量限流，
  单个慢节点不会阻塞其他分片。数据**全部**复制到目标后才原子切换权威
  归属（提交新版本），随后清理源端；因此读取方永远不会被路由到尚未
  完成迁移的副本。取消或超时只丢弃目标端的部分数据，映射停留在上一个
  自洽版本，不存在中间态。
- **分裂/合并**：分裂在中点切分，两个子分片继承父分片归属，不发生数据
  搬迁；合并要求两个分片相邻且当前归属相同（不同则先再平衡），保证
  任一时刻一个分片只有一个权威归属，归属不丢失不重复。
- **恢复**：每个迁移步骤的提交都写入本地 journal（`journal.jsonl`）并
  原子落盘 `state.json`。进程重启时扫描 journal 识别未完成的再平衡，
  按 `recovery_policy` 处理：`abort`（默认）安全收尾——已提交的分片保留，
  未提交的维持原归属；`resume` 自动重放剩余迁移步骤。

## 权限与配额

- 每个调用方（HTTP 头 `X-Caller`）可配置 `CallerPolicy`：
  `allowed_prefixes` 限制可操作的分片 ID 前缀，`max_concurrent_ops`
  限制并发操作数。
- 越权与超限在任何映射变更**之前**被拒绝，错误码可区分：
  `permission_denied`（403）、`quota_exceeded`（429）、
  `not_found`（404）、`conflict`（409）、`invalid`（400）。
- 全部操作（含拒绝）写入有界审计日志，`GET /audit` 可查。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/map?version=N` | 读取指定/最新映射版本 |
| GET | `/map/refresh?version=N` | 判断版本是否过期 |
| POST | `/rebalance` | body: `{"target_nodes": [...]}`，返回 `job_id` 与迁移方案 |
| GET | `/rebalance/{job_id}` | 查询进度（每分片阶段与进度） |
| POST | `/rebalance/{job_id}/cancel` | 取消，映射回到自洽状态 |
| POST | `/split` | body: `{"shard_id", "new_id_a?", "new_id_b?"}` |
| POST | `/merge` | body: `{"shard_id_a", "shard_id_b", "new_id?"}` |
| GET | `/audit` | 操作审计记录 |

## 配置项

`ShardMapService` / `create_app` 参数：

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `state_dir` | `.shardmap-state` | state.json 与 journal.jsonl 所在目录 |
| `nodes` | `["node-1","node-2"]` | 初始节点集合 |
| `num_shards` | 16 | 初始分片数 |
| `max_versions` | 16 | 保留的历史映射版本上限 |
| `max_concurrent_migrations` | 4 | 迁移并发上限 |
| `per_shard_timeout` | 30.0 | 单分片迁移超时（秒），超时自动回滚该分片 |
| `recovery_policy` | `abort` | 重启恢复策略：`abort` / `resume`，也可用环境变量 `SHARDMAP_RECOVERY_POLICY` |
| `policies` | `None` | 调用方权限/配额策略表 |
| `seed_keys_per_shard` | 64 | 本地模拟数据每分片采样的键数 |

## 代码结构

```
shardmap/
  models.py      类型与错误码（分片、版本、迁移任务、方案）
  store.py       版本化映射存储：原子提交、读取钉住、有界历史回收
  planner.py     确定性最少迁移再平衡规划器
  splitmerge.py  分裂/合并（纯函数，产出新分片集与归属）
  executor.py    在线迁移执行器 + 进程内数据面
  auth.py        调用方权限、并发配额、审计日志
  service.py     编排层：journal 持久化与崩溃恢复
  app.py         FastAPI HTTP 接口
tests/           并发读写、节点增减、分裂合并、中断恢复、越权配额等场景
```
