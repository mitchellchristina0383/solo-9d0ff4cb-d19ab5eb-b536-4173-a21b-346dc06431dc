# HTTP 入口网关（支持有界排队接流 / 优雅排空 / 健康探测 / 轮询转发）

滚动更新场景下的入口网关：运维人员先排空待停机实例，使新请求不再进入，
同时保证在途长请求完整取回原响应，全部结束后才报告排空完成。
服务突发变慢时，网关按「每节点并发上限」接流，超出部分进入有界全局队列按
到达顺序等待；队列满或等待超时立即返回 429，避免请求在实例内无界堆积。

技术栈：**FastAPI + httpx**，由 **Docker Compose** 启动 1 个网关 + 2 个可模拟
慢请求 / 故障的上游。

## 目录结构

```
.
├── docker-compose.yml      # 编排：gateway + upstream1 + upstream2
├── gateway/
│   ├── app.py              # 网关：仅转发 GET、轮询、探测、排空/恢复/状态
│   ├── requirements.txt
│   └── Dockerfile
├── upstream/
│   ├── app.py              # 模拟上游：slow / flaky / 故障模式控制
│   ├── requirements.txt
│   └── Dockerfile
└── tests/
    ├── self_test.py        # 本地端到端自测（15 组场景，52 项断言）
    ├── queue_test.py       # 有界排队端到端自测（7 组场景，48 项断言）
    └── queue_unit_test.py  # 排队并发语义进程内单测（7 组场景，42 项断言）
```

## 一键启动

```bash
docker compose up --build
# 网关:        http://localhost:18080
# 上游 node1:  http://localhost:18081
# 上游 node2:  http://localhost:18082
# 交互文档:    http://localhost:18080/docs
```

网关环境变量（见 `docker-compose.yml`）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `UPSTREAMS` | `node1=http://upstream1:8000,node2=http://upstream2:8000` | 上游列表 `name=url` |
| `PROBE_INTERVAL` | `2.0` | 周期探测间隔（秒） |
| `PROBE_TIMEOUT` | `1.5` | 探测超时（秒） |
| `REQUEST_TIMEOUT` | `15.0` | 转发请求超时（秒） |
| `DRAIN_DEFAULT_TIMEOUT` | `30.0` | 排空 API 默认等待上限（秒） |
| `NODE_MAX_INFLIGHT` | `8` | 每个节点允许的在途（已接流）请求上限 |
| `QUEUE_MAX_WAITING` | `32` | 全局排队（未接流）请求数上限，含正在等待的请求 |
| `QUEUE_MAX_WAIT` | `5.0` | 排队最长等待时间（秒） |

## API 一览（均可在 /docs 直接操作）

转发（仅 GET）：

```
GET /proxy/{path}      # 轮询转发到健康、未排空且有并发空位的节点，透传查询串与响应
                       # 节点满载 -> 进入有界全局队列按到达顺序等待空位
                       # 队满 -> 429 queue_full；等候超时 -> 429 queue_timeout
                       # 无可用节点 -> 503；上游超时 -> 504；连不上 -> 502
                       # 非 GET 方法 -> 405
```

管理：

```
GET  /admin/status                     # 全部节点健康/排空/在途/并发上限/计数/最近探测
                                       # 以及全局等待数 waiting、队列配置
POST /admin/nodes/{name}/drain?timeout=# 排空：立即停接新流，阻塞到在途清零（不等排队者）
                                       #   在途归零 -> 200 drained=true
                                       #   超时仍有在途 -> 202 drained=false
POST /admin/nodes/{name}/resume        # 恢复：清排空，立即探测，由探测决定能否接流
POST /admin/probe                      # 手动触发一轮探测
```

上游模拟控制（演练用，直连上游端口，不经过网关）：

```
GET  /control/state            # 查看模式与在途数
POST /control/mode?mode=       # healthy | slow | error | hang
GET  /slow?seconds=N           # 慢请求（验证在途不被截断）
GET  /flaky                    # 按模式返回 200 / 500 / 挂起
GET  /                         # 健康端点（error->503, hang->挂起）
```

## 典型演练

```bash
# 1. 发一个慢请求（10 秒），在另一终端立刻排空 node2
curl "http://localhost:18080/proxy/slow?seconds=10" &
curl -X POST "http://localhost:18080/admin/nodes/node2/drain?timeout=30"
# 现象：drain 请求阻塞，约 10 秒后才返回 drained=true；慢请求拿到完整 200

# 2. 让 node1 进入故障模式，观察探测自动摘除
curl -X POST "http://localhost:18081/control/mode?mode=error"
sleep 3
curl http://localhost:18080/admin/status      # node1 healthy=false

# 3. 故障未消除时恢复 node1：不会绕过探测，节点仍不可接流
curl -X POST http://localhost:18080/admin/nodes/node1/resume
# eligible=false；上游修好后经周期探测自动恢复接流

# 4. 突发慢请求：观察有界排队（默认每节点 8 并发、全局 32 人、最长等 5s）
for i in $(seq 40); do curl -s "http://localhost:18080/proxy/slow?seconds=3" -o /dev/null & done
sleep 1
curl -s http://localhost:18080/admin/status    # waiting>0, 各节点 inflight<=8
# 超出配额 / 等太久的请求直接得到 429（queue_full / queue_timeout）

# 5. 排队期间排空 node2：等待者立即改选 node1，排空回执不被排队者拖延
curl -X POST "http://localhost:18080/admin/nodes/node2/drain?timeout=30"
```

## 关键设计与并发正确性

- **两个独立标志位**：`healthy` 只由探测（含请求失败降级）写入；`draining` 只由
  人工 drain/resume 写入。因此**探测成功永不解除排空**，两者交错互不串改。
- **有界接流**：每节点 `NODE_MAX_INFLIGHT` 个并发名额，选中即在同一把锁内
  `inflight += 1`；名额用完的请求不再打向上游，而是进入**全局 FIFO 队列**。
  排队中的请求**不计入节点在途**，也不占用节点名额。
- **严格 FIFO**：派发器只从队头取人，队头拿不到空位时后到者绝不越过；
  队满（`QUEUE_MAX_WAITING`）时拒绝**新到**请求，先到者不被挤掉。
- **重新选择**：等待期间节点被排空 / 探测故障时，等待者在锁内重新挑选其余
  健康非排空节点；所有节点都不可接流则**立即 503**，不会进入排空节点，
  也不会拖延 drain 回执（drain 只统计已接流的 `inflight`）。
- **释放与取消**：请求结束、5xx、超时、客户端断开、任务取消都会归还名额并
  唤醒队头；取消与派发竞态下若已被派发，会立刻把名额还回，绝无泄漏。
- **恢复不绕过故障探测**：`resume` 先清排空并保守置 `healthy=false`，再做一次
  真实探测；探测通过前该节点不会被轮询选中。
- **在途计数可靠**：转发用 `try/finally` 归还计数，成功、5xx、超时、连接错误、
  客户端取消都会准确递减；排空等待在条件变量上，请求一结束即被唤醒。
- **排空语义**：置位后新请求立即绕过该节点；已在途请求不被取消，继续等待并
  透传原响应；只有 `inflight` 归零才返回 `drained=true`。
- **不虚报成功**：5xx 如实透传并即时降级；超时返回 504、连接错误返回 502，
  均标记失败并刷新健康状态，绝不返回虚假 200。
- **无节点可接流**：所有节点都不健康或排空中时，`GET /proxy/*` 返回 503；
  节点可接流但满载时最多等 `QUEUE_MAX_WAIT` 秒，超时返回 429（带 `Retry-After`）。

## 自测

```bash
# 无需 docker；需本地具备 fastapi/uvicorn/httpx（可 pip install 到独立目录）
python3 tests/self_test.py          # 原有转发/探测/排空回归：15 组 52 项
python3 tests/queue_test.py         # 有界排队端到端：7 组 48 项
python3 tests/queue_unit_test.py    # 排队并发语义进程内单测：7 组 42 项
```

`self_test.py` 覆盖原有 15 组场景、52 项断言，含两个关键交错：
**探测成功不解除排空**、**恢复不绕过故障探测**；以及在途慢请求排空、
多并发在途排空、503/504/502、超时不虚报、在途计数准确、405、/docs 等。

`queue_test.py` 覆盖每节点并发上限、全局排队、队满 429、FIFO 不抢先、
等候超时 429、排队中排空 / 探测故障交错、全部不可接流立即 503、
客户端取消释放名额，以及排空回执不被排队者拖延。

`queue_unit_test.py` 直接驱动 `Gateway`，确定性覆盖取消与派发竞态、
名额不泄漏、探测恢复后重新派发等 HTTP 黑盒难以稳定触发的路径。
