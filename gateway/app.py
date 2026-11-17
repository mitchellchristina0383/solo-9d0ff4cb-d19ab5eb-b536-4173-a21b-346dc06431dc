"""HTTP 入口网关（FastAPI + httpx）。

职责：
  * 仅转发 GET 请求到上游，按轮询（round-robin）选择「健康、未排空且有空位」的节点；
  * 有界排队接流：每节点并发上限、全局等待人数上限、最长等待时间均可由环境变量配置；
  * 排队严格按到达顺序（FIFO），后来的请求不得越过宿头占空位；
  * 等待期间不占用节点在途名额；队满或等待超时返回 429，全部节点不可接流返回 503；
  * 排队中若节点被排空或探测为故障，等待者重新选择可接流节点；无从选择时立即 503，
    既不进入排空节点，也不拖延排空回执；
  * 请求取消（客户端断开）立即释放队列名额；已接流的请求不受影响，照常结束；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志；
  * 提供排空 / 恢复 / 状态管理 API；排空 API 阻塞到在途请求全部结束才报告 drained=true；
  * 超时 / 连接错误 / 5xx 不虚报成功，并同步刷新健康状态，在途计数靠 try/finally 保证准确。
"""
from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional, Union

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

# ----------------------------- 配置 -----------------------------


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 需要整数，收到 {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"环境变量 {name} 不能小于 {minimum}，收到 {value}")
    return value


def _env_float(name: str, default: float, minimum: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 需要数字，收到 {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"环境变量 {name} 不能小于 {minimum}，收到 {value}")
    return value


UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))

# 有界排队接流配置
NODE_MAX_INFLIGHT = _env_int("NODE_MAX_INFLIGHT", 8, minimum=1)
QUEUE_MAX_WAITING = _env_int("QUEUE_MAX_WAITING", 32, minimum=0)
QUEUE_MAX_WAIT = _env_float("QUEUE_MAX_WAIT", 5.0, minimum=0.0)

# 排队时轮询客户端断开的间隔；也决定取消 / 超时的检测粒度
_DISCONNECT_POLL_INTERVAL = 0.1

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not _:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), url.strip().rstrip("/")))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


# ----------------------------- 节点状态 -----------------------------

@dataclass
class NodeState:
    name: str
    base_url: str
    # 该节点的并发接流上限；等待中的请求不占用名额
    max_inflight: int = NODE_MAX_INFLIGHT
    # 健康标志：只由探测任务（以及请求失败时的降级）写入
    healthy: bool = False
    # 排空标志：只由人工 drain/resume 写入，探测永不触碰
    draining: bool = False
    inflight: int = 0
    rr_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    last_change_at: float = field(default_factory=time.time)

    def can_serve(self) -> bool:
        """健康且未排空：允许接流（是否有空位另算）。"""
        return self.healthy and not self.draining

    def has_capacity(self) -> bool:
        return self.can_serve() and self.inflight < self.max_inflight

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "draining": self.draining,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "max_inflight": self.max_inflight,
            "rr_hits": self.rr_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
        }


# 等待者被唤醒时，future 的结果只可能是：
#   * NodeState  —— 已成功接流（inflight 已加一，调用方必须负责 release）
#   * _NO_ELIGIBLE —— 等待期间所有节点都变得不可接流，调用方应返回 503
_NO_ELIGIBLE = object()


@dataclass(eq=False)
class Waiter:
    """排队中的请求。future 由派发器在锁内填充。"""

    future: "asyncio.Future[object]"


class Gateway:
    def __init__(self, nodes: list[NodeState]) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        # 一把条件变量保护所有节点状态、等待队列与轮询游标；
        # 请求结束 / 排空等待 / 探测结论 / 入队出队都在其上通知。
        self.cond = asyncio.Condition()
        self._rr = 0
        # 全局等待队列，队头优先，严格 FIFO
        self._waiters: deque[Waiter] = deque()
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    def waiting(self) -> int:
        """当前排队等待（尚未接流）的请求数；调用方需持有 cond。

        已被取消但尚未出队的残留项不计入，状态 API 与队满判断因此不会被虚高。
        """
        return sum(1 for w in self._waiters if not w.future.done())

    # ---- 以下 _locked 方法均要求在持有 self.cond 时调用 ----

    def _has_eligible_locked(self) -> bool:
        return any(n.can_serve() for n in self.nodes)

    def _pick_locked(self) -> Optional[NodeState]:
        """轮询选取健康、未排空且有空位的节点，选中即登记在途。"""
        n = len(self.nodes)
        for offset in range(n):
            idx = (self._rr + offset) % n
            node = self.nodes[idx]
            if node.has_capacity():
                self._rr = (idx + 1) % n
                node.inflight += 1
                node.rr_hits += 1
                return node
        return None

    def _fail_waiters_locked(self) -> None:
        """所有节点都不可接流：唤醒全部等待者返回 503。"""
        while self._waiters:
            w = self._waiters.popleft()
            if not w.future.done():
                w.future.set_result(_NO_ELIGIBLE)

    def _admit_locked(self) -> None:
        """按 FIFO 从队头派发空位；队头拿不到空位时后到者绝不越过。"""
        while self._waiters:
            w = self._waiters[0]
            if w.future.done():
                # 已被取消的残留项，直接丢弃
                self._waiters.popleft()
                continue
            if not self._has_eligible_locked():
                self._fail_waiters_locked()
                return
            node = self._pick_locked()
            if node is None:
                return  # 有可接流节点但全部满载，队头继续等待
            self._waiters.popleft()
            w.future.set_result(node)

    def _discard_locked(self, w: Waiter) -> None:
        try:
            self._waiters.remove(w)
        except ValueError:
            pass

    async def _abandon(self, w: Waiter) -> Optional[str]:
        """等待者放弃（超时 / 断开 / 取消）：归还名额并让下一位继续。

        若恰好在我们放弃的同时被派发，则立刻把已登记的在途名额还回去。
        返回 "no_eligible_upstream" 表示其实是被派发器以「无节点」唤醒。
        """
        async with self.cond:
            reason: Optional[str] = None
            if w.future.done() and not w.future.cancelled():
                result = w.future.result()
                if isinstance(result, NodeState):
                    result.inflight -= 1
                elif result is _NO_ELIGIBLE:
                    reason = "no_eligible_upstream"
            else:
                self._discard_locked(w)
            self._admit_locked()
            self.cond.notify_all()
            return reason

    async def acquire(self, request: Request) -> tuple[Optional[NodeState], Optional[str]]:
        """有界排队接流。

        返回值 (node, error)：
          * (NodeState, None)           —— 已接流，调用方必须在 finally 中 release_node；
          * (None, "no_eligible_upstream") —— 无任何健康未排空节点，应返回 503；
          * (None, "queue_full")        —— 全局排队人数已满，应返回 429；
          * (None, "queue_timeout")     —— 等候超过 QUEUE_MAX_WAIT，应返回 429；
          * (None, "client_disconnected") —— 客户端已断开，调用方无需再回包。
        """
        loop = asyncio.get_running_loop()
        w = Waiter(loop.create_future())

        async with self.cond:
            if not self._has_eligible_locked():
                return None, "no_eligible_upstream"
            self._waiters.append(w)
            self._admit_locked()
            # 派发后仍在排队说明当前无空位；超出全局等待上限则拒绝自己（后来的不挤掉先到的）
            if not w.future.done() and self.waiting() > QUEUE_MAX_WAITING:
                self._discard_locked(w)
                return None, "queue_full"

        deadline = loop.time() + QUEUE_MAX_WAIT
        admitted: Union[NodeState, object, None] = None
        reason: Optional[str] = None
        try:
            while True:
                # 客户端断开：尽快释放队列名额，避免占着位置却无人接收响应。
                # 放在检查派发结果之前，连「刚被派发就发现已断开」也不会漏掉。
                if await request.is_disconnected():
                    reason = "client_disconnected"
                    break
                if w.future.done():
                    if not w.future.cancelled():
                        admitted = w.future.result()
                    break
                remaining = deadline - loop.time()
                if remaining <= 0:
                    reason = "queue_timeout"
                    break
                await asyncio.wait(
                    {w.future},
                    timeout=min(_DISCONNECT_POLL_INTERVAL, remaining),
                )
        except asyncio.CancelledError:
            # 任务被取消（服务关闭等）：先把 waiter 标记为已结束，避免派发器继续给它名额；
            # 若名额已在取消前派发，_abandon 会立刻归还。shield 防止清理被二次取消打断。
            w.future.cancel()
            await asyncio.shield(self._abandon(w))
            raise

        if admitted is _NO_ELIGIBLE:
            return None, "no_eligible_upstream"
        if isinstance(admitted, NodeState):
            return admitted, None

        # 未被派发就退出等待：原子地出队 / 如需归还名额，并唤醒下一位。
        # shield 保证即使此刻被取消，清理也会在后台完成，队列与在途不会泄漏。
        abandoned_reason = await asyncio.shield(self._abandon(w))
        return None, abandoned_reason or reason or "queue_timeout"

    async def release_node(self, node: NodeState, ok: bool) -> None:
        async with self.cond:
            node.inflight -= 1
            if ok:
                node.success_count += 1
            else:
                node.failure_count += 1
            # 让出的空位立即派发给队头等待者；在途归零也可能满足 drain 等待者
            self._admit_locked()
            self.cond.notify_all()

    async def mark_unhealthy(self, node: NodeState, detail: str) -> None:
        """请求失败时的即时降级；只改健康标志，不动 draining。"""
        async with self.cond:
            node.healthy = False
            node.last_probe_ok = False
            node.last_probe_detail = f"请求失败降级: {detail}"
            node.last_probe_at = time.time()
            node.last_change_at = time.time()
            # 等待者须重新选择其余可接流节点，或立即得到 503
            self._admit_locked()
            self.cond.notify_all()

    async def probe_one(self, node: NodeState) -> None:
        """对单个节点做主动探测。成功只置 healthy=True，绝不解除 draining。"""
        assert self.client is not None
        try:
            resp = await self.client.get(
                f"{node.base_url}/", timeout=PROBE_TIMEOUT
            )
            ok = resp.status_code < 500
            detail = f"HTTP {resp.status_code}"
        except httpx.TimeoutException as exc:
            ok, detail = False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            ok, detail = False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

        async with self.cond:
            node.healthy = ok
            # 注意：这里刻意不修改 node.draining
            node.last_probe_at = time.time()
            node.last_probe_ok = ok
            node.last_probe_detail = detail
            node.last_change_at = time.time()
            # 探测恢复的节点可以接走排队请求；探测故障的节点则让等待者改选或 503
            self._admit_locked()
            self.cond.notify_all()

    async def probe_all(self) -> None:
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    async def drain(
        self, node: NodeState, timeout: Optional[float]
    ) -> tuple[bool, int]:
        """置排空标志并等待在途清零。返回 (是否排空完成, 剩余在途数)。

        只在途请求计入；排队未接流的等待者不占用该节点名额，因此不会拖延本接口。
        """
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            node.draining = True
            node.last_change_at = time.time()
            # 立即把队头等待者重新派发到其他可接流节点（绝不进入本节点）
            self._admit_locked()
            self.cond.notify_all()
            while node.inflight > 0:
                if deadline is None:
                    await self.cond.wait()
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False, node.inflight
                # asyncio.Condition.wait 不支持 timeout，用 wait_for 限时等待
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # 回到循环重新计算剩余时间 / 在途数
            return True, 0

    async def resume(self, node: NodeState) -> None:
        """恢复接流：先清排空并保守置为不健康，再立即探测，由探测决定能否接流。"""
        async with self.cond:
            node.draining = False
            # 恢复不绕过故障探测：探测结论出来前一律不接流，杜绝状态窗口
            node.healthy = False
            node.last_probe_ok = None
            node.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
        await self.probe_one(node)


# ----------------------------- 应用生命周期 -----------------------------

gateway = Gateway([NodeState(name, url) for name, url in _parse_upstreams(UPSTREAMS_RAW)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass


app = FastAPI(
    title="http-entry-gateway",
    version="1.1.0",
    description=(
        "仅转发 GET 的入口网关：轮询健康、未排空且有空位的上游；"
        "按每节点并发上限有界排队，队满 / 等超时返回 429，无可接流节点返回 503。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET",
        "limits": {
            "node_max_inflight": NODE_MAX_INFLIGHT,
            "queue_max_waiting": QUEUE_MAX_WAITING,
            "queue_max_wait": QUEUE_MAX_WAIT,
        },
        "admin": [
            "GET  /admin/status",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/probe",
        ],
    }


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(path: str, request: Request) -> Response:
    node, error = await gateway.acquire(request)
    if error is not None:
        if error == "queue_full":
            return JSONResponse(
                status_code=429,
                headers={"Retry-After": "1"},
                content={
                    "error": "queue_full",
                    "detail": f"排队人数已达上限 {QUEUE_MAX_WAITING}，请稍后重试",
                },
            )
        if error == "queue_timeout":
            return JSONResponse(
                status_code=429,
                headers={"Retry-After": "1"},
                content={
                    "error": "queue_timeout",
                    "detail": f"等待空闲节点超时（{QUEUE_MAX_WAIT:g}s），请稍后重试",
                },
            )
        if error == "client_disconnected":
            # 客户端已断开，回包不会被送达；返回 499 仅用于本地日志语义
            return JSONResponse(
                status_code=499,
                content={"error": "client_disconnected"},
            )
        return JSONResponse(
            status_code=503,
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可接流",
            },
        )

    assert node is not None
    target_url = f"{node.base_url}/{path}"
    ok = False
    try:
        try:
            upstream_resp = await gateway.client.request(
                "GET",
                target_url,
                params=request.query_params,
                timeout=REQUEST_TIMEOUT,
            )
        except httpx.TimeoutException as exc:
            await gateway.mark_unhealthy(node, f"请求超时 {type(exc).__name__}")
            return JSONResponse(
                status_code=504,
                content={"error": "upstream_timeout", "node": node.name},
            )
        except httpx.RequestError as exc:
            await gateway.mark_unhealthy(
                node, f"{type(exc).__name__}: {str(exc)[:100]}"
            )
            return JSONResponse(
                status_code=502,
                content={"error": "upstream_unreachable", "node": node.name},
            )

        # 5xx 视为上游故障：如实透传状态码并降级，绝不虚报成功
        if upstream_resp.status_code >= 500:
            await gateway.mark_unhealthy(
                node, f"上游返回 HTTP {upstream_resp.status_code}"
            )

        ok = upstream_resp.status_code < 500
        headers = {
            k: v
            for k, v in upstream_resp.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        headers["X-Gateway"] = "http-entry-gateway"
        headers["X-Upstream-Node"] = node.name
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=headers,
            media_type=upstream_resp.headers.get("content-type"),
        )
    finally:
        # 无论成功、失败还是取消，在途计数都必须准确归还
        await gateway.release_node(node, ok)


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        return {
            "probe_interval": PROBE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
            "node_max_inflight": NODE_MAX_INFLIGHT,
            "queue_max_waiting": QUEUE_MAX_WAITING,
            "queue_max_wait": QUEUE_MAX_WAIT,
            "waiting": gateway.waiting(),
            "nodes": [n.snapshot() for n in gateway.nodes],
            "eligible_nodes": [n.name for n in gateway.nodes if n.can_serve()],
        }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求，并阻塞等待在途请求全部结束。

    只有在途数归零才返回 drained=true；超时仍有在途请求时返回 202，
    drained=false 且节点继续保持排空状态。排队未接流的请求不计入在途，
    因此不会拖延本接口。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    drained, inflight = await gateway.drain(node, timeout)
    body = {
        "node": name,
        "draining": True,
        "drained": drained,
        "inflight": inflight,
        "detail": "在途请求已全部结束" if drained else "等待在途请求结束超时，节点仍处于排空中",
    }
    return JSONResponse(status_code=200 if drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空；是否可接流以健康探测结论为准",
    }


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（探测不会改变任何节点的排空标志）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {
            "waiting": gateway.waiting(),
            "nodes": [n.snapshot() for n in gateway.nodes],
        }
