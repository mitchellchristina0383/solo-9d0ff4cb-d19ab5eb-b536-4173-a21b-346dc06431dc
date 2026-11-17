#!/usr/bin/env python3
"""网关排队接流的进程内确定性单测（不启 HTTP 服务，直接驱动 Gateway）。

覆盖 HTTP 黑盒难以稳定触发的竞态：
  1. 每节点并发上限 + FIFO 派发 + release 立即移交空位
  2. 等待者严格按入队顺序接流（取消中间者不影响其余顺序）
  3. 全局排队上限：后到者被拒，不挤掉先到者
  4. 等候超时返回 queue_timeout 且队列归零
  5. 排空节点：等待者改选其他节点；drain 只等已接流在途
  6. 全部不可接流：等待者立即得到 no_eligible_upstream（不拖延 drain）
  7. 客户端断开：释放名额；取消与派发竞态下不泄漏在途名额
  8. 探测故障：等待者不进入故障节点；恢复后继续派发
"""
from __future__ import annotations

import asyncio
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "gateway"))
os.environ.setdefault("UPSTREAMS", "node1=http://127.0.0.1:1,node2=http://127.0.0.1:2")
os.environ.setdefault("NODE_MAX_INFLIGHT", "1")
os.environ.setdefault("QUEUE_MAX_WAITING", "2")
os.environ.setdefault("QUEUE_MAX_WAIT", "5")

import app as gw_app  # noqa: E402

passed = 0
failed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


class FakeRequest:
    def __init__(self) -> None:
        self.disconnected = False

    async def is_disconnected(self) -> bool:
        return self.disconnected


class Env:
    """每个用例一套干净的 Gateway（模块级常量按用例覆写）。"""

    def __init__(self, max_inflight=1, max_waiting=2, max_wait=5.0) -> None:
        gw_app.NODE_MAX_INFLIGHT = max_inflight
        gw_app.QUEUE_MAX_WAITING = max_waiting
        gw_app.QUEUE_MAX_WAIT = max_wait
        nodes = [gw_app.NodeState("node1", "http://127.0.0.1:1"),
                 gw_app.NodeState("node2", "http://127.0.0.1:2")]
        self.gw = gw_app.Gateway(nodes)
        for n in nodes:
            n.healthy = True
        self.n1, self.n2 = nodes

    async def acquire(self, req=None):
        return await self.gw.acquire(req or FakeRequest())

    def inflight(self):
        return [n.inflight for n in self.gw.nodes]

    def waiting(self):
        return self.gw.waiting()


async def drain_finished(drain_task: asyncio.Task) -> None:
    """允许后台 drain 协程推进。"""
    for _ in range(20):
        await asyncio.sleep(0)


async def test_capacity_fifo() -> None:
    print("== 1. 并发上限 / FIFO / release 移交 ==")
    env = Env(max_waiting=8)
    n1, e1 = await env.acquire()
    n2, e2 = await env.acquire()
    check("两节点各接 1 个", (n1, n2) == (env.n1, env.n2) and e1 is e2 is None,
          f"{n1} {n2} {e1} {e2}")
    check("两节点均满载，inflight=[1,1]", env.inflight() == [1, 1])

    admitted = []
    waiters = []

    async def waiter(tag):
        node, err = await env.acquire()
        admitted.append((tag, node.name if node else None, err))
        if node:
            await asyncio.sleep(0.01)
            await env.gw.release_node(node, True)

    for i in range(4):
        waiters.append(asyncio.create_task(waiter(i)))
        await asyncio.sleep(0)
    await asyncio.sleep(0.02)
    check("4 个请求全部排队", env.waiting() == 4, str(env.waiting()))
    check("排队期间不增加在途", env.inflight() == [1, 1], str(env.inflight()))

    await env.gw.release_node(n1, True)
    await env.gw.release_node(n2, True)
    for w in waiters:
        await w
    check("全部排队请求最终接流成功", [a[2] for a in admitted] == [None] * 4, str(admitted))
    check("派发顺序与入队顺序一致", [a[0] for a in admitted] == [0, 1, 2, 3], str(admitted))
    check("结束后队列与在途归零", env.waiting() == 0 and env.inflight() == [0, 0],
          f"{env.waiting()} {env.inflight()}")


async def test_queue_full() -> None:
    print("== 2. 全局排队上限（后到被拒，不挤掉先到） ==")
    env = Env(max_waiting=2)
    await env.acquire()
    await env.acquire()
    admitted = []

    async def waiter(tag):
        node, err = await env.acquire()
        admitted.append((tag, err))
        if node:
            await asyncio.sleep(0.05)
            await env.gw.release_node(node, True)

    t1 = asyncio.create_task(waiter(1))
    await asyncio.sleep(0)
    t2 = asyncio.create_task(waiter(2))
    await asyncio.sleep(0)
    check("队满前两位正常排队", env.waiting() == 2, str(env.waiting()))
    node, err = await env.acquire()
    check("队满后新请求返回 queue_full", node is None and err == "queue_full", f"{node} {err}")
    check("新请求被拒未挤掉先到者", env.waiting() == 2, str(env.waiting()))
    await env.gw.release_node(env.n1, True)
    await env.gw.release_node(env.n2, True)
    await asyncio.gather(t1, t2)
    check("先到的两位都成功接流", all(a[1] is None for a in admitted), str(admitted))


async def test_timeout() -> None:
    print("== 3. 等候超时 ==")
    env = Env(max_wait=0.2)
    await env.acquire()
    await env.acquire()
    t0 = asyncio.get_running_loop().time()
    node, err = await env.acquire()
    elapsed = asyncio.get_running_loop().time() - t0
    check("超时返回 queue_timeout", node is None and err == "queue_timeout", f"{node} {err}")
    check("约 0.2s 后返回", 0.15 <= elapsed <= 0.8, f"{elapsed:.2f}")
    check("超时后队列无残留", env.waiting() == 0, str(env.waiting()))
    check("超时不影响已接流在途", env.inflight() == [1, 1], str(env.inflight()))


async def test_drain_redispatches() -> None:
    print("== 4. 排空：等待者改选，drain 只等在途 ==")
    env = Env()
    n1, _ = await env.acquire()
    await env.acquire()
    wait_task = asyncio.create_task(env.acquire())
    await asyncio.sleep(0)
    check("等待者已入队", env.waiting() == 1)

    drain_task = asyncio.create_task(env.gw.drain(env.n2, timeout=5))
    await asyncio.sleep(0)
    check("drain 立即置排空标志", env.n2.draining is True)
    check("等待者不会进入排空节点", env.waiting() == 1 and env.n2.inflight == 1)

    await env.gw.release_node(env.n2, True)  # 排空节点的在途结束
    drained, remaining = await asyncio.wait_for(drain_task, 1)
    check("drain 在在途归零后完成", drained is True and remaining == 0, f"{drained} {remaining}")
    check("等待者尚未接流（node1 仍满载）", env.waiting() == 1, str(env.waiting()))

    await env.gw.release_node(n1, True)  # node1 让出空位
    node, err = await asyncio.wait_for(wait_task, 1)
    check("等待者改选 node1", node is env.n1 and err is None, f"{node} {err}")
    await env.gw.release_node(node, True)
    check("全部结束后归零", env.waiting() == 0 and env.inflight() == [0, 0],
          f"{env.waiting()} {env.inflight()}")


async def test_all_ineligible_503() -> None:
    print("== 5. 全部不可接流：等待者立即 503 ==")
    env = Env()
    await env.acquire()
    await env.acquire()
    wait_task = asyncio.create_task(env.acquire())
    await asyncio.sleep(0)
    check("等待者已入队", env.waiting() == 1)

    t0 = asyncio.get_running_loop().time()
    # 对两个节点排空：不等待在途（用 0 超时），模拟运维排空动作
    d1 = asyncio.create_task(env.gw.drain(env.n1, timeout=0))
    d2 = asyncio.create_task(env.gw.drain(env.n2, timeout=0))
    node, err = await asyncio.wait_for(wait_task, 1)
    elapsed = asyncio.get_running_loop().time() - t0
    check("等待者得到 no_eligible_upstream", node is None and err == "no_eligible_upstream",
          f"{node} {err}")
    check("在途未结束前即被立即拒绝", elapsed < 0.2, f"{elapsed:.3f}s")
    check("排空回执不被等待者拖延",
          (await asyncio.gather(d1, d2)) == [(False, 1), (False, 1)])


async def test_disconnect_and_cancel() -> None:
    print("== 6. 断开 / 取消释放名额 ==")
    env = Env(max_waiting=1)
    await env.acquire()
    await env.acquire()
    req = FakeRequest()
    dis_task = asyncio.create_task(env.acquire(req))
    await asyncio.sleep(0.05)
    check("请求已排队", env.waiting() == 1)
    check("队满时后来者被拒", (await env.acquire())[1] == "queue_full")

    req.disconnected = True
    node, err = await asyncio.wait_for(dis_task, 1)
    check("断开后返回 client_disconnected", node is None and err == "client_disconnected",
          f"{node} {err}")
    check("断开后名额立即释放", env.waiting() == 0)

    # 取消一个排队中的请求
    cancel_task = asyncio.create_task(env.acquire())
    await asyncio.sleep(0.05)
    check("取消前已排队", env.waiting() == 1)
    cancel_task.cancel()
    try:
        await cancel_task
    except asyncio.CancelledError:
        pass
    check("取消后名额释放", env.waiting() == 0, str(env.waiting()))

    # 取消与派发竞态：先取消任务，同一事件循环轮次立刻释放节点
    race_task = asyncio.create_task(env.acquire())
    await asyncio.sleep(0.05)
    await env.gw.release_node(env.n1, True)  # 派发 race_task
    race_task.cancel()
    try:
        await race_task
    except asyncio.CancelledError:
        pass
    await asyncio.sleep(0.05)
    check("取消竞态下在途名额不泄漏", env.inflight() == [0, 1], str(env.inflight()))
    check("取消竞态下队列不残留", env.waiting() == 0, str(env.waiting()))


class FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class FakeClient:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    async def get(self, url, timeout=None):
        return FakeResponse(self.status_code)


async def test_probe_failure() -> None:
    print("== 7. 探测故障不派发 / 恢复后继续 ==")
    env = Env()
    held1, _ = await env.acquire()
    held2, _ = await env.acquire()
    wait_task = asyncio.create_task(env.acquire())
    await asyncio.sleep(0)
    check("等待者已入队", env.waiting() == 1)

    # 请求失败降级 node1：node2 仍可接流，等待者不得被派入故障节点
    await env.gw.mark_unhealthy(env.n1, "boom")
    await asyncio.sleep(0.02)
    check("故障节点不会被派发（等待者继续等待 node2）", env.waiting() == 1)
    await env.gw.release_node(held2, False)
    node, err = await asyncio.wait_for(wait_task, 1)
    check("node2 空出后等待者接流 node2", node is env.n2 and err is None, f"{node} {err}")
    await env.gw.release_node(held1, True)
    await env.gw.release_node(node, True)

    # 唯一健康节点也故障：新请求 503
    await env.gw.mark_unhealthy(env.n2, "boom")
    check("全部故障时新请求 503", (await env.acquire())[1] == "no_eligible_upstream")

    # 探测恢复 node1：真实 probe_one 路径应唤醒等待者并接流
    env.gw.client = FakeClient(200)
    wait_task2 = asyncio.create_task(env.acquire())
    await asyncio.sleep(0)
    node2, err2 = await asyncio.wait_for(wait_task2, 1)
    check("无可接流节点时不会排队，直接 503", node2 is None and err2 == "no_eligible_upstream",
          f"{node2} {err2}")

    await env.gw.probe_one(env.n1)
    check("探测恢复 node1", env.n1.healthy is True and env.n1.can_serve())
    node3, err3 = await env.acquire()
    check("恢复后请求重新接流 node1", node3 is env.n1 and err3 is None, f"{node3} {err3}")
    await env.gw.release_node(node3, True)
    check("探测恢复后计数归零", env.inflight() == [0, 0], str(env.inflight()))


async def main() -> int:
    await test_capacity_fifo()
    await test_queue_full()
    await test_timeout()
    await test_drain_redispatches()
    await test_all_ineligible_503()
    await test_disconnect_and_cancel()
    await test_probe_failure()
    print(f"\n==== 结果: {passed} passed, {failed} failed ====")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
