#!/usr/bin/env python3
"""有界排队接流自测（不依赖 docker，使用 stdlib + 三个 uvicorn 子进程）。

覆盖：
  A. 每节点并发上限、全局排队、排队不计入在途、队满 429、状态 API 字段
  B. 严格 FIFO：后到请求不得抢先占空位
  C. 等候超过 QUEUE_MAX_WAIT -> 429 且名额释放
  D. 排队期间排空某节点：等待者改选其他节点，排空回执只等在途不被拖延
  E. 排队期间探测故障：等待者不进入故障节点，空位出现后改选健康节点
  F. 排空导致全部节点不可接流：等待者立即 503，排空仍只等在途
  G. 客户端取消（断开）释放排队名额，已接流请求照常结束
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

PYLIBS = os.environ.get("PYLIBS", "/tmp/pylibs")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable

GW_PORT = 19180
U1_PORT = 19181
U2_PORT = 19182
GW = f"http://127.0.0.1:{GW_PORT}"

passed = 0
failed = 0
procs: list[subprocess.Popen] = []
gw: subprocess.Popen | None = None


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def http(method: str, url: str, timeout: float = 20.0, data: bytes | None = None):
    """返回 (status, headers, body_text)。4xx/5xx 不抛异常。"""
    req = urllib.request.Request(url, method=method, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()


def jget(url: str, timeout: float = 20.0):
    status, _, body = http("GET", url, timeout)
    return status, json.loads(body)


def wait_ready(url: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = http("GET", url, timeout=2)
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def wait_until(pred, timeout: float = 6.0, interval: float = 0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def spawn(module_dir: str, port: int, extra_env: dict | None = None):
    # -I 隔离模式会忽略 PYTHONPATH 且不含 cwd，故用 runpy 包装器显式注入依赖与应用目录
    launcher = (
        "import sys,runpy,os;"
        f"sys.path[:0]=[{PYLIBS!r}, os.getcwd()];"
        "sys.argv=['uvicorn']+sys.argv[1:];"
        "runpy.run_module('uvicorn', run_name='__main__')"
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.update(extra_env or {})
    return subprocess.Popen(
        [PYTHON, "-I", "-c", launcher, "app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=module_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=8)
        except Exception:
            proc.kill()


def gateway_status() -> dict:
    status, body = jget(f"{GW}/admin/status")
    assert status == 200
    return body


def node_status(node: str) -> dict:
    return next(n for n in gateway_status()["nodes"] if n["name"] == node)


def set_mode(port: int, mode: str) -> None:
    status, _, body = http("POST", f"http://127.0.0.1:{port}/control/mode?mode={mode}")
    assert status == 200, body


def start_gateway(env: dict) -> None:
    global gw
    if gw is not None:
        kill(gw)
        procs.remove(gw)
    gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, env)
    procs.append(gw)
    assert wait_ready(f"{GW}/docs"), "网关启动失败"
    ok = wait_until(lambda: gateway_status()["eligible_nodes"] == ["node1", "node2"], 8)
    assert ok, "网关启动后两个节点未同时可接流"


def start_request(path: str, timeout: float = 25.0) -> tuple[threading.Thread, dict]:
    """后台发起一个网关请求；返回 (线程, 结果字典)。"""
    box: dict = {}

    def run() -> None:
        box["start"] = time.time()
        try:
            s, h, b = http("GET", f"{GW}{path}", timeout=timeout)
            box["status"], box["headers"], box["body"] = s, h, b
        except Exception as exc:  # 便于定位连接层失败
            box["error"] = repr(exc)
        box["end"] = time.time()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, box


def join(t: threading.Thread, box: dict, timeout: float = 30.0) -> dict:
    t.join(timeout)
    assert not t.is_alive(), "请求线程未在预期时间内结束"
    return box


def body_json(box: dict) -> dict:
    try:
        return json.loads(box.get("body") or "{}")
    except Exception:
        return {}


def occupy_nodes(seconds1: float = 2.5, seconds2: float = 2.5):
    """让两个节点各接一个慢请求，返回两个 (thread, box)。"""
    t1, b1 = start_request(f"/proxy/slow?seconds={seconds1}")
    assert wait_until(lambda: node_status("node1")["inflight"] == 1, 5), "node1 未被占用"
    t2, b2 = start_request(f"/proxy/slow?seconds={seconds2}")
    assert wait_until(lambda: node_status("node2")["inflight"] == 1, 5), "node2 未被占用"
    return (t1, b1), (t2, b2)


def open_waiting_socket() -> socket.socket:
    """发一个会排队/挂住的原始请求，不读取响应，便于模拟客户端取消。"""
    s = socket.create_connection(("127.0.0.1", GW_PORT), timeout=5)
    s.sendall(
        f"GET /proxy/slow?seconds=0.1 HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{GW_PORT}\r\nConnection: close\r\n\r\n".encode()
    )
    return s


def scenario_a(main_env: dict) -> None:
    print("== A. 每节点并发上限 + 全局排队 + 队满 429 ==")
    start_gateway(main_env)
    status, _, body = http("GET", f"{GW}/docs")
    check("/docs 可打开", status == 200 and "swagger" in body.lower())

    o1, o2 = occupy_nodes(2.5, 2.5)
    st = gateway_status()
    check("两节点各接 1 个在途，达到每节点上限",
          [n["inflight"] for n in st["nodes"]] == [1, 1], str(st["nodes"]))
    check("状态 API 暴露 waiting 全局等待数", st.get("waiting") == 0, str(st))
    check("状态 API 暴露每节点 max_inflight 与 inflight",
          all(n["max_inflight"] == 1 and n["inflight"] == 1 for n in st["nodes"]),
          str(st["nodes"]))

    queued = [start_request("/proxy/echo") for _ in range(3)]
    check("3 个请求进入全局排队", wait_until(lambda: gateway_status()["waiting"] == 3, 5))
    st = gateway_status()
    check("等待期间节点在途仍为 1（等待不计入在途）",
          [n["inflight"] for n in st["nodes"]] == [1, 1], str(st["nodes"]))

    status, headers, raw = http("GET", f"{GW}/proxy/echo", timeout=5)
    b = json.loads(raw)
    check("队满时新请求立即 429 queue_full",
          status == 429 and b.get("error") == "queue_full", raw)
    check("429 带 Retry-After 提示", headers.get("retry-after") == "1", str(headers))

    for t, box in queued:
        join(t, box)
    check("3 个排队请求最终全部 200",
          all(box.get("status") == 200 for _, box in queued),
          str([box.get("status") for _, box in queued]))
    for pair in (o1, o2):
        join(*pair)
    check("占用请求均正常完成",
          all(box.get("status") == 200 for _, box in (o1, o2)))
    check("结束后等待数与在途均归零",
          wait_until(lambda: gateway_status()["waiting"] == 0
                     and all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")), 5))


def scenario_b(main_env: dict) -> None:
    print("== B. 严格 FIFO：后到不抢先 ==")
    start_gateway(main_env)
    status, _, raw = http("POST", f"{GW}/admin/nodes/node2/drain?timeout=5")
    assert status == 200, raw

    occ_t, occ = start_request("/proxy/slow?seconds=2")
    assert wait_until(lambda: node_status("node1")["inflight"] == 1, 5)

    q1t, q1 = start_request("/proxy/slow?seconds=1.5")
    check("第一个等待者先入队", wait_until(lambda: gateway_status()["waiting"] == 1, 5))
    q2t, q2 = start_request("/proxy/slow?seconds=0.5")
    check("第二个等待者排在队尾（等待数 2）",
          wait_until(lambda: gateway_status()["waiting"] == 2, 5))

    join(occ_t, occ)
    join(q1t, q1)
    join(q2t, q2)
    check("占用请求 200", occ.get("status") == 200, str(occ))
    check("两个排队请求都 200 且都落在 node1",
          all(box.get("status") == 200 and body_json(box).get("node") == "node1"
              for box in (q1, q2)), f"{q1} {q2}")
    q1_end, q2_end = q1.get("end", 0.0), q2.get("end", 0.0)
    # 若后到者抢先，短请求（0.5s）会先完成；FIFO 下先入队的长请求（1.5s）先完成
    check("先入队的请求先接流、先完成",
          q1_end < q2_end and (q2_end - q1_end) >= 0.3,
          f"q1_end={q1_end:.2f} q2_end={q2_end:.2f}")


def scenario_c(main_env: dict) -> None:
    print("== C. 等候超时返回 429 ==")
    env = dict(main_env)
    env["QUEUE_MAX_WAIT"] = "0.5"
    start_gateway(env)
    status, _, raw = http("POST", f"{GW}/admin/nodes/node2/drain?timeout=5")
    assert status == 200, raw

    occ_t, occ = start_request("/proxy/slow?seconds=2.5")
    assert wait_until(lambda: node_status("node1")["inflight"] == 1, 5)
    t0 = time.time()
    status, _, raw = http("GET", f"{GW}/proxy/echo", timeout=6)
    elapsed = time.time() - t0
    b = json.loads(raw)
    check("等候超时返回 429 queue_timeout",
          status == 429 and b.get("error") == "queue_timeout", raw)
    check("约在配置的 0.5s 后返回", 0.4 <= elapsed <= 1.5, f"{elapsed:.2f}s")
    check("超时后排队名额已释放", gateway_status()["waiting"] == 0)
    check("超时不影响节点在途", node_status("node1")["inflight"] == 1)
    join(occ_t, occ)
    check("占用请求仍正常结束 200", occ.get("status") == 200, str(occ))


def scenario_d(main_env: dict) -> None:
    print("== D. 排队中排空节点：等待者改选，排空只等在途 ==")
    start_gateway(main_env)
    n1t, n1 = start_request("/proxy/slow?seconds=2.5")
    assert wait_until(lambda: node_status("node1")["inflight"] == 1, 5)
    n2t, n2 = start_request("/proxy/slow?seconds=1.5")
    assert wait_until(lambda: node_status("node2")["inflight"] == 1, 5)

    wt, w = start_request("/proxy/slow?seconds=0.2")
    check("等待者已排队", wait_until(lambda: gateway_status()["waiting"] == 1, 5))

    t0 = time.time()
    status, _, raw = http("POST", f"{GW}/admin/nodes/node2/drain?timeout=10")
    elapsed = time.time() - t0
    b = json.loads(raw)
    check("排空返回 200 drained=true",
          status == 200 and b.get("drained") is True, raw)
    check("排空回执不被排队请求拖延（< 2s）", elapsed < 2.0, f"{elapsed:.2f}s")

    join(wt, w)
    wb = body_json(w)
    check("等待者改选健康节点 node1，未进入排空节点",
          w.get("status") == 200 and wb.get("node") == "node1", str(w))
    join(n1t, n1)
    join(n2t, n2)
    check("原在途请求均正常完成",
          n1.get("status") == 200 and n2.get("status") == 200, f"{n1} {n2}")
    check("结束后 node2 在途为 0 且无等待者",
          wait_until(lambda: node_status("node2")["inflight"] == 0
                     and gateway_status()["waiting"] == 0, 5))


def scenario_e(main_env: dict) -> None:
    print("== E. 排队中探测故障：等待者不进入故障节点 ==")
    set_mode(U1_PORT, "healthy")
    set_mode(U2_PORT, "healthy")
    start_gateway(main_env)
    o1t, o1 = start_request("/proxy/slow?seconds=3")
    assert wait_until(lambda: node_status("node1")["inflight"] == 1, 5)
    o2t, o2 = start_request("/proxy/slow?seconds=2")
    assert wait_until(lambda: node_status("node2")["inflight"] == 1, 5)
    wt, w = start_request("/proxy/slow?seconds=0.2")
    check("等待者已排队", wait_until(lambda: gateway_status()["waiting"] == 1, 5))

    set_mode(U1_PORT, "error")
    http("POST", f"{GW}/admin/probe")
    st1 = node_status("node1")
    check("探测将 node1 标记为不健康",
          st1["healthy"] is False and st1["eligible"] is False, str(st1))
    check("等待者未被派入故障节点，继续等待 node2 空位",
          gateway_status()["waiting"] == 1, str(gateway_status()))

    join(wt, w)
    wb = body_json(w)
    check("node2 空出后等待者接流健康节点 node2",
          w.get("status") == 200 and wb.get("node") == "node2", str(w))
    join(o2t, o2)
    check("node2 上原在途请求正常完成", o2.get("status") == 200, str(o2))
    join(o1t, o1)
    check("故障前已接流的 node1 请求照常结束（非 200 即如实透传）",
          o1.get("status") is not None, str(o1))

    set_mode(U1_PORT, "healthy")
    http("POST", f"{GW}/admin/probe")
    check("上游恢复后经探测重新可接流",
          wait_until(lambda: node_status("node1")["eligible"] is True, 5))


def scenario_f(main_env: dict) -> None:
    print("== F. 排空导致全部不可接流：等待者立即 503 ==")
    start_gateway(main_env)
    status, _, raw = http("POST", f"{GW}/admin/nodes/node2/drain?timeout=5")
    assert status == 200, raw

    occ_t, occ = start_request("/proxy/slow?seconds=3")
    assert wait_until(lambda: node_status("node1")["inflight"] == 1, 5)
    wt, w = start_request("/proxy/echo")
    check("等待者已在 node1 满载时排队",
          wait_until(lambda: gateway_status()["waiting"] == 1, 5))

    t0 = time.time()
    status, _, raw = http("POST", f"{GW}/admin/nodes/node1/drain?timeout=10")
    drain_elapsed = time.time() - t0
    b = json.loads(raw)
    check("排空只等在途并最终 drained=true",
          status == 200 and b.get("drained") is True, raw)
    check("排空未被等待者拖延（耗时只反映在途慢请求）",
          drain_elapsed >= 2.0, f"{drain_elapsed:.2f}s")

    join(wt, w)
    wb = body_json(w)
    check("等待者立即收到 503 no_eligible_upstream",
          w.get("status") == 503 and wb.get("error") == "no_eligible_upstream", str(w))
    check("等待者远早于在途结束即被拒（未拖延排空）",
          w.get("end", 1e9) - t0 < 1.0, f"{w.get('end', 0) - t0:.2f}s")

    join(occ_t, occ)
    check("已接流的慢请求照常结束", occ.get("status") == 200, str(occ))
    check("结束后等待数归零", gateway_status()["waiting"] == 0)


def scenario_g(main_env: dict) -> None:
    print("== G. 取消等待释放队列名额 ==")
    start_gateway(main_env)
    o1, o2 = occupy_nodes(3, 3)

    socks = [open_waiting_socket() for _ in range(3)]
    check("3 个挂起客户端进入排队",
          wait_until(lambda: gateway_status()["waiting"] == 3, 5))

    status, _, raw = http("GET", f"{GW}/proxy/echo", timeout=3)
    b = json.loads(raw)
    check("3 个名额占满时新请求 429 queue_full",
          status == 429 and b.get("error") == "queue_full", raw)

    socks[1].close()
    check("断开连接后排队名额释放（3 -> 2）",
          wait_until(lambda: gateway_status()["waiting"] == 2, 5))

    t0 = time.time()
    status, _, raw = http("GET", f"{GW}/proxy/echo", timeout=15)
    elapsed = time.time() - t0
    check("名额释放后新请求可入队并最终成功",
          status == 200 and elapsed >= 1.0, f"status={status} elapsed={elapsed:.2f}s")

    for s in (socks[0], socks[2]):
        s.close()
    check("其余断开后等待数归零",
          wait_until(lambda: gateway_status()["waiting"] == 0, 5))

    for pair in (o1, o2):
        join(*pair)
    check("已接流的在途请求照常完成 200",
          all(box.get("status") == 200 for _, box in (o1, o2)))
    check("全部结束后两节点在途归零",
          wait_until(lambda: all(node_status(n)["inflight"] == 0
                                 for n in ("node1", "node2")), 5))


def main() -> int:
    try:
        print("== 启动两个上游和网关 ==")
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        u2 = spawn(os.path.join(ROOT, "upstream"), U2_PORT, {"NODE_ID": "node2"})
        procs.extend([u1, u2])
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U2_PORT}/")

        main_env = {
            "UPSTREAMS": f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT}",
            "PROBE_INTERVAL": "1",
            "PROBE_TIMEOUT": "1",
            "REQUEST_TIMEOUT": "15",
            "DRAIN_DEFAULT_TIMEOUT": "10",
            "NODE_MAX_INFLIGHT": "1",
            "QUEUE_MAX_WAITING": "3",
            "QUEUE_MAX_WAIT": "10",
        }

        scenario_a(main_env)
        scenario_b(main_env)
        scenario_c(main_env)
        scenario_d(main_env)
        scenario_e(main_env)
        scenario_f(main_env)
        scenario_g(main_env)

        print(f"\n==== 结果: {passed} passed, {failed} failed ====")
        return 1 if failed else 0
    finally:
        for p in procs:
            kill(p)


if __name__ == "__main__":
    sys.exit(main())
