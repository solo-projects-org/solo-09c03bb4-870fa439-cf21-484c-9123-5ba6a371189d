#!/usr/bin/env python3
"""并发接单 / 按原点重试 / 重启去重 自测（需在 Compose 环境运行）。

用法：
    docker compose up -d --build
    python3 scripts/selftest_single_active.py

环境变量：
    API_URL    默认 http://localhost:8000
    COMPOSE    默认 "docker compose"（在仓库根目录执行）

验证点：
  1. 8 路并发新建恢复：仅一条 202，其余 409，且不留待执行队列；
  2. 第一条 ready 后可再发起下一条（源库保持可写），进行中时再发仍 409；
  3. 6 路并发按原点重试：仅一条 202，其余 409；
  4. 重启时 SQLite 已有多条历史待办：仅最早一条续跑至 ready，
     其余标记 cancelled 及未执行原因；被保留任务的核对点与就绪查询对应。
"""
import json
import os
import shlex
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

API = os.environ.get("API_URL", "http://localhost:8000")
COMPOSE = os.environ.get("COMPOSE", "docker compose")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TS = time.strftime("%H%M%S")

FAILED: list[str] = []


# ------------------------------------------------------------------ 工具

def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def compose(*args: str, capture: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        shlex.split(COMPOSE) + list(args),
        cwd=REPO_ROOT, check=True, text=True,
        capture_output=capture)


def psql(sql: str) -> str:
    r = compose("exec", "-T", "db", "psql", "-U", "postgres", "-d", "water",
                "-tA", "-v", "ON_ERROR_STOP=1", "-c", sql, capture=True)
    return r.stdout.strip()


def wait_for(desc: str, fn, timeout: int = 300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = fn()
        if r:
            return r
        time.sleep(2)
    raise SystemExit(f"超时：{desc}")


def check(desc: str, cond: bool) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {desc}")
    if not cond:
        FAILED.append(desc)


def concurrent(n: int, fn) -> list:
    """n 个线程同一时刻放行 fn(i)，返回 [(status, body), ...]。"""
    bar = threading.Barrier(n)
    out: list = [None] * n

    def worker(i: int) -> None:
        bar.wait()
        out[i] = fn(i)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return out


def active_jobs() -> list[dict]:
    _, jobs = api("GET", "/api/recoveries")
    return [j for j in jobs if j["status"] in
            ("queued", "waiting_wal", "preparing", "recovering", "verifying")]


def wait_terminal(job_id: int) -> dict:
    return wait_for(
        f"任务 #{job_id} 到达终结状态",
        lambda: (lambda j: j if j.get("status") in
                 ("ready", "failed", "cancelled", "superseded", "stopped")
                 else None)(api("GET", f"/api/recoveries/{job_id}")[1]))


def create_checkpoint(name: str) -> dict:
    code, body = api("POST", "/api/checkpoints", {"name": name})
    assert code == 201, f"创建核对点 {name} 失败: {code} {body}"
    wait_for(f"核对点 {name} 就绪",
             lambda: api("GET", f"/api/checkpoints/{name}")[1].get("status") == "ready")
    return api("GET", f"/api/checkpoints/{name}")[1]


# ------------------------------------------------------------------ 主流程

def main() -> int:
    print("== 0. 等待 API / 基准备份，并确保无遗留进行中任务 ==")
    wait_for("API 健康", lambda: api("GET", "/api/health")[1].get("status") == "ok")
    wait_for("基准备份完成",
             lambda: (api("GET", "/api/basebackup")[1].get("latest_done") or {})
             .get("status") == "done")
    for j in active_jobs():
        api("DELETE", f"/api/recoveries/{j['id']}")
        wait_terminal(j["id"])
    check("起始无进行中任务", not active_jobs())

    cp_a = create_checkpoint(f"st-{TS}-a")
    print(f"== 1. 8 路并发新建恢复（核对点 {cp_a['name']}）==")
    results = concurrent(8, lambda i: api("POST", "/api/recoveries",
                                          {"checkpoint": cp_a["name"]}))
    accepted = [b for code, b in results if code == 202]
    rejected = [b for code, b in results if code == 409]
    check("仅一条 202", len(accepted) == 1)
    check("其余 7 条均为 409", len(rejected) == 7)
    check("409 说明中指明进行中任务", all("进行中" in r.get("detail", "")
                                          for r in rejected))
    check("恰有一条进行中任务（无多余队列）", len(active_jobs()) == 1)

    job1 = wait_terminal(accepted[0]["id"])
    assert job1["status"] == "ready", f"恢复失败: {job1.get('error')}"
    code, body = api("GET", f"/api/recoveries/{job1['id']}/samples")
    check("第一条任务 ready 且样品查询开放", code == 200
          and body.get("checkpoint") == cp_a["name"])

    print("== 2. 第一条已结束（ready）后可主动发起下一次恢复 ==")
    code, job2 = api("POST", "/api/recoveries", {"checkpoint": cp_a["name"]})
    check("ready 后再次发起返回 202", code == 202)
    code, _ = api("POST", "/api/recoveries", {"checkpoint": cp_a["name"]})
    check("新任务进行中时再发返回 409", code == 409)
    # 恢复进行期间生产库保持可写
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         f" VALUES ('ST-WRITABLE-{TS}', 'ST-09', 7.00)")
    check("恢复进行期间源库可写", psql(
        f"SELECT count(*) FROM samples WHERE sample_no='ST-WRITABLE-{TS}'") == "1")
    job2 = wait_terminal(job2["id"])
    assert job2["status"] == "ready", f"恢复失败: {job2.get('error')}"
    check("旧 ready 任务被取代（superseded）",
          api("GET", f"/api/recoveries/{job1['id']}")[1]["status"] == "superseded")

    print("== 3. 6 路并发按原点重试 ==")
    api("DELETE", f"/api/recoveries/{job2['id']}")  # 停止 ready 实例 → stopped
    assert api("GET", f"/api/recoveries/{job2['id']}")[1]["status"] == "stopped"
    results = concurrent(6, lambda i: api("POST",
                                          f"/api/recoveries/{job2['id']}/retry"))
    accepted = [b for code, b in results if code == 202]
    rejected = [b for code, b in results if code == 409]
    check("仅一条 202", len(accepted) == 1)
    check("其余 5 条均为 409", len(rejected) == 5)
    check("重试任务沿用原核对点", accepted[0]["checkpoint"] == cp_a["name"])
    job3 = wait_terminal(accepted[0]["id"])
    assert job3["status"] == "ready", f"重试恢复失败: {job3.get('error')}"
    print(f"  重试任务 #{job3['id']} 已 ready")

    print("== 4. 重启时 SQLite 已有多条历史待办 → 仅续跑最早一条 ==")
    cp_r1 = create_checkpoint(f"st-{TS}-r1")
    cp_r2 = create_checkpoint(f"st-{TS}-r2")
    # 模拟旧版本遗留：摘掉兜底索引，直接写入两条 queued 待办
    seed = (
        "import sqlite3,sys,datetime;"
        "con=sqlite3.connect('/state/api.db',timeout=30);"
        "con.execute('PRAGMA busy_timeout=30000');"
        "con.execute('DROP INDEX IF EXISTS idx_jobs_single_active');"
        "now=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds');"
        "[con.execute('INSERT INTO jobs (checkpoint_name,status,detail,created_at,"
        "updated_at) VALUES (?,?,?,?,?)',(cp,'queued','模拟旧版本遗留待办',now,now))"
        " for cp in sys.argv[1:3]];"
        "con.commit();"
        "ids=[r[0] for r in con.execute('SELECT id FROM jobs WHERE checkpoint_name"
        " IN (?,?) ORDER BY id',sys.argv[1:3])];"
        "print(ids[0],ids[1])"
    )
    r = compose("exec", "-T", "api", "python3", "-c", seed,
                cp_r1["name"], cp_r2["name"], capture=True)
    legacy1, legacy2 = (int(x) for x in r.stdout.strip().splitlines()[-1].split())
    print(f"  已写入历史待办 #{legacy1}（{cp_r1['name']}）与 #{legacy2}（{cp_r2['name']}）")
    compose("restart", "api")
    wait_for("API 重启后恢复健康",
             lambda: api("GET", "/api/health")[1].get("status") == "ok")

    max_active = 0
    t0 = time.time()
    while time.time() - t0 < 300:
        max_active = max(max_active, len(active_jobs()))
        if api("GET", f"/api/recoveries/{legacy1}")[1].get("status") in (
                "ready", "failed", "cancelled"):
            break
        time.sleep(2)
    j1 = api("GET", f"/api/recoveries/{legacy1}")[1]
    j2 = api("GET", f"/api/recoveries/{legacy2}")[1]
    check("最早接单的续跑至 ready", j1["status"] == "ready")
    if j1["status"] != "ready":
        print(f"  任务 #{legacy1} 状态: {j1['status']} {j1.get('error')}")
    check("较晚一条标记 cancelled 且写明未执行原因",
          j2["status"] == "cancelled" and "未执行" in (j2.get("detail") or ""))
    check("重启后进行中任务始终至多一条", max_active <= 1)
    code, body = api("GET", f"/api/recoveries/{legacy1}/samples")
    check("被保留任务的就绪查询开放", code == 200)
    check("就绪查询对应原核对点",
          body.get("checkpoint") == cp_r1["name"]
          and body.get("restore_lsn") == cp_r1["restore_lsn"])
    idx = compose("exec", "-T", "api", "python3", "-c",
                  "import sqlite3;con=sqlite3.connect('/state/api.db');"
                  "print(con.execute(\"SELECT count(*) FROM sqlite_master"
                  " WHERE type='index' AND name='idx_jobs_single_active'\""
                  ").fetchone()[0])", capture=True)
    check("重启后兜底索引已重建", idx.stdout.strip() == "1")
    code, job4 = api("POST", "/api/recoveries", {"checkpoint": cp_r2["name"]})
    check("被取消的历史待办可按原点重新发起（202）", code == 202)
    if code == 202:
        job4 = wait_terminal(job4["id"])
        check("重新发起的任务跑至 ready", job4["status"] == "ready")

    print()
    if FAILED:
        print(f"共 {len(FAILED)} 项校验失败：")
        for f in FAILED:
            print(f"  - {f}")
        return 1
    print("全部校验通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
