#!/usr/bin/env python3
"""Sobe/derruba o cluster local: Redis (docker) + head (supervisionado) + 2 workers."""
import argparse, os, signal, subprocess, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *

HERE = pathlib.Path(__file__).resolve().parent
COMPOSE = ROOT / "docker-compose.yml"


def _sh(cmd, **kw):
    return subprocess.run(cmd, **kw)


# ---------- Redis ----------
def redis_up():
    _sh(["docker", "compose", "-f", str(COMPOSE), "up", "-d"], check=True)
    for _ in range(30):
        r = _sh(["docker", "exec", REDIS_CONTAINER, "redis-cli", "-a", REDIS_PASSWORD, "--no-auth-warning", "ping"],
                capture_output=True, text=True)
        if "PONG" in r.stdout:
            return
        time.sleep(1)
    raise RuntimeError("Redis não respondeu")


def redis_flush():
    _sh(["docker", "exec", REDIS_CONTAINER, "redis-cli", "-a", REDIS_PASSWORD, "--no-auth-warning", "FLUSHALL"],
        capture_output=True)


def redis_stop():
    _sh(["docker", "stop", REDIS_CONTAINER], capture_output=True)


def redis_start():
    _sh(["docker", "start", REDIS_CONTAINER], capture_output=True)


# ---------- Ray ----------
def start_head():
    (RUN_DIR / "stop").unlink(missing_ok=True)
    log = open(RUN_DIR / "supervisor.log", "a")
    p = subprocess.Popen([sys.executable, str(HERE / "head_supervisor.py")], stdout=log,
                         stderr=subprocess.STDOUT, start_new_session=True)
    (RUN_DIR / "supervisor.pid").write_text(str(p.pid))
    if not wait_tcp(GCS_PORT, 60):
        raise RuntimeError("GCS não abriu a porta (veja .run/head.log)")
    for _ in range(60):
        if health_ok():
            return
        time.sleep(1)
    raise RuntimeError("GCS não ficou saudável")


def start_workers():
    mode = get_mode()
    log = open(RUN_DIR / "workers.log", "a")
    for w in WORKERS:
        subprocess.run(worker_cmd(w), env=ray_env(mode), stdout=log, stderr=subprocess.STDOUT, check=True)


def kill_supervisor(sig=signal.SIGTERM):
    p = RUN_DIR / "supervisor.pid"
    if p.exists():
        try:
            os.kill(int(p.read_text()), sig)
        except ProcessLookupError:
            pass


def up(mode: str):
    set_mode(mode)
    if mode == "redis":
        redis_up()
    if mode == "rocksdb":
        (RUN_DIR / "rocksdb").mkdir(exist_ok=True)
    start_head()
    start_workers()
    event("cluster_up", mode=mode)


def down(flush: bool = True):
    (RUN_DIR / "stop").write_text("1")
    kill_supervisor(signal.SIGTERM)
    time.sleep(1)
    _sh(["ray", "stop", "--force"], capture_output=True)
    if flush and get_mode() == "redis":
        redis_flush()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["up", "down", "status"])
    ap.add_argument("--storage", choices=["redis", "rocksdb", "none"], default="redis")
    ap.add_argument("--keep-redis-data", action="store_true")
    a = ap.parse_args()
    if a.cmd == "up":
        down(); up(a.storage)
    elif a.cmd == "down":
        down(flush=not a.keep_redis_data)
    else:
        _sh(["ray", "status", "--address", f"{HOST}:{GCS_PORT}"])
