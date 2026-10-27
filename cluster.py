#!/usr/bin/env python3
"""Sobe/derruba o cluster local: Redis (docker) + head (supervisionado) + 2 workers."""
import argparse, os, shutil, signal, subprocess, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *

HERE = pathlib.Path(__file__).resolve().parent
COMPOSE = ROOT / "docker-compose.yml"


def _sh(cmd, **kw):
    return subprocess.run(cmd, **kw)


# ---------- Redis ----------
# Dois backends: "docker" (docker-compose.yml) ou "native" (redis-server local, ex.: `brew install redis`).
# LAB_REDIS_BACKEND=docker|native|auto (auto = docker se o daemon responder, senão redis-server local).
REDIS_DIR = RUN_DIR / "redis"


def redis_backend() -> str:
    b = os.getenv("LAB_REDIS_BACKEND", "auto")
    if b != "auto":
        return b
    if shutil.which("docker") and _sh(["docker", "info"], capture_output=True).returncode == 0:
        return "docker"
    if shutil.which("redis-server"):
        return "native"
    raise RuntimeError("Nem Docker nem redis-server disponíveis (instale um deles: `brew install redis`)")


def _redis_cli(*args):
    if redis_backend() == "docker":
        base = ["docker", "exec", REDIS_CONTAINER, "redis-cli"]
    else:
        base = ["redis-cli", "-h", HOST, "-p", str(REDIS_PORT)]
    return _sh(base + ["-a", REDIS_PASSWORD, "--no-auth-warning", *args], capture_output=True, text=True)


def redis_ping() -> bool:
    try:
        return "PONG" in _redis_cli("ping").stdout
    except FileNotFoundError:
        return False


def _native_start():
    REDIS_DIR.mkdir(parents=True, exist_ok=True)
    _sh(["redis-server", "--port", str(REDIS_PORT), "--bind", HOST, "--requirepass", REDIS_PASSWORD,
         "--appendonly", "yes", "--appendfsync", "everysec", "--dir", str(REDIS_DIR),
         "--daemonize", "yes", "--pidfile", str(REDIS_DIR / "redis.pid"),
         "--logfile", str(REDIS_DIR / "redis.log")], check=True)


def redis_up():
    if redis_ping():
        return
    if redis_backend() == "docker":
        _sh(["docker", "compose", "-f", str(COMPOSE), "up", "-d"], check=True)
    else:
        _native_start()
    for _ in range(30):
        if redis_ping():
            return
        time.sleep(1)
    raise RuntimeError("Redis não respondeu")


def redis_flush():
    _redis_cli("FLUSHALL")


def redis_stop():
    if redis_backend() == "docker":
        _sh(["docker", "stop", REDIS_CONTAINER], capture_output=True)
    else:
        _redis_cli("SHUTDOWN")   # com AOF ligado, os dados ficam em .run/redis/
        for _ in range(50):
            if not tcp_up(REDIS_PORT):
                break
            time.sleep(0.1)


def redis_start():
    if redis_backend() == "docker":
        _sh(["docker", "start", REDIS_CONTAINER], capture_output=True)
    else:
        _native_start()
    for _ in range(30):
        if redis_ping():
            return
        time.sleep(0.5)


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
        except (ProcessLookupError, ValueError):
            pass


def _kill_stale_supervisors():
    """Supervisores órfãos de execuções interrompidas (Ctrl-C) ficariam reiniciando o head."""
    import psutil
    me = os.getpid()
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            argv = proc.info["cmdline"] or []
            if (proc.info["pid"] != me and len(argv) >= 2 and "python" in os.path.basename(argv[0])
                    and argv[1].endswith("head_supervisor.py")):
                proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass


def _wait_ports_closed(ports, timeout=20):
    end = time.time() + timeout
    while time.time() < end and any(tcp_up(p) for p in ports):
        time.sleep(0.3)


def up(mode: str):
    set_mode(mode)
    if mode == "redis":
        redis_up()
        redis_flush()   # garante que o GCS nasce de um storage limpo
    if mode == "rocksdb":
        (RUN_DIR / "rocksdb").mkdir(exist_ok=True)
    start_head()
    start_workers()
    event("cluster_up", mode=mode)


def down(flush: bool = True):
    """Limpeza completa e idempotente: supervisor -> processos Ray -> portas -> Redis."""
    (RUN_DIR / "stop").write_text("1")
    kill_supervisor(signal.SIGTERM)
    time.sleep(1)
    _kill_stale_supervisors()
    _sh(["ray", "stop", "--force"], capture_output=True, env=ray_cli_env())
    _wait_ports_closed([GCS_PORT, HEAD_NM_PORT, HEAD_OM_PORT])
    reset_ray_address_file()
    if flush and get_mode() == "redis" and redis_ping():
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
        _sh(["ray", "status", "--address", f"{HOST}:{GCS_PORT}"], env=ray_cli_env())
