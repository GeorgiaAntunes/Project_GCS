"""Constantes, helpers e comandos `ray start` compartilhados pelo laboratório."""
from __future__ import annotations
import json, os, pathlib, socket, subprocess, sys, time

ROOT = pathlib.Path(__file__).resolve().parent
RUN_DIR = ROOT / ".run"
LOGS = ROOT / "logs"
PROBE_DIR = LOGS / "probes"
for _d in (RUN_DIR, LOGS, PROBE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

HOST = "127.0.0.1"
GCS_PORT = int(os.getenv("LAB_GCS_PORT", 6380))
REDIS_PORT = int(os.getenv("LAB_REDIS_PORT", 6399))
REDIS_PASSWORD = os.getenv("LAB_REDIS_PASSWORD", "raypass")
REDIS_CONTAINER = "ray-ft-redis"
STORAGE_NS = "ray-ft-lab"      # RAY_external_storage_namespace: tem de ser o MESMO entre restarts
HEAD_NM_PORT = 6390
HEAD_OM_PORT = 6391
WORKERS = ["wk_a", "wk_b"]     # cada worker expõe um recurso custom com o próprio nome
EVENTS_FILE = RUN_DIR / "events.jsonl"


def reconnect_timeout() -> int:
    return int(os.getenv("LAB_RECONNECT_TIMEOUT", 60))


def restart_delay() -> float:
    return float(os.getenv("LAB_RESTART_DELAY", 2))


def get_mode() -> str:
    p = RUN_DIR / "mode"
    return p.read_text().strip() if p.exists() else "redis"


def set_mode(mode: str) -> None:
    (RUN_DIR / "mode").write_text(mode)


def event(name: str, **kw) -> None:
    rec = {"t": time.time(), "event": name, **kw}
    with open(EVENTS_FILE, "a") as f:
        f.write(json.dumps(rec) + "\n")
    print(f"[{time.strftime('%H:%M:%S')}] {name} {kw if kw else ''}", flush=True)


def ray_cli_env() -> dict:
    env = os.environ.copy()
    env["RAY_USAGE_STATS_ENABLED"] = "0"
    if sys.platform in ("darwin", "win32"):
        env["RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER"] = "1"
    return env


def ray_env(mode: str) -> dict:
    env = ray_cli_env()
    # Quanto tempo raylets/workers tentam reconectar ao GCS antes de desistir (default Ray: 60s)
    env["RAY_gcs_rpc_server_reconnect_timeout_s"] = str(reconnect_timeout())
    if mode == "redis":
        env["RAY_REDIS_ADDRESS"] = f"{HOST}:{REDIS_PORT}"
        env["RAY_external_storage_namespace"] = STORAGE_NS
    elif mode == "rocksdb":   # alpha / versões recentes do Ray
        env["RAY_gcs_storage"] = "rocksdb"
        env["RAY_gcs_storage_path"] = str(RUN_DIR / "rocksdb")
    return env


def head_cmd(mode: str, block: bool = True) -> list[str]:
    c = ["ray", "start", "--head", "--node-ip-address", HOST, "--port", str(GCS_PORT),
         "--node-manager-port", str(HEAD_NM_PORT), "--object-manager-port", str(HEAD_OM_PORT),
         "--include-dashboard=false", "--num-cpus", "1", "--resources", json.dumps({"head": 1})]
    if mode == "redis":
        c += ["--redis-password", REDIS_PASSWORD, "--redis-username", "default"]
    if block:
        c.append("--block")
    return c


def worker_cmd(name: str) -> list[str]:
    return ["ray", "start", "--address", f"{HOST}:{GCS_PORT}", "--node-ip-address", HOST,
            "--num-cpus", "2", "--resources", json.dumps({name: 1})]


def reset_ray_address_file() -> None:
    """Apaga <temp_dir_do_ray>/ray_current_cluster (ver head_supervisor.py)."""
    try:
        from ray._common.utils import get_ray_address_file
        path = get_ray_address_file(None)
    except Exception:  # noqa  (versões antigas do Ray)
        path = "/tmp/ray/ray_current_cluster"
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def tcp_up(port: int = GCS_PORT, timeout: float = 0.3) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=timeout):
            return True
    except OSError:
        return False


def wait_tcp(port: int, timeout: float = 60) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if tcp_up(port):
            return True
        time.sleep(0.2)
    return False


def health_ok(timeout: float = 5) -> bool:
    """`ray health-check` contra o GCS (resolução ~1-2s por causa do import do ray)."""
    try:
        r = subprocess.run(["ray", "health-check", "--address", f"{HOST}:{GCS_PORT}"],
                           capture_output=True, timeout=timeout,
                           env=ray_cli_env())
        return r.returncode == 0
    except subprocess.TimeoutExpired:
        return False
