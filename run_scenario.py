#!/usr/bin/env python3
"""Orquestra um experimento: cluster limpo -> probes -> falha -> monitoramento -> relatório.

Cenários:
  gcs_kill       kill -9 só no gcs_server (crash parcial do head)
  gcs_freeze     SIGSTOP no gcs_server por --down s e depois SIGCONT (GCS indisponível sem perder memória)
  head_kill      kill -9 em TODA a árvore do head (GCS + raylet do head) (crash total do head)
  redis_outage   para o Redis por --down s com o cluster rodando
  cluster_kill   mata head + workers, espera --down s e sobe tudo de novo (recuperação 100% via storage)
"""
import argparse, json, os, shutil, signal, subprocess, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import psutil
from common import *
import analyze, cluster

HERE = pathlib.Path(__file__).resolve().parent


# ---------------- localização de processos ----------------
def head_tree():
    p = psutil.Process(int((RUN_DIR / "head.pid").read_text()))
    return p, p.children(recursive=True)


def find_gcs():
    _, kids = head_tree()
    for k in kids:
        if k.name().startswith("gcs_server"):
            return k
    raise RuntimeError("gcs_server não encontrado na árvore do head")


# ---------------- falhas ----------------
def f_gcs_kill(a):
    g = find_gcs()
    event("fault_gcs_kill", pid=g.pid)
    g.kill()


def f_gcs_freeze(a):
    g = find_gcs()
    event("fault_gcs_freeze", pid=g.pid, seconds=a.down)
    g.send_signal(signal.SIGSTOP)
    time.sleep(a.down)
    g.send_signal(signal.SIGCONT)
    event("fault_end_gcs_resume")


def f_head_kill(a):
    p, kids = head_tree()
    event("fault_head_kill", pids=[p.pid] + [k.pid for k in kids])
    for proc in [p] + kids:
        try:
            proc.kill()
        except psutil.NoSuchProcess:
            pass


def f_redis_outage(a):
    if get_mode() != "redis":
        sys.exit("redis_outage exige --storage redis")
    event("fault_redis_stop", seconds=a.down)
    cluster.redis_stop()
    time.sleep(a.down)
    cluster.redis_start()
    event("fault_end_redis_start")


def f_cluster_kill(a):
    event("fault_cluster_kill")
    cluster.kill_supervisor(signal.SIGKILL)
    subprocess.run(["ray", "stop", "--force"], capture_output=True, env=ray_cli_env())
    event("cluster_dead")
    time.sleep(a.down)
    event("fault_end_cluster_restart")
    cluster.start_head()
    event("head_back")
    cluster.start_workers()
    event("workers_restarted")


FAULTS = {"gcs_kill": f_gcs_kill, "gcs_freeze": f_gcs_freeze, "head_kill": f_head_kill,
          "redis_outage": f_redis_outage, "cluster_kill": f_cluster_kill}


# ---------------- monitor externo ----------------
class Monitor:
    """Registra transições da porta TCP do GCS (100ms) e do `ray health-check` (~1-2s)."""

    def __init__(self):
        self.stop_ev = threading.Event()

    def start(self):
        threading.Thread(target=self._tcp, daemon=True).start()
        threading.Thread(target=self._health, daemon=True).start()

    def _tcp(self):
        last = None
        while not self.stop_ev.is_set():
            up = tcp_up()
            if up != last:
                event("gcs_tcp_up" if up else "gcs_tcp_down")
                last = up
            time.sleep(0.1)

    def _health(self):
        last = None
        while not self.stop_ev.is_set():
            ts = time.time()
            ok = health_ok()
            if ok != last:
                event("gcs_health_ok" if ok else "gcs_health_fail", check_started_at=round(ts, 2))
                last = ok
            time.sleep(0.3)

    def stop(self):
        self.stop_ev.set()


def run(cmd, timeout):
    return subprocess.run([sys.executable, str(HERE / "probes.py"), cmd], capture_output=True, text=True, timeout=timeout)


def save_run_artifacts(run_dir):
    if EVENTS_FILE.exists():
        shutil.copy(EVENTS_FILE, run_dir / "events.jsonl")
    for f in ("head.log", "supervisor.log", "workers.log"):
        if (RUN_DIR / f).exists():
            shutil.copy(RUN_DIR / f, run_dir / f)
    for f in PROBE_DIR.glob("probe_*.jsonl"):
        shutil.copy(f, run_dir / f.name)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("scenario", choices=FAULTS)
    ap.add_argument("--storage", choices=["redis", "rocksdb", "none"], default="redis",
                    help="backend do GCS (none = baseline sem tolerância a falhas)")
    ap.add_argument("--down", type=float, default=10, help="duração da indisponibilidade (freeze/redis/cluster_kill)")
    ap.add_argument("--warmup", type=float, default=15)
    ap.add_argument("--observe", type=float, default=60, help="segundos observados após a falha")
    ap.add_argument("--restart-delay", type=float, default=2, help="atraso do supervisor para reiniciar o head")
    ap.add_argument("--reconnect-timeout", type=int, default=60, help="RAY_gcs_rpc_server_reconnect_timeout_s")
    ap.add_argument("--thr", type=float, default=2.0, help="limiar de latência considerado degradação")
    ap.add_argument("--keep", action="store_true", help="não derruba o cluster no fim")
    a = ap.parse_args()

    os.environ["LAB_RESTART_DELAY"] = str(a.restart_delay)
    os.environ["LAB_RECONNECT_TIMEOUT"] = str(a.reconnect_timeout)

    run_id = f"{time.strftime('%Y%m%d-%H%M%S')}_{a.scenario}_{a.storage}"
    run_dir = LOGS / run_id
    run_dir.mkdir(parents=True)
    EVENTS_FILE.unlink(missing_ok=True)
    (run_dir / "params.json").write_text(json.dumps(vars(a), indent=2))

    try:
        event("scenario_init", **vars(a))
        cluster.down(flush=True)
        cluster.up(a.storage)
        r = run("setup", 300)
        if r.returncode != 0:
            print(r.stdout, r.stderr); sys.exit("falha no setup dos probes")
        event("probes_ready")
        time.sleep(a.warmup)

        mon = Monitor(); mon.start()
        time.sleep(1.5)
        FAULTS[a.scenario](a)
        time.sleep(a.observe)
        mon.stop()
        event("observe_end")

        # Estado sobreviveu? (KV marcador + detached actors)
        try:
            v = run("verify", 120)
            line = next((l for l in v.stdout.splitlines() if l.startswith("VERIFY_JSON ")), None)
            event("verify_state", **(json.loads(line[12:]) if line else {"error": (v.stderr or v.stdout)[-300:]}))
        except subprocess.TimeoutExpired:
            event("verify_state", error="timeout (cluster inacessível)")

        save_run_artifacts(run_dir)
        analyze.report(run_dir, a.thr)
    finally:
        save_run_artifacts(run_dir)
        if not a.keep:
            cluster.down(flush=True)


if __name__ == "__main__":
    main()
