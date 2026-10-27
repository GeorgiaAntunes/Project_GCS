#!/usr/bin/env python3
"""Probes que rodam DENTRO dos workers (detached actors) e gravam JSONL localmente.
Assim observamos o comportamento do worker mesmo quando GCS/head estão fora.
  python lab/probes.py setup    -> cria os actors + marcador no KV (persistido no Redis)
  python lab/probes.py verify   -> checa se KV e actors sobreviveram"""
import json, os, sys, threading, time, uuid
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from common import *


@ray.remote(num_cpus=0)
def _noop():
    return 1


@ray.remote(num_cpus=0, max_restarts=0, max_concurrency=4)
class Heartbeat:
    def __init__(self, name, peer, log_path, interval=0.5, task_timeout=5.0):
        self.name, self.peer, self.interval, self.tt = name, peer, interval, task_timeout
        self.lock = threading.Lock()
        self.fh = open(log_path, "a", buffering=1)
        self.node_id = ray.get_runtime_context().get_node_id()
        self.n = 0
        self.started = False

    def start(self):
        """Dispara as threads dos probes (separado do __init__ para o setup não competir com elas)."""
        if self.started:
            return True
        self.started = True
        probes = [
            ("local", self._tick),                    # não toca em Ray: prova que o processo vive
            ("task_local", self._task_local),         # tarefa presa ao próprio nó (só raylet local)
            ("task_any", self._task_any),             # tarefa em qualquer nó (precisa de visão do cluster)
            ("gcs_nodes", self._nodes),               # RPC direta ao GCS
            ("actor_lookup", self._lookup),           # lookup por nome no GCS
        ]
        for probe, fn in probes:
            threading.Thread(target=self._loop, args=(probe, fn), daemon=True).start()
        return True

    def _w(self, probe, t_start, ok, **extra):
        t = time.time()
        rec = {"t": t, "t_start": t_start, "probe": probe, "ok": ok, "lat": t - t_start, **extra}
        with self.lock:
            self.fh.write(json.dumps(rec) + "\n")

    def _loop(self, probe, fn):
        while True:
            t0 = time.time()
            try:
                self._w(probe, t0, True, **(fn() or {}))
            except Exception as e:  # noqa
                self._w(probe, t0, False, err=type(e).__name__)
            time.sleep(max(0.0, self.interval - (time.time() - t0)))

    def _tick(self):
        self.n += 1
        return {"n": self.n}

    def _task_local(self):
        s = NodeAffinitySchedulingStrategy(node_id=self.node_id, soft=False)
        ray.get(_noop.options(scheduling_strategy=s).remote(), timeout=self.tt)

    def _task_any(self):
        ray.get(_noop.remote(), timeout=self.tt)

    def _nodes(self):
        return {"alive": sum(1 for n in ray.nodes() if n["Alive"])}

    def _lookup(self):
        ray.get_actor(self.peer, namespace="ft")

    def ping(self):
        return self.n


def _init():
    ray.init(address=f"{HOST}:{GCS_PORT}", namespace="ft", logging_level="ERROR")


def setup():
    from ray.experimental.internal_kv import _internal_kv_put
    for f in PROBE_DIR.glob("probe_*.jsonl"):
        f.unlink()
    _init()
    end = time.time() + 90
    while not all(ray.cluster_resources().get(w, 0) >= 1 for w in WORKERS):
        if time.time() > end:
            raise RuntimeError("workers não registraram a tempo")
        time.sleep(1)
    token = uuid.uuid4().hex
    _internal_kv_put(b"ft_marker", token.encode(), overwrite=True, namespace=b"ft")
    (RUN_DIR / "marker").write_text(token)
    actors = []
    for i, w in enumerate(WORKERS):
        peer = WORKERS[(i + 1) % len(WORKERS)]
        a = Heartbeat.options(name=f"hb_{w}", namespace="ft", lifetime="detached",
                              resources={w: 0.1}).remote(w, f"hb_{peer}", str(PROBE_DIR / f"probe_{w}.jsonl"))
        actors.append(a)
    ray.get([a.ping.remote() for a in actors], timeout=120)   # os dois actors sobem em paralelo
    ray.get([a.start.remote() for a in actors], timeout=60)
    print("probes ok")
    ray.shutdown()


def verify():
    from ray.experimental.internal_kv import _internal_kv_get
    _init()
    marker = (RUN_DIR / "marker").read_text()
    v = _internal_kv_get(b"ft_marker", namespace=b"ft")
    res = {"kv_marker_preserved": v is not None and v.decode() == marker, "actors": {}}
    for w in WORKERS:
        try:
            a = ray.get_actor(f"hb_{w}", namespace="ft")
            ray.get(a.ping.remote(), timeout=10)
            res["actors"][w] = "alive"
        except ValueError:
            res["actors"][w] = "not_found"
        except Exception as e:  # noqa
            res["actors"][w] = type(e).__name__
    print("VERIFY_JSON " + json.dumps(res))
    ray.shutdown()


if __name__ == "__main__":
    {"setup": setup, "verify": verify}[sys.argv[1]]()
