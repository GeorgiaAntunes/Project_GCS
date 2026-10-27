#!/usr/bin/env python3
"""Simula o kubelet/KubeRay: se o processo do head (ray start --head --block) morre
(ex.: gcs_server crashou), reinicia o head com o MESMO endereço e o MESMO storage.
Em modo 'none' o novo GCS nasce vazio (baseline sem tolerância a falhas).

Detalhe importante para o cluster "multi-nó" numa máquina só: antes de subir um head novo,
o `ray start --head` verifica se já existe um cluster no mesmo endereço olhando
(1) o arquivo <temp_dir>/ray_current_cluster e (2) os raylets locais que apontam para esse GCS.
Como os raylets dos workers continuam vivos (tentando reconectar), essa verificação
sempre falharia com "Ray is trying to start at X:6380, but is already running at X:6380".
Num cluster real (máquinas/pods diferentes) isso não acontece. Por isso o supervisor
remove o ray_current_cluster antes de cada restart e espera as portas fixas do head liberarem."""
import os, signal, socket, subprocess, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *

child = None


def _term(*_):
    if child and child.poll() is None:
        child.terminate()
        try:
            child.wait(10)
        except subprocess.TimeoutExpired:
            child.kill()
    sys.exit(0)


def _port_free(port) -> bool:
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)   # ignora TIME_WAIT
    try:
        s.bind(("0.0.0.0", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def _wait_ports_free(ports, timeout=20) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        busy = [p for p in ports if not _port_free(p)]
        if not busy:
            return True
        time.sleep(0.3)
    event("head_ports_still_busy", ports=busy)
    return False


def main():
    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)
    global child
    n = 0
    log = open(RUN_DIR / "head.log", "a")
    while not (RUN_DIR / "stop").exists():
        mode = get_mode()
        reset_ray_address_file()
        _wait_ports_free([GCS_PORT, HEAD_NM_PORT, HEAD_OM_PORT])
        t0 = time.time()
        child = subprocess.Popen(head_cmd(mode), env=ray_env(mode), stdout=log, stderr=subprocess.STDOUT)
        (RUN_DIR / "head.pid").write_text(str(child.pid))
        event("head_process_started", restart=n, pid=child.pid, mode=mode)
        rc = child.wait()
        event("head_process_exited", rc=rc, uptime=round(time.time() - t0, 1))
        if (RUN_DIR / "stop").exists():
            break
        time.sleep(restart_delay())
        n += 1


if __name__ == "__main__":
    main()
