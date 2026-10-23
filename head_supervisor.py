#!/usr/bin/env python3
"""Simula o kubelet/KubeRay: se o processo do head (ray start --head --block) morre
(ex.: gcs_server crashou), reinicia o head com o MESMO endereço e o MESMO storage.
Em modo 'none' o novo GCS nasce vazio (baseline sem tolerância a falhas)."""
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


def _wait_port_free(port, timeout=15):
    end = time.time() + timeout
    while time.time() < end:
        s = socket.socket()
        try:
            s.bind((HOST, port)); s.close(); return
        except OSError:
            s.close(); time.sleep(0.3)


def main():
    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)
    global child
    n = 0
    log = open(RUN_DIR / "head.log", "a")
    while not (RUN_DIR / "stop").exists():
        mode = get_mode()
        _wait_port_free(HEAD_NM_PORT)
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
