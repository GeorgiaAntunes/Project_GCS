#!/usr/bin/env python3
"""Cruza events.jsonl (orquestrador/supervisor/monitor) com os probes dos workers.
Uso: python lab/analyze.py logs/<run_id> [--thr 2.0]"""
import argparse, json, os, statistics, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import *


def _load(p):
    out = []
    for line in open(p):
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def _probe(worker, probe, rs, t0, t_end, thr):
    rs.sort(key=lambda r: r["t"])
    pre = [r for r in rs if r["t"] < t0 and r["ok"]]
    post = [r for r in rs if r["t"] >= t0]
    row = {"worker": worker, "probe": probe,
           "p50_pre_ms": round(statistics.median([r["lat"] for r in pre]) * 1000, 1) if pre else None,
           "fails": sum(1 for r in post if not r["ok"]),
           "max_lat_s": round(max([r["lat"] for r in post], default=0), 2),
           "status": "NO IMPACT", "degraded_at": None, "recovered_at": None, "outage_s": None}
    if not post or t_end - rs[-1]["t"] > 5:
        row["status"] = "SILENT"   # parou de logar: worker/actor morreu ou ficou bloqueado até o fim
        row["silent_for_s"] = round(t_end - (rs[-1]["t"] if rs else t0), 1)
    bad_i = next((i for i, r in enumerate(post) if (not r["ok"]) or r["lat"] > thr), None)
    if bad_i is not None:
        bad = post[bad_i]
        row["degraded_at"] = round(max(bad["t_start"], t0) - t0, 2)
        rec = next((r for r in post[bad_i + 1:] if r["ok"] and r["lat"] <= thr and r["t_start"] >= bad["t"]), None)
        if rec:
            row["status"] = "RECOVERED" if row["status"] != "SILENT" else "SILENT"
            row["recovered_at"] = round(rec["t_start"] - t0, 2)
            row["outage_s"] = round(rec["t_start"] - max(bad["t_start"], t0), 2)
        elif row["status"] != "SILENT":
            row["status"] = "NOT RECOVERED"
    if probe == "gcs_nodes":
        alive_pre = max([r.get("alive", 0) for r in pre], default=0)
        alive_post = [r for r in post if r["ok"]]
        if alive_post:
            low = min(alive_post, key=lambda r: r.get("alive", 0))
            row["alive_min"] = low.get("alive")
            back = next((r for r in alive_post if r["t"] > low["t"] and r.get("alive", 0) >= alive_pre), None)
            row["alive_back_at"] = round(back["t_start"] - t0, 2) if back else None
            row["alive_expected"] = alive_pre
    return row


def report(run_dir, thr=2.0, pre_window=15):
    run_dir = pathlib.Path(run_dir)
    ev = _load(run_dir / "events.jsonl")
    faults = [e for e in ev if e["event"].startswith("fault_") and not e["event"].startswith("fault_end")]
    if not faults:
        print("nenhum evento fault_* encontrado"); return
    t0 = faults[0]["t"]
    t_end = next((e["t"] for e in ev if e["event"] == "observe_end"), ev[-1]["t"])

    print("\n=== LINHA DO TEMPO (relativa à injeção da falha) ===")
    for e in ev:
        if e["t"] >= t0 - 1:
            extra = {k: v for k, v in e.items() if k not in ("t", "event")}
            print(f"  {e['t'] - t0:+8.2f}s  {e['event']:<28} {extra if extra else ''}")

    rows = []
    for f in sorted(PROBE_DIR.glob("probe_*.jsonl")):
        worker = f.stem.replace("probe_", "")
        by = {}
        for r in _load(f):
            if t0 - pre_window <= r["t_start"] <= t_end:
                by.setdefault(r["probe"], []).append(r)
        for probe, rs in by.items():
            rows.append(_probe(worker, probe, rs, t0, t_end, thr))

    print(f"\n=== PROBES NOS WORKERS (degradação: erro ou latência > {thr}s) ===")
    hdr = f"{'worker':<6} {'probe':<13} {'status':<14} {'p50 pré':>8} {'degrad@':>8} {'recup@':>8} {'outage':>8} {'falhas':>6} {'lat máx':>8}"
    print(hdr); print("-" * len(hdr))
    f = lambda v, s="": "-" if v is None else f"{v}{s}"
    for r in rows:
        print(f"{r['worker']:<6} {r['probe']:<13} {r['status']:<14} {f(r['p50_pre_ms'], 'ms'):>8} "
              f"{f(r['degraded_at'], 's'):>8} {f(r['recovered_at'], 's'):>8} {f(r['outage_s'], 's'):>8} "
              f"{r['fails']:>6} {r['max_lat_s']:>7}s")
    for r in rows:
        if r["probe"] == "gcs_nodes" and "alive_min" in r:
            print(f"  nós vivos visto por {r['worker']}: mín={r['alive_min']} (esperado {r['alive_expected']}), "
                  f"voltou a {r['alive_expected']} em {f(r['alive_back_at'], 's')}")
    (run_dir / "summary.json").write_text(json.dumps({"t_fault": t0, "rows": rows}, indent=2))
    print(f"\nsummary: {run_dir / 'summary.json'}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("run_dir")
    ap.add_argument("--thr", type=float, default=2.0)
    a = ap.parse_args()
    report(a.run_dir, a.thr)
