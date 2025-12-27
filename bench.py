#!/usr/bin/env python3
"""
bench.py — Minimal benchmarking client (Gateway -> ProxySQL -> MySQL cluster)

Changes vs previous:
  - NO "N threads" load generator.
  - Only TWO concurrent streams:
      * one sequential READ stream
      * one sequential WRITE stream
    running in parallel (2 threads total).

Generates ONLY graphs that can be produced from this client-side program:
  - TPS vs time (1s buckets), split READ/WRITE
  - Latency vs time (1s buckets), split READ/WRITE (mean/p50/p95/p99/max)
  - Summary counts (sent vs ok) per phase and combined
  - raw_requests.csv (audit/debug; optional)

Does NOT prove routing correctness (manager vs workers) unless your Gateway returns a JSON
field "target". If present, it is logged in raw_requests.csv for later use, but no routing
graphs are generated here.

Outputs (in --outdir):
  - summary.csv
  - tps_timeseries.csv
  - latency_timeseries.csv
  - raw_requests.csv   (unless --no-raw)

Usage:
  python3 bench.py \
    --gateway-url http://<GATEWAY_PUBLIC_IP> \
    --api-key <KEY> \
    --strategy random \
    --reads 1000 --writes 1000 \
    --outdir ./benchmarking/random
"""

import argparse
import csv
import json
import os
import time
import threading
import urllib.request
import urllib.error
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


# ------------------------
# Helpers
# ------------------------

def iso_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def percentile(values: List[float], p: float) -> Optional[float]:
    """Simple deterministic percentile with linear interpolation."""
    if not values:
        return None
    if p <= 0:
        return min(values)
    if p >= 100:
        return max(values)
    s = sorted(values)
    k = (p / 100.0) * (len(s) - 1)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    if hi == lo:
        return s[lo]
    frac = k - lo
    return s[lo] * (1.0 - frac) + s[hi] * frac


def http_post_json(url: str, api_key: str, payload: Dict[str, Any], timeout_s: float = 10.0) -> Tuple[int, str]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": api_key,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.getcode(), body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if e.fp else str(e)
        return e.code, body
    except Exception as e:
        return 0, str(e)


# ------------------------
# Data model
# ------------------------

@dataclass
class RequestRecord:
    phase: str           # "parallel_rw"
    kind: str            # "read" or "write"
    ok: int              # 1 if HTTP 200 else 0
    http_code: int
    lat_ms: float
    t_wall_end: float    # time.time() end timestamp (bucketing)
    iso_end: str
    target: str          # optional from response JSON, else "unknown"


# ------------------------
# Two-stream runner (READ stream + WRITE stream)
# ------------------------

def run_stream(
    kind: str,
    n: int,
    endpoint: str,
    api_key: str,
    sql: str,
    timeout_s: float,
    phase: str,
    out: List[RequestRecord],
    lock: threading.Lock,
) -> None:
    for _ in range(n):
        t0 = time.perf_counter()
        code, body = http_post_json(endpoint, api_key, {"query": sql}, timeout_s=timeout_s)
        t1 = time.perf_counter()

        lat_ms = (t1 - t0) * 1000.0
        ok = 1 if code == 200 else 0

        target = "unknown"
        if ok and body:
            # Optional: if your gateway returns {"target":"manager|worker1|worker2"}.
            try:
                j = json.loads(body)
                if isinstance(j, dict):
                    target = str(j.get("target", "unknown"))
            except Exception:
                pass

        t_wall_end = time.time()
        rec = RequestRecord(
            phase=phase,
            kind=kind,
            ok=ok,
            http_code=code,
            lat_ms=lat_ms,
            t_wall_end=t_wall_end,
            iso_end=iso_utc(t_wall_end),
            target=target,
        )
        with lock:
            out.append(rec)


def run_parallel_reads_writes(
    endpoint: str,
    api_key: str,
    n_reads: int,
    n_writes: int,
    read_sql: str,
    write_sql: str,
    timeout_s: float,
) -> Tuple[List[RequestRecord], float]:
    records: List[RequestRecord] = []
    lock = threading.Lock()

    phase = "parallel_rw"

    t0 = time.time()

    th_r = threading.Thread(
        target=run_stream,
        args=("read", n_reads, endpoint, api_key, read_sql, timeout_s, phase, records, lock),
        daemon=True,
    )
    th_w = threading.Thread(
        target=run_stream,
        args=("write", n_writes, endpoint, api_key, write_sql, timeout_s, phase, records, lock),
        daemon=True,
    )

    th_r.start()
    th_w.start()
    th_r.join()
    th_w.join()

    t1 = time.time()
    return records, max(1e-9, t1 - t0)


# ------------------------
# Aggregations (Graph A + Graph B) + Summary
# ------------------------

def compute_tps_timeseries(records: List[RequestRecord]) -> List[Dict[str, Any]]:
    if not records:
        return []

    rs = sorted(records, key=lambda r: r.t_wall_end)
    t_start = int(rs[0].t_wall_end)
    t_end = int(rs[-1].t_wall_end)

    buckets: Dict[int, Dict[str, int]] = {}
    for t in range(t_start, t_end + 1):
        buckets[t] = {
            "total": 0, "read": 0, "write": 0,
            "ok_total": 0, "ok_read": 0, "ok_write": 0,
        }

    for r in rs:
        b = int(r.t_wall_end)
        if b not in buckets:
            buckets[b] = {"total": 0, "read": 0, "write": 0, "ok_total": 0, "ok_read": 0, "ok_write": 0}
        buckets[b]["total"] += 1
        buckets[b][r.kind] += 1
        buckets[b]["ok_total"] += r.ok
        buckets[b]["ok_" + r.kind] += r.ok

    rows: List[Dict[str, Any]] = []
    for b in sorted(buckets.keys()):
        c = buckets[b]
        rows.append({
            "iso": iso_utc(float(b)),
            "t_sec": b,
            "total_tps": c["total"],
            "read_tps": c["read"],
            "write_tps": c["write"],
            "total_count": c["total"],
            "read_count": c["read"],
            "write_count": c["write"],
            "ok_total": c["ok_total"],
            "ok_read": c["ok_read"],
            "ok_write": c["ok_write"],
        })
    return rows


def compute_latency_timeseries(records: List[RequestRecord]) -> List[Dict[str, Any]]:
    """
    1-second buckets, per kind, computed over successful requests (HTTP 200) only.
    """
    if not records:
        return []

    rs = sorted(records, key=lambda r: r.t_wall_end)
    t_start = int(rs[0].t_wall_end)
    t_end = int(rs[-1].t_wall_end)

    buckets: Dict[int, Dict[str, List[float]]] = {}
    for t in range(t_start, t_end + 1):
        buckets[t] = {"read": [], "write": []}

    for r in rs:
        if r.ok != 1:
            continue
        b = int(r.t_wall_end)
        if b not in buckets:
            buckets[b] = {"read": [], "write": []}
        buckets[b][r.kind].append(r.lat_ms)

    def stats(prefix: str, vals: List[float]) -> Dict[str, Any]:
        if not vals:
            return {
                f"{prefix}_count": 0,
                f"{prefix}_mean_ms": "",
                f"{prefix}_p50_ms": "",
                f"{prefix}_p95_ms": "",
                f"{prefix}_p99_ms": "",
                f"{prefix}_max_ms": "",
            }
        mean = sum(vals) / len(vals)
        p50 = percentile(vals, 50.0)
        p95 = percentile(vals, 95.0)
        p99 = percentile(vals, 99.0)
        mx = max(vals)
        return {
            f"{prefix}_count": len(vals),
            f"{prefix}_mean_ms": f"{mean:.3f}",
            f"{prefix}_p50_ms": f"{(p50 if p50 is not None else 0.0):.3f}",
            f"{prefix}_p95_ms": f"{(p95 if p95 is not None else 0.0):.3f}",
            f"{prefix}_p99_ms": f"{(p99 if p99 is not None else 0.0):.3f}",
            f"{prefix}_max_ms": f"{mx:.3f}",
        }

    rows: List[Dict[str, Any]] = []
    for b in sorted(buckets.keys()):
        row = {
            "iso": iso_utc(float(b)),
            "t_sec": b,
            **stats("read", buckets[b]["read"]),
            **stats("write", buckets[b]["write"]),
        }
        rows.append(row)

    return rows


def compute_summary(records: List[RequestRecord], duration_s: float, strategy: str) -> Dict[str, Any]:
    total = len(records)
    reads = sum(1 for r in records if r.kind == "read")
    writes = total - reads
    ok_total = sum(r.ok for r in records)
    ok_reads = sum(r.ok for r in records if r.kind == "read")
    ok_writes = ok_total - ok_reads

    return {
        "strategy": strategy,
        "phase": "parallel_rw",
        "duration_s": f"{duration_s:.3f}",
        "total_sent": total,
        "read_sent": reads,
        "write_sent": writes,
        "ok_total": ok_total,
        "ok_read": ok_reads,
        "ok_write": ok_writes,
        "avg_tps_total": f"{(total / max(1e-9, duration_s)):.3f}",
        "avg_tps_ok": f"{(ok_total / max(1e-9, duration_s)):.3f}",
    }


# ------------------------
# CSV writing
# ------------------------

def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError(f"No rows to write: {path}")
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def write_raw_requests(path: str, records: List[RequestRecord], strategy: str) -> None:
    rows: List[Dict[str, Any]] = []
    for r in records:
        rows.append({
            "strategy": strategy,
            "phase": r.phase,
            "iso_end": r.iso_end,
            "t_end": f"{r.t_wall_end:.6f}",
            "kind": r.kind,
            "ok": r.ok,
            "http_code": r.http_code,
            "lat_ms": f"{r.lat_ms:.3f}",
            "target": r.target,
        })
    write_csv(path, rows)


# ------------------------
# Defaults (your fixed queries)
# ------------------------

FIXED_WRITE_SQL = (
    "INSERT INTO sakila.bench_events (created_at, payload) "
    "VALUES (NOW(6), 'x')"
)

FIXED_READ_SQL = (
    "SELECT actor_id, first_name, last_name "
    "FROM sakila.actor "
    "WHERE actor_id = 1"
)


# ------------------------
# Main
# ------------------------stats.stats_mysql_connection_pool


def main() -> int:
    ap = argparse.ArgumentParser(description="Benchmark: 2 parallel streams (READ + WRITE), TPS + latency series")
    ap.add_argument("--gateway-url", required=True, help="e.g. http://<GATEWAY_PUBLIC_IP>")
    ap.add_argument("--api-key", default="MY_API_KEY", help="API key for X-API-Key")
    ap.add_argument("--strategy", required=True, choices=["direct", "random", "customized"], help="Label for outputs")
    ap.add_argument("--reads", type=int, default=1000, help="Number of READ requests (default 1000)")
    ap.add_argument("--writes", type=int, default=1000, help="Number of WRITE requests (default 1000)")
    ap.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout seconds (default 10)")
    ap.add_argument("--endpoint", default="/query", help="Gateway endpoint path (default /query)")
    ap.add_argument("--read-sql", default=FIXED_READ_SQL, help="READ query")
    ap.add_argument("--write-sql", default=FIXED_WRITE_SQL, help="WRITE query (INSERT/UPDATE/DELETE)")
    ap.add_argument("--outdir", default="./benchmarking", help="Output directory")
    ap.add_argument("--no-raw", action="store_true", help="Do not write raw_requests.csv")
    args = ap.parse_args()

    base = args.gateway_url.rstrip("/")
    endpoint = base + args.endpoint

    ensure_dir(args.outdir)

    # One run: READ stream and WRITE stream in parallel
    records, dur = run_parallel_reads_writes(
        endpoint=endpoint,
        api_key=args.api_key,
        n_reads=args.reads,
        n_writes=args.writes,
        read_sql=args.read_sql,
        write_sql=args.write_sql,
        timeout_s=args.timeout,
    )

    # Outputs
    summary_path = os.path.join(args.outdir, "summary.csv")
    tps_path = os.path.join(args.outdir, "tps_timeseries.csv")
    lat_path = os.path.join(args.outdir, "latency_timeseries.csv")
    raw_path = os.path.join(args.outdir, "raw_requests.csv")

    summary_row = compute_summary(records, dur, args.strategy)
    write_csv(summary_path, [summary_row])

    tps_rows = compute_tps_timeseries(records)
    for r in tps_rows:
        r["strategy"] = args.strategy
    write_csv(tps_path, tps_rows)

    lat_rows = compute_latency_timeseries(records)
    for r in lat_rows:
        r["strategy"] = args.strategy
    write_csv(lat_path, lat_rows)

    if not args.no_raw:
        write_raw_requests(raw_path, records, args.strategy)

    # Console report + hard alignment checks
    print(f"[{args.strategy}] done.")
    print(f"  sent: total={summary_row['total_sent']} reads={summary_row['read_sent']} writes={summary_row['write_sent']}")
    print(f"  ok:   total={summary_row['ok_total']} reads={summary_row['ok_read']} writes={summary_row['ok_write']}")
    print(f"  duration_s={summary_row['duration_s']} avg_tps_ok={summary_row['avg_tps_ok']}")
    print(f"  wrote: {summary_path}")
    print(f"  wrote: {tps_path}")
    print(f"  wrote: {lat_path}")
    if not args.no_raw:
        print(f"  wrote: {raw_path}")

    if int(summary_row["read_sent"]) != args.reads or int(summary_row["write_sent"]) != args.writes:
        print("WARNING: sent counts do not match requested reads/writes (unexpected).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

