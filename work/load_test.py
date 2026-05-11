"""
Load test for the Synexis Rep Agent backend (item ④ from CODE_BRIEFING.md).

Sends N parallel requests to /query and reports p50 / p95 / p99 latency.
Detects whether the Starter-tier Render instance shows CPU contention under
concurrent load vs. single-query baseline.

Usage:
    python work/load_test.py                        # defaults: 10 workers, live Render URL
    python work/load_test.py --url http://127.0.0.1:8000 --workers 5
    python work/load_test.py --key YOUR_PARTNER_KEY --workers 10

Environment:
    PARTNER_KEY  — optional bearer token (falls back to --key arg, then anonymous)
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import statistics
import time
import uuid
from typing import Optional

import urllib.request
import urllib.error

BASE_URL = "https://synexis-rep-agent.onrender.com"

# Representative questions spread across different topic areas so retrieval
# and generation aren't trivially identical across workers.
QUESTIONS = [
    "What pathogens is DHP effective against?",
    "Can DHP be used in a walk-in cooler?",
    "How does DHP work in a poultry hatchery?",
    "What are the maintenance intervals for the Sentry XL?",
    "Does DHP affect food safety in meat processing facilities?",
    "What does the NEJM study say about HAI rates?",
    "Is DHP safe for use around patients in an ICU?",
    "How does DHP compare to UV-C for surface disinfection?",
    "Can DHP be used in a USP 797 compliant pharmacy cleanroom?",
    "What is the contact time for SARS-CoV-2 reduction?",
]


def _make_request(
    url: str,
    query: str,
    session_id: str,
    partner_key: Optional[str],
    turn_id: int = 1,
    timeout: int = 90,
) -> dict:
    """Send one /query POST and return a timing dict."""
    payload = json.dumps({
        "query": query,
        "session_id": session_id,
        "turn_id": turn_id,
        "user": "load_test",
        "history": [],
    }).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",  # non-streaming for load test simplicity
    }
    if partner_key:
        headers["Authorization"] = f"Bearer {partner_key}"

    t0 = time.perf_counter()
    status = None
    error = None
    server_timing: dict = {}

    try:
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.status
            body = json.loads(resp.read().decode("utf-8"))
            server_timing = body.get("timing") or {}
    except urllib.error.HTTPError as exc:
        status = exc.code
        error = str(exc)
    except Exception as exc:
        error = str(exc)

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    return {
        "query": query[:60],
        "session_id": session_id,
        "status": status,
        "elapsed_ms": elapsed_ms,
        "server_timing": server_timing,
        "error": error,
    }


def _baseline(url: str, partner_key: Optional[str]) -> dict:
    """Single-query baseline before concurrent run."""
    print("Running single-query baseline…")
    q = QUESTIONS[0]
    result = _make_request(url, q, session_id=str(uuid.uuid4()), partner_key=partner_key)
    print(f"  Baseline: {result['elapsed_ms']} ms  (status={result['status']})")
    if result["error"]:
        print(f"  Error: {result['error']}")
    return result


def _concurrent_run(url: str, partner_key: Optional[str], workers: int) -> list[dict]:
    """Fire `workers` requests in parallel, one unique session per worker."""
    print(f"\nRunning {workers}-worker concurrent test…")
    tasks = []
    for i in range(workers):
        tasks.append({
            "query": QUESTIONS[i % len(QUESTIONS)],
            "session_id": str(uuid.uuid4()),
        })

    results = []
    t_wall_start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _make_request,
                url,
                t["query"],
                t["session_id"],
                partner_key,
            ): t
            for t in tasks
        }
        for fut in concurrent.futures.as_completed(futures):
            results.append(fut.result())
    t_wall_ms = int((time.perf_counter() - t_wall_start) * 1000)

    return results, t_wall_ms


def _percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0.0
    sorted_d = sorted(data)
    k = (len(sorted_d) - 1) * pct / 100
    lo = int(k)
    hi = lo + 1
    if hi >= len(sorted_d):
        return sorted_d[-1]
    return sorted_d[lo] + (k - lo) * (sorted_d[hi] - sorted_d[lo])


def _report(baseline: dict, results: list[dict], wall_ms: int, workers: int) -> None:
    latencies = [r["elapsed_ms"] for r in results if not r["error"]]
    errors = [r for r in results if r["error"]]

    print(f"\n{'─'*60}")
    print(f"  LOAD TEST RESULTS  ({workers} concurrent workers)")
    print(f"{'─'*60}")
    print(f"  Baseline (single query):  {baseline['elapsed_ms']} ms")
    print()
    if latencies:
        print(f"  Concurrent — p50:  {_percentile(latencies, 50):.0f} ms")
        print(f"  Concurrent — p95:  {_percentile(latencies, 95):.0f} ms")
        print(f"  Concurrent — p99:  {_percentile(latencies, 99):.0f} ms")
        print(f"  Concurrent — min:  {min(latencies)} ms")
        print(f"  Concurrent — max:  {max(latencies)} ms")
        p95 = _percentile(latencies, 95)
        delta_pct = ((p95 - baseline["elapsed_ms"]) / max(baseline["elapsed_ms"], 1)) * 100
        print(f"\n  p95 vs baseline:   {delta_pct:+.1f}%  ", end="")
        if delta_pct > 20:
            print("⚠️  >20% spike — consider upgrading Render tier")
        else:
            print("✅  within acceptable range (<20%)")
    print(f"\n  Wall clock for all {workers} concurrent queries:  {wall_ms} ms")
    print(f"  Successes: {len(latencies)}/{workers}   Errors: {len(errors)}/{workers}")
    if errors:
        print("\n  Errors:")
        for e in errors:
            print(f"    [{e['status']}] {e['query']} — {e['error']}")
    print(f"{'─'*60}\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Render load test for Synexis Rep Agent")
    ap.add_argument("--url", default=BASE_URL, help="Backend base URL")
    ap.add_argument("--workers", type=int, default=10, help="Concurrent workers (default: 10)")
    ap.add_argument("--key", default=os.getenv("PARTNER_KEY"), help="Partner API key (optional)")
    ap.add_argument("--skip-baseline", action="store_true", help="Skip single-query baseline")
    args = ap.parse_args()

    query_url = f"{args.url.rstrip('/')}/query"
    print(f"Target: {query_url}")
    print(f"Workers: {args.workers}\n")

    baseline = {"elapsed_ms": 0, "status": None, "error": None}
    if not args.skip_baseline:
        baseline = _baseline(query_url, args.key)
        if baseline["error"] and not baseline["status"]:
            print("Baseline request failed with a network error. Check URL and try again.")
            return

    results, wall_ms = _concurrent_run(query_url, args.key, args.workers)
    _report(baseline, results, wall_ms, args.workers)


if __name__ == "__main__":
    main()
