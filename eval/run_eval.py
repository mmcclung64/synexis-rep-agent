"""Gold-query eval harness — exercises the HTTP API end-to-end.

Usage:
    # 1. Start the API in another terminal:
    #    python3 -m uvicorn api.main:app --host 127.0.0.1 --port 8000
    # 2. Run the eval:
    #    python3 -m eval.run_eval
    #    python3 -m eval.run_eval --api-url http://localhost:8000 --api-key <partner-key>

Outputs:
    work/eval_results.md   — per-query scoring detail + summary table
    stdout                 — summary counts and failing query ids

Scoring:
    Each row of query_set.csv defines independent checks. A query PASSES when
    every configured check passes. Supported checks:
      - must_contain_any   → at least one pipe-separated substring appears (case-insensitive)
      - must_contain_all   → every pipe-separated substring appears (case-insensitive)
      - must_not_contain   → none of the pipe-separated substrings appear (case-insensitive)
      - min_citations      → len(citations) >= this integer
      - max_answer_chars   → len(answer) <= this integer (catches "equip" leaks on hard-stop rules)
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx


REPO_ROOT = Path(__file__).resolve().parent.parent
QUERY_SET_PATH = REPO_ROOT / "eval" / "query_set.csv"
REPORT_PATH = REPO_ROOT / "work" / "eval_results.md"


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class RowResult:
    id: str
    category: str
    query: str
    http_status: int
    elapsed_ms: int
    answer: str
    citations_count: int
    checks: List[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.http_status == 200 and all(c.passed for c in self.checks)


def _split_pipe(s: str) -> List[str]:
    return [p.strip() for p in (s or "").split("|") if p.strip()]


def _run_checks(row: dict, answer: str, citations: list) -> List[CheckResult]:
    checks: List[CheckResult] = []
    lowered = (answer or "").lower()

    any_terms = _split_pipe(row.get("must_contain_any", ""))
    if any_terms:
        found = [t for t in any_terms if t.lower() in lowered]
        checks.append(
            CheckResult(
                name="must_contain_any",
                passed=bool(found),
                detail=f"matched={found!r} required_any_of={any_terms!r}",
            )
        )

    all_terms = _split_pipe(row.get("must_contain_all", ""))
    if all_terms:
        missing = [t for t in all_terms if t.lower() not in lowered]
        checks.append(
            CheckResult(
                name="must_contain_all",
                passed=not missing,
                detail=f"missing={missing!r}" if missing else "all terms present",
            )
        )

    not_terms = _split_pipe(row.get("must_not_contain", ""))
    if not_terms:
        leaked = [t for t in not_terms if t.lower() in lowered]
        checks.append(
            CheckResult(
                name="must_not_contain",
                passed=not leaked,
                detail=f"leaked={leaked!r}" if leaked else "no forbidden terms",
            )
        )

    min_c = (row.get("min_citations") or "").strip()
    if min_c:
        try:
            min_c_int = int(min_c)
        except ValueError:
            min_c_int = 0
        checks.append(
            CheckResult(
                name="min_citations",
                passed=len(citations) >= min_c_int,
                detail=f"got={len(citations)} required>={min_c_int}",
            )
        )

    max_chars = (row.get("max_answer_chars") or "").strip()
    if max_chars:
        try:
            max_chars_int = int(max_chars)
        except ValueError:
            max_chars_int = 10_000
        alen = len(answer or "")
        checks.append(
            CheckResult(
                name="max_answer_chars",
                passed=alen <= max_chars_int,
                detail=f"len={alen} limit<={max_chars_int}",
            )
        )

    return checks


def _run_one(client: httpx.Client, api_url: str, row: dict) -> RowResult:
    started = time.time()
    try:
        resp = client.post(f"{api_url}/query", json={"query": row["query"]}, timeout=120.0)
        elapsed_ms = int((time.time() - started) * 1000)
    except httpx.HTTPError as exc:
        return RowResult(
            id=row["id"], category=row["category"], query=row["query"],
            http_status=-1, elapsed_ms=int((time.time() - started) * 1000),
            answer="", citations_count=0,
            checks=[CheckResult(name="http_request", passed=False, detail=f"{type(exc).__name__}: {exc}")],
        )

    if resp.status_code != 200:
        return RowResult(
            id=row["id"], category=row["category"], query=row["query"],
            http_status=resp.status_code, elapsed_ms=elapsed_ms,
            answer=resp.text[:500], citations_count=0,
            checks=[CheckResult(name="http_status", passed=False, detail=f"status={resp.status_code}")],
        )

    data = resp.json()
    answer = data.get("answer", "") or ""
    citations = data.get("citations", []) or []
    checks = _run_checks(row, answer, citations)
    return RowResult(
        id=row["id"], category=row["category"], query=row["query"],
        http_status=200, elapsed_ms=elapsed_ms, answer=answer,
        citations_count=len(citations), checks=checks,
    )


def _ping(client: httpx.Client, api_url: str) -> Tuple[bool, str]:
    try:
        r = client.get(f"{api_url}/health", timeout=5.0)
        if r.status_code == 200:
            return True, json.dumps(r.json())
        return False, f"health returned {r.status_code}: {r.text}"
    except httpx.HTTPError as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _render_report(results: List[RowResult]) -> str:
    passed = sum(1 for r in results if r.passed)
    total = len(results)

    by_cat: Dict[str, Tuple[int, int]] = {}
    for r in results:
        p, t = by_cat.get(r.category, (0, 0))
        by_cat[r.category] = (p + (1 if r.passed else 0), t + 1)

    lines: List[str] = []
    lines.append(f"# Eval Results — {passed}/{total} passed\n")
    lines.append("## Summary by category\n")
    lines.append("| Category | Passed | Total |")
    lines.append("|---|---:|---:|")
    for cat, (p, t) in sorted(by_cat.items()):
        lines.append(f"| {cat} | {p} | {t} |")
    lines.append("")

    lines.append("## Per-query detail\n")
    for r in results:
        status_icon = "PASS" if r.passed else "FAIL"
        lines.append(f"### {status_icon} #{r.id} — {r.category}")
        lines.append(f"- **Query:** {r.query}")
        lines.append(f"- **HTTP:** {r.http_status}  |  **Elapsed:** {r.elapsed_ms} ms  |  **Citations:** {r.citations_count}")
        lines.append("- **Checks:**")
        for c in r.checks:
            icon = "ok" if c.passed else "FAIL"
            lines.append(f"  - [{icon}] `{c.name}` — {c.detail}")
        snippet = (r.answer or "").strip().replace("\n", " ")
        if len(snippet) > 400:
            snippet = snippet[:400] + "…"
        lines.append(f"- **Answer snippet:** {snippet}")
        lines.append("")

    return "\n".join(lines)


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Run the Synexis rep agent eval against the HTTP API.")
    ap.add_argument("--api-url", default="http://127.0.0.1:8000", help="Base URL of the running API.")
    ap.add_argument("--api-key", default="", help="Partner key; sent as X-Partner-Key header. Optional if PARTNER_KEYS is empty.")
    ap.add_argument("--query-set", default=str(QUERY_SET_PATH), help="Path to query_set.csv")
    ap.add_argument("--report", default=str(REPORT_PATH), help="Path to write the Markdown report.")
    ap.add_argument("--only-ids", default="", help="Comma-separated ids to run (subset).")
    args = ap.parse_args(argv)

    headers = {"X-Partner-Key": args.api_key} if args.api_key else {}
    client = httpx.Client(headers=headers)

    ok, info = _ping(client, args.api_url)
    if not ok:
        print(f"API not reachable at {args.api_url} — {info}", file=sys.stderr)
        print("Start it with: python3 -m uvicorn api.main:app --host 127.0.0.1 --port 8000", file=sys.stderr)
        return 2
    print(f"API OK: {info}")

    with open(args.query_set, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if args.only_ids:
        wanted = {s.strip() for s in args.only_ids.split(",") if s.strip()}
        rows = [r for r in rows if r["id"] in wanted]

    print(f"Running {len(rows)} queries against {args.api_url} ...\n")
    results: List[RowResult] = []
    for row in rows:
        r = _run_one(client, args.api_url, row)
        icon = "PASS" if r.passed else "FAIL"
        print(f"  [{icon}] #{r.id:>2}  {r.category:<32} {r.elapsed_ms:>5} ms  cite={r.citations_count}  {r.query[:70]}")
        if not r.passed:
            for c in r.checks:
                if not c.passed:
                    print(f"         - {c.name}: {c.detail}")
        results.append(r)

    passed = sum(1 for r in results if r.passed)
    total = len(results)
    print(f"\n{passed}/{total} passed  ({passed * 100 // max(1,total)}%)")
    failing = [r.id for r in results if not r.passed]
    if failing:
        print(f"Failing ids: {', '.join(failing)}")

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(_render_report(results), encoding="utf-8")
    print(f"Full report: {report_path}")

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
