"""Summarize measured live trials from experiments/live/*/trial.json.

Writes experiments/live/summary.csv (one row per trial) and
experiments/live/summary.md (one row per strategy x failure x sink).
Every number is computed from the trial.json files; nothing is typed in.
"""
import csv
import glob
import json
import os
import statistics
from collections import defaultdict

ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "experiments", "live")


def med(xs):
    xs = [x for x in xs if x is not None]
    return f"{statistics.median(xs):.1f} ({min(xs):.1f}-{max(xs):.1f})" if xs else "-"


def main():
    trials = []
    for f in sorted(glob.glob(os.path.join(ROOT, "*", "trial.json"))):
        with open(f, encoding="utf-8") as fh:
            r = json.load(fh)
        if r.get("run_type") != "measured":
            continue
        c, cfg, ev = r["committed"], r["config"], r["events"]
        trials.append({
            "run_id": r["run_id"], "strategy": cfg["strategy"], "failure": cfg["failure"],
            "sink": cfg["sink"], "trial": cfg["trial"], "outcome": r["outcome"],
            "expected": r["produced_acked"], "rows": c["rows"], "distinct": c["distinct"],
            "duplicate_rows": c["duplicate_rows"], "missing": c["missing_records"],
            "raw_duplicate_rows": r["raw_files"]["duplicate_rows"],
            "kill_delay_s": ev.get("kill_delay_s"), "recovery_latency_s": ev.get("recovery_latency_s"),
            "git_commit": (r["environment"].get("git_commit") or "")[:7],
        })
    if not trials:
        raise SystemExit("no measured trials in experiments/live/")
    with open(os.path.join(ROOT, "summary.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(trials[0]))
        w.writeheader()
        w.writerows(trials)
    groups = defaultdict(list)
    for t in trials:
        groups[(t["strategy"], t["failure"], t["sink"])].append(t)
    lines = ["| Strategy | Failure | Sink | Trials | Outcomes | Trials with duplicates | "
             "Duplicate rows (median, max) | Trials missing records | Recovery s, median (min-max) |",
             "|---|---|---|---|---|---|---|---|---|"]
    for (s, fl, k), ts in sorted(groups.items()):
        outs = defaultdict(int)
        for t in ts:
            outs[t["outcome"]] += 1
        dups = [t["duplicate_rows"] for t in ts]
        ok = [t for t in ts if t["outcome"] == "ok"]
        lines.append(
            f"| {s} | {fl} | {k} | {len(ts)} | {', '.join(f'{o} {n}' for o, n in sorted(outs.items()))} | "
            f"{sum(d > 0 for d in dups)}/{len(ts)} | {statistics.median(dups):.0f}, {max(dups)} | "
            f"{sum(t['missing'] > 0 for t in ok)}/{len(ok)} (completed trials) | "
            f"{med([t['recovery_latency_s'] for t in ts])} |")
    commits = sorted({t["git_commit"] for t in trials})
    lines += ["", f"{len(trials)} measured trials, code commit(s) {', '.join(commits)}. "
              "Missing records are counted only for trials whose restart succeeded; "
              "a failed restart stops the pipeline, which is reported as its outcome."]
    with open(os.path.join(ROOT, "summary.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
