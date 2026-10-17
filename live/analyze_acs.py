"""Summarize the ACS comparison trials in experiments/live_acs/*/trial.json.

Writes experiments/live_acs/summary.csv (one row per trial) and summary.md
(one row per strategy x workload x failure x sink). Every number is computed
from the trial files; nothing is typed in. The first grid (experiments/live/)
is summarized separately by live/analyze.py and is not read here.

    python live/analyze_acs.py
    python live/analyze_acs.py --root experiments/live_acs
    python live/analyze_acs.py --include-stalled   # keep stalled trials in the table

Stalled trials. The producer is paced by the wall clock alone, so the time it
needs to send all records is fixed by the workload schedule. A trial whose
producer finished more than STALL_S (10 s) later than its schedule was paused
from outside (laptop sleep, WSL or Docker suspended): its timings measure the
pause, not Spark. Such trials stay in summary.csv (column stalled = True) and
are left out of summary.md, which reports how many were left out per row.
"""
import argparse
import csv
import glob
import json
import os
import statistics
from collections import defaultdict

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STALL_S = 10.0


def producer_overrun(r):
    """Seconds by which the producer finished later than its rate schedule allows."""
    ev, sch, n = r["events"], r.get("workload_schedule") or [], r["config"]["records"]
    if not sch or not ev.get("producer_done_s") or not ev.get("producer_start"):
        return None
    sent, planned = 0.0, None
    for i, (start, rate) in enumerate(sch):
        end = sch[i + 1][0] if i + 1 < len(sch) else float("inf")
        if sent + rate * (end - start) >= n:
            planned = start + (n - sent) / rate
            break
        sent += rate * (end - start)
    if planned is None:
        return None
    return round(ev["producer_done_s"] - ev["producer_start"] - planned, 2)


def label(cfg):
    if cfg["strategy"] == "ACS":
        return f"ACS {cfg.get('acs_variant', 'v1')}"
    if cfg["strategy"] == "C":
        return f"C ({cfg.get('fixed_s')} s)"
    return cfg["strategy"]


def med(xs, fmt="{:.1f}"):
    xs = [x for x in xs if x is not None]
    if not xs:
        return "-"
    return f"{fmt.format(statistics.median(xs))} ({fmt.format(min(xs))}-{fmt.format(max(xs))})"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.path.join(REPO, "experiments", "live_acs"))
    ap.add_argument("--include-stalled", action="store_true")
    a = ap.parse_args()
    rows = []
    for f in sorted(glob.glob(os.path.join(a.root, "*", "trial.json"))):
        with open(f, encoding="utf-8") as fh:
            r = json.load(fh)
        if r.get("run_type") != "measured":
            continue
        cfg, ev, c = r["config"], r["events"], r["committed"]
        k, lat, ctl = r.get("at_kill") or {}, r.get("latency") or {}, r.get("control") or {}
        over = producer_overrun(r)
        rows.append({
            "run_id": r["run_id"], "strategy": label(cfg), "workload": cfg.get("workload", "steady"),
            "failure": cfg["failure"], "sink": cfg["sink"], "trial": cfg["trial"], "outcome": r["outcome"],
            "expected": r["produced_acked"], "duplicate_rows": c["duplicate_rows"],
            "missing": c["missing_records"], "recovery_latency_s": ev.get("recovery_latency_s"),
            "replay_records": k.get("replay_records"),
            "seconds_since_last_commit": k.get("seconds_since_last_commit"),
            "latency_p50_s": lat.get("p50_s"), "latency_p95_s": lat.get("p95_s"),
            "switches": ctl.get("switches"), "median_switch_downtime_s": ctl.get("median_switch_downtime_s"),
            "time_weighted_interval_s": ctl.get("time_weighted_interval_s"),
            "batch_commits": r.get("n_batch_commits"),
            "commits_per_min_whole_run": r.get("commits_per_min"),
            "producer_overrun_s": over,
            "stalled": over is not None and over > STALL_S,
            "git_commit": (r["environment"].get("git_commit") or "")[:7],
        })
    if not rows:
        raise SystemExit(f"no measured trials in {a.root}/")
    with open(os.path.join(a.root, "summary.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    overs = sorted(t["producer_overrun_s"] for t in rows if t["producer_overrun_s"] is not None)
    print(f"producer overrun over {len(overs)} trials: <=1 s: {sum(o <= 1 for o in overs)}, "
          f"1-10 s: {sum(1 < o <= STALL_S for o in overs)}, "
          f">10 s (stalled): {sum(o > STALL_S for o in overs)}")
    for t in rows:
        if t["stalled"]:
            print(f"  stalled: {t['run_id']}  overrun {t['producer_overrun_s']} s")
    groups, left_out = defaultdict(list), defaultdict(int)
    for t in rows:
        key = (t["workload"], t["failure"], t["sink"], t["strategy"])
        if t["stalled"] and not a.include_stalled:
            left_out[key] += 1
            continue
        groups[key].append(t)
    lines = ["Median (min-max) over trials. Replay: records in batches planned but not committed "
             "when the job was killed. Latency: producer send to output file write. Batch commits: "
             "total micro-batches committed in the trial (same 36,000 records in every trial), the "
             "checkpoint overhead measure; commits per minute is kept in summary.csv only, because "
             "the end-of-trial wait differs by strategy.", "",
             "| Workload | Failure | Sink | Strategy | Trials | Outcomes | Trials with duplicates | "
             "Replay records | Recovery s | Latency p50 s | Latency p95 s | Mean interval s | "
             "Switches | Switch downtime s | Batch commits | Stalled, left out |",
             "|" + "---|" * 16]
    for (wl, fl, sk, st), ts in sorted(groups.items()):
        outs = defaultdict(int)
        for t in ts:
            outs[t["outcome"]] += 1
        g = lambda key: [t[key] for t in ts]  # noqa: E731
        lines.append(
            f"| {wl} | {fl} | {sk} | {st} | {len(ts)} | "
            f"{', '.join(f'{o} {n}' for o, n in sorted(outs.items()))} | "
            f"{sum(d > 0 for d in g('duplicate_rows'))}/{len(ts)} | "
            f"{med(g('replay_records'), '{:.0f}')} | {med(g('recovery_latency_s'))} | "
            f"{med(g('latency_p50_s'), '{:.2f}')} | {med(g('latency_p95_s'), '{:.2f}')} | "
            f"{med(g('time_weighted_interval_s'))} | {med(g('switches'), '{:.0f}')} | "
            f"{med(g('median_switch_downtime_s'), '{:.2f}')} | {med(g('batch_commits'), '{:.0f}')} | "
            f"{left_out[(wl, fl, sk, st)]} |")
    commits = sorted({t["git_commit"] for t in rows})
    n_st = sum(t["stalled"] for t in rows)
    lines += ["", f"{len(rows)} measured trials, code commit(s) {', '.join(commits)}. "
              f"{n_st} stalled trial(s) (producer more than {STALL_S:g} s behind its schedule) "
              + ("are included." if a.include_stalled else "are left out of this table and kept in summary.csv.")]
    with open(os.path.join(a.root, "summary.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
