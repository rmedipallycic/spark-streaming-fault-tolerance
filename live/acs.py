"""Adaptive Checkpoint Selection (ACS): the controller used by live/stream_job.py.

In Structured Streaming the processing-time trigger also sets how often the
offset and commit logs are written, so the trigger interval plays the role of
the checkpoint interval. ACS changes it at runtime from three signals:

  instability     coefficient of variation (CV) of input throughput over the
                  last 10 observation windows that saw a batch, scaled so that
                  CV >= 0.3 counts as fully unstable (0.2 during warm-up)
  error_pressure  fraction of batches in the window whose run time exceeded
                  the trigger interval (the query falling behind). The
                  simulation used exceptions per 1,000 records, which is always
                  0 in a healthy live pipeline, so the live definition is this
                  measurable stress signal instead.
  recency         1 - (seconds since the last failure) / 120, floored at 0

  risk = 0.4 * instability + 0.3 * error_pressure + 0.3 * recency

Every 5 s: risk > 0.45 tightens the interval by 30% and resets the stable
count; otherwise the stable count grows, and after 4 stable windows the
interval relaxes by 20%. Bounds 2-45 s. These constants are the ones in
src/adaptive_checkpoint.py; they are frozen here and must not be tuned on
measured runs.

Variant v1 (the default) is exactly that rule. Its threshold cannot be crossed
by any single signal: instability alone gives at most 0.4, and a failure or
full error pressure alone at most 0.38 (with warm-up instability 0.2). And
because every window at or below 0.45 counts as stable, the interval relaxes
during a burst. Variant v2 is a revision decided from this analysis, before any
measured run: tighten when risk > 0.30 (any one saturated signal), count a
window as stable only when risk < 0.15, and hold the interval in between.

Two additions that the simulation did not need, because a live change costs a
query restart: the proposed interval is rounded to 0.5 s, and a change is
applied only if it differs from the current interval by at least 0.5 s.
"""
import math
import statistics

OBS_WINDOW_S = 5.0
MIN_INTERVAL_S = 2.0
MAX_INTERVAL_S = 45.0
START_INTERVAL_S = 10.0
RISK_THRESHOLD = 0.45
STABLE_WINDOWS = 4
TIGHTEN = 0.7
RELAX = 1.2
RECENCY_HORIZON_S = 120.0
CV_FULL = 0.3
HISTORY = 10
WEIGHTS = (0.4, 0.3, 0.3)
VARIANTS = {"v1": (0.45, 0.45), "v2": (0.30, 0.15)}   # (tighten above, stable at or below)


def _round_half(x):
    return math.floor(x * 2 + 0.5) / 2


class ACS:
    def __init__(self, interval=START_INTERVAL_S, last_failure=None, variant="v1"):
        self.variant = variant
        self.tighten_above, self.stable_at_or_below = VARIANTS[variant]
        self.interval = interval          # interval currently applied to the query
        self.target = interval            # controller's internal (unrounded) value
        self.stable = 0
        self.history = []
        self.last_failure = last_failure

    def signals(self, now, batches):
        """batches: progress dicts of the batches that finished in this window."""
        rates = [b.get("inputRowsPerSecond") for b in batches
                 if b.get("numInputRows", 0) > 0 and b.get("inputRowsPerSecond") is not None]
        if rates:
            self.history.append(sum(rates) / len(rates))
            self.history = self.history[-HISTORY:]
        if len(self.history) >= 3:
            mean = statistics.mean(self.history)
            cv = statistics.stdev(self.history) / (mean + 1e-9)
            instability = min(cv / CV_FULL, 1.0)
        else:
            cv, instability = None, 0.2
        if batches:
            over = sum(1 for b in batches
                       if (b.get("durationMs") or {}).get("triggerExecution", 0) > self.interval * 1000)
            error_pressure = over / len(batches)
        else:
            error_pressure = 0.0
        if self.last_failure is None:
            recency = 0.0
        else:
            recency = max(0.0, 1.0 - (now - self.last_failure) / RECENCY_HORIZON_S)
        w = WEIGHTS
        risk = round(w[0] * instability + w[1] * error_pressure + w[2] * recency, 4)
        return {"cv": cv, "instability": instability, "error_pressure": error_pressure,
                "recency": recency, "risk": risk, "n_batches": len(batches)}

    def step(self, now, batches):
        """One observation window. Returns (signals, new_interval or None)."""
        s = self.signals(now, batches)
        if s["risk"] > self.tighten_above:
            self.target = max(MIN_INTERVAL_S, self.target * TIGHTEN)
            self.stable = 0
        elif s["risk"] > self.stable_at_or_below:
            self.stable = 0          # v2 only: neither risky nor stable, hold
        else:
            self.stable += 1
            if self.stable >= STABLE_WINDOWS:
                self.target = min(MAX_INTERVAL_S, self.target * RELAX)
                self.stable = 0
        proposed = min(MAX_INTERVAL_S, max(MIN_INTERVAL_S, _round_half(self.target)))
        s["target"] = round(self.target, 3)
        if abs(proposed - self.interval) >= 0.5:
            return s, proposed
        return s, None
