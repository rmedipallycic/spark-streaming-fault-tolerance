Median (min-max) over trials. Replay: records in batches planned but not committed when the job was killed. Latency: producer send to output file write. Batch commits: total micro-batches committed in the trial (same 36,000 records in every trial), the overhead proxy (checkpoint I/O is not measured); commits per minute is kept in summary.csv only, because the end-of-trial wait differs by strategy.

| Workload | Failure | Sink | Strategy | Trials | Outcomes | Trials with duplicates | Replay records | Recovery s | Latency p50 s | Latency p95 s | Mean interval s | Switches | Switch downtime s | Batch commits | Stalled, left out |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| bursty | driver_failure | foreachbatch | A | 10 | ok 10 | 0/10 | 0 (0-301) | 10.5 (9.6-16.6) | 0.84 (0.71-1.38) | 8.75 (3.79-14.39) | - | 0 (0-0) | - | 82 (43-101) | 1 |
| bursty | driver_failure | foreachbatch | ACS v1 | 10 | ok 10 | 0/10 | 0 (0-0) | 10.3 (9.5-14.1) | 3.68 (1.81-6.40) | 10.88 (9.29-13.30) | 5.8 (3.1-6.4) | 10 (8-13) | 0.54 (0.54-0.56) | 24 (8-34) | 0 |
| bursty | driver_failure | foreachbatch | ACS v2 | 10 | ok 10 | 0/10 | 0 (0-981) | 10.3 (9.5-14.7) | 1.99 (1.43-4.36) | 8.89 (6.68-12.15) | 3.5 (2.9-4.3) | 6 (5-6) | 0.54 (0.54-1.04) | 35 (14-56) | 0 |
| bursty | driver_failure | foreachbatch | B | 10 | ok 10 | 0/10 | 0 (0-15750) | 10.5 (10.0-25.6) | 13.98 (9.16-24.36) | 30.44 (25.76-46.45) | - | 0 (0-0) | - | 6 (3-6) | 2 |
| bursty | driver_failure | foreachbatch | C (3.3 s) | 10 | ok 10 | 0/10 | 0 (0-0) | 10.5 (8.5-14.1) | 2.18 (1.91-2.91) | 8.61 (4.29-10.83) | - | 0 (0-0) | - | 28 (17-40) | 2 |
| bursty | driver_failure | foreachbatch | C (5.8 s) | 10 | ok 10 | 0/10 | 0 (0-0) | 10.0 (9.5-15.0) | 3.46 (3.25-4.27) | 8.11 (5.97-11.61) | - | 0 (0-0) | - | 18 (10-24) | 1 |

66 measured trials, code commit(s) 3d1f93f, 7d66520. 6 stalled trial(s) (producer more than 10 s behind its schedule) are left out of this table and kept in summary.csv.
