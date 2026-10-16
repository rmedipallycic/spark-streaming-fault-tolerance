Median (min-max) over trials. Replay: records in batches planned but not committed when the job was killed. Latency: producer send to output file write. Batch commits: total micro-batches committed in the trial (same 36,000 records in every trial), the checkpoint overhead measure; commits per minute is kept in summary.csv only, because the end-of-trial wait differs by strategy.

| Workload | Failure | Sink | Strategy | Trials | Outcomes | Trials with duplicates | Replay records | Recovery s | Latency p50 s | Latency p95 s | Mean interval s | Switches | Switch downtime s | Batch commits |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| bursty | driver_failure | foreachbatch | A | 10 | ok 10 | 0/10 | 0 (0-150) | 9.8 (9.0-14.1) | 0.76 (0.70-1.10) | 6.10 (3.61-10.08) | - | 0 (0-0) | - | 84 (45-101) |
| bursty | driver_failure | foreachbatch | ACS v1 | 10 | ok 10 | 0/10 | 0 (0-0) | 10.0 (9.0-11.0) | 3.48 (1.59-5.74) | 10.27 (8.77-11.97) | 5.8 (4.4-7.3) | 11 (8-13) | 0.54 (0.53-0.55) | 26 (8-35) |
| bursty | driver_failure | foreachbatch | ACS v2 | 10 | ok 10 | 0/10 | 0 (0-490) | 10.0 (9.0-11.0) | 1.73 (1.35-3.49) | 8.36 (6.08-12.00) | 3.3 (2.9-3.6) | 5 (5-6) | 0.54 (0.53-0.56) | 37 (17-53) |
| bursty | driver_failure | foreachbatch | B | 10 | ok 10 | 0/10 | 0 (0-0) | 10.3 (9.5-11.0) | 12.65 (8.95-18.77) | 29.23 (26.70-34.18) | - | 0 (0-0) | - | 6 (4-7) |
| bursty | driver_failure | foreachbatch | C (3.3 s) | 10 | ok 10 | 0/10 | 0 (0-0) | 10.5 (9.5-14.5) | 2.12 (1.96-3.26) | 7.01 (4.04-12.40) | - | 0 (0-0) | - | 30 (16-39) |
| bursty | driver_failure | foreachbatch | C (5.8 s) | 10 | ok 10 | 1/10 | 0 (0-1739) | 14.3 (13.5-15.5) | 3.96 (3.48-6.57) | 11.92 (8.38-16.09) | - | 0 (0-0) | - | 17 (10-23) |

60 measured trials, code commit(s) 369028b.
