| Strategy | Failure | Sink | Trials | Outcomes | Trials with duplicates | Duplicate rows (median, max) | Trials missing records | Recovery s, median (min-max) |
|---|---|---|---|---|---|---|---|---|
| A | checkpoint_corruption | file | 10 | restart_failed 10 | 0/10 | 0, 0 | 0/0 (completed trials) | - |
| A | checkpoint_corruption | foreachbatch | 10 | restart_failed 10 | 0/10 | 0, 0 | 0/0 (completed trials) | - |
| A | driver_failure | file | 10 | ok 10 | 0/10 | 0, 0 | 0/10 (completed trials) | 11.0 (9.5-15.5) |
| A | driver_failure | foreachbatch | 10 | ok 10 | 1/10 | 0, 300 | 0/10 (completed trials) | 10.0 (9.5-28.6) |
| A | none | file | 1 | ok 1 | 0/1 | 0, 0 | 0/1 (completed trials) | - |
| A | none | foreachbatch | 1 | ok 1 | 0/1 | 0, 0 | 0/1 (completed trials) | - |
| B | checkpoint_corruption | file | 10 | restart_failed 10 | 0/10 | 0, 0 | 0/0 (completed trials) | - |
| B | checkpoint_corruption | foreachbatch | 10 | restart_failed 10 | 0/10 | 0, 0 | 0/0 (completed trials) | - |
| B | driver_failure | file | 10 | ok 10 | 0/10 | 0, 0 | 0/10 (completed trials) | 14.8 (12.0-17.0) |
| B | driver_failure | foreachbatch | 10 | ok 10 | 0/10 | 0, 0 | 0/10 (completed trials) | 13.3 (11.0-20.5) |
| B | none | file | 1 | ok 1 | 0/1 | 0, 0 | 0/1 (completed trials) | - |
| B | none | foreachbatch | 1 | ok 1 | 0/1 | 0, 0 | 0/1 (completed trials) | - |

84 measured trials, code commit(s) 4a2dfe3, 5607d08. Missing records are counted only for trials whose restart succeeded; a failed restart stops the pipeline, which is reported as its outcome.
