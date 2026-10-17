# Laptop standby during the second ACS run (October 16, 2026)

Six trials of `experiments/live_acs2/` finished correctly but their producer ran
4 to 40 minutes behind its rate schedule (see `paper/make_tables.py`, "stalled"
trials). The Windows System log of the machine shows that it entered Modern
Standby during each of those periods, although it was plugged in.

Source: `Get-WinEvent` on the System log, provider Microsoft-Windows-Kernel-Power,
event 506 (entering Modern Standby) and 507 (exiting). Times are local
(US Eastern, EDT = UTC-4). Only October 16, 6 pm to 10 pm, is shown.

| Local time | Event | Reason |
|---|---|---|
| 6:26:27 PM | exit (507) | Lid |
| 6:30:09 PM | enter (506) | Idle Timeout |
| 7:15:29 PM | exit (507) | Input Keyboard |
| 7:19:48 PM | enter (506) | Idle Timeout |
| 7:22:27 PM | exit (507) | Input Keyboard |
| 7:27:23 PM | enter (506) | Idle Timeout |
| 7:46:01 PM | exit (507) | Input Keyboard |
| 7:54:12 PM | enter (506) | Idle Timeout |
| 7:55:16 PM | exit (507) | Input Keyboard |
| 7:58:26 PM | enter (506) | Idle Timeout |
| 8:09:31 PM | exit (507) | Input Keyboard |
| 8:10:56 PM | enter (506) | Idle Timeout |
| 8:18:13 PM | exit (507) | Input Mouse |
| 8:30:56 PM | enter (506) | Idle Timeout |
| 8:37:28 PM | exit (507) | Input Keyboard |
| 8:40:21 PM | enter (506) | Idle Timeout |
| 8:53:51 PM | exit (507) | Input Keyboard |
| 8:54:48 PM | enter (506) | Idle Timeout |
| 8:57:05 PM | exit (507) | AC/DC Display Burst Suppressed |
| 8:57:05 PM | enter (506) | AC/DC Display Burst Suppressed |
| 9:01:40 PM | exit (507) | Input Keyboard |
| 9:02:31 PM | enter (506) | Idle Timeout |
| 9:17:03 PM | exit (507) | AC/DC Display Burst |
| 9:18:01 PM | enter (506) | Idle Timeout |
| 9:51:17 PM | exit (507) | Input Keyboard |

Stalled trials (run id = start time in UTC; local = UTC-4):

| Trial (run id) | Local start | Producer overrun |
|---|---|---|
| 20261016T223348Z_C3.3 t00 | 6:33 PM | 2431 s |
| 20261017T000019Z_B t02 | 8:00 PM | 575 s |
| 20261017T003005Z_C3.3 t03 | 8:30 PM | 376 s |
| 20261017T005701Z_A t04 | 8:57 PM | 264 s |
| 20261017T010347Z_B t04 | 9:03 PM | 684 s |
| 20261017T012115Z_C5.8 t04 | 9:21 PM | 1691 s |

Each stalled trial overlaps a standby period. The stalled trials are kept in
the data, left out of the paper's tables, and were rerun with the same trial
numbers (same input schedule and kill time); none of the reruns stalled.
