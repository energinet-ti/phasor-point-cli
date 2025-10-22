# Dynamics Analysis Events

## Overview

Dynamics analysis events can be retrieved using the PhasorPoint SQL interface. There is a single dynamics event table named:

```
dynamics_events
```

## Dynamic Events Database Schema

The dynamics event table contains the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `sts` | TIMESTAMP | System time-stamp. |
| `classification` | INTEGER | Event classification. 0 for Normal, 1 for Alert, 2 for Alarm. |
| `synchronous_group` | TEXT | Synchronous group name of the signal that the event occurred on. |
| `measurement_group` | TEXT | Measurement group name of the signal that the event occurred on. |
| `measurement` | TEXT | Measurement name of the signal that the event occurred on. |
| `parameter` | TEXT | Parameter of the signal (i.e. "P", "f") that the event occurred on. |
| `band_start` | REAL | Frequency of the start of the mode band that the event occurred in. |
| `band_end` | REAL | Frequency of the end of the mode band that the event occurred in. |
| `message` | TEXT | Message attached to the event. |

## Event Classifications

| Value | Classification |
|-------|---------------|
| 0 | Normal |
| 1 | Alert |
| 2 | Alarm |

## Notes

- This is a single shared table for all dynamics analysis events across the system.
- Events are classified by severity: Normal (0), Alert (1), or Alarm (2).
- Each event includes both a measurement timestamp (`ts`) and a system timestamp (`sts`).
- The `parameter` field indicates which signal parameter triggered the event (e.g., "P" for active power, "f" for frequency).
- Unique to dynamics events: `band_start` and `band_end` define the frequency range of the oscillatory mode band where the event occurred.
