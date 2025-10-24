# Magnitude Events

## Overview

Magnitude events can be retrieved using the PhasorPoint SQL interface. There is a single magnitude event table named:

```
magnitude_events
```

## Magnitude Event Data Schema

The magnitude events table contains the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `sts` | TIMESTAMP | System time-stamp. |
| `classification` | INTEGER | Event classification. 0 for Normal, 1 for Alert, 2 for Alarm. |
| `synchronous_group` | TEXT | Synchronous group name of the signal that the event occurred on. |
| `measurement_group` | TEXT | Measurement group name of the signal that the event occurred on. |
| `measurement` | TEXT | Measurement name of the signal that the event occurred on. |
| `parameter` | TEXT | Parameter of the signal (i.e. "P", "Q", "f") that the event occurred on. |
| `message` | TEXT | Message attached to the event. |

## Event Classifications

| Value | Classification |
|-------|---------------|
| 0 | Normal |
| 1 | Alert |
| 2 | Alarm |

## Notes

- This is a single shared table for all magnitude events across the system.
- Events are classified by severity: Normal (0), Alert (1), or Alarm (2).
- Each event includes both a measurement timestamp (`ts`) and a system timestamp (`sts`).
- The `parameter` field indicates which signal parameter triggered the event (e.g., "P" for active power, "Q" for reactive power, "f" for frequency).
