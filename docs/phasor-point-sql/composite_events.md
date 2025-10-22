# Composite Events

## Overview

Composite events can be retrieved using the PhasorPoint SQL interface. There is a single composite event table named:

```
composite_events
```

## Composite Event Database Schema

The composite events table contains the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `sts` | TIMESTAMP | System time-stamp. |
| `classification` | INTEGER | Event classification. 0 for Normal, 1 for Alert, 2 for Alarm. |
| `name` | TEXT | Name of the composite event triggering this event. |
| `message` | TEXT | Message attached to the event. |

## Event Classifications

| Value | Classification |
|-------|---------------|
| 0 | Normal |
| 1 | Alert |
| 2 | Alarm |

## Notes

- This is a single shared table for all composite events across the system.
- Events are classified by severity: Normal (0), Alert (1), or Alarm (2).
- Each event includes both a measurement timestamp (`ts`) and a system timestamp (`sts`).
- Composite events are higher-level events that combine multiple conditions or measurements.
- The `name` field identifies which composite event definition triggered the event.
