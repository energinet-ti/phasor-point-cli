# System Events

## Overview

System events can be retrieved using the PhasorPoint SQL interface. There is a single system event table named:

```
system_events
```

## System Event Database Schema

The system events table contains the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `sts` | TIMESTAMP | System time-stamp. |
| `classification` | INTEGER | Event classification. 0 for Normal, 1 for Alert, 2 for Alarm. |
| `component` | TEXT | Name of the system component triggering this event. |
| `message` | TEXT | Message attached to the event. |

## Event Classifications

| Value | Classification |
|-------|---------------|
| 0 | Normal |
| 1 | Alert |
| 2 | Alarm |

## Notes

- This is a single shared table for all system events across the system.
- Events are classified by severity: Normal (0), Alert (1), or Alarm (2).
- Each event includes both a measurement timestamp (`ts`) and a system timestamp (`sts`).
- System events relate to the PhasorPoint system itself rather than measurement data.
- The `component` field identifies which system component triggered the event (e.g., database, network, processing engine).
