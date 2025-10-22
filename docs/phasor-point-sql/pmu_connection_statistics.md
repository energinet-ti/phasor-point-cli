# PMU Connection Statistics

## Overview

PMU Connection Statistics are available for retrieval via the SQL interface. The table name is:

```
pmu_connection_statistics
```

## PMU Connection Statistics Database Schema

The table contains the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `id` | INTEGER | The PMU ID. |
| `interruption_count` | INTEGER | The number of interruptions during the requested time period. |
| `avg_interruption` | REAL | The average length in seconds of interruptions during the requested time period. |
| `max_interruption` | REAL | The maximum length in seconds of an interruption during the requested time period. |

## Notes

- This is a single shared table providing connection statistics for all PMUs in the system.
- Statistics focus on connection interruptions and their duration.
- `interruption_count` tracks the total number of connection interruptions.
- `avg_interruption` and `max_interruption` provide duration metrics in seconds.
- These statistics help monitor PMU communication reliability and identify problematic connections.
