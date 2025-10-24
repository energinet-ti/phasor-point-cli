# Calculated Data

## Overview

User calculated data can be retrieved using the PhasorPoint SQL interface. For example, if there is a user defined calculated value named "Corridor 17", the table name providing access to the associated data at a 50 Hz sample rate would be:

```
c_corridor_17_50
```

## Calculated Data Table Schema

Calculated data tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `data` | REAL | User calculated data value. |
| `resampled` | BIT | True/False value indicating whether the data is resampled (True) or at the original received data rate (False). |

## Notes

- Calculated data tables store user-defined calculated values.
- The `resampled` flag indicates whether the data has been resampled or is at the original received rate.
- Table names follow the pattern `c_<calculated_name>_<sample_rate>`.
