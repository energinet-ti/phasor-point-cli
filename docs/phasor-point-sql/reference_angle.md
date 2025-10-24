# Reference Angle

## Overview

Reference angle is made available for each synchronous area configured within PhasorPoint. For example, the reference angle at a 50Hz sample rate for the synchronous area named "mainland" would be accessible using the table name:

```
ref_mainland_50
```

## Reference Angle Table Schema

Reference angle tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `angle` | REAL | Reference angle. |

## Notes

- Reference angle tables are created per synchronous area.
- This is the simplest schema, containing only timestamp and angle value.
- The reference angle provides a common reference for angle measurements across the synchronous area.
