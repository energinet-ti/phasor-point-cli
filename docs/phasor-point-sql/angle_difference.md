# Angle Difference

## Overview

Angle difference can be retrieved using the SQL interface. For example, an angle difference named "Angle Difference" at a 50Hz sample rate would be accessible using the table name:

```
ad_angle_difference_1_50
```

## Angle Difference Table Schema

Angle difference tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `value` | REAL | The value expressed in radians. |
| `resampled` | BIT | True/False value indicating whether the data is resampled (True) or at the original received data rate (False). |

## Notes

- Angle difference values are expressed in radians.
- The `resampled` flag indicates whether the data has been resampled or is at the original received rate.
