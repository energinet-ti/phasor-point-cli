# Circuit Summary Data

## Overview

Summary data for 15 minute maximum, minimum and average values for a configured circuit can be retrieved using circuit summary data tables. For example, for a PMU with an IEEE C37.118 ID of 1234 if there is a circuit called "i_102upper", the table name providing access to the associated summary data would be:

```
i_1234_i_102upper_summary
```

## Circuit Summary Data Table Schema

Circuit summary data tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp in milliseconds. |
| `p_max` | REAL | Maximum calculated active power. |
| `p_min` | REAL | Minimum calculated active power. |
| `p_avg` | REAL | Average calculated active power. |
| `q_max` | REAL | Maximum calculated reactive power. |
| `q_min` | REAL | Minimum calculated reactive power. |
| `q_avg` | REAL | Average calculated reactive power. |
| `pm_max` | REAL | Maximum positive sequence current magnitude. |
| `pm_min` | REAL | Minimum positive sequence current magnitude. |
| `pm_avg` | REAL | Average positive sequence current magnitude. |
| `pa_max` | REAL | Maximum positive sequence current angle. |
| `pa_min` | REAL | Minimum positive sequence current angle. |
| `pa_avg` | REAL | Average positive sequence current angle. |
| `nm_max` | REAL | Maximum negative sequence current magnitude. |
| `nm_min` | REAL | Minimum negative sequence current magnitude. |
| `nm_avg` | REAL | Average negative sequence current magnitude. |
| `na_max` | REAL | Maximum negative sequence current angle. |
| `na_min` | REAL | Minimum negative sequence current angle. |
| `na_avg` | REAL | Average negative sequence current angle. |
| `zm_max` | REAL | Maximum zero sequence current magnitude. |
| `zm_min` | REAL | Minimum zero sequence current magnitude. |
| `zm_avg` | REAL | Average zero sequence current magnitude. |
| `za_max` | REAL | Maximum zero sequence current angle. |
| `za_min` | REAL | Minimum zero sequence current angle. |
| `za_avg` | REAL | Average zero sequence current angle. |
| `am_max` | REAL | Maximum Phase A current magnitude. |
| `am_min` | REAL | Minimum Phase A current magnitude. |
| `am_avg` | REAL | Average Phase A current magnitude. |
| `aa_max` | REAL | Maximum Phase A current angle. |
| `aa_min` | REAL | Minimum Phase A current angle. |
| `aa_avg` | REAL | Average Phase A current angle. |
| `bm_max` | REAL | Maximum Phase B current magnitude. |
| `bm_min` | REAL | Minimum Phase B current magnitude. |
| `bm_avg` | REAL | Average Phase B current magnitude. |
| `ba_max` | REAL | Maximum Phase B current angle. |
| `ba_min` | REAL | Minimum Phase B current angle. |
| `ba_avg` | REAL | Average Phase B current angle. |
| `cm_max` | REAL | Maximum Phase C current magnitude. |
| `cm_min` | REAL | Minimum Phase C current magnitude. |
| `cm_avg` | REAL | Average Phase C current magnitude. |
| `ca_max` | REAL | Maximum Phase C current angle. |
| `ca_min` | REAL | Minimum Phase C current angle. |
| `ca_avg` | REAL | Average Phase C angle. |

## Notes

- Summary data is calculated over 15 minute intervals.
- Each measurement type (power, sequence components, phase values) has three statistics: maximum (`_max`), minimum (`_min`), and average (`_avg`).
- Includes both active and reactive power statistics in addition to current measurements.
