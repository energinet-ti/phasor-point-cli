# Bus Summary Data

## Overview

Summary data for 15 minute maximum, minimum and average values for voltage buses can be retrieved using summary bus tables. For example, for a PMU with an IEEE C37.118 ID of 1234 if there is a voltage bus called "bus 440", the table name providing access to the associated summary voltage data would be:

```
v_1234_bus440_summary
```

## Bus Summary Data Table Schema

Bus summary data tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp in milliseconds. |
| `f_max` | REAL | Maximum frequency. |
| `f_min` | REAL | Minimum frequency. |
| `f_avg` | REAL | Average frequency. |
| `dfdt_max` | REAL | Maximum df/dt. |
| `dfdt_min` | REAL | Minimum df/dt. |
| `dfdt_avg` | REAL | Average df/dt. |
| `pm_max` | REAL | Maximum positive sequence voltage magnitude. |
| `pm_min` | REAL | Minimum positive sequence voltage magnitude. |
| `pm_avg` | REAL | Average positive sequence voltage magnitude. |
| `pa_max` | REAL | Maximum positive sequence voltage angle. |
| `pa_min` | REAL | Minimum positive sequence voltage angle. |
| `pa_avg` | REAL | Average positive sequence voltage angle. |
| `nm_max` | REAL | Maximum negative sequence voltage magnitude. |
| `nm_min` | REAL | Minimum negative sequence voltage magnitude. |
| `nm_avg` | REAL | Average negative sequence voltage magnitude. |
| `na_max` | REAL | Maximum negative sequence voltage angle. |
| `na_min` | REAL | Minimum negative sequence voltage angle. |
| `na_avg` | REAL | Average negative sequence voltage angle. |
| `zm_max` | REAL | Maximum zero sequence voltage magnitude. |
| `zm_min` | REAL | Minimum zero sequence voltage magnitude. |
| `zm_avg` | REAL | Average zero sequence voltage magnitude. |
| `za_max` | REAL | Maximum zero sequence voltage angle. |
| `za_min` | REAL | Minimum zero sequence voltage angle. |
| `za_avg` | REAL | Average zero sequence voltage angle. |
| `am_max` | REAL | Maximum Phase A voltage magnitude. |
| `am_min` | REAL | Minimum Phase A voltage magnitude. |
| `am_avg` | REAL | Average Phase A voltage magnitude. |
| `aa_max` | REAL | Maximum Phase A voltage angle. |
| `aa_min` | REAL | Minimum Phase A voltage angle. |
| `aa_avg` | REAL | Average Phase A voltage angle. |
| `bm_max` | REAL | Maximum Phase B voltage magnitude. |
| `bm_min` | REAL | Minimum Phase B voltage magnitude. |
| `bm_avg` | REAL | Average Phase B voltage magnitude. |
| `ba_max` | REAL | Maximum Phase B voltage angle. |
| `ba_min` | REAL | Minimum Phase B voltage angle. |
| `ba_avg` | REAL | Average Phase B voltage angle. |
| `cm_max` | REAL | Maximum Phase C voltage magnitude. |
| `cm_min` | REAL | Minimum Phase C voltage magnitude. |
| `cm_avg` | REAL | Average Phase C voltage magnitude. |
| `ca_max` | REAL | Maximum Phase C voltage angle. |
| `ca_min` | REAL | Minimum Phase C voltage angle. |
| `ca_avg` | REAL | Average Phase C voltage angle. |

## Notes

- Summary data is calculated over 15 minute intervals.
- Each measurement type (frequency, sequence components, phase values) has three statistics: maximum (`_max`), minimum (`_min`), and average (`_avg`).
