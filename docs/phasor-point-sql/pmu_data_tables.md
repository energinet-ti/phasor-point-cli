# PMU Data Tables

## Overview

PMU data tables allow data for a specific PMU identified by the IEEE C37.118 ID to be selected. For example, data at a 50Hz sample rate for a PMU with an IEEE C37.118 ID of 1234, can be accessed through the table named:

```
pmu_1234_50
```

## PMU Table Schema

PMU data tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `f` | REAL | Frequency. |
| `dfdt` | REAL | df/dt. |
| `v_<PHASOR NAME>_m` | REAL | Magnitude value for phasor given by the phasor name. |
| `v_<PHASOR NAME>_a` | REAL | Angle value for phasor given by the phasor name. |
| `i_<PHASOR NAME>_a` | REAL | Angle value for given current phasor. |
| `a_<ANALOG NAME>` | REAL | Value of given analog. |
| `d_<DIGITAL NAME>` | BIT | True/false value indicating the given digital state. |
| `locked` | BIT | True/False value indicating if the data has a GPS lock. |
| `error` | BIT | True/False value indicating if the data is marked with an error. |
| `resampled` | BIT | True/False value indicating whether the data is resampled (True) or at the original received data rate (False). |

## Notes

- Columns for phasor magnitude and angle values `v_<PHASOR NAME>_m` and `v_<PHASOR NAME>_a` respectively exist for each named phasor being sent from the PMU.
- Column names with placeholders like `<PHASOR NAME>`, `<ANALOG NAME>`, and `<DIGITAL NAME>` are dynamically created based on the actual PMU configuration.
