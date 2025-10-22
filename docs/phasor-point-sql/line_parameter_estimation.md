# Line Parameter Estimation Results

## Overview

Line Parameter Estimation Results can be retrieved using the SQL interface. For example, a line parameter estimation named "Line Parameter Estimation" would be accessible using the table name:

```
lpe_line_parameter_estimation
```

## Line Parameter Estimation Results Schema

Line Parameter Estimation results tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `r` | REAL | Line resistance. |
| `x` | REAL | Line reactance. |
| `ysh` | REAL | Shunt admittance. |
| `confidence` | REAL | Confidence level. |

## Notes

- Line Parameter Estimation (LPE) tables store calculated electrical parameters for transmission lines.
- `r` (resistance) and `x` (reactance) represent the series impedance of the line.
- `ysh` (shunt admittance) represents the line's shunt capacitance effect.
- The `confidence` field indicates the reliability or quality of the estimation.
- These parameters are estimated from PMU measurements and can vary over time based on operating conditions.
- Table names follow the pattern `lpe_<estimation_name>`.
