# Measurement Group Analog

## Overview

All analog data within a configured Aggregate Measurement Group (AMG) can be retrieved using AMG analog tables. For example, if there is an AMG named "Zone 1" and a configured analog named "analog1", the table name providing access to the associated data at 50 Hz sample rate is:

```
a_zone_1_analog1_50
```

## Analog Data Table Schema

Analog data tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp in milliseconds. |
| `value` | REAL | Analog value. |
| `locked` | BIT | True/false value indicating if the data has a GPS lock. |
| `error` | BIT | True/false value indicating if the data is marked with an error. |
| `resampled` | BIT | True/false value indicating whether the data is resampled (true) or at the originally received data rate (false). |
