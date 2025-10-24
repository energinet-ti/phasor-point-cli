# Measurement Group Circuit

## Overview

All circuit data within a configured Aggregate Measurement Group (AMG) can be retrieved using AMG circuit tables. For example, if there is an AMG called "Zone 1" and a configured circuit called "i_102upper", the table name providing access to the associated data at 50 Hz sample rate would be:

```
i_zone_1_i_102upper_50
```

> **Note:** SQL table names prohibit special characters, including space. PhasorPoint SQL automatically replaces prohibited characters with underscores. In this example, the space has been replaced with an underscore.

## Circuit Measurement Group Table Schema

Circuit measurement group tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `pm` | REAL | Positive sequence current magnitude. |
| `pa` | REAL | Positive sequence current angle. |
| `nm` | REAL | Negative sequence current magnitude. |
| `na` | REAL | Negative sequence current angle. |
| `zm` | REAL | Zero sequence current magnitude. |
| `za` | REAL | Zero sequence current angle. |
| `am` | REAL | Phase A current magnitude. |
| `aa` | REAL | Phase A current angle. |
| `bm` | REAL | Phase B current magnitude. |
| `ba` | REAL | Phase B current angle. |
| `cm` | REAL | Phase C current magnitude. |
| `ca` | REAL | Phase C current angle. |
| `p` | REAL | Calculated active power. |
| `q` | REAL | Calculated reactive power. |
| `locked` | BIT | True/False value indicating if the data has a GPS lock. |
| `error` | BIT | True/False value indicating if the data is marked with an error. |
| `resampled` | BIT | True/False value indicating whether the data is resampled (True) or at the original received data rate (False). |

## Notes

- Where symmetrical components are not all available or derived, the columns containing absent components will contain NULL values.
