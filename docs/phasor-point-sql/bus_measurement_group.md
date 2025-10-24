# Measurement Group Bus

## Overview

All voltage bus data within a configured Aggregate Measurement Group (AMG) can be retrieved using AMG bus tables. For example, if there is an AMG called "Zone 1" and a voltage bus called "bus440", the table name providing access to the associated voltage data at 50 Hz sample rate would be:

```
v_bus_zone_1_bus440_50
```

> **Note:** SQL table names prohibit special characters, including space. PhasorPoint SQL automatically replaces invalid characters with underscores. In this example, the space has been replaced with an underscore.

## Bus Measurement Group Table Schema

Bus measurement group tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `pm` | REAL | Positive sequence voltage magnitude. |
| `pa` | REAL | Positive sequence voltage angle. |
| `nm` | REAL | Negative sequence voltage magnitude. |
| `na` | REAL | Negative sequence voltage angle. |
| `zm` | REAL | Zero sequence voltage magnitude. |
| `za` | REAL | Zero sequence voltage angle. |
| `am` | REAL | Phase A voltage magnitude. |
| `aa` | REAL | Phase A voltage angle. |
| `bm` | REAL | Phase B voltage magnitude. |
| `ba` | REAL | Phase B voltage angle. |
| `cm` | REAL | Phase C voltage magnitude. |
| `ca` | REAL | Phase C voltage angle. |
| `f` | REAL | Frequency. |
| `dfdt` | REAL | df/dt. |
| `locked` | BIT | True/False value indicating if the data has a GPS lock. |
| `error` | BIT | True/False value indicating if the data is marked with an error. |
| `resampled` | BIT | True/False value indicating whether the data is resampled (True) or at the original received data rate (False). |

## Notes

- Where symmetrical components are not all available or derived, the columns containing absent components will contain NULL values.
