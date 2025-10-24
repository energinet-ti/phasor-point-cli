# PMU Data Statistics

## Overview

PMU Data Statistics are available for retrieval via the SQL interface. The table name is:

```
pmu_data_statistics
```

## PMU Data Statistics Database Schema

The table contains the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `from_ts` | TIMESTAMP | Time-stamp for the start of the requested time period. |
| `to_ts` | TIMESTAMP | Time-stamp for the end of the requested time period. |
| `id` | INTEGER | The PMU ID. |
| `station_name` | TEXT | The PMU station name. |
| `available` | REAL | The % of the requested time period when data is available. |
| `error` | REAL | The % of the requested time period when the data was marked with an error. |
| `valid` | REAL | The % of the requested time period when data is valid. |
| `locked` | REAL | The % of the requested time period when data is locked. |
| `trigger_detected` | REAL | The % of the requested time period when a trigger was detected for the value. |
| `sort_by_arrival` | REAL | The % of the requested time period when data was sorted by arrival. |
| `config_changed` | REAL | The % of the requested time period when configuration changed for the value. |
| `overall_valid` | REAL | The % of the overall data which is valid. |

## Notes

- This is a single shared table providing statistics for all PMUs in the system.
- Statistics are calculated over a specified time period defined by `from_ts` and `to_ts`.
- Most columns represent percentages (0-100) of the time period when various conditions were true.
- The `overall_valid` field provides a summary measure of overall data quality.
- Statistics include data availability, quality indicators (error, valid, locked), and operational conditions (triggers, sorting, configuration changes).
