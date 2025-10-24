# Measurement Group Digital

## Overview

All digital data within a configured Aggregate Measurement Group (AMG) can be retrieved using AMG digital tables. For example, if there is an AMG named "Zone 1" and a configured digital named "digital1", the table name providing access to the associated data at 50 Hz sample rate is:

```
d_zone_1_digital1_50
```

## Digital Data Table Schema

Digital data tables contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp in milliseconds. |
| `value` | BIT | True/false value indicating the state of the digital input. |
| `locked` | BIT | True/false value indicating if the data has a GPS lock. |
| `error` | BIT | True/false value indicating if the data is marked with an error. |
| `resampled` | BIT | True/false value indicating whether the data is resampled (True) or at the originally received data rate (False). |
