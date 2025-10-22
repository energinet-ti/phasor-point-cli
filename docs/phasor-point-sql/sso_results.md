# Sub-Synchronous Oscillations Results

## Overview

The SSO Advanced Application provides analysis tools for high frequency observable modes of oscillation. Sub-Synchronous Oscillations (SSO) Results can be retrieved using the SQL interface.

For more information, see "Sub-Synchronous Oscillations".

If the SSO Advanced Application is installed, SSO-PDX dynamics analysis results are available for each analyzed signal in a common table type. Tables are named by analysis type and source signal.

### Example Table Names

For example, a measurement group named "Zone 1" with SSO analog measurements "Voltage", "Current" and "Speed" has the following tables available:

- `sso_zone_1_voltage_v` - Provides access to analysis results for the selected voltage measurement.
- `sso_zone_1_current_i` - Provides access to analysis results for the selected current measurement.
- `sso_zone_1_speed_s` - Provides access to analysis results for the selected speed measurement.

> **Note:** SQL table names prohibit special characters, including spaces. PhasorPoint SQL automatically replaces prohibited characters with underscores. In this example, spaces have been replaced with underscores.

## SSO Results Table Schema

The tables providing access to analysis results for voltage, current and speed contain the columns shown in the table below.

Frequency values are exported in their native perspectives: electrical perspective for voltage and current, and in mechanical perspective for speed.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `nf0` | REAL | Native frequency of the largest mode of oscillation. |
| `a0` | REAL | Amplitude of the largest mode of oscillation. |
| `d0` | REAL | Damping of the largest mode of oscillation. |
| `nf1` | REAL | Native frequency of the second largest mode of oscillation. |
| `a1` | REAL | Amplitude of the second largest mode of oscillation. |
| `d1` | REAL | Damping of the second largest mode of oscillation. |
| `nf2` | REAL | Native frequency of the third largest mode of oscillation. |
| `a2` | REAL | Amplitude of the third largest mode of oscillation. |
| `d2` | REAL | Damping of the third largest mode of oscillation. |
| `nf3` | REAL | Native frequency of the fourth largest mode of oscillation. |
| `a3` | REAL | Amplitude of the fourth largest mode of oscillation. |
| `d3` | REAL | Damping of the fourth largest mode of oscillation. |
| `nf4` | REAL | Native frequency of the fifth largest mode of oscillation. |
| `a4` | REAL | Amplitude of the fifth largest mode of oscillation. |
| `d4` | REAL | Damping of the fifth largest mode of oscillation. |
| `nf5` | REAL | Native frequency of the sixth largest mode of oscillation. |
| `a5` | REAL | Amplitude of the sixth largest mode of oscillation. |
| `d5` | REAL | Damping of the sixth largest mode of oscillation. |
| `nf6` | REAL | Native frequency of the seventh largest mode of oscillation. |
| `a6` | REAL | Amplitude of the seventh largest mode of oscillation. |
| `d6` | REAL | Damping of the seventh largest mode of oscillation. |
| `nf7` | REAL | Native frequency of the eighth largest mode of oscillation. |
| `a7` | REAL | Amplitude of the eighth largest mode of oscillation. |
| `d7` | REAL | Damping of the eighth largest mode of oscillation. |

## Notes

- SSO analysis tracks up to eight modes of oscillation (indexed 0-7).
- Each mode includes native frequency (`nf`), amplitude (`a`), and damping (`d`) values.
- Modes are ordered by amplitude, with the largest mode first (index 0).
- **Native frequency perspective:**
  - Voltage and current: electrical perspective
  - Speed: mechanical perspective
- SSO focuses on high-frequency oscillations, unlike the low-frequency dynamics analyzed by PDX1-3 and PDX2-20.
- Requires the SSO Advanced Application to be installed.
