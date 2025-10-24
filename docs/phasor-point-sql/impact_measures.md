# Impact Measures Results

## Overview

The Impact Measures capability of System Disturbance Management (SDM: IM) provides static and dynamic Impact Measures by which to quantify the severity of a disturbance. SDM: IM results can be retrieved using the SQL interface.

For more information, see "Impact Measures".

If the SDM: IM capability is licensed, Impact Measures results are available for each analyzed signal in a common table type. Tables are named by type and source signal, and both local and global Impact Measures are available.

### Global Impact Measures

For global measures, a Synchronous Area named "synchronous_area_1" has the following tables available:

- `impact_global_synchronous_area_1_f` - Provides access to global Impact Measures for the frequency measurement.
- `impact_global_synchronous_area_1_va` - Provides access to global Impact Measures for the voltage angle measurement.
- `impact_global_synchronous_area_1_vm` - Provides access to global Impact Measures for the voltage magnitude measurement.

### Local Impact Measures

For local measures, a bus named "100_pmu_1_voltage_bus_1" in Aggregate Measurement Group "measurement_group_1" has the following tables available:

- `impact_measurement_group_1_100_pmu_1_voltage_bus_1_f` - Provides access to local Impact Measures for the frequency measurement.
- `impact_measurement_group_1_100_pmu_1_voltage_bus_1_va` - Provides access to local Impact Measures for the voltage angle measurement.
- `impact_measurement_group_1_100_pmu_1_voltage_bus_1_vm` - Provides access to local Impact Measures for the voltage magnitude measurement.

> **Note:** SQL table names prohibit special characters, including spaces. PhasorPoint SQL automatically replaces prohibited characters with underscores. In this example, spaces have been replaced with underscores.

## Impact Measures Table Schema

The tables providing access to the Impact Measures each contain the columns shown in the table below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `static` | FLOAT | Static Impact Measure calculation for the selected measurement. |
| `dynamic` | FLOAT | Dynamic Impact Measure calculation for the selected measurement. |

## Measurement Types

Impact Measures are calculated for three measurement types:

| Suffix | Measurement Type |
|--------|-----------------|
| `_f` | Frequency |
| `_va` | Voltage Angle |
| `_vm` | Voltage Magnitude |

## Notes

- Impact Measures quantify the severity of power system disturbances.
- **Static Impact Measures** assess the steady-state impact of a disturbance.
- **Dynamic Impact Measures** assess the transient behavior during a disturbance.
- **Global measures** evaluate system-wide impact across a synchronous area.
- **Local measures** evaluate impact at specific measurement locations (buses).
- Requires the SDM: IM capability to be licensed.
- Both static and dynamic measures are provided for each measurement type (frequency, voltage angle, voltage magnitude).
