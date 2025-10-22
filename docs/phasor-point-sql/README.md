# PhasorPoint SQL

## Introduction

Providing convenient access to the comprehensive data collected by a WAMS is an important part of the PhasorPoint suite. The PhasorPoint SQL optional Advanced Application provides an avenue of access to archived data via a subset of Structure Query Language (SQL), a widely supported and standardized database computer language designed for accessing relational database management systems.

In order to retrieve data through PhasorPoint SQL, a connector driver is required on the client computer. PhasorPoint SQL provides two standardized connectors to access data using SQL: Open Database Connectivity (ODBC), and Java Database Connectivity (JDBC).

The PhasorPoint ODBC driver supports a relevant subset of version 3.51 of the standard and is available for x86 and x64 Microsoft Windows® platforms.

The PhasorPoint JDBC driver is a platform independent Type 4 driver and supports a relevant subset of version 4.0 of the standard.

PhasorPoint SQL exposes raw phasor, processed, and derived data in a simple table schema allowing access to a standardized view of the available data, transparent to underlying PMU and PhasorPoint configuration changes.

## Database Connectors

In order to retrieve data through PhasorPoint SQL, a connector driver is required on the client computer. PhasorPoint SQL provides two standardized connectors to access data using SQL: Open Database Connectivity (ODBC), and Java Database Connectivity (JDBC).

### ODBC Driver
- **Standard Version:** Supports a relevant subset of ODBC 3.51
- **Platform:** Microsoft Windows® (x86 and x64)
- **Use Case:** Windows-based applications and tools

### JDBC Driver
- **Type:** Type 4 (platform independent)
- **Standard Version:** Supports a relevant subset of JDBC 4.0
- **Use Case:** Java applications and cross-platform tools

> **Note:** For more information on how to use these connectors, refer to the Configure External Application Interfaces Guide.

## Key Features

- **Comprehensive Data Access:** Raw phasor, processed, and derived data
- **Standardized Schema:** Simple, consistent table structure
- **Configuration Transparency:** Access remains consistent despite underlying PMU and PhasorPoint configuration changes
- **SQL Standard:** Industry-standard query language for data retrieval
- **Wide Tool Support:** Compatible with many SQL-capable applications and programming languages

## Schema Overview

Tables are provided for each PMU named according to its unique PMU ID, affording a low level view of data. Information is also presented as configured within PhasorPoint by Measurement Group, Bus, Circuit, Analog and Digital providing access to derived values such as Negative Sequence or Active Power. This provides a common WAMS data nomenclature across interfaces.

The data tables provide a coherent view of phasor data at a given sample rate, independent of the original received/stored data rate. In addition, this allows retrieval of more manageable amounts of data when full resolution is not required by a user or application. The primary PMU, Measurement Group Bus and Circuit tables all require a suffix specifying a valid standard report sample rate. Any data resampling that was required for returning the data at the requested data rate is clearly indicated via a dedicated table column.

## PhasorPoint SQL Tables

The following lists the tables that are available via the PhasorPoint SQL interface.

| Table Name Pattern | Table Description | Reference |
|-------------------|-------------------|-----------|
| `pmu_<PMU_ID>_<X>` | PMU data for a given C37.118 ID at X samples per second, where X is a valid standard report sample rate. | [PMU Data Tables](pmu_data_tables.md) |
| `v_<MEASUREMENT_GROUP>_<BUS>_<X>` | Bus data as configured within PhasorPoint with frequency and voltage values at X samples per second, where X is a valid standard report sample rate. | [Measurement Group Bus](bus_measurement_group.md) |
| `i_<MEASUREMENT_GROUP>_<CIRCUIT>_<X>` | Circuit data as configured within PhasorPoint with current and power values at X samples per second, where X is a valid standard report sample rate. | [Measurement Group Circuit](circuit_measurement_group.md) |
| `a_<MEASUREMENT_GROUP>_<ANALOG>_<X>` | Analog data as configured within PhasorPoint with values at X samples per second, where X is a valid standard report sample rate. | [Measurement Group Analog](analog_measurement_group.md) |
| `d_<MEASUREMENT_GROUP>_<DIGITAL>_<X>` | Digital data as configured within PhasorPoint with values at X samples per second, where X is a standard valid report sample rate. | [Measurement Group Digital](digital_measurement_group.md) |
| `ref_<SYNCHRONOUS_AREA>_<X>` | Reference angle for a specific synchronous area at X samples per second. | [Reference Angle](reference_angle.md) |
| `ad_<ANGLE_DIFFERENCE>_<X>` | Angle difference data at X samples per second. | [Angle Difference](angle_difference.md) |
| `c_<CALCULATED_VALUE>_<X>` | Calculated data at X samples per second. | [Calculated Data](calculated_data.md) |
| `v_<PMU_ID>_<BUS>_summary` | Bus summary data for a given C37.118 ID. | [Bus Summary Data](bus_summary_data.md) |
| `i_<PMU_ID>_<CIRCUIT>_summary` | Circuit summary for a given C37.118 ID. | [Circuit Summary Data](circuit_summary_data.md) |
| `magnitude_events` | Magnitude events. | [Magnitude Events](magnitude_events.md) |
| `dynamics_events` | Dynamic events. | [Dynamics Analysis Events](dynamics_analysis_events.md) |
| `composite_events` | Composite events. | [Composite Events](composite_events.md) |
| `system_events` | System events. | [System Events](system_events.md) |
| `pmu_data_statistics` | PMU Data Statistics. | [PMU Data Statistics](pmu_data_statistics.md) |
| `pmu_connection_statistics` | PMU Connection Statistics. | [PMU Connection Statistics](pmu_connection_statistics.md) |
| `lpe_<ESTIMATION_NAME>` | Line Parameter Estimation results. | [Line Parameter Estimation](line_parameter_estimation.md) |
| `pdx1_<MEASUREMENT_GROUP>_<BUS>_f` | Oscillatory Stability PDX1-3 analysis data for bus frequency. | [Dynamics Analysis Results](dynamics_analysis_results.md) |
| `pdx1_<MEASUREMENT_GROUP>_<BUS>_mag` | Oscillatory Stability PDX1-3 analysis data for bus magnitude. | [Dynamics Analysis Results](dynamics_analysis_results.md) |
| `pdx1_<MEASUREMENT_GROUP>_<CIRCUIT>_p` | Oscillatory Stability PDX1-3 analysis data for active power. | [Dynamics Analysis Results](dynamics_analysis_results.md) |
| `pdx1_<MEASUREMENT_GROUP>_<CIRCUIT>_q` | Oscillatory Stability PDX1-3 analysis data for reactive power. | [Dynamics Analysis Results](dynamics_analysis_results.md) |
| `pdx1_<ANGLE_DIFFERENCE>` | Oscillatory Stability PDX1-3 analysis data for angle difference specified by the angle difference name. | [Dynamics Analysis Results](dynamics_analysis_results.md) |
| `pdx2_<MEASUREMENT_GROUP>_<BUS>_f` | Oscillatory Stability PDX2-20 analysis data for bus frequency. | [Dynamics Analysis Results](dynamics_analysis_results.md) |
| `pdx2_<MEASUREMENT_GROUP>_<BUS>_mag` | Oscillatory Stability PDX2-20 analysis data for bus magnitude. | [Dynamics Analysis Results](dynamics_analysis_results.md) |
| `pdx2_<MEASUREMENT_GROUP>_<CIRCUIT>_p` | Oscillatory Stability PDX2-20 analysis data for active power. | [Dynamics Analysis Results](dynamics_analysis_results.md) |
| `pdx2_<MEASUREMENT_GROUP>_<CIRCUIT>_q` | Oscillatory Stability PDX2-20 analysis data for reactive power. | [Dynamics Analysis Results](dynamics_analysis_results.md) |
| `pdx2_<ANGLE_DIFFERENCE>` | Oscillatory Stability PDX2-20 analysis data for the given angle difference name. | [Dynamics Analysis Results](dynamics_analysis_results.md) |
| `sso_<SSO_MEASUREMENT_NAME>_v` | Sub-synchronous oscillations analysis results for voltage waveform. | [SSO Results](sso_results.md) |
| `sso_<SSO_MEASUREMENT_NAME>_i` | Sub-synchronous oscillations analysis results for current waveform. | [SSO Results](sso_results.md) |
| `sso_<SSO_MEASUREMENT_NAME>_s` | Sub-synchronous oscillations analysis results for speed signal. | [SSO Results](sso_results.md) |
| `impact_global_<SYNCHRONOUS_AREA>_f` | Global impact measure data for the given Synchronous Area frequency signal. | [Impact Measures](impact_measures.md) |
| `impact_global_<SYNCHRONOUS_AREA>_va` | Global impact measure data for the given Synchronous Area voltage angle signal. | [Impact Measures](impact_measures.md) |
| `impact_global_<SYNCHRONOUS_AREA>_vm` | Global impact measure data for the given Synchronous Area voltage magnitude signal. | [Impact Measures](impact_measures.md) |
| `impact_<MEASUREMENT_GROUP>_<BUS>_f` | Local impact measure data for the given bus frequency signal. | [Impact Measures](impact_measures.md) |
| `impact_<MEASUREMENT_GROUP>_<BUS>_va` | Local impact measure data for the given bus voltage angle signal. | [Impact Measures](impact_measures.md) |
| `impact_<MEASUREMENT_GROUP>_<BUS>_vm` | Local impact measure data for the given bus voltage magnitude signal. | [Impact Measures](impact_measures.md) |

## Table Categories

### Time-Series Measurement Data
- [PMU Data Tables](pmu_data_tables.md) - Raw PMU measurements
- [Bus Measurement Group](bus_measurement_group.md) - Voltage and frequency data
- [Circuit Measurement Group](circuit_measurement_group.md) - Current and power data
- [Analog Measurement Group](analog_measurement_group.md) - Analog values
- [Digital Measurement Group](digital_measurement_group.md) - Digital states
- [Reference Angle](reference_angle.md) - Synchronous area reference
- [Angle Difference](angle_difference.md) - Angle differences
- [Calculated Data](calculated_data.md) - User-defined calculations

### Summary and Statistics
- [Bus Summary Data](bus_summary_data.md) - 15-minute voltage summaries
- [Circuit Summary Data](circuit_summary_data.md) - 15-minute current/power summaries
- [PMU Data Statistics](pmu_data_statistics.md) - Data quality metrics
- [PMU Connection Statistics](pmu_connection_statistics.md) - Connection reliability

### Events
- [Magnitude Events](magnitude_events.md) - Magnitude-based alarms
- [Dynamics Analysis Events](dynamics_analysis_events.md) - Oscillation events
- [Composite Events](composite_events.md) - Combined condition events
- [System Events](system_events.md) - System health events

### Advanced Analysis
- [Dynamics Analysis Results](dynamics_analysis_results.md) - PDX1-3 and PDX2-20 oscillation analysis
- [SSO Results](sso_results.md) - Sub-synchronous oscillation analysis
- [Impact Measures](impact_measures.md) - Disturbance severity quantification
- [Line Parameter Estimation](line_parameter_estimation.md) - Transmission line parameters

## Naming Conventions

### Special Characters
SQL table names prohibit special characters, including spaces. PhasorPoint SQL automatically replaces prohibited characters with underscores.

**Example:** "Zone 1" becomes "zone_1" in table names.

### Sample Rate Suffix
Many tables require a sample rate suffix (`<X>`) indicating the data rate in samples per second (e.g., `50` for 50 Hz).

**Example:** `pmu_1234_50` for 50 Hz data from PMU ID 1234.

### Resampled Data
When data is returned at a different rate than originally received, a `resampled` column indicates this transformation.

## Standard Report Sample Rates

Valid standard report sample rates that can be used as the `<X>` suffix include common power system frequencies and their multiples (e.g., 1, 10, 25, 50, 60 Hz).

## Query Syntax

PhasorPoint SQL is focused on providing a method of easily and efficiently retrieving blocks of time series WAMS data.

As many features of SQL are oriented towards operations on relational databases, PhasorPoint SQL supports only a limited subset of queries that provide common WAMS data-related functionality.

Extraction of time series data is the only intended use of PhasorPoint SQL queries.

Accordingly, only filters on the time stamp (`ts`) column in the WHERE clause of queries are supported.

### Query Characteristics

- **Primary Purpose:** Time series data extraction
- **Supported Filtering:** Timestamp (`ts`) column only
- **WHERE Clause:** Must filter on the `ts` column
- **Unsupported Features:** Complex joins, subqueries, aggregations (use summary tables instead)

### Query Guidelines

1. Always include a time range filter using the `ts` column
2. Use appropriate sample rate suffix for your data resolution needs
3. Refer to individual schema documentation for available columns
4. Leverage summary tables for pre-aggregated statistics

## Query Examples

### Time-Bounded Queries

Phasor data can only be retrieved based on time stamp. Time stamps are specified in local time. The following examples illustrate the forms of queries that are supported by PhasorPoint SQL.

#### Example 1: Basic Time Range Query

Select the time stamp and frequency columns for the PMU with ID 1 at fifty samples per second between midnight and 1am on the 21st July 2010:

```sql
SELECT ts, f 
FROM pmu_1_50 
WHERE ts BETWEEN '2010-07-21 00:00:00' AND '2010-07-21 01:00:00'
```

#### Example 2: Measurement Group Query

Select the positive sequence phasor from a measurement group named 'Zone 1' with a bus named 'bus440' at ten samples per second:

```sql
SELECT ts, pm, pa 
FROM v_zone_1_bus440_10 
WHERE ts BETWEEN '2010-07-21 00:00:00' AND '2010-07-21 01:00:00'
```

#### Example 3: Alternative Time Range Syntax

The following example returns the same results as above except the greater-than-or-equal-to and less-than-or-equal-to operators are used in place of BETWEEN:

```sql
SELECT ts, pm, pa 
FROM v_zone_1_bus440_10 
WHERE ts >= '2010-07-21 00:00:00' AND ts <= '2010-07-21 01:00:00'
```

> **Caution:** PhasorPoint SQL currently only supports specifying one time period in a single query. If a time period is not explicitly specified only a subset of current data is returned to prevent unintended retrieval of potentially months, and hence millions of rows, of data.

### Joins

Table joins in PhasorPoint SQL are provided as a simple method of retrieving data across multiple time-synchronized PMU streams using the common time stamp column.

#### Example: Multi-PMU Query

Select the common time stamp and frequency values for PMU ID 1 and PMU ID 2 at 50 samples per second:

```sql
SELECT ts, pmu_1_50.f, pmu_2_50.f 
FROM pmu_1_50, pmu_2_50 
WHERE ts BETWEEN '2010-07-21 00:00:00' AND '2010-07-21 01:00:00'
```

#### Simplified Join Syntax

As all data is time stamped consistently, PhasorPoint SQL provides a convenient shorthand notation for dealing with multiple streams.

The time-stamp column `ts` is treated as a special case as it appears in all tables. When data is requested from multiple source tables, a virtual `ts` column exists outside of any specified table, which takes on every time stamp between the start of the query and the end of the query.

This allows the user to leave out long redundant join specifications on the time stamp columns of each source table, simplifying the process of synchronizing data across many locations. Traditional SQL syntax is also supported to retain compatibility with requests generated by, for example, query builder tools.

> **Caution:** Joins on other columns are currently not supported by PhasorPoint SQL.
