# Dynamics Analysis Results

## Overview

The OSM Advanced Application provides foremost analysis and visualization tools for monitoring the dynamics of the power system through a study of its oscillatory modes (see "Oscillatory Stability Management (Low Frequency)" for full details).

If the OSM Advanced Application is installed, PDX1-3 and PDX2-20 dynamics analysis results are available for each analyzed signal in a common table type. Tables are named by analysis type and source signal.

### Example Table Names

For example, an AMG "Zone 1" with a bus "bus440" associated with a circuit "i_102upper" will have the following tables available:

- `pdx1_zone_1_bus440_f` - PDX1-3 analysis for bus frequency
- `pdx2_zone_1_bus440_f` - PDX2-20 analysis for bus frequency
- `pdx1_zone_1_bus440_mag` - PDX1-3 analysis for bus voltage magnitude
- `pdx2_zone_1_bus440_mag` - PDX2-20 analysis for bus voltage magnitude
- `pdx1_zone_1_i_102upper_p` - PDX1-3 analysis for active power
- `pdx2_zone_1_i_102upper_p` - PDX2-20 analysis for active power
- `pdx1_zone_1_i_102upper_q` - PDX1-3 analysis for reactive power
- `pdx2_zone_1_i_102upper_q` - PDX2-20 analysis for reactive power

In the case of a voltage angle difference, an angle difference configured within PhasorPoint named "upper1-upper2" will have the following tables available:

- `pdx1_upper1_upper2` - PDX1-3 analysis for angle difference
- `pdx2_upper1_upper2` - PDX2-20 analysis for angle difference

> **Note:** SQL table names prohibit special characters, including spaces. PhasorPoint SQL automatically replaces prohibited characters with underscores. In this example, spaces have been replaced with underscores.

## Dynamics Results Table Schema - Frequency

The tables providing access to frequency analysis results for both PDX1-3 and PDX2-20 contain the columns shown below. Modes are ordered by frequency in ascending order.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `avg` | REAL | Average frequency. |
| `validity` | INTEGER | Validity quality indicator (10, 20, 30, 40 or 50). |
| `f0` | REAL | Frequency of the first mode of oscillation. |
| `a0` | REAL | Amplitude of the first mode of oscillation. |
| `d0` | REAL | Damping of the first mode of oscillation. |
| `cm0` | REAL | Mode index corresponding to common modes of f0. |
| `p0` | REAL | Phase differences between common modes cm0. |
| `ic0` | REAL | Individual contribution to source location for f0. |
| `gc0` | REAL | Group contribution to source location for f0. |
| `gid0` | REAL | Group ID for source location for f0. |
| `f1` | REAL | Frequency of the second mode of oscillation. |
| `a1` | REAL | Amplitude of the second mode of oscillation. |
| `d1` | REAL | Damping of the second mode of oscillation. |
| `cm1` | REAL | Mode index corresponding to common modes of f1. |
| `p1` | REAL | Phase differences between common modes cm1. |
| `ic1` | REAL | Individual contribution to source location for f1. |
| `gc1` | REAL | Group contribution to source location for f1. |
| `gid1` | REAL | Group ID for source location for f1. |
| `f2` | REAL | Frequency of the third mode of oscillation. |
| `a2` | REAL | Amplitude of the third mode of oscillation. |
| `d2` | REAL | Damping of the third mode of oscillation. |
| `cm2` | REAL | Mode index corresponding to common modes of f2. |
| `p2` | REAL | Phase differences between common modes cm2. |
| `ic2` | REAL | Individual contribution to source location for f2. |
| `gc2` | REAL | Group contribution to source location for f2. |
| `gid2` | REAL | Group ID for source location for f2. |
| `f3` | REAL | Frequency of the fourth mode of oscillation. |
| `a3` | REAL | Amplitude of the fourth mode of oscillation. |
| `d3` | REAL | Damping of the fourth mode of oscillation. |
| `cm3` | REAL | Mode index corresponding to common modes of f3. |
| `p3` | REAL | Phase differences between common modes cm3. |
| `ic3` | REAL | Individual contribution to source location for f3. |
| `gc3` | REAL | Group contribution to source location for f3. |
| `gid3` | REAL | Group ID for source location for f3. |
| `f4` | REAL | Frequency of the fifth mode of oscillation. |
| `a4` | REAL | Amplitude of the fifth mode of oscillation. |
| `d4` | REAL | Damping of the fifth mode of oscillation. |
| `cm4` | REAL | Mode index corresponding to common modes of f4. |
| `p4` | REAL | Phase differences between common modes cm4. |
| `ic4` | REAL | Individual contribution to source location for f4. |
| `gc4` | REAL | Group contribution to source location for f4. |
| `gid4` | REAL | Group ID for source location for f4. |

## Dynamics Results Table Schema - Power

The tables providing access to active and reactive power analysis results for both PDX1-3 and PDX2-20 contain the columns shown below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `avg` | REAL | Average power. |
| `validity` | INTEGER | Validity quality indicator (10, 20, 30, 40 or 50). |
| `f0` | REAL | Frequency of the first mode of oscillation. |
| `a0` | REAL | Amplitude of the first mode of oscillation. |
| `d0` | REAL | Damping of the first mode of oscillation. |
| `f1` | REAL | Frequency of the second mode of oscillation. |
| `a1` | REAL | Amplitude of the second mode of oscillation. |
| `d1` | REAL | Damping of the second mode of oscillation. |
| `f2` | REAL | Frequency of the third mode of oscillation. |
| `a2` | REAL | Amplitude of the third mode of oscillation. |
| `d2` | REAL | Damping of the third mode of oscillation. |
| `f3` | REAL | Frequency of the fourth mode of oscillation. |
| `a3` | REAL | Amplitude of the fourth mode of oscillation. |
| `d3` | REAL | Damping of the fourth mode of oscillation. |
| `f4` | REAL | Frequency of the fifth mode of oscillation. |
| `a4` | REAL | Amplitude of the fifth mode of oscillation. |
| `d4` | REAL | Damping of the fifth mode of oscillation. |

## Dynamics Results Table Schema - Voltage Magnitude

The tables providing access to bus voltage magnitude analysis results for both PDX1-3 and PDX2-20 contain the columns shown below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `avg` | REAL | Average voltage magnitude. |
| `validity` | INTEGER | Validity quality indicator (10, 20, 30, 40 or 50). |
| `f0` | REAL | Frequency of the first mode of oscillation. |
| `a0` | REAL | Amplitude of the first mode of oscillation. |
| `d0` | REAL | Damping of the first mode of oscillation. |
| `f1` | REAL | Frequency of the second mode of oscillation. |
| `a1` | REAL | Amplitude of the second mode of oscillation. |
| `d1` | REAL | Damping of the second mode of oscillation. |
| `f2` | REAL | Frequency of the third mode of oscillation. |
| `a2` | REAL | Amplitude of the third mode of oscillation. |
| `d2` | REAL | Damping of the third mode of oscillation. |
| `f3` | REAL | Frequency of the fourth mode of oscillation. |
| `a3` | REAL | Amplitude of the fourth mode of oscillation. |
| `d3` | REAL | Damping of the fourth mode of oscillation. |
| `f4` | REAL | Frequency of the fifth mode of oscillation. |
| `a4` | REAL | Amplitude of the fifth mode of oscillation. |
| `d4` | REAL | Damping of the fifth mode of oscillation. |

## Dynamics Results Table Schema - Angle Difference

The tables providing access to angle difference analysis results for both PDX1-3 and PDX2-20 contain the columns shown below.

| Column Name | SQL Type | Column Description |
|-------------|----------|-------------------|
| `ts` | TIMESTAMP | Time-stamp to millisecond resolution. |
| `avg` | REAL | Average angle difference. |
| `validity` | INTEGER | Validity quality indicator (10, 20, 30, 40 or 50). |
| `f0` | REAL | Frequency of the first mode of oscillation. |
| `a0` | REAL | Amplitude of the first mode of oscillation. |
| `d0` | REAL | Damping of the first mode of oscillation. |
| `f1` | REAL | Frequency of the second mode of oscillation. |
| `a1` | REAL | Amplitude of the second mode of oscillation. |
| `d1` | REAL | Damping of the second mode of oscillation. |
| `f2` | REAL | Frequency of the third mode of oscillation. |
| `a2` | REAL | Amplitude of the third mode of oscillation. |
| `d2` | REAL | Damping of the third mode of oscillation. |
| `f3` | REAL | Frequency of the fourth mode of oscillation. |
| `a3` | REAL | Amplitude of the fourth mode of oscillation. |
| `d3` | REAL | Damping of the fourth mode of oscillation. |
| `f4` | REAL | Frequency of the fifth mode of oscillation. |
| `a4` | REAL | Amplitude of the fifth mode of oscillation. |
| `d4` | REAL | Damping of the fifth mode of oscillation. |

## Notes

- All dynamics analysis tables track up to five modes of oscillation (indexed 0-4).
- Each mode includes frequency (`f`), amplitude (`a`), and damping (`d`) values.
- Frequency analysis tables include additional source location information (common mode index, phase differences, individual/group contributions, and group IDs).
- The `validity` field is an integer quality indicator with values of 10, 20, 30, 40, or 50.
- Power tables handle both active (P) and reactive (Q) power with identical schemas.
