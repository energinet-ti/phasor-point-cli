# PhasorPoint CLI Tool

A comprehensive command-line interface for extracting, processing, and analyzing PMU (Phasor Measurement Unit) data from PhasorPoint databases.

**Author:** Frederik Fast (Energinet)  
**Repository:** [energinet-ti/phasor-point-cli](https://github.com/energinet-ti/phasor-point-cli)

## Features

- **Data Extraction**: Flexible time ranges (minutes, hours, days, or custom dates)
- **Data Processing**: Automatic power calculations (S, P, Q) and quality validation
- **Extraction Logs**: Automatic metadata tracking of all transformations
- **Multiple Formats**: Parquet (recommended) or CSV
- **Batch Operations**: Extract from multiple PMUs simultaneously
- **Performance Options**: Chunking, parallel processing, and connection pooling
- **Custom Queries**: Execute arbitrary SQL queries
- **Cross-Platform**: Windows, Linux, and macOS

## Quick Start

### Installation

#### Install from GitHub Releases (Recommended)

Download the latest `.whl` file from the [Releases page](https://github.com/energinet-ti/phasor-point-cli/releases) and install:

```bash
pip install phasor_point_cli-<version>-py3-none-any.whl
phasor-cli --help
```

#### Install from Source

**Clone the repository:**
```bash
git clone https://github.com/energinet-ti/phasor-point-cli.git
cd phasor-point-cli
```

**Quick setup using scripts:**

Linux/macOS:
```bash
./scripts/setup.sh
```

Windows (PowerShell):
```powershell
.\scripts\setup.ps1
```

**Manual setup:**
```bash
python3 -m venv venv
source venv/bin/activate    # Linux/macOS
# venv\Scripts\Activate.ps1  # Windows

pip install -e .[dev]       # Developers (editable mode)
# pip install .             # Users (standard install)
```

**Note:** Requires the PhasorPoint ODBC driver ("Psymetrix PhasorPoint").

### Configuration

```bash
# Create configuration files
phasor-cli setup              # User-level (works from anywhere)
phasor-cli setup --local      # Project-specific

# View active configuration
phasor-cli config-path
```

**Configuration priority:** Environment Variables > Local Files (`./.env`, `./config.json`) > User Config (`~/.config/phasor-cli/`) > Defaults

After setup, edit:
- `.env` - Database credentials (never commit!)
- `config.json` - Settings and preferences

### Basic Usage

```bash
# Extract 1 hour of data
phasor-cli extract --pmu 45020 --hours 1 --output data.parquet

# Extract with processing (power calculations)
phasor-cli extract --pmu 45020 --hours 1 --processed --output data.parquet

# List available PMU tables
phasor-cli list-tables

# Get info about a specific PMU
phasor-cli table-info --pmu 45020
```

## Command Reference

### Data Extraction

```bash
# Time-based extraction (relative to NOW)
phasor-cli extract --pmu <pmu_number> --hours <hours> --output <file>
phasor-cli extract --pmu <pmu_number> --minutes <minutes> --output <file>
phasor-cli extract --pmu <pmu_number> --days <days> --output <file>

# Custom date range (absolute times)
phasor-cli extract --pmu <pmu_number> --start "YYYY-MM-DD HH:MM:SS" --end "YYYY-MM-DD HH:MM:SS" --output <file>

# Start time + duration (goes FORWARD from start time)
phasor-cli extract --pmu <pmu_number> --start "2025-10-10 10:00:00" --minutes 30 --output <file>  # 10:00 to 10:30
phasor-cli extract --pmu <pmu_number> --start "2025-10-10 08:00:00" --hours 2 --output <file>     # 08:00 to 10:00

# With processing (power calculations)
phasor-cli extract --pmu <pmu_number> --hours 1 --processed --output <file>

# Large time range with chunking (automatically enabled for >5 minutes)
phasor-cli extract --pmu <pmu_number> --hours 24 --output <file>

# Custom chunk size for very large extractions
phasor-cli extract --pmu <pmu_number> --hours 48 --chunk-size 10 --output <file>

# Parallel processing for faster extraction (4 workers)
phasor-cli extract --pmu <pmu_number> --hours 24 --parallel 4 --output <file>

# Performance diagnostics to identify bottlenecks
phasor-cli extract --pmu <pmu_number> --hours 1 --parallel 4 --diagnostics --output <file>

# I/O optimization for slow endpoints (larger chunks + connection pooling)
phasor-cli extract --pmu <pmu_number> --hours 2 --chunk-size 15 --connection-pool 3 --diagnostics --output <file>
```

### Batch Extraction

```bash
# Extract from multiple PMUs
phasor-cli batch-extract --pmus "45020,45022,45052" --hours 1 --output-dir ./data/

# With performance optimization
phasor-cli batch-extract --pmus "45020,45022" --hours 24 --chunk-size 30 --parallel 2 --output-dir ./data/
```

Files are named: `pmu_{number}_{resolution}hz_{start_date}_to_{end_date}.{format}`

### Database Exploration

```bash
# List all PMU tables
phasor-cli list-tables

# Get information about a specific PMU
phasor-cli table-info --pmu 45020

# Execute custom SQL query
phasor-cli query --sql "SELECT TOP 100 * FROM pmu_45020_1"
```

## Data Structure

Extracted data uses the original PhasorPoint database column names:

**Timestamps:**
- `ts` - Local timestamp (converted from UTC)
- `ts_utc` - Original UTC timestamp

**Measurements:** (column names from database, e.g., `f`, `dfdt`, `va1_m`, `va1_a`, `ia1_m`, `ia1_a`)

**Calculated Power (with --processed flag):**
- `apparent_power_mva`, `active_power_mw`, `reactive_power_mvar`

### Usage in Pandas

```python
import pandas as pd

df = pd.read_parquet('data.parquet')
print(df.f.mean())  # frequency
print(df.active_power_mw.std())  # calculated power
```

## Extraction Logs

Each extraction automatically creates a companion `_extraction_log.json` file that documents column changes, data quality issues, and transformation details.

## Performance Features

- **Automatic Chunking**: Large time ranges (>5 minutes) are automatically split for memory efficiency
- **Parallel Processing**: Use `--parallel N` to process multiple chunks simultaneously
- **Connection Pooling**: Use `--connection-pool N` to reuse database connections
- **Performance Diagnostics**: Use `--diagnostics` to identify bottlenecks

Recommended for large extractions:
```bash
phasor-cli extract --pmu 45020 --hours 24 --chunk-size 15 --parallel 2 --connection-pool 3 --output <file>
```

## Data Quality Features

The CLI includes automatic data quality checks:

- **Type Conversion**: Automatic conversion to proper numeric types
- **NaN Handling**: Detection and removal of null values
- **Empty Column Detection**: Removal of completely empty columns
- **Frequency Validation**: Check for valid frequency ranges (45-65 Hz)
- **Time Gap Detection**: Identify missing samples in time series
- **Voltage Range Checks**: Validate voltage levels

## Output Formats

- **Parquet** (recommended): Compressed, fast, preserves types, best for Python/pandas
- **CSV**: Human-readable, works in Excel, good for small datasets

## Security

⚠️ **Never commit `.env` files** to version control. Always add `.env` to `.gitignore`.

## Troubleshooting

### Connection Issues
```bash
# Test database connection
phasor-cli list-tables
```

### Encoding Errors
- Use `.parquet` format instead of `.csv` for large datasets
- CSV files use UTF-8 encoding automatically

### Missing Data
```bash
# Check available date range for a PMU
phasor-cli table-info --pmu 45020
```

## Requirements

- Python 3.8+
- PhasorPoint ODBC driver ("Psymetrix PhasorPoint")
- Dependencies: pandas, numpy, pyodbc, pyarrow, sqlalchemy, and more (see `pyproject.toml`)

## Development

### Setup Development Environment

Use the provided setup scripts for quick setup:

**Linux/macOS:**
```bash
./scripts/setup.sh
```

**Windows (PowerShell):**
```powershell
.\scripts\setup.ps1
```

Both scripts will:
- Create a virtual environment
- Install the package in editable mode
- Install all development dependencies (pytest, ruff, etc.)

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src/phasor_point_cli --cov-report=html

# Run all checks (lint + format check + tests)
make check
```

### Building Distribution Package

To build a wheel distribution package:

```bash
make build
```

This will create a `.whl` file in the `dist/` directory that can be installed with pip or distributed to others.

### Versioning

This project uses **setuptools-scm** for automatic version management based on git tags:

- **Version is derived from git tags** - no manual version bumping needed
- **Development builds** get automatic `.devN` suffixes based on commits since last tag
- **Clean releases** require a git tag (e.g., `v1.0.0`)

**Creating a release:**
```bash
# Tag the release
git tag v1.0.0
git push origin v1.0.0

# Build will now use version 1.0.0
make build
```

**Development builds** (without tags) will have versions like `0.1.dev3` based on commit count.

## License

Apache License 2.0 - See LICENSE file for details

## Author

**Frederik Fast**  
Energinet  
Email: ffb@energinet.dk

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

**Need Help?** Run `phasor-cli --help` for command reference
