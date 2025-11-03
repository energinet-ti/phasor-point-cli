# PhasorPoint CLI

Command-line interface for extracting and processing PMU (Phasor Measurement Unit) data from PhasorPoint databases.

**Author:** Frederik Fast (Energinet)  
**Repository:** [energinet-ti/phasor-point-cli](https://github.com/energinet-ti/phasor-point-cli)

## Features

- **Flexible Time Ranges**: Extract by relative time (hours, days) or absolute dates
- **Automatic Processing**: Power calculations (S, P, Q) and data quality validation
- **Performance Options**: Chunking, parallel processing, connection pooling
- **Batch Operations**: Extract from multiple PMUs simultaneously
- **Extraction Logs**: Automatic metadata tracking with timezone information
- **Multiple Formats**: Parquet (recommended) or CSV

## Installation

### From PyPI (Recommended)

Install directly from PyPI:

```bash
python -m pip install phasor-point-cli
```

Verify installation:

```bash
python -m phasor_point_cli --help
```

### From GitHub Releases

Download the latest `.whl` file from the [Releases page](https://github.com/energinet-ti/phasor-point-cli/releases):

```bash
python -m pip install phasor_point_cli-<version>-py3-none-any.whl
```

### From Source

Clone and install:

```bash
git clone https://github.com/energinet-ti/phasor-point-cli.git
cd phasor-point-cli
./scripts/setup.sh           # Linux/macOS
# .\scripts\setup.ps1         # Windows PowerShell
```

Manual installation:

```bash
python3 -m venv venv
source venv/bin/activate      # Linux/macOS
# venv\Scripts\Activate.ps1   # Windows

pip install -e .[dev]         # Development mode
# pip install .               # Standard install
```

**Requirements:**
- Python 3.8+
- PhasorPoint ODBC driver ("Psymetrix PhasorPoint")

## Quick Start

### Setup

```bash
# Create configuration files (interactive by default)
python -m phasor_point_cli setup              # User-level (~/.config/phasor-cli/)
python -m phasor_point_cli setup --local      # Project-specific (./)

# Non-interactive setup (creates template files)
python -m phasor_point_cli setup --no-interactive

# View active configuration
python -m phasor_point_cli config
```

Interactive setup (default) will prompt you for:
- Database host, port, name
- Database credentials (password input is hidden)

Non-interactive setup creates template files you can edit:
- `.env` - Database credentials (never commit!)
- `config.json` - Settings and PMU metadata

**Configuration priority:** Environment Variables > Local Files > User Config > Defaults

### Basic Usage

```bash
# List available PMUs
python -m phasor_point_cli list-tables

# Get PMU information
python -m phasor_point_cli table-info --pmu 45020

# Extract 1 hour of data
python -m phasor_point_cli extract --pmu 45020 --hours 1 --output data.parquet

# Extract with power calculations
python -m phasor_point_cli extract --pmu 45020 --hours 1 --processed --output data.parquet
```

## Command Reference

### Data Extraction

**Relative time (from now, going backwards):**

```bash
python -m phasor_point_cli extract --pmu 45020 --minutes 30 --output data.parquet
python -m phasor_point_cli extract --pmu 45020 --hours 2 --output data.parquet
python -m phasor_point_cli extract --pmu 45020 --days 1 --output data.parquet
```

**Absolute date range:**

```bash
python -m phasor_point_cli extract --pmu 45020 \
  --start "2024-07-15 08:00:00" \
  --end "2024-07-15 10:00:00" \
  --output data.parquet
```

**Start time + duration (goes forward):**

```bash
python -m phasor_point_cli extract --pmu 45020 \
  --start "2024-07-15 08:00:00" \
  --hours 2 \
  --output data.parquet
```

**With processing (power calculations):**

```bash
python -m phasor_point_cli extract --pmu 45020 --hours 1 --processed --output data.parquet
```

**Performance optimization:**

```bash
# Parallel processing (4 workers)
python -m phasor_point_cli extract --pmu 45020 --hours 24 --parallel 4 --output data.parquet

# Custom chunk size + connection pooling
python -m phasor_point_cli extract --pmu 45020 --hours 48 \
  --chunk-size 15 \
  --connection-pool 3 \
  --output data.parquet

# Performance diagnostics
python -m phasor_point_cli extract --pmu 45020 --hours 1 --diagnostics --output data.parquet
```

### Batch Extraction

Extract from multiple PMUs:

```bash
python -m phasor_point_cli batch-extract --pmus "45020,45022,45052" --hours 1 --output-dir ./data/

# With performance optimization
python -m phasor_point_cli batch-extract --pmus "45020,45022" --hours 24 \
  --chunk-size 30 \
  --parallel 2 \
  --output-dir ./data/
```

Files are named: `pmu_{number}_{resolution}hz_{start_date}_to_{end_date}.{format}`

### Database Exploration

```bash
# List all PMU tables
python -m phasor_point_cli list-tables

# Get PMU information
python -m phasor_point_cli table-info --pmu 45020

# Custom SQL query
python -m phasor_point_cli query --sql "SELECT TOP 100 * FROM pmu_45020_1"
```

## Data Structure

### Columns

**Timestamps:**
- `ts` - UTC timestamp (authoritative, unambiguous)
- `ts_local` - Local wall-clock time (converted from UTC with per-row DST handling)

**Measurements:** Original PhasorPoint column names (e.g., `f`, `dfdt`, `va1_m`, `va1_a`, `ia1_m`, `ia1_a`)

**Calculated Power** (with `--processed` flag):
- `apparent_power_mva` - Apparent power (S)
- `active_power_mw` - Active power (P)
- `reactive_power_mvar` - Reactive power (Q)

### Daylight Saving Time (DST)

DST transitions are handled automatically:

**User Input:**
- Specify dates in local wall-clock time
- The system applies the correct DST offset for that date, not the current season
- Example: "2024-07-15 10:00:00" is interpreted as summer time even if requested in January

**Output Data:**
- `ts`: Authoritative UTC timestamps (always unambiguous)
- `ts_local`: Local wall-clock times (may have duplicates during fall-back transition, per-row DST aware)

**Ambiguous Times:**
- During DST fall-back, ambiguous times (e.g., "02:30") use the first occurrence (DST active)

**Extraction Log:**
- Check `_extraction_log.json` for UTC offset information:
  ```json
  {
    "extraction_info": {
      "timezone": "Europe/Copenhagen",
      "utc_offset_start": "+02:00",
      "utc_offset_end": "+01:00"
    }
  }
  ```

### Using Data in Python

```python
import pandas as pd

df = pd.read_parquet('data.parquet')

# Access measurements
print(df.f.mean())  # frequency
print(df.va1_m.describe())  # voltage magnitude

# Use ts (UTC) for unambiguous time operations
df_sorted = df.sort_values('ts')
df_filtered = df[df.ts >= '2024-07-15 08:00:00']

# Use ts_local for wall-clock time display
print(df[['ts', 'ts_local', 'f']].head())

# Access calculated power (if --processed was used)
print(df.active_power_mw.sum())
```

## Extraction Logs

Each extraction creates a `_extraction_log.json` file documenting:
- Extraction parameters and timestamps
- Timezone and UTC offset information
- Column transformations and calculations
- Data quality issues detected
- Processing steps applied

## Performance

**Automatic Chunking:**
- Large time ranges (>5 minutes) are automatically chunked for memory efficiency
- Customize with `--chunk-size N` (in minutes)

**Parallel Processing:**
- Use `--parallel N` to process chunks simultaneously
- Best for large extractions (>1 hour)

**Connection Pooling:**
- Use `--connection-pool N` to reuse database connections
- Reduces connection overhead for chunked extractions

**Recommended for large extractions:**
```bash
python -m phasor_point_cli extract --pmu 45020 --hours 24 \
  --chunk-size 15 \
  --parallel 2 \
  --connection-pool 3 \
  --output data.parquet
```

## Data Quality

Automatic validation includes:
- Type conversion to proper numeric types
- Empty column detection and removal
- Null value detection
- Frequency range validation (45-65 Hz)
- Time gap detection
- Voltage range checks

Results are logged in `_extraction_log.json`.

## Output Formats

**Parquet** (recommended):
- Compressed and fast
- Preserves data types
- Best for Python/pandas workflows

**CSV**:
- Human-readable
- Works in Excel
- Good for small datasets and manual inspection

## Security

⚠️ **Never commit `.env` files** to version control. They contain database credentials.

Ensure `.env` is in `.gitignore`:
```bash
echo ".env" >> .gitignore
```

## Troubleshooting

### Connection Issues

Test database connection:
```bash
python -m phasor_point_cli list-tables
```

Check credentials in `.env` file.

### Missing Data

Check available date range:
```bash
python -m phasor_point_cli table-info --pmu 45020
```

### Encoding Errors

Use Parquet format instead of CSV for large datasets:
```bash
python -m phasor_point_cli extract --pmu 45020 --hours 1 --output data.parquet
```

## Development

### Setup

```bash
./scripts/setup.sh           # Linux/macOS
# .\scripts\setup.ps1         # Windows
```

This creates a virtual environment and installs dev dependencies.

### Testing

```bash
# Run all tests
make test

# Run with coverage
make coverage

# Run all quality checks (lint + format + tests)
make check

# Run type checking
make type-check
```

### Code Quality

```bash
# Auto-format code
make format

# Fix linting issues
make fix
```

### Building

```bash
# Build wheel distribution
make build
```

Output: `dist/phasor_point_cli-<version>-py3-none-any.whl`

### Versioning

This project uses **setuptools-scm** for automatic version management:
- Version is derived from git tags
- Development builds get automatic `.devN` suffixes
- Clean releases require a git tag (e.g., `v1.0.0`)

**Creating a release:**

```bash
# Create release branch
./scripts/create_release.sh 1.0.0 "Release description"

# Then:
# 1. Create PR: release/1.0.0 → main
# 2. Review and merge
# 3. Create git tag v1.0.0 on main
# 4. GitHub Actions auto-publishes
```

See [docs/RELEASING.md](docs/RELEASING.md) for details.

## License

Apache License 2.0

## Contributing

Contributions are welcome! Submit a Pull Request or open an Issue.

## Contact

**Frederik Fast**  
Energinet  
ffb@energinet.dk

---

**Need Help?** Run `python -m phasor_point_cli --help` for command reference
