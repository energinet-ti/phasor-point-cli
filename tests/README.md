# PhasorPoint CLI Test Suite

Comprehensive test suite for the PhasorPoint CLI tool using pytest.

## Overview

This test suite follows best practices for Python testing:
- **Arrange-Act-Assert (AAA) pattern** for clear, maintainable tests
- **Mocked dependencies** to avoid database connections
- **Comprehensive fixtures** for reusable test data
- **Organized by component** for easy navigation

## Test Structure

```
tests/
├── __init__.py                    # Package initialization
├── conftest.py                    # Shared fixtures and configuration
├── test_utilities.py              # Utility function tests
├── test_data_processing.py        # Data processing and transformation tests
├── test_data_validation.py        # Data quality validation tests
├── test_cli_initialization.py     # CLI setup and configuration tests
└── README.md                      # This file
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run with verbose output
```bash
pytest -v
```

### Run specific test file
```bash
pytest tests/test_utilities.py
```

### Run specific test class
```bash
pytest tests/test_utilities.py::TestSanitizeFilename
```

### Run specific test
```bash
pytest tests/test_utilities.py::TestSanitizeFilename::test_sanitize_spaces
```

### Run tests by marker
```bash
# Run only unit tests (fast)
pytest -m unit

# Run only data processing tests
pytest -m data_processing

# Run only validation tests
pytest -m validation
```

### Run with coverage report
```bash
pytest --cov=src --cov-report=html
# Open htmlcov/index.html to view coverage report
```

### Run tests matching a pattern
```bash
pytest -k "sanitize"
pytest -k "power_calculation"
```

## Test Markers

Tests are organized with markers for easy filtering:

- `@pytest.mark.unit` - Fast unit tests, no external dependencies
- `@pytest.mark.integration` - Integration tests (may require database)
- `@pytest.mark.slow` - Slower running tests
- `@pytest.mark.db` - Tests requiring database access
- `@pytest.mark.cli` - CLI command tests
- `@pytest.mark.data_processing` - Data processing function tests
- `@pytest.mark.validation` - Data validation tests

## Writing New Tests

### Test Structure (Arrange-Act-Assert Pattern)

All tests should follow the AAA pattern for clarity:

```python
def test_example_function(fixture1, fixture2):
    """Test that example_function does X when Y"""
    # Arrange - Set up test data and preconditions
    input_data = create_test_data()
    expected_result = 42
    
    # Act - Execute the function being tested
    result = example_function(input_data)
    
    # Assert - Verify the expected outcome
    assert result == expected_result
```

### Example Test File

```python
"""
Tests for example module

Description of what this test module covers.
"""

import pytest
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / 'src'
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from phasor_point_cli.cli import PhasorPointCLI


@pytest.mark.unit
class TestExampleFunction:
    """Tests for the example_function"""
    
    def test_basic_case(self, mock_env_vars, mocker):
        """Test basic functionality"""
        # Arrange
        mocker.patch.object(PhasorPointCLI, '_load_config', return_value={})
        cli = PhasorPointCLI(skip_validation=True)
        test_input = "test_data"
        
        # Act
        result = cli.example_function(test_input)
        
        # Assert
        assert result is not None
        assert result == "expected_value"
```

## Available Fixtures

### Configuration Fixtures

- `sample_config` - Sample configuration dictionary
- `mock_env_vars` - Mocked environment variables for DB credentials
- `sample_extraction_log` - Empty extraction log structure

### Data Fixtures

- `sample_pmu_dataframe` - Clean PMU data with realistic values
- `sample_pmu_dataframe_with_nulls` - PMU data with intentional null values
- `fixed_time` - Fixed datetime for time-dependent tests

### Mock Fixtures

- `mock_db_connection` - Mocked database connection (no real DB needed)
- `mock_logger` - Mocked logger instance
- `mock_cli_instance` - Fully mocked CLI instance ready to use

### Utility Fixtures

- `temp_output_dir` - Temporary directory (auto-cleaned)

## Test Coverage Goals

Target coverage areas:
- ✅ Utility functions (sanitize_filename, get_station_name, etc.)
- ✅ Data validation (null checks, frequency ranges, time continuity)
- ✅ Data processing (voltage corrections, angle conversions, power calculations)
- ✅ CLI initialization and configuration
- ⏳ Database query functions (with mocks)
- ⏳ File I/O operations
- ⏳ CLI argument parsing
- ⏳ Batch extraction workflow

## Mocking Database Connections

Since the database is not accessible in the test environment, all database operations are mocked:

```python
def test_with_database(mock_db_connection, mock_env_vars, mocker):
    """Test database operation without real database"""
    # Arrange
    mocker.patch.object(PhasorPointCLI, '_load_config', return_value={})
    cli = PhasorPointCLI(skip_validation=False)
    
    # Mock query result
    mock_cursor = mock_db_connection.cursor.return_value
    mock_cursor.fetchone.return_value = ('result',)
    
    # Act
    result = cli.some_database_function()
    
    # Assert
    assert result is not None
    mock_db_connection.cursor.assert_called_once()
```

## Testing Time-Dependent Code

Use `freezegun` for testing time-dependent code:

```python
from freezegun import freeze_time

@freeze_time("2025-01-15 10:00:00")
def test_time_dependent_function():
    """Test function that depends on current time"""
    # Arrange
    expected_time = datetime(2025, 1, 15, 10, 0, 0)
    
    # Act
    result = get_current_time()
    
    # Assert
    assert result == expected_time
```

## Debugging Tests

### Run with debugging output
```bash
pytest -vv --tb=long
```

### Run with print statements
```bash
pytest -s
```

### Run last failed tests only
```bash
pytest --lf
```

### Run with PDB debugger
```bash
pytest --pdb
```

### Show local variables on failure
```bash
pytest --showlocals
```

## Continuous Integration

This test suite is designed to run in CI/CD pipelines:

```yaml
# Example GitHub Actions configuration
- name: Run tests
  run: |
    make setup
    pytest --cov=src --cov-report=xml
```

## Best Practices

1. **One concept per test** - Each test should verify one specific behavior
2. **Descriptive names** - Test names should describe what is being tested
3. **Clear AAA pattern** - Separate arrange, act, assert sections
4. **Mock external dependencies** - Database, file I/O, network calls
5. **Fast tests** - Unit tests should run in milliseconds
6. **Independent tests** - Tests should not depend on each other
7. **Use fixtures** - Reuse common setup code with fixtures
8. **Test edge cases** - Empty data, null values, boundary conditions

## Common Issues

### Import Errors
If you get import errors, ensure the `src` directory is in the Python path:
```python
import sys
from pathlib import Path

src_path = Path(__file__).parent.parent / 'src'
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
```

### Database Connection Errors
Ensure you're using the `mock_db_connection` fixture to avoid real database connections.

### Environment Variable Errors
Use the `mock_env_vars` fixture to set up required environment variables.

## Contributing

When adding new tests:
1. Follow the AAA pattern
2. Add appropriate markers
3. Use existing fixtures when possible
4. Update this README if adding new test categories
5. Ensure tests are independent and can run in any order

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-mock documentation](https://pytest-mock.readthedocs.io/)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
- [Arrange-Act-Assert pattern](http://wiki.c2.com/?ArrangeActAssert)

