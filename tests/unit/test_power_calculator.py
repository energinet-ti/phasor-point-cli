"""
Unit tests for the PowerCalculator class.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from phasor_point_cli.models import PhasorColumnMap
from phasor_point_cli.power_calculator import (
    PowerCalculator,
    apply_voltage_corrections,
    build_required_columns_list,
    calculate_power_values,
    convert_angles_to_degrees,
    detect_phasor_columns,
    log_power_calculations,
)


def build_sample_dataframe():
    timestamps = pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq=timedelta(seconds=1))
    return pd.DataFrame(
        {
            "ts": timestamps,
            "va1_m": np.full(4, 230_000.0),
            "vb1_m": np.full(4, 230_000.0),
            "vc1_m": np.full(4, 230_000.0),
            "ia1_m": np.full(4, 400.0),
            "ib1_m": np.full(4, 400.0),
            "ic1_m": np.full(4, 400.0),
            "va1_a": np.linspace(0.0, 0.01, 4),
            "vb1_a": np.linspace(-2.09, -2.08, 4),
            "vc1_a": np.linspace(2.09, 2.10, 4),
            "ia1_a": np.linspace(-0.5, -0.49, 4),
            "ib1_a": np.linspace(-2.59, -2.58, 4),
            "ic1_a": np.linspace(1.59, 1.60, 4),
            "f": np.full(4, 50.0),
        }
    )


def test_detect_columns_returns_expected_mapping():
    # Arrange
    df = build_sample_dataframe()
    calculator = PowerCalculator()

    # Act
    column_map = calculator.detect_columns(df)

    # Assert
    assert column_map.voltage_magnitude["va"] == "va1_m"
    assert column_map.current_magnitude["ia"] == "ia1_m"
    assert column_map.frequency == ["f"]


def test_apply_voltage_corrections_scales_magnitudes():
    # Arrange
    df = build_sample_dataframe()
    calculator = PowerCalculator()
    column_map = calculator.detect_columns(df)

    # Act
    corrected = calculator.apply_voltage_corrections(df, column_map)

    # Assert
    assert corrected["va1_m"].iloc[0] == pytest.approx(df["va1_m"].iloc[0] * np.sqrt(3))


def test_calculate_power_values_missing_columns_logs_issue():
    # Arrange
    df = build_sample_dataframe()[["ts", "va1_m", "ia1_m", "va1_a", "ia1_a"]]  # minimal columns
    calculator = PowerCalculator()
    column_map = calculator.detect_columns(df)
    extraction_log = {"column_changes": {"added": []}, "issues_found": []}

    # Act
    result = calculator.calculate_power_values(df, column_map, extraction_log)

    # Assert
    assert "apparent_power_mva" not in result
    assert extraction_log["issues_found"]


def test_build_required_columns_list():
    """Test building the required columns list for power calculations."""
    # Arrange
    column_map = PhasorColumnMap(
        voltage_magnitude={"va": "va1_m", "vb": "vb1_m", "vc": "vc1_m"},
        voltage_angle={"va": "va1_a", "vb": "vb1_a", "vc": "vc1_a"},
        current_magnitude={"ia": "ia1_m", "ib": "ib1_m", "ic": "ic1_m"},
        current_angle={"ia": "ia1_a", "ib": "ib1_a", "ic": "ic1_a"},
        frequency=["f"],
    )

    # Act
    required = PowerCalculator.build_required_columns_list(column_map)

    # Assert
    assert len(required) == 12  # 3 phases * 4 measurements each
    assert "va1_m" in required
    assert "ia1_a" in required
    assert "vc1_m" in required


def test_build_required_columns_list_partial_phases():
    """Test building required columns list with partial phases."""
    # Arrange - only phase A
    column_map = PhasorColumnMap(
        voltage_magnitude={"va": "va1_m"},
        voltage_angle={"va": "va1_a"},
        current_magnitude={"ia": "ia1_m"},
        current_angle={"ia": "ia1_a"},
        frequency=["f"],
    )

    # Act
    required = PowerCalculator.build_required_columns_list(column_map, phases=("va", "vb"))

    # Assert
    assert len(required) == 4  # Only phase A has values
    assert "va1_m" in required
    assert "va1_a" in required


def test_log_power_calculations_with_extraction_log():
    """Test logging power calculations to extraction log."""
    # Arrange
    extraction_log = {"column_changes": {"added": []}}
    calculated_cols = ["apparent_power_mva", "active_power_mw", "reactive_power_mvar"]

    # Act
    PowerCalculator.log_power_calculations(extraction_log, calculated_cols)

    # Assert
    assert len(extraction_log["column_changes"]["added"]) == 3
    assert extraction_log["column_changes"]["added"][0]["column"] == "apparent_power_mva"
    assert extraction_log["column_changes"]["added"][0]["reason"] == "calculated_power_value"


def test_log_power_calculations_with_none():
    """Test logging power calculations with None extraction log."""
    # Act - should not raise
    PowerCalculator.log_power_calculations(None, ["test_col"])

    # Assert - no exception


def test_apply_voltage_corrections_with_logger():
    """Test voltage correction with logger output."""
    # Arrange
    df = build_sample_dataframe()
    logger = MagicMock()
    calculator = PowerCalculator(logger=logger)
    column_map = calculator.detect_columns(df)

    # Act
    corrected = calculator.apply_voltage_corrections(df, column_map)

    # Assert
    assert corrected["va1_m"].iloc[0] == pytest.approx(df["va1_m"].iloc[0] * np.sqrt(3))
    logger.info.assert_called()


def test_apply_voltage_corrections_no_voltage_columns():
    """Test voltage correction warning when no voltage columns present."""
    # Arrange
    df = pd.DataFrame({"ts": [1, 2, 3], "f": [50.0, 50.0, 50.0]})
    logger = MagicMock()
    calculator = PowerCalculator(logger=logger)
    column_map = PhasorColumnMap(voltage_magnitude={}, frequency=["f"])

    # Act
    calculator.apply_voltage_corrections(df, column_map)

    # Assert
    logger.warning.assert_called_once()
    assert "No voltage magnitude columns" in str(logger.warning.call_args)


def test_convert_angles_to_degrees():
    """Test conversion of angles from radians to degrees."""
    # Arrange
    df = build_sample_dataframe()
    calculator = PowerCalculator()
    column_map = calculator.detect_columns(df)

    # Act
    converted = calculator.convert_angles_to_degrees(df, column_map)

    # Assert
    # Radians near 0 should convert to degrees near 0
    assert converted["va1_a"].iloc[0] == pytest.approx(np.degrees(df["va1_a"].iloc[0]))
    assert converted["ia1_a"].iloc[0] == pytest.approx(np.degrees(df["ia1_a"].iloc[0]))


def test_convert_angles_to_degrees_with_logger():
    """Test angle conversion with logger."""
    # Arrange
    df = build_sample_dataframe()
    logger = MagicMock()
    calculator = PowerCalculator(logger=logger)
    column_map = calculator.detect_columns(df)

    # Act
    calculator.convert_angles_to_degrees(df, column_map)

    # Assert
    logger.info.assert_called()
    assert "Converted angle columns" in str(logger.info.call_args)


def test_convert_angles_to_degrees_no_angles():
    """Test angle conversion warning when no angle columns present."""
    # Arrange
    df = pd.DataFrame({"ts": [1, 2, 3], "va1_m": [230000, 230000, 230000]})
    logger = MagicMock()
    calculator = PowerCalculator(logger=logger)
    column_map = PhasorColumnMap(voltage_magnitude={"va": "va1_m"})

    # Act
    calculator.convert_angles_to_degrees(df, column_map)

    # Assert
    logger.warning.assert_called_once()
    assert "No phasor angle columns" in str(logger.warning.call_args)


def test_calculate_power_values_full_success():
    """Test full power calculation with all required columns."""
    # Arrange
    df = build_sample_dataframe()
    logger = MagicMock()
    calculator = PowerCalculator(logger=logger)
    column_map = calculator.detect_columns(df)

    # Apply corrections first
    df = calculator.apply_voltage_corrections(df, column_map)
    df = calculator.convert_angles_to_degrees(df, column_map)

    extraction_log = {"column_changes": {"added": []}, "issues_found": []}

    # Act
    result = calculator.calculate_power_values(df, column_map, extraction_log)

    # Assert
    assert "apparent_power_mva" in result.columns
    assert "active_power_mw" in result.columns
    assert "reactive_power_mvar" in result.columns

    # Power values should be positive and reasonable
    assert result["apparent_power_mva"].iloc[0] > 0
    assert result["active_power_mw"].iloc[0] > 0

    # Check extraction log was updated
    assert len(extraction_log["column_changes"]["added"]) == 3

    # Logger should confirm success
    logger.info.assert_called()


def test_calculate_power_values_missing_voltage_angle():
    """Test power calculation when voltage angle columns are missing."""
    # Arrange
    df = build_sample_dataframe()[["ts", "va1_m", "vb1_m", "vc1_m", "ia1_m", "ib1_m", "ic1_m"]]
    calculator = PowerCalculator()
    column_map = calculator.detect_columns(df)
    extraction_log = {"column_changes": {"added": []}, "issues_found": []}

    # Act
    result = calculator.calculate_power_values(df, column_map, extraction_log)

    # Assert
    assert "apparent_power_mva" not in result.columns
    assert len(extraction_log["issues_found"]) > 0
    assert extraction_log["issues_found"][0]["type"] == "missing_columns_for_calculation"


def test_process_phasor_data_full_workflow():
    """Test complete phasor data processing workflow."""
    # Arrange
    df = build_sample_dataframe()
    logger = MagicMock()
    calculator = PowerCalculator(logger=logger)
    extraction_log = {"column_changes": {"added": []}, "issues_found": []}

    # Act
    result_df, column_map = calculator.process_phasor_data(df, extraction_log=extraction_log)

    # Assert
    assert "ts" in result_df.columns  # Timestamp preserved
    assert "apparent_power_mva" in result_df.columns
    assert "active_power_mw" in result_df.columns
    assert "reactive_power_mvar" in result_df.columns
    assert len(column_map.voltage_magnitude) == 3  # va, vb, vc


def test_process_phasor_data_empty_dataframe():
    """Test processing empty dataframe."""
    # Arrange
    df = pd.DataFrame()
    calculator = PowerCalculator()

    # Act
    result_df, column_map = calculator.process_phasor_data(df)

    # Assert - should return empty dataframe and empty column map
    assert len(result_df) == 0
    assert len(column_map.voltage_magnitude) == 0


def test_process_phasor_data_none_dataframe():
    """Test processing None dataframe."""
    # Arrange
    calculator = PowerCalculator()

    # Act
    result_df, column_map = calculator.process_phasor_data(None)

    # Assert
    assert result_df is None
    assert len(column_map.voltage_magnitude) == 0


def test_process_phasor_data_without_timestamp():
    """Test processing dataframe without timestamp column."""
    # Arrange
    df = build_sample_dataframe().drop(columns=["ts"])
    calculator = PowerCalculator()

    # Act
    result_df, column_map = calculator.process_phasor_data(df)

    # Assert
    assert "ts" not in result_df.columns  # Should not add ts if not present
    assert "apparent_power_mva" in result_df.columns


# ---------------------------------------------------------------- Wrapper Tests --
def test_detect_phasor_columns_wrapper():
    """Test module-level detect_phasor_columns wrapper function."""
    # Arrange
    df = build_sample_dataframe()
    logger = MagicMock()

    # Act
    column_map = detect_phasor_columns(df, logger=logger)

    # Assert
    assert column_map.voltage_magnitude["va"] == "va1_m"
    assert column_map.frequency == ["f"]


def test_apply_voltage_corrections_wrapper():
    """Test module-level apply_voltage_corrections wrapper function."""
    # Arrange
    df = build_sample_dataframe()
    calculator = PowerCalculator()
    column_map = calculator.detect_columns(df)

    # Act
    corrected = apply_voltage_corrections(df, column_map)

    # Assert
    assert corrected["va1_m"].iloc[0] == pytest.approx(df["va1_m"].iloc[0] * np.sqrt(3))


def test_convert_angles_to_degrees_wrapper():
    """Test module-level convert_angles_to_degrees wrapper function."""
    # Arrange
    df = build_sample_dataframe()
    calculator = PowerCalculator()
    column_map = calculator.detect_columns(df)

    # Act
    converted = convert_angles_to_degrees(df, column_map)

    # Assert
    assert converted["va1_a"].iloc[0] == pytest.approx(np.degrees(df["va1_a"].iloc[0]))


def test_build_required_columns_list_wrapper():
    """Test module-level build_required_columns_list wrapper function."""
    # Arrange
    column_map = PhasorColumnMap(
        voltage_magnitude={"va": "va1_m"},
        voltage_angle={"va": "va1_a"},
        current_magnitude={"ia": "ia1_m"},
        current_angle={"ia": "ia1_a"},
        frequency=["f"],
    )

    # Act
    required = build_required_columns_list(column_map)

    # Assert
    assert "va1_m" in required


def test_log_power_calculations_wrapper():
    """Test module-level log_power_calculations wrapper function."""
    # Arrange
    extraction_log = {"column_changes": {"added": []}}
    calculated_cols = ["test_col"]

    # Act
    log_power_calculations(extraction_log, calculated_cols)

    # Assert
    assert len(extraction_log["column_changes"]["added"]) == 1


def test_calculate_power_values_wrapper():
    """Test module-level calculate_power_values wrapper function."""
    # Arrange
    df = build_sample_dataframe()
    calculator = PowerCalculator()
    column_map = calculator.detect_columns(df)
    df = calculator.apply_voltage_corrections(df, column_map)
    df = calculator.convert_angles_to_degrees(df, column_map)

    # Act
    result = calculate_power_values(df, column_map)

    # Assert
    assert "apparent_power_mva" in result.columns
