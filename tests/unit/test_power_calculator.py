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
    df: pd.DataFrame = build_sample_dataframe()[["ts", "va1_m", "ia1_m", "va1_a", "ia1_a"]]  # type: ignore[assignment]
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


def test_detect_columns_with_positive_sequence():
    """Test detection of positive sequence current (i1) and voltage (v1) columns."""
    # Arrange
    timestamps = pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq=timedelta(seconds=1))
    df = pd.DataFrame(
        {
            "ts": timestamps,
            "va1_m": np.full(4, 230_000.0),
            "va1_a": np.linspace(0.0, 0.01, 4),
            "v1_m": np.full(4, 230_000.0),
            "v1_a": np.linspace(0.0, 0.01, 4),
            "ia1_m": np.full(4, 400.0),
            "ia1_a": np.linspace(-0.5, -0.49, 4),
            "i1_m": np.full(4, 400.0),
            "i1_a": np.linspace(-0.0466, -0.0456, 4),
            "f": np.full(4, 50.0),
        }
    )
    calculator = PowerCalculator()

    # Act
    column_map = calculator.detect_columns(df)

    # Assert
    assert column_map.voltage_magnitude["v1"] == "v1_m"
    assert column_map.voltage_angle["v1"] == "v1_a"
    assert column_map.current_magnitude["i1"] == "i1_m"
    assert column_map.current_angle["i1"] == "i1_a"


def test_convert_i1_angle_to_degrees():
    """Test conversion of i1 angle from radians to degrees - fixes bug where i1 was exported in radians."""
    # Arrange
    timestamps = pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq=timedelta(seconds=1))
    i1_angle_radians = np.array([-0.0466, -0.0456, -0.0446, -0.0436])
    df = pd.DataFrame(
        {
            "ts": timestamps,
            "i1_m": np.full(4, 400.0),
            "i1_a": i1_angle_radians,
        }
    )
    calculator = PowerCalculator()
    column_map = calculator.detect_columns(df)

    # Act
    converted = calculator.convert_angles_to_degrees(df, column_map)

    # Assert
    # Verify conversion: -0.0466 radians should become approximately -2.67 degrees
    assert converted["i1_a"].iloc[0] == pytest.approx(np.degrees(i1_angle_radians[0]))
    assert converted["i1_a"].iloc[0] == pytest.approx(-2.67, abs=0.01)
    # Verify all values are converted correctly
    for i in range(4):
        assert converted["i1_a"].iloc[i] == pytest.approx(np.degrees(i1_angle_radians[i]))


def test_convert_v1_and_i1_angles_together():
    """Test that both v1 and i1 positive sequence angles are converted correctly."""
    # Arrange
    timestamps = pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq=timedelta(seconds=1))
    df = pd.DataFrame(
        {
            "ts": timestamps,
            "v1_m": np.full(4, 230_000.0),
            "v1_a": np.linspace(0.0, 0.01, 4),
            "i1_m": np.full(4, 400.0),
            "i1_a": np.linspace(-0.0466, -0.0456, 4),
        }
    )
    calculator = PowerCalculator()
    column_map = calculator.detect_columns(df)

    # Act
    converted = calculator.convert_angles_to_degrees(df, column_map)

    # Assert
    # Both v1 and i1 angles should be converted
    assert converted["v1_a"].iloc[0] == pytest.approx(np.degrees(df["v1_a"].iloc[0]))
    assert converted["i1_a"].iloc[0] == pytest.approx(np.degrees(df["i1_a"].iloc[0]))
    # Verify the specific i1 conversion that was failing before
    assert converted["i1_a"].iloc[0] == pytest.approx(-2.67, abs=0.01)


def test_detect_columns_with_pmu_naming_convention():
    """Test detection of i1 columns using PMU naming convention (e.g., i_tje_400_rev_i1_a)."""
    # Arrange
    timestamps = pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq=timedelta(seconds=1))
    df = pd.DataFrame(
        {
            "ts": timestamps,
            "i_tje_400_rev_i1_m": np.full(4, 400.0),
            "i_tje_400_rev_i1_a": np.linspace(-0.0466, -0.0456, 4),
            "i_edr_220_hrc_i1_m": np.full(4, 350.0),
            "i_edr_220_hrc_i1_a": np.linspace(-0.0500, -0.0490, 4),
        }
    )
    calculator = PowerCalculator()

    # Act
    column_map = calculator.detect_columns(df)

    # Assert
    # Should detect one of the i1 columns (the preferred one based on _find_candidates logic)
    assert "i1" in column_map.current_magnitude
    assert "i1" in column_map.current_angle
    assert column_map.current_magnitude["i1"] in ["i_tje_400_rev_i1_m", "i_edr_220_hrc_i1_m"]
    assert column_map.current_angle["i1"] in ["i_tje_400_rev_i1_a", "i_edr_220_hrc_i1_a"]


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
    df: pd.DataFrame = build_sample_dataframe()[
        ["ts", "va1_m", "vb1_m", "vc1_m", "ia1_m", "ib1_m", "ic1_m"]
    ]  # type: ignore[assignment]
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
    result_df, column_map = calculator.process_phasor_data(None)  # type: ignore[arg-type]

    # Assert
    assert result_df is None
    assert len(column_map.voltage_magnitude) == 0


def test_process_phasor_data_without_timestamp():
    """Test processing dataframe without timestamp column."""
    # Arrange
    df = build_sample_dataframe().drop(columns=["ts"])
    calculator = PowerCalculator()

    # Act
    result_df, _column_map = calculator.process_phasor_data(df)

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


def test_detect_sequence_components_with_real_pmu_patterns():
    """Test detection of sequence components using column patterns that mimic PMU data structure.

    Uses synthetic test data with column naming patterns similar to real PMU exports.
    Pattern style: v_<station>_<phasor>_<suffix>
    """
    # Arrange - synthetic test data with realistic column patterns
    timestamps = pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq=timedelta(seconds=1))
    df = pd.DataFrame(
        {
            "ts": timestamps,
            # Phase voltages (pattern: v_p3_va1_m)
            "v_p3_va1_m": np.full(4, 220_000.0),  # Test value
            "v_p3_va1_a": np.linspace(0.0, 0.02, 4),  # Test angles in radians
            "v_p3_vb1_m": np.full(4, 220_000.0),
            "v_p3_vb1_a": np.linspace(-2.1, -2.08, 4),
            "v_p3_vc1_m": np.full(4, 220_000.0),
            "v_p3_vc1_a": np.linspace(2.1, 2.12, 4),
            # Positive sequence (pattern: v_p3_v1_1_m)
            "v_p3_v1_1_m": np.full(4, 220_000.0),
            "v_p3_v1_1_a": np.linspace(0.0, 0.02, 4),
            # Zero sequence (pattern: v_p3_v0_1_m) - previously missing detection
            "v_p3_v0_1_m": np.full(4, 250.0),  # Test value (needs √3 correction)
            "v_p3_v0_1_a": np.array([0.3, 0.31, 0.32, 0.33]),  # Test angles in radians
            # Negative sequence (pattern: v_p3_v2_1_m) - previously missing detection
            "v_p3_v2_1_m": np.full(4, 250.0),  # Test value (needs √3 correction)
            "v_p3_v2_1_a": np.array([0.3, 0.31, 0.32, 0.33]),  # Test angles in radians
            # Phase currents
            "i_p3_ia1_m": np.full(4, 350.0),
            "i_p3_ia1_a": np.linspace(-0.4, -0.38, 4),
            "i_p3_ib1_m": np.full(4, 350.0),
            "i_p3_ib1_a": np.linspace(-2.5, -2.48, 4),
            "i_p3_ic1_m": np.full(4, 350.0),
            "i_p3_ic1_a": np.linspace(1.7, 1.72, 4),
            # Positive sequence current
            "i_p3_i1_1_m": np.full(4, 350.0),
            "i_p3_i1_1_a": np.linspace(-0.05, -0.04, 4),
            # Zero sequence current (pattern: i_p3_i0_1_m) - previously missing detection
            "i_p3_i0_1_m": np.full(4, 45.0),
            "i_p3_i0_1_a": np.array([0.3, 0.31, 0.32, 0.33]),  # Test angles in radians
            # Negative sequence current (pattern: i_p3_i2_1_m) - previously missing detection
            "i_p3_i2_1_m": np.full(4, 45.0),
            "i_p3_i2_1_a": np.array([0.3, 0.31, 0.32, 0.33]),  # Test angles in radians
            "f": np.full(4, 50.0),
        }
    )
    calculator = PowerCalculator()

    # Act
    column_map = calculator.detect_columns(df)

    # Assert - verify ALL sequence components are detected
    assert column_map.voltage_magnitude["va"] == "v_p3_va1_m"
    assert column_map.voltage_magnitude["vb"] == "v_p3_vb1_m"
    assert column_map.voltage_magnitude["vc"] == "v_p3_vc1_m"
    assert column_map.voltage_magnitude["v1"] == "v_p3_v1_1_m"
    assert column_map.voltage_magnitude["v0"] == "v_p3_v0_1_m"  # Zero sequence
    assert column_map.voltage_magnitude["v2"] == "v_p3_v2_1_m"  # Negative sequence

    assert column_map.current_magnitude["ia"] == "i_p3_ia1_m"
    assert column_map.current_magnitude["i1"] == "i_p3_i1_1_m"
    assert column_map.current_magnitude["i0"] == "i_p3_i0_1_m"  # Zero sequence
    assert column_map.current_magnitude["i2"] == "i_p3_i2_1_m"  # Negative sequence

    # Apply transformations
    corrected = calculator.apply_voltage_corrections(df, column_map)
    converted = calculator.convert_angles_to_degrees(corrected, column_map)

    # Assert voltage corrections (√3 multiplier)
    assert converted["v_p3_va1_m"].iloc[0] == pytest.approx(220_000.0 * np.sqrt(3))
    assert converted["v_p3_v0_1_m"].iloc[0] == pytest.approx(250.0 * np.sqrt(3), rel=1e-3)
    assert converted["v_p3_v2_1_m"].iloc[0] == pytest.approx(250.0 * np.sqrt(3), rel=1e-3)

    # Assert angle conversions (radians to degrees)
    assert converted["v_p3_v0_1_a"].iloc[0] == pytest.approx(np.degrees(0.3), rel=1e-3)
    assert converted["v_p3_v2_1_a"].iloc[0] == pytest.approx(np.degrees(0.3), rel=1e-3)
    assert converted["i_p3_i0_1_a"].iloc[0] == pytest.approx(np.degrees(0.3), rel=1e-3)
    assert converted["i_p3_i2_1_a"].iloc[0] == pytest.approx(np.degrees(0.3), rel=1e-3)


def test_detect_sequence_components_alternative_pmu_pattern():
    """Test detection with alternative column naming pattern (v_ta95_v0_1_m).

    Uses synthetic test data with alternative station identifier format.
    Pattern style: v_<station_code>_<phasor>_<suffix>
    """
    # Arrange - synthetic test data with alternative pattern
    timestamps = pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq=timedelta(seconds=1))
    df = pd.DataFrame(
        {
            "ts": timestamps,
            # Pattern with direct station prefix (v_ta95_ instead of v_p3_)
            "v_ta95_v1_1_m": np.full(4, 210_000.0),  # Test value
            "v_ta95_v1_1_a": np.linspace(0.0, 0.015, 4),
            "v_ta95_v0_1_m": np.full(4, 280.0),  # Test value
            "v_ta95_v0_1_a": np.array([0.25, 0.26, 0.27, 0.28]),
            "v_ta95_v2_1_m": np.full(4, 280.0),  # Test value
            "v_ta95_v2_1_a": np.array([0.25, 0.26, 0.27, 0.28]),
            "i_ta95_i1_1_m": np.full(4, 380.0),  # Test value
            "i_ta95_i1_1_a": np.linspace(-0.055, -0.045, 4),
            "i_ta95_i0_1_m": np.full(4, 42.0),  # Test value
            "i_ta95_i0_1_a": np.array([0.25, 0.26, 0.27, 0.28]),
            "i_ta95_i2_1_m": np.full(4, 42.0),  # Test value
            "i_ta95_i2_1_a": np.array([0.25, 0.26, 0.27, 0.28]),
        }
    )
    calculator = PowerCalculator()

    # Act
    column_map = calculator.detect_columns(df)

    # Assert
    assert column_map.voltage_magnitude["v1"] == "v_ta95_v1_1_m"
    assert column_map.voltage_magnitude["v0"] == "v_ta95_v0_1_m"
    assert column_map.voltage_magnitude["v2"] == "v_ta95_v2_1_m"
    assert column_map.current_magnitude["i1"] == "i_ta95_i1_1_m"
    assert column_map.current_magnitude["i0"] == "i_ta95_i0_1_m"
    assert column_map.current_magnitude["i2"] == "i_ta95_i2_1_m"


def test_detect_sequence_components_with_no_underscore_1_suffix():
    """Test detection with pattern without _1 suffix (v_sfb_30_ta95_v1_m).

    Uses synthetic test data with bus measurement naming convention.
    Pattern style: v_<location>_<bus_id>_<station>_<phasor>_<suffix>
    """
    # Arrange - synthetic test data with bus measurement pattern
    timestamps = pd.date_range(datetime(2025, 1, 1, 0, 0, 0), periods=4, freq=timedelta(seconds=1))
    df = pd.DataFrame(
        {
            "ts": timestamps,
            # Pattern without _1 suffix (v_sfb_30_ta95_v1_m instead of v_sfb_30_ta95_v1_1_m)
            "v_sfb_30_ta95_v1_m": np.full(4, 240_000.0),  # Test value
            "v_sfb_30_ta95_v1_a": np.linspace(0.0, 0.018, 4),  # Test angles
            "i_sfb_30_ta95_i1_m": np.full(4, 420.0),  # Test value
            "i_sfb_30_ta95_i1_a": np.linspace(-0.06, -0.05, 4),  # Test angles
        }
    )
    calculator = PowerCalculator()

    # Act
    column_map = calculator.detect_columns(df)

    # Assert - should match even without _1 suffix
    assert column_map.voltage_magnitude["v1"] == "v_sfb_30_ta95_v1_m"
    assert column_map.voltage_angle["v1"] == "v_sfb_30_ta95_v1_a"
    assert column_map.current_magnitude["i1"] == "i_sfb_30_ta95_i1_m"
    assert column_map.current_angle["i1"] == "i_sfb_30_ta95_i1_a"
