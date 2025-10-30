"""
Power calculation utilities for PhasorPoint CLI.

The ``PowerCalculator`` class encapsulates phasor column detection, voltage
corrections, angle conversion, and the derivation of apparent/active/reactive
power metrics.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from .models import PhasorColumnMap

if TYPE_CHECKING:
    from .user_output import UserOutput


class PowerCalculator:
    """Compute power metrics from voltage and current phasor measurements."""

    def __init__(
        self, logger: logging.Logger | None = None, output: UserOutput | None = None
    ) -> None:
        self.logger = logger or logging.getLogger("phasor_cli")
        self.output = output

    # ---------------------------------------------------------------- Private --
    def _find_candidates_regex(
        self,
        df: pd.DataFrame,
        phase_patterns: Sequence[tuple[str, str]],
        suffix: str,
    ) -> dict[str, str]:
        """Match columns using regex for robust detection based on schema structure."""
        mapping: dict[str, str] = {}
        for phase_name, pattern in phase_patterns:
            regex = re.compile(pattern)
            # Schema: {v|i}_<PHASOR_NAME>_{m|a}
            candidates = [col for col in df.columns if col.endswith(suffix) and regex.search(col)]
            if candidates:
                # Prefer columns with "1" after phase letter (e.g., va1_m over va_m)
                phase_letter = phase_name[-1]  # 'a', 'b', 'c', '0', '1', '2'
                preferred = next(
                    (col for col in candidates if f"{phase_letter}1_" in col), candidates[0]
                )
                mapping[phase_name] = preferred
        return mapping

    # --------------------------------------------------------------- Detection --
    def detect_columns(self, df: pd.DataFrame) -> PhasorColumnMap:
        """Analyse dataframe columns and return a structured phasor column map."""
        if self.logger:
            self.logger.info("Detecting voltage and current columns...")

        freq_cols = [column for column in df.columns if column.startswith(("f", "dfdt"))]

        voltage_phases = [
            ("va", r"^v_.*_va1?_|^va1?_"),  # v_p3_va1_m, va1_m (not v1a, v2a, etc.)
            ("vb", r"^v_.*_vb1?_|^vb1?_"),  # v_p3_vb1_m, vb1_m (not v1b, v2b, etc.)
            ("vc", r"^v_.*_vc1?_|^vc1?_"),  # v_p3_vc1_m, vc1_m (not v1c, v2c, etc.)
            ("v1", r"^v_.*_v1(_1)?_|^v1(_1)?_"),  # v_p3_v1_1_m, v1_m (positive)
            ("v0", r"^v_.*_v0(_1)?_|^v0(_1)?_"),  # v_p3_v0_1_m, v0_m (zero)
            ("v2", r"^v_.*_v2(_1)?_|^v2(_1)?_"),  # v_p3_v2_1_m, v2_m (negative)
        ]
        current_phases = [
            ("ia", r"^i_.*_ia1?_|^ia1?_"),  # i_p3_ia1_m, ia1_m (not i1a, i2a, etc.)
            ("ib", r"^i_.*_ib1?_|^ib1?_"),  # i_p3_ib1_m, ib1_m (not i1b, i2b, etc.)
            ("ic", r"^i_.*_ic1?_|^ic1?_"),  # i_p3_ic1_m, ic1_m (not i1c, i2c, etc.)
            ("i1", r"^i_.*_i1(_1)?_|^i1(_1)?_"),  # i_p3_i1_1_m, i1_m (positive)
            ("i0", r"^i_.*_i0(_1)?_|^i0(_1)?_"),  # i_p3_i0_1_m, i0_m (zero)
            ("i2", r"^i_.*_i2(_1)?_|^i2(_1)?_"),  # i_p3_i2_1_m, i2_m (negative)
        ]

        voltage_magnitude = self._find_candidates_regex(df, voltage_phases, "_m")
        voltage_angle = self._find_candidates_regex(df, voltage_phases, "_a")
        current_magnitude = self._find_candidates_regex(df, current_phases, "_m")
        current_angle = self._find_candidates_regex(df, current_phases, "_a")

        if self.logger:
            self.logger.info("Frequency columns: %s", len(freq_cols))
            self.logger.info(
                "Voltage magnitude columns found: %s",
                dict(voltage_magnitude.items()),
            )
            self.logger.info(
                "Current magnitude columns found: %s",
                dict(current_magnitude.items()),
            )

        return PhasorColumnMap(
            voltage_magnitude=voltage_magnitude,
            voltage_angle=voltage_angle,
            current_magnitude=current_magnitude,
            current_angle=current_angle,
            frequency=freq_cols,
        )

    # -------------------------------------------------------------- Utilities --
    @staticmethod
    def build_required_columns_list(
        column_map: PhasorColumnMap, phases: Sequence[str] = ("va", "vb", "vc")
    ) -> list[str]:
        """Return the minimum set of columns required to calculate power."""
        required: list[str] = []
        for phase in phases:
            voltage_mag = column_map.voltage_magnitude.get(phase)
            current_mag = column_map.current_magnitude.get(f"{'i' + phase[1:]}")
            voltage_ang = column_map.voltage_angle.get(phase)
            current_ang = column_map.current_angle.get(f"{'i' + phase[1:]}")  # ia/ib/ic

            if voltage_mag:
                required.append(voltage_mag)
            if current_mag:
                required.append(current_mag)
            if voltage_ang:
                required.append(voltage_ang)
            if current_ang:
                required.append(current_ang)
        return required

    @staticmethod
    def log_power_calculations(extraction_log: dict | None, calculated_cols: Iterable[str]) -> None:
        if extraction_log is None:
            return
        for column in calculated_cols:
            extraction_log["column_changes"]["added"].append(
                {
                    "column": column,
                    "reason": "calculated_power_value",
                    "description": "Calculated from voltage and current phasor measurements",
                }
            )

    # ---------------------------------------------------------- Transformations
    def apply_voltage_corrections(
        self, df: pd.DataFrame, column_map: PhasorColumnMap
    ) -> pd.DataFrame:
        corrected = df.copy()
        for column in column_map.voltage_magnitude.values():
            corrected[column] = corrected[column] * np.sqrt(3)
        if self.logger:
            if column_map.voltage_magnitude:
                self.logger.info("Applied sqrt(3) correction to voltage magnitudes")
            else:
                self.logger.warning("No voltage magnitude columns found for correction")
        return corrected

    def convert_angles_to_degrees(
        self, df: pd.DataFrame, column_map: PhasorColumnMap
    ) -> pd.DataFrame:
        converted = df.copy()
        for column in column_map.voltage_angle.values():
            converted[column] = np.degrees(converted[column])
        for column in column_map.current_angle.values():
            converted[column] = np.degrees(converted[column])

        if self.logger:
            if column_map.voltage_angle or column_map.current_angle:
                self.logger.info("Converted angle columns from radians to degrees")
            else:
                self.logger.warning("No phasor angle columns found for conversion")
        return converted

    def calculate_power_values(
        self,
        df: pd.DataFrame,
        column_map: PhasorColumnMap,
        extraction_log: dict | None = None,
    ) -> pd.DataFrame:
        required_phases = ("va", "vb", "vc")

        missing_voltage = [
            phase for phase in required_phases if phase not in column_map.voltage_magnitude
        ]
        missing_current = [
            phase for phase in ("ia", "ib", "ic") if phase not in column_map.current_magnitude
        ]
        missing_voltage_angle = [
            phase for phase in required_phases if phase not in column_map.voltage_angle
        ]
        missing_current_angle = [
            phase for phase in ("ia", "ib", "ic") if phase not in column_map.current_angle
        ]

        if missing_voltage or missing_current or missing_voltage_angle or missing_current_angle:
            missing = {
                "voltage_magnitude": missing_voltage,
                "current_magnitude": missing_current,
                "voltage_angle": missing_voltage_angle,
                "current_angle": missing_current_angle,
            }
            if self.logger:
                self.logger.warning("Cannot calculate power - missing columns: %s", missing)
            if extraction_log is not None:
                extraction_log["issues_found"].append(
                    {
                        "type": "missing_columns_for_calculation",
                        "missing_columns": missing,
                        "description": "Cannot calculate power values due to missing columns",
                    }
                )
            return df

        working = df.copy()

        va_m = working[column_map.voltage_magnitude["va"]]
        vb_m = working[column_map.voltage_magnitude["vb"]]
        vc_m = working[column_map.voltage_magnitude["vc"]]

        ia_m = working[column_map.current_magnitude["ia"]]
        ib_m = working[column_map.current_magnitude["ib"]]
        ic_m = working[column_map.current_magnitude["ic"]]

        va_a = working[column_map.voltage_angle["va"]]
        vb_a = working[column_map.voltage_angle["vb"]]
        vc_a = working[column_map.voltage_angle["vc"]]

        ia_a = working[column_map.current_angle["ia"]]
        ib_a = working[column_map.current_angle["ib"]]
        ic_a = working[column_map.current_angle["ic"]]

        working["apparent_power_mva"] = (
            (va_m * ia_m) / 1_000_000 + (vb_m * ib_m) / 1_000_000 + (vc_m * ic_m) / 1_000_000
        )

        working["active_power_mw"] = (
            (va_m * ia_m * np.cos(np.deg2rad(va_a - ia_a))) / 1_000_000
            + (vb_m * ib_m * np.cos(np.deg2rad(vb_a - ib_a))) / 1_000_000
            + (vc_m * ic_m * np.cos(np.deg2rad(vc_a - ic_a))) / 1_000_000
        )

        working["reactive_power_mvar"] = (
            (va_m * ia_m * np.sin(np.deg2rad(va_a - ia_a))) / 1_000_000
            + (vb_m * ib_m * np.sin(np.deg2rad(vb_a - ib_a))) / 1_000_000
            + (vc_m * ic_m * np.sin(np.deg2rad(vc_a - ic_a))) / 1_000_000
        )

        calculated = ["apparent_power_mva", "active_power_mw", "reactive_power_mvar"]
        self.log_power_calculations(extraction_log, calculated)

        if self.logger:
            self.logger.info("Successfully calculated power metrics")

        return working

    # -------------------------------------------------------------- Orchestration
    def process_phasor_data(
        self,
        df: pd.DataFrame,
        *,
        extraction_log: dict | None = None,
    ) -> tuple[pd.DataFrame, PhasorColumnMap]:
        """
        Apply power-related transformations.

        Returns a tuple of ``(dataframe_with_power_columns, phasor_column_map)``.
        """
        if df is None or len(df) == 0:
            return df, PhasorColumnMap()

        working = df.copy()
        had_timestamp_column = "ts" in working.columns

        if had_timestamp_column:
            working = working.set_index("ts")

        column_map = self.detect_columns(working)
        working = self.apply_voltage_corrections(working, column_map)
        working = self.convert_angles_to_degrees(working, column_map)
        working = self.calculate_power_values(working, column_map, extraction_log)

        if had_timestamp_column:
            working = working.reset_index(drop=False)

        return working, column_map


# --------------------------------------------------------------- Helper Functions --
def detect_phasor_columns(
    df: pd.DataFrame, logger: logging.Logger | None = None
) -> PhasorColumnMap:
    return PowerCalculator(logger=logger).detect_columns(df)


def apply_voltage_corrections(
    df: pd.DataFrame, column_map: PhasorColumnMap, logger: logging.Logger | None = None
) -> pd.DataFrame:
    return PowerCalculator(logger=logger).apply_voltage_corrections(df, column_map)


def convert_angles_to_degrees(
    df: pd.DataFrame, column_map: PhasorColumnMap, logger: logging.Logger | None = None
) -> pd.DataFrame:
    return PowerCalculator(logger=logger).convert_angles_to_degrees(df, column_map)


def build_required_columns_list(column_map: PhasorColumnMap) -> list[str]:
    return PowerCalculator.build_required_columns_list(column_map)


def log_power_calculations(extraction_log, calculated_cols):
    PowerCalculator.log_power_calculations(extraction_log, calculated_cols)


def calculate_power_values(
    df: pd.DataFrame,
    column_map: PhasorColumnMap,
    extraction_log=None,
    logger: logging.Logger | None = None,
) -> pd.DataFrame:
    return PowerCalculator(logger=logger).calculate_power_values(df, column_map, extraction_log)
