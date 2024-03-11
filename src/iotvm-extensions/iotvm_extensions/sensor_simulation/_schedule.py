import datetime
from typing import Dict, List, Union

import numpy as np
import pandas as pd

from ._data_types import (
    IntendedSimulatedSensorOperation,
    EvaluatedSimulatedSensorOperation,
    RecurringWindow,
)
from ._macros import evaluate_macro


def build_template_df(
    recurring_windows_numbers: List[int],
    start_dt: datetime.datetime,
    cycle_iterations: int,
    frequency: str,
) -> pd.DataFrame:
    assert len(recurring_windows_numbers) == len(set(recurring_windows_numbers))
    recurring_windows_numbers.sort()
    min_number: int = min(recurring_windows_numbers)
    max_number: int = max(recurring_windows_numbers)
    assert min_number == 1
    assert recurring_windows_numbers == list(range(min_number, max_number + 1))

    recurring_windows_numbers_count: int = len(recurring_windows_numbers)

    dt_index: pd.DatetimeIndex = pd.date_range(
        start=start_dt,
        # +1 because the last row will have NaT end_dt.
        periods=(recurring_windows_numbers_count * cycle_iterations) + 1,
        freq=frequency,
        unit="ns",
    )

    dataframe: pd.DataFrame = pd.DataFrame(data={"start_dt": dt_index})
    dataframe["end_dt"] = dataframe["start_dt"].shift(-1)
    t_delta: pd.Timedelta = pd.Timedelta(nanoseconds=1)
    dataframe["end_dt"] = dataframe["end_dt"] - t_delta
    dataframe.dropna(inplace=True)
    dataframe["recurring_window"] = recurring_windows_numbers * cycle_iterations

    cycle_values: List[int] = []
    for cycle in range(1, cycle_iterations + 1):
        # noinspection PyTypeChecker
        cycle_values.extend(np.full((recurring_windows_numbers_count,), fill_value=cycle, dtype=int).tolist())

    dataframe["cycle"] = cycle_values

    return dataframe


def build_schedule_df(
    macros_df: pd.DataFrame,
    start_dt: datetime.datetime,
    cycle_iterations: int,
    frequency: str,
    start_dt_pct: float,
    end_dt_pct: float,
    # Meta.
    additional_static: Dict[str, Union[str, int, bool]],
) -> pd.DataFrame:
    # Extract information from dataframe.
    # --------------------------------------------------

    sensor_ids: List[str] = list(macros_df.columns)
    sensor_ids.remove("recurring_window")
    sensor_ids.sort()
    assert len(sensor_ids) == len(set(sensor_ids))

    recurring_windows_numbers: List[int] = list(macros_df["recurring_window"].unique())
    recurring_windows_numbers.sort()
    assert len(recurring_windows_numbers) == len(set(recurring_windows_numbers))
    min_number: int = min(recurring_windows_numbers)
    max_number: int = max(recurring_windows_numbers)
    assert min_number >= 1
    assert recurring_windows_numbers == list(range(min_number, max_number + 1))

    # Build recurring windows instances.
    # --------------------------------------------------

    recurring_windows: List[RecurringWindow] = []
    recurring_windows_by_number: Dict[int, RecurringWindow] = {}
    grouped = macros_df.groupby(by="recurring_window")

    for number, group in grouped:
        intended_simulated_sensor_operation_list: List[IntendedSimulatedSensorOperation] = []
        evaluated_simulated_sensor_operation_list: List[EvaluatedSimulatedSensorOperation] = []

        for row_index, row_values in group.iterrows():
            for sensor_id in sensor_ids:
                if pd.isna(row_values[sensor_id]):
                    continue

                cell_value: str = row_values[sensor_id]
                cell_value = cell_value.strip().lower()
                if cell_value == "" or cell_value.startswith("empty"):
                    continue

                intended_simulated_sensor_operation: IntendedSimulatedSensorOperation = (
                    IntendedSimulatedSensorOperation(sensor_id=sensor_id, macro=row_values[sensor_id])
                )
                intended_simulated_sensor_operation_list.append(intended_simulated_sensor_operation)

                # TODO ΠΡΟΣΟΧΗ: πιο κάτω το ξανακάνω. Συνεπώς αυτό είναι ενδεικτικό. ΧΡΕΙΑΖΕΤΑΙ;;;!
                evaluated_simulated_sensor_operation: EvaluatedSimulatedSensorOperation = evaluate_macro(
                    string=intended_simulated_sensor_operation.macro,
                    sensor_id=intended_simulated_sensor_operation.sensor_id,
                    timestamp=0,
                    additional={},
                )
                evaluated_simulated_sensor_operation_list.append(evaluated_simulated_sensor_operation)

        recurring_window: RecurringWindow = RecurringWindow(
            number=int(number),
            intended_ops=intended_simulated_sensor_operation_list,
            evaluated_ops=evaluated_simulated_sensor_operation_list,
        )
        recurring_windows.append(recurring_window)
        recurring_windows_by_number[recurring_window.number] = recurring_window

    # Inspection.
    # --------------------------------------------------

    for recurring_window in recurring_windows:
        print(recurring_window.to_print_string())

    # Build schedule template.
    # Schedule template is a DataFrame containing the intervals
    # in which the operations will be planned to be executed.
    # --------------------------------------------------

    template_df: pd.DataFrame = build_template_df(
        recurring_windows_numbers=recurring_windows_numbers,
        start_dt=start_dt,
        cycle_iterations=cycle_iterations,
        frequency=frequency,
    )

    # Calculate deltas to for operations intervals.
    # --------------------------------------------------

    t_delta: pd.Timedelta = template_df["end_dt"].iloc[-1] - template_df["start_dt"].iloc[-1]
    seconds: float = t_delta.total_seconds()
    add_to_start_dt: pd.Timedelta = pd.Timedelta(seconds=seconds * start_dt_pct, unit="ns")
    sub_from_end_dt: pd.Timedelta = pd.Timedelta(seconds=seconds * end_dt_pct, unit="ns")

    # Calculate operations intervals.
    # --------------------------------------------------

    template_df["operations_start_dt"] = template_df["start_dt"] + add_to_start_dt
    template_df["operations_end_dt"] = template_df["end_dt"] - sub_from_end_dt

    # Build schedule dataframe.
    # --------------------------------------------------

    data: List[Dict] = []

    for index, row in template_df.iterrows():
        recurring_window_number = row["recurring_window"]
        cycle = row["cycle"]
        start_dt = row["start_dt"]
        end_dt = row["end_dt"]
        operations_start_dt = row["operations_start_dt"]
        operations_end_dt = row["operations_end_dt"]

        intended_ops: List[IntendedSimulatedSensorOperation] = recurring_windows_by_number[
            recurring_window_number
        ].intended_ops
        intended_ops_count: int = len(intended_ops)

        pdt_list: List[pd.Timestamp] = pd.date_range(
            start=operations_start_dt,
            end=operations_end_dt,
            periods=intended_ops_count,
        ).to_list()

        assert len(pdt_list) == intended_ops_count

        for i, intended_op in enumerate(intended_ops):
            evaluated_op: EvaluatedSimulatedSensorOperation = evaluate_macro(
                string=intended_op.macro,
                sensor_id=intended_op.sensor_id,
                timestamp=int(pdt_list[i].timestamp() * 1_000),
                additional={
                    "simulation": True,
                    "cycle": str(cycle),
                    "recurring_window": str(recurring_window_number),
                    **additional_static,
                },
            )
            data.append(
                {
                    "cycle": cycle,
                    "recurring_window": recurring_window_number,
                    "start_dt": start_dt,
                    "end_dt": end_dt,
                    "operations_start_dt": operations_start_dt,
                    "operations_end_dt": operations_end_dt,
                    "timestamp": pdt_list[i],
                    "operation": evaluated_op,
                    "planned": False,
                }
            )

    schedule_df: pd.DataFrame = pd.DataFrame(data=data)

    return schedule_df
