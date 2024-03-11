import datetime
import pprint
import os
import zoneinfo
from typing import Dict, List
import logging
import pandas as pd

from iotvm_extensions.sensor_simulation import build_schedule_df, execute_schedule

_logger = logging.getLogger("iotvm_extensions.examples.sensor_simulation")


def _save_schedule_df(schedule_df: pd.DataFrame, path_to_file: str) -> None:
    df: pd.DataFrame = schedule_df.copy(deep=True)

    dt_columns = df.select_dtypes(include=["datetime64[ns, UTC]"]).columns
    for dt_column in dt_columns:
        df[dt_column] = df[dt_column].dt.tz_localize(None)
        # schedule_df[dt_column] = schedule_df[dt_column].dt.date

    df.to_excel(path_to_file)


# noinspection PyProtectedMember
def run_sensor_simulation_example(experiment_name: str, path_to_dir: str) -> None:
    # Parameters
    # --------------------------------------------------

    start_dt: datetime.datetime = datetime.datetime(
        year=2023,
        month=8,
        day=26,
        hour=23,
        minute=10,
        second=0,
        microsecond=0,
        tzinfo=zoneinfo.ZoneInfo("Europe/Athens"),
    )
    start_dt: datetime.datetime = datetime.datetime.now(tz=zoneinfo.ZoneInfo("Europe/Athens"))

    hour = start_dt.hour
    minute = start_dt.minute
    minute = minute + 1
    if minute == 60:
        hour = hour + 1
        minute = 0
    start_dt = start_dt.replace(hour=hour, minute=minute, second=0, microsecond=0)
    start_dt_string: str = start_dt.strftime("%Y-%m-%d-%H-%M-%S")
    start_dt = start_dt.astimezone(tz=datetime.timezone.utc)

    cycle_iterations: int = 3
    frequency: str = "5S"
    assert cycle_iterations >= 1

    start_dt_pct: float = 0.01
    end_dt_pct: float = 0.2

    dry_run: bool = False
    fail_silently: bool = True

    sheet_name: str = "data"

    simulation_name: str = f"simulation-{start_dt_string}"
    additional_static: Dict = {
        "experiment_name": experiment_name,
        "simulation_name": simulation_name,
    }
    _logger.info(pprint.pformat(additional_static, sort_dicts=False, indent=2))

    # Read Excel.
    # --------------------------------------------------

    file_name: str = os.path.join(path_to_dir, "macros_generated.xlsx")
    macros_df: pd.DataFrame = pd.read_excel(file_name, sheet_name=sheet_name)

    columns: List[str] = macros_df.columns.to_list()
    for column in columns:
        if column.startswith("comment"):
            del macros_df[column]

    schedule_df: pd.DataFrame = build_schedule_df(
        macros_df=macros_df,
        start_dt=start_dt,
        cycle_iterations=cycle_iterations,
        frequency=frequency,
        start_dt_pct=start_dt_pct,
        end_dt_pct=end_dt_pct,
        additional_static=additional_static,
    )

    path_to_file: str = os.path.join(path_to_dir, f"schedule_df-{simulation_name}.xlsx")
    _save_schedule_df(schedule_df=schedule_df, path_to_file=path_to_file)

    # Scheduled ops execution.
    # --------------------------------------------------

    execute_schedule(schedule_df=schedule_df, dry_run=dry_run, fail_silently=fail_silently)

    _logger.info(pprint.pformat(additional_static, sort_dicts=False, indent=2))
