import datetime
import zoneinfo
from typing import List

import pandas as pd

from iotvm_extensions.sensor_simulation import build_schedule_df, execute_schedule


# noinspection PyProtectedMember
def run_sensor_simulation_example() -> None:
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
    start_dt = start_dt.replace(minute=start_dt.minute + 1, second=0, microsecond=0)
    start_dt = start_dt.astimezone(tz=datetime.timezone.utc)

    cycle_iterations: int = 10
    frequency: str = "10S"
    assert cycle_iterations >= 1

    start_dt_pct: float = 0.01
    end_dt_pct: float = 0.2

    dry_run: bool = False
    fail_silently: bool = True

    path_to_file: str = "macros.xlsx"
    sheet_name: str = "data"

    # Read Excel.
    # --------------------------------------------------

    macros_df: pd.DataFrame = pd.read_excel(path_to_file, sheet_name=sheet_name)

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
    )

    # Scheduled ops execution.
    # --------------------------------------------------

    execute_schedule(schedule_df=schedule_df, dry_run=dry_run, fail_silently=fail_silently)

    # DataFrame to excel (BE CAREFUL, this operation makes DTs naive).
    # --------------------------------------------------

    dt_columns = schedule_df.select_dtypes(include=["datetime64[ns, UTC]"]).columns
    for dt_column in dt_columns:
        schedule_df[dt_column] = schedule_df[dt_column].dt.tz_localize(None)
        # schedule_df[dt_column] = schedule_df[dt_column].dt.date

    # Uncomment to export as xlsx.
    # schedule_df.to_excel(f"schedule_df-{time.time_ns()}.xlsx")
