import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Dict

import pandas as pd

from ._data_types import EvaluatedSimulatedSensorOperation
from ._operations import simulate_sensor_operation

_logger = logging.getLogger("iotvm_extensions.sensor_simulation._schedule_execution")


@dataclass
class _Item:
    operation: EvaluatedSimulatedSensorOperation
    raw: Dict


class _ScheduleExecutionContext:
    def __init__(self):
        self.has_future_items: bool = False
        self.queue: queue.Queue | None = None
        self.thread: threading.Thread | None = None
        self._dry_run: bool = True
        self._fail_silently: bool = False

    def _process_queued_items(self) -> None:
        while self.has_future_items is True or self.queue.qsize() > 0:
            try:
                item: _Item = self.queue.get(block=False, timeout=None)
            except queue.Empty:
                continue
            cycle: str = item.raw["cycle"]
            recurring_window_number: int = item.raw["recurring_window"]
            _logger.info(
                f"simulate_sensor_operation "
                f"(dry_run={self._dry_run}, cycle={cycle} [{recurring_window_number}]) "
                f": {item.operation.to_print_string()}"
            )
            if self._dry_run is False:
                simulate_sensor_operation(op=item.operation, fail_silently=self._fail_silently)
            self.queue.task_done()

    def initialize(self, dry_run: bool, fail_silently: bool) -> None:
        self._dry_run = dry_run
        self._fail_silently = fail_silently
        self.has_future_items = True
        self.queue = queue.Queue()
        self.thread = threading.Thread(target=self._process_queued_items, daemon=True)
        self.thread.start()

    def graceful_stop(self) -> None:
        self.thread.join()


_C: _ScheduleExecutionContext = _ScheduleExecutionContext()


def execute_schedule(schedule_df: pd.DataFrame, dry_run: bool, fail_silently: bool) -> None:
    """
    Limitations:
    - One call per process.
    """

    if schedule_df.empty:
        _logger.warning("schedule_df is empty. Aborting...")
        return

    _C.initialize(dry_run=dry_run, fail_silently=fail_silently)

    while True:
        count: int = schedule_df.query("planned == False")["planned"].count()
        if count == 0:
            _logger.info("all planed execution are queued")
            _C.has_future_items = False
            break

        now_time_ns: int = time.time_ns()
        now_timestamp: pd.Timestamp = pd.Timestamp(now_time_ns, unit="ns", tz="UTC")

        slice_df: pd.DataFrame = schedule_df[
            (schedule_df["timestamp"] <= now_timestamp) & (schedule_df["planned"] == False)
        ]

        if slice_df.empty:
            continue

        records = slice_df.to_dict(orient="records")
        for record in records:
            item: _Item = _Item(
                operation=record["operation"],
                raw=record,
            )
            _C.queue.put_nowait(item=item)

        schedule_df.loc[slice_df.index, "planned"] = True

        time.sleep(0.85)  # TODO as param.

    _C.graceful_stop()
