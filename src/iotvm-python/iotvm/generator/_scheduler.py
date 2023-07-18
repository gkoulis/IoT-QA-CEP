"""
Author: Dimitris Gkoulis
Created at: Tuesday 21 September 2021 (June 2021)
Modified at: Tuesday 14 February 2023
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict

import pytz
from apscheduler.events import EVENT_JOB_SUBMITTED, EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler

_logger = logging.getLogger("iotvm.generator._scheduler")


class IScheduler(ABC):
    """
    Contract for creating and managing scheduled tasks aka jobs.
    Scheduling implementation is provided by APScheduler.
    """

    _EXECUTORS = {
        "default": ThreadPoolExecutor(max_workers=20),
    }
    _JOB_DEFAULTS = {"coalesce": False, "max_instances": 1}

    def __init__(self, name: str, blocking: bool):
        self._name: str = name
        scheduler_params = {"timezone": pytz.timezone("UTC")}
        if blocking is True:
            self._scheduler: BlockingScheduler = BlockingScheduler(**scheduler_params)
        else:
            self._scheduler: BackgroundScheduler = BackgroundScheduler(
                **scheduler_params
            )
        # noinspection PyAttributeOutsideInit
        self._scheduler_jobs_execution_state: Dict[str, False] = {}

    def _get_scheduler_job_execution_state(self, key: str) -> bool:
        try:
            return self._scheduler_jobs_execution_state[key]
        except KeyError:
            _logger.warning(
                f"Key {key} is not present in _scheduler_jobs_execution_state dict!"
            )
            return False

    def _no_running_jobs(self) -> bool:
        for _bool in self._scheduler_jobs_execution_state.values():
            if _bool is True:
                return False
        return True

    @abstractmethod
    def _prepare(self):
        ...

    @abstractmethod
    def _schedule(self):
        ...

    def _start(self) -> None:
        self._scheduler.configure(
            executors=self._EXECUTORS, job_defaults=self._JOB_DEFAULTS
        )

        self._scheduler_jobs_execution_state = {
            f"{name}": False
            for name in list(map(lambda job: job.id, self._scheduler.get_jobs()))
        }

        def scheduler_jobs_execution_state_listener(event):
            if event.code in [EVENT_JOB_SUBMITTED]:
                self._scheduler_jobs_execution_state[event.job_id] = True
            elif event.code in [EVENT_JOB_EXECUTED, EVENT_JOB_ERROR]:
                self._scheduler_jobs_execution_state[event.job_id] = False

        self._scheduler.add_listener(
            callback=scheduler_jobs_execution_state_listener,
            mask=(EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR),
        )

        self._scheduler.start()

    def invoke(self) -> None:
        self._prepare()
        self._schedule()
        self._start()

    def pause(self) -> None:
        self._scheduler.pause()

    def signal_graceful_shutdown(self) -> None:
        _logger.info(
            f"Signaling graceful shutdown for {self.__class__.__qualname__} {self._name}."
        )
        self._scheduler.pause()

    def ensure_graceful_shutdown(self, sleep: float) -> None:
        # @future I think it's redundant.
        # _logger.info(
        #     f"Ensuring graceful shutdown for {self.__class__.__qualname__} {self._name}."
        # )
        # while self._no_running_jobs() is False:
        #     time.sleep(sleep)
        #     continue

        _logger.info(
            f"Shutting down APScheduler of {self.__class__.__qualname__} {self._name}. "
            f"(join and delete thread)"
        )
        self._scheduler.shutdown(wait=True)
