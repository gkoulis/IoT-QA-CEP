"""
Author: Dimitris Gkoulis
Created at: Saturday 14 October 2023
Modified at: Friday 03 November 2023
"""

import os
from typing import Dict, List
from ._common_errors_distributions import CED1, CED2, CED3, CED4, CED5, CED6


# DO NOT CHANGE.
CONFIGURATION_SCRIPT_FILE = __file__
CONFIGURATION_SCRIPT_NAME = __name__


# ####################################################################################################
# General
# ####################################################################################################


EXPERIMENT_NAME: str = "refactor-experiment-1"


# ####################################################################################################
# Persistence (and FS)
# ####################################################################################################


_BASE_PATH: str = "/home/dgk/projects/PhD/dgk-phd-monorepo/src/iotvm-extensions"
# TODO Change.
EXPERIMENTS_DIRECTORY: str = os.path.join(_BASE_PATH, "local_data", "experiments")
EXPERIMENT_DIRECTORY: str = os.path.join(EXPERIMENTS_DIRECTORY, EXPERIMENT_NAME)
EXPERIMENT_INPUT_DIRECTORY: str = os.path.join(EXPERIMENT_DIRECTORY, "input")
EXPERIMENT_OUTPUT_DIRECTORY: str = os.path.join(EXPERIMENT_DIRECTORY, "output")

os.makedirs(EXPERIMENT_DIRECTORY, exist_ok=True)
os.makedirs(EXPERIMENT_INPUT_DIRECTORY, exist_ok=True)
os.makedirs(EXPERIMENT_OUTPUT_DIRECTORY, exist_ok=True)


# ####################################################################################################
# Event Engine and Composite Transformations
# ####################################################################################################


PHYSICAL_QUANTITY: str = "TEMPERATURE"
TIME_WINDOW_SIZE_LIST: List[int] = [5]  # ["5S", "10S", "30S", "1M"]
NUMBER_OF_CONTRIBUTING_SENSORS_LIST: List[int] = [2, 4, 6]  # [1, 2, 3, 4, 5, 6]
IGNORE_COMPLETENESS_FILTERING_LIST: List[bool] = [False]  # [True, False]
FABRICATION_PAST_EVENTS_STEPS_BEHIND_LIST: List[int] = [0, 2, 4, 6]  # [0, 2, 4, 6, 8, 10]
FABRICATION_FORECASTING_STEPS_AHEAD_LIST: List[int] = [0, 2, 4, 6]  # [0, 4, 8, 12, 16, 20]

SENSOR_ID_LIST: List[str] = [
    "sensor-1",
    "sensor-2",
    "sensor-3",
    "sensor-4",
    "sensor-5",
    "sensor-6",
]
PHYSICAL_QUANTITY_LOWER: str = PHYSICAL_QUANTITY.lower()
TOPIC_NAME: str = f"ga.sensor_telemetry_measurement_event.0001.{PHYSICAL_QUANTITY_LOWER}"
FREQUENCY_IN_SECONDS_LIST: List[int] = [5]


# ####################################################################################################
# Macros Generation (MG)
# ####################################################################################################


MG__DATASET_PATH_TO_FILE: str = os.path.join(_BASE_PATH, "datasets", "dataset-1-slice-9-13.csv")

_MG__FREQUENCY_DISTRIBUTION: Dict = {"type": "constant", "loc": 5.0}

MG__PARAMETERS_BY_SENSOR_ID: Dict[str, Dict] = {
    "sensor-1": {
        "sample_name": "sample1",
        "frequency_distribution": _MG__FREQUENCY_DISTRIBUTION,
        "up_sampling_distribution": None,
        "noise_distributions": [
            {"type": "constant", "loc": 1.0},
            {
                "type": "normal",
                "loc": 0.5,
                "scale": 0.2,
            },
        ],
        "ttp_between_errors_distribution": CED1.ttp_between_errors_distribution,
        "error_ttp_distribution": CED1.error_ttp_distribution,
        "frequency_distribution_seed": 1,
        "up_sampling_distribution_seed": 1,
        "noise_distributions_seed": 1,
        "errors_distributions_seed": CED1.errors_distributions_seed,
    },
    "sensor-2": {
        "sample_name": "sample1",
        "frequency_distribution": _MG__FREQUENCY_DISTRIBUTION,
        "up_sampling_distribution": None,
        "noise_distributions": [
            {"type": "constant", "loc": -1.0},
            {
                "type": "normal",
                "loc": 0.5,
                "scale": 0.1,
            },
        ],
        "ttp_between_errors_distribution": CED2.ttp_between_errors_distribution,
        "error_ttp_distribution": CED2.error_ttp_distribution,
        "frequency_distribution_seed": 2,
        "up_sampling_distribution_seed": 2,
        "noise_distributions_seed": 2,
        "errors_distributions_seed": CED2.errors_distributions_seed,
    },
    "sensor-3": {
        "sample_name": "sample1",
        "frequency_distribution": _MG__FREQUENCY_DISTRIBUTION,
        "up_sampling_distribution": None,
        "noise_distributions": [
            {
                "type": "normal",
                "loc": 0.0,
                "scale": 2.0,
            },
        ],
        "ttp_between_errors_distribution": CED3.ttp_between_errors_distribution,
        "error_ttp_distribution": CED3.error_ttp_distribution,
        "frequency_distribution_seed": 3,
        "up_sampling_distribution_seed": 3,
        "noise_distributions_seed": 3,
        "errors_distributions_seed": CED3.errors_distributions_seed,
    },
    "sensor-4": {
        "sample_name": "sample1",
        "frequency_distribution": _MG__FREQUENCY_DISTRIBUTION,
        "up_sampling_distribution": None,
        "noise_distributions": [
            {"type": "constant", "loc": 4.0},
            {
                "type": "normal",
                "loc": 0.0,
                "scale": 2.0,
            },
        ],
        "ttp_between_errors_distribution": CED1.ttp_between_errors_distribution,
        "error_ttp_distribution": CED1.error_ttp_distribution,
        "frequency_distribution_seed": 4,
        "up_sampling_distribution_seed": 4,
        "noise_distributions_seed": 4,
        "errors_distributions_seed": CED1.errors_distributions_seed,
    },
    "sensor-5": {
        "sample_name": "sample1",
        "frequency_distribution": _MG__FREQUENCY_DISTRIBUTION,
        "up_sampling_distribution": None,
        "noise_distributions": [
            {"type": "constant", "loc": -0.5},
            {
                "type": "normal",
                "loc": 0.2,
                "scale": 0.2,
            },
        ],
        "ttp_between_errors_distribution": CED2.ttp_between_errors_distribution,
        "error_ttp_distribution": CED2.error_ttp_distribution,
        "frequency_distribution_seed": 5,
        "up_sampling_distribution_seed": 5,
        "noise_distributions_seed": 5,
        "errors_distributions_seed": CED2.errors_distributions_seed,
    },
    "sensor-6": {
        "sample_name": "sample1",
        "frequency_distribution": _MG__FREQUENCY_DISTRIBUTION,
        "up_sampling_distribution": None,
        "noise_distributions": [
            {"type": "constant", "loc": 0.5},
            {
                "type": "normal",
                "loc": 0.2,
                "scale": 0.2,
            },
        ],
        "ttp_between_errors_distribution": CED3.ttp_between_errors_distribution,
        "error_ttp_distribution": CED3.error_ttp_distribution,
        "frequency_distribution_seed": 6,
        "up_sampling_distribution_seed": 6,
        "noise_distributions_seed": 6,
        "errors_distributions_seed": CED3.errors_distributions_seed,
    },
}
MG__EXPECTED_RECURRING_TIME_WINDOWS: int = 50


# ####################################################################################################
# Report
# ####################################################################################################


"""
paper-experiment-1-v1    simulation-2023-11-04-20-23-00    2
paper-experiment-1-v2    simulation-2023-11-08-20-01-00    1
paper-experiment-1-v3    simulation-2024-01-17-14-34-00    1
---
after-paper-experiment-1-v1    simulation-2024-01-22-18-50-00    3
after-paper-experiment-2-v1    simulation-2024-01-22-19-54-00    2
after-paper-experiment-3-v1    simulation-2024-01-24-18-08-00    2
after-paper-experiment-4-v1    simulation-2024-01-24-18-32-00    2
after-paper-experiment-5-v1    simulation-2024-02-17-20-20-00    2
after-paper-experiment-6-v1    simulation-2024-02-17-20-30-00    2
"""


REPORT__EXPERIMENT_NAME_LIST: List[str] = [EXPERIMENT_NAME]
REPORT__SIMULATION_NAME_LIST: List[str] = ["simulation-2024-02-17-20-30-00"]
REPORT__CYCLE_LIST: List[int] = [2]
