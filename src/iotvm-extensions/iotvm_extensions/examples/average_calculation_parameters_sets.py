"""
Generates the JSON for the iotvm-eventengine resource file (average-calculation-parameters-sets.json).

TODO Move to simulation1 module (in Python).

Author: Dimitris Gkoulis
Created at: Thursday 12 October 2023
Modified at: Wednesday 08 November 2023
"""

from typing import Dict, List
from dataclasses import dataclass, asdict
import itertools


# Defined by the Java back-end in the corresponding class.
_PREFIX: str = "w_avg_"


def generate_composite_transformation_parameters_ids(
    physical_quantity_list: List[str],
    time_window_size: List[int],
    number_of_contributing_sensors: List[int],
    ignore_completeness_filtering_list: List[bool],
    fabrication_past_events_steps_behind: List[int],
    fabrication_forecasting_steps_ahead: List[int],
) -> List[str]:
    combinations = itertools.product(
        physical_quantity_list,
        time_window_size,
        number_of_contributing_sensors,
        ignore_completeness_filtering_list,
        fabrication_past_events_steps_behind,
        fabrication_forecasting_steps_ahead,
    )
    combinations = list(combinations)

    ctp_id_list: List[str] = []

    multiplier: int = 1

    for combination in combinations:
        ctp_id: str = f"{_PREFIX}{combination[0].lower()}_PT{combination[1]}S_null_null_{combination[2]}_{str(combination[3]).lower()}_{combination[4]}_PT{combination[1] * multiplier}S_{combination[5]}"
        ctp_id_list.append(ctp_id)

    return ctp_id_list


@dataclass
class CompositeTransformationParameterID:
    # TODO physical quantity.
    time_window_size: int
    number_of_contributing_sensors: int
    ignore_completeness_filtering: bool
    fabrication_past_events_steps_behind: int
    fabrication_forecasting_time_window_size: int
    fabrication_forecasting_steps_ahead: int

    def __str__(self) -> str:
        string: str = f"time window size of {self.time_window_size} secs with at least {self.number_of_contributing_sensors} sensors"

        if self.fabrication_past_events_steps_behind == 0 and self.fabrication_forecasting_steps_ahead == 0:
            return string

        if self.fabrication_past_events_steps_behind != 0:
            string = f"{string}, past events lookup {self.fabrication_past_events_steps_behind} windows"

        if self.fabrication_forecasting_steps_ahead != 0:
            string = f"{string}, forecasted events lookup {self.fabrication_forecasting_steps_ahead} windows of size {self.fabrication_forecasting_time_window_size} secs"

        if self.ignore_completeness_filtering is True:
            string = f"{string} (ignore completeness)"

        return string

    def to_dict(self) -> Dict:
        return asdict(obj=self)


def parse_ctp_id(ctp_id: str) -> CompositeTransformationParameterID:
    # TODO Fix.
    ctp_id = ctp_id.removeprefix("w_avg_temperature_")
    # TODO Physical quantity!
    parts: List[str] = ctp_id.split("_")

    time_window_size: str = parts[0]
    time_window_size = time_window_size.removeprefix("PT")
    time_window_size = time_window_size.removesuffix("S")

    number_of_contributing_sensors: str = parts[3]

    ignore_completeness_filtering: str = parts[4]
    assert ignore_completeness_filtering in ["true", "false"]

    fabrication_past_events_steps_behind: str = parts[5]

    fabrication_forecasting_time_window_size: str = parts[6]
    fabrication_forecasting_time_window_size = fabrication_forecasting_time_window_size.removeprefix("PT")
    fabrication_forecasting_time_window_size = fabrication_forecasting_time_window_size.removesuffix("S")

    fabrication_forecasting_steps_ahead: str = parts[7]

    return CompositeTransformationParameterID(
        time_window_size=int(time_window_size),
        number_of_contributing_sensors=int(number_of_contributing_sensors),
        ignore_completeness_filtering=ignore_completeness_filtering == "true",
        fabrication_past_events_steps_behind=int(fabrication_past_events_steps_behind),
        fabrication_forecasting_time_window_size=int(fabrication_forecasting_time_window_size),
        fabrication_forecasting_steps_ahead=int(fabrication_forecasting_steps_ahead),
    )


def generate_average_calculation_parameters_sets(
    physical_quantity_list: List[str],
    time_window_size: List[int],
    number_of_contributing_sensors: List[int],
    ignore_completeness_filtering_list: List[bool],
    fabrication_past_events_steps_behind: List[int],
    fabrication_forecasting_steps_ahead: List[int],
) -> List[Dict]:
    combinations = itertools.product(
        physical_quantity_list,
        time_window_size,
        number_of_contributing_sensors,
        ignore_completeness_filtering_list,
        fabrication_past_events_steps_behind,
        fabrication_forecasting_steps_ahead,
    )
    combinations = list(combinations)

    data = []
    for combination in combinations:
        data.append(
            {
                "physicalQuantity": combination[0],
                "timeWindowSize": combination[1],
                "timeWindowGrace": None,
                "timeWindowAdvance": None,
                "minimumNumberOfContributingSensors": combination[2],
                "ignoreCompletenessFiltering": combination[3],
                "pastWindowsLookup": combination[4],
                "forecastingWindowSize": None,
                "futureWindowsLookup": combination[5],
            }
        )

    return data
