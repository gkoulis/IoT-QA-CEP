"""
Sensor synthetic data generator [EXPERIMENTAL].

Abbreviation: `ttp` time, tick or period.

Assumptions:
- Η ελάχιστη μονάδα χρόνου ορίζεται ως 1 δευτερόλεπτο.
  Αυτό σημαίνει ότι όλες οι κατανομές, οι σχετικοί χρόνοι, κ.τ.λ.
  χρησιμοποιούν τιμές που αντανακλούν δευτερόλεπτα.

Limitations:
- Τα loss distribution είναι overlapping.
  Δηλαδή, αν το time to error/time between errors είναι 4 περίοδοι,
  και το error duration είναι 4 περίοδοι επίσης,
  κατά τη διάρκεια του σφάλματος μετράει ο χρόνος του error/time between errors.

TODO Future implementations:
- Add more distributions!
- Figures! (the implementation is ready)

Author: Dimitris Gkoulis
Created at: Wednesday 04 October 2023
Modified at: Saturday 04 November 2023
"""

import json
import logging
import os
import pprint
import random
import re
import sys
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from iotvm_extensions.examples.average_calculation_parameters_sets import CompositeTransformationParameterID, parse_ctp_id
from iotvm_extensions.constants import EPS, SEED
from iotvm_extensions.examples.average_calculation_parameters_sets import generate_average_calculation_parameters_sets

_logger = logging.getLogger(__name__)


# ####################################################################################################
# Random Context.
# ####################################################################################################


def _persist_dataframe(df: pd.DataFrame, directory: str, file_name: str) -> None:
    path_to_file: str = os.path.join(directory, file_name)
    df.to_json(f"{path_to_file}.json", orient="records")
    df.to_parquet(f"{path_to_file}.parquet")
    df.to_csv(f"{path_to_file}.csv")
    df.to_excel(f"{path_to_file}.xlsx")


def _to_json(obj, directory: str, file_name: str) -> None:
    json_object = json.dumps(obj, indent=4, allow_nan=False, sort_keys=False)
    path_to_file: str = os.path.join(directory, file_name)
    # assert os.path.exists(path_to_file) is False
    with open(path_to_file, "w") as outfile:
        outfile.write(json_object)


def _from_json(path_to_file: str) -> Any:
    with open(path_to_file, "r") as input_file:
        data = json.load(input_file)
        return data


# ####################################################################################################
# Random Context.
# ####################################################################################################


class _RandomContext:
    def __init__(self) -> None:
        self.np_rng_map: Dict[str, np.random.Generator] = {}
        self.py_rng_map: Dict[str, random.Random] = {}
        self._set_default()

    def _set_default(self) -> None:
        self.init_seed(name="default", seed=42)

    def init_seed(self, name: str, seed: Optional[int]) -> None:
        self.np_rng_map[name] = np.random.default_rng(seed=seed)
        self.py_rng_map[name] = random.Random(x=seed)

    def ensure_seed(self, name: str, seed: Optional[int]) -> bool:
        if name in self.np_rng_map:
            assert name in self.py_rng_map
            return False
        if name in self.py_rng_map:
            assert name in self.np_rng_map
            return False
        self.init_seed(name=name, seed=seed)
        return True


_RC: _RandomContext = _RandomContext()


# ####################################################################################################
# Distributions
# ####################################################################################################


@dataclass
class NormalDistribution:
    seed_name: str
    loc: float
    scale: float
    size: Optional[int] = None

    def random(self, size: Optional[int]) -> Any:
        return _RC.np_rng_map[self.seed_name].normal(loc=self.loc, scale=self.scale, size=size)

    def random2(self) -> Any:
        return self.random(size=self.size)


class ConstantDistribution(NormalDistribution):
    def __init__(self, seed_name: str, loc: float, size: Optional[int] = None) -> None:
        super().__init__(seed_name=seed_name, loc=loc, scale=0, size=size)


@dataclass
class ExponentialDistribution:
    seed_name: str
    scale: float
    size: Optional[int] = None

    def random(self, size: Optional[int]) -> Any:
        return _RC.np_rng_map[self.seed_name].exponential(scale=self.scale, size=size)

    def random2(self) -> None:
        return self.random(size=self.size)


DistributionType = Union[NormalDistribution, ConstantDistribution, ExponentialDistribution]


def _obj_to_distribution(seed_name: str, obj: Dict) -> DistributionType:
    assert obj is not None
    assert type(obj) == dict

    if obj["type"] == "constant":
        return ConstantDistribution(seed_name=seed_name, loc=obj["loc"])

    elif obj["type"] == "normal":
        return NormalDistribution(
            seed_name=seed_name,
            loc=obj["loc"],
            scale=obj["scale"],
        )

    elif obj["type"] == "exponential":
        return ExponentialDistribution(seed_name=seed_name, scale=obj["scale"])

    raise ValueError("invalid type!")


def _any_to_distribution(
    seed_name: str, obj: Union[List[Dict], Dict, None]
) -> Union[List[DistributionType], DistributionType, None]:
    if obj is None:
        return None

    if type(obj) == list:
        lst = []
        for item in obj:
            lst.append(_obj_to_distribution(seed_name=seed_name, obj=item))
        return lst

    if type(obj) == dict:
        return _obj_to_distribution(seed_name=seed_name, obj=obj)

    raise ValueError(f"obj is not one of: Dict, List, None!")


# ####################################################################################################
# Date and Time.
# ####################################################################################################


def t_convert(value: Union[int, float], from_unit: str, to_unit: str) -> Union[int, float]:
    _CONVERSION_FACTORS = {
        "seconds": 1,
        "sec": 1,
        "minutes": 60,
        "min": 60,
        "hours": 3600,
    }

    assert from_unit in _CONVERSION_FACTORS, f"{from_unit} is not a valid unit!"
    assert to_unit in _CONVERSION_FACTORS, f"{to_unit} is not a valid unit!"

    value_in_seconds = value * _CONVERSION_FACTORS[from_unit]
    return value_in_seconds / _CONVERSION_FACTORS[to_unit]


def t_min_to_sec(value: Union[int, float]) -> Union[int, float]:
    return t_convert(value=value, from_unit="minutes", to_unit="seconds")


def extract_frequency_unit(frequency: str) -> str:
    # Regular expression pattern to match the unit part of the frequency string
    pattern = r"(\d+)([a-z]+)"
    match = re.search(pattern, frequency)
    if match:
        return match.group(2)
    else:
        raise ValueError("Invalid frequency string")


# ####################################################################################################
# Error Interval
# ####################################################################################################


@dataclass
class ErrorInterval:
    start: float
    end: float
    duration: float


def generate_errors_intervals(
    ttp_between_errors_distribution: DistributionType,
    error_ttp_distribution: DistributionType,
    ttp: float,
) -> List[ErrorInterval]:
    """

    ...
    Parameters
    ----------

    ttp_between_errors_distribution : DistributionType
        Distribution to calculate times between failures.
    error_ttp_distribution : DistributionType
        Distribution to calculate error durations.
    ttp : float
        The total ttp, i.e., the size of the series.

    Returns
    -------

    list of ErrorInterval instance.
        Each instance contains details about the error (start, end, interval).
    """

    # Generate error times.
    # --------------------------------------------------

    errors_times = []
    current_time = 0
    while current_time < ttp:
        interval = ttp_between_errors_distribution.random(size=None)
        current_time += interval
        errors_times.append(current_time)

    # Generate the duration of each error.
    # --------------------------------------------------

    errors_durations = error_ttp_distribution.random(size=len(errors_times))

    # Build instances.
    # --------------------------------------------------

    instances: List[ErrorInterval] = []
    for idx, start_time in enumerate(errors_times):
        if start_time >= ttp:
            # TODO Okay? I assume that the loop will break after this.
            continue

        duration: float = errors_durations[idx]
        end_time = start_time + duration
        instances.append(
            ErrorInterval(
                start=start_time,
                end=end_time,
                duration=duration,
            )
        )

    return instances


# ####################################################################################################
# Array-Like.
# ####################################################################################################


array_like_t = Union[pd.Series, np.ndarray, List[Any]]


def _array_like_to_list(arr: array_like_t, typ: Any = None) -> List:
    data: List = []
    if isinstance(arr, list):
        data = arr
    elif isinstance(arr, pd.Series):
        data = arr.tolist()
    elif isinstance(arr, np.ndarray):
        data = arr.tolist()
    else:
        raise TypeError(f"arr has a non-supported type: {type(arr)}")

    if typ is not None:
        if not all(isinstance(datum, typ) for datum in data):
            raise TypeError(f"Not all elements in the array are of type {typ}")

    return data


# ####################################################################################################
# Reasoners: generators, combiners, composers, synthesizers
# ####################################################################################################


class MeasurementBasicGenerator:
    X_SEED_NAME: str = "MeasurementBasicGenerator-x"
    INTERACTIONS_SEED_NAME: str = "MeasurementBasicGenerator-interactions"

    # noinspection PyTypeChecker
    def __init__(self) -> None:
        self.x: List[pd.Timestamp] = None
        self.x_relative: List[float] = None
        self.y: List[float] = None
        self.df: pd.DataFrame = None

    def set_x(
        self,
        distribution: DistributionType,
        seed: int,
        start: pd.Timestamp,
        timezone: Optional[str],
    ) -> None:
        assert self.x is None
        assert self.y is not None

        assert distribution.seed_name == self.X_SEED_NAME
        _RC.init_seed(name=self.X_SEED_NAME, seed=seed)

        if timezone is None:
            assert start.tz is None
        else:
            assert start.tz.__str__() == timezone

        # If distribution is Constant it generates timestamps equivalent to those generated by ```pd.date_range```.
        relative_timestamps: np.ndarray = distribution.random(size=len(self.y))
        relative_timestamps = np.cumsum(relative_timestamps)
        relative_timestamps = relative_timestamps[:-1]
        relative_timestamps = np.append([0], relative_timestamps)

        x: List[pd.Timestamp] = []
        for relative_timestamp in relative_timestamps:
            timestamp: pd.Timestamp = start + pd.to_timedelta(relative_timestamp, unit="seconds", errors="raise")
            x.append(timestamp)

        # Duration is given by: ```(x[-1] - x[0]).total_seconds()```
        # Also, ```(relative_timestamps[-1] - relative_timestamps[0])```
        # Due to rounding issues they are not exactly the same.

        self.x_relative = relative_timestamps
        self.x = x

        assert len(self.x) == len(self.y)

    def set_y(self, y: array_like_t) -> None:
        assert self.y is None
        self.y = _array_like_to_list(arr=y, typ=float)

    def construct_dataframe(self) -> None:
        assert self.df is None
        assert self.x is not None
        assert self.x_relative is not None
        assert self.y is not None

        self.df = pd.DataFrame(
            data={
                "x": self.x,
                "x_relative": self.x_relative,
                "y1": self.y,
                "y2": self.y,
            }
        )

    def set_interactions(self, distributions: List[DistributionType], seed: int) -> None:
        """
        Given a list of interactions, it modifies the sample accordingly.
        It is noted that the new sample includes the updated ground truth.
        These interactions do not reflect non-ideal realities.
        Their purpose is to create diversified time-series for a set of sensors.
        """
        assert self.df is not None

        for distribution in distributions:
            assert distribution.seed_name == self.INTERACTIONS_SEED_NAME
        _RC.init_seed(name=self.INTERACTIONS_SEED_NAME, seed=seed)

        df: pd.DataFrame = self.df

        interaction_arrays_list: List[np.ndarray] = []
        for distribution in distributions:
            interaction_array: np.ndarray = distribution.random(size=len(df))
            interaction_arrays_list.append(interaction_array)

        for interaction_array in interaction_arrays_list:
            df["y2"] = df["y2"] + interaction_array

        self.df = df


class MultiMeasurementCombiner:
    LOSS_SEED_NAME: str = "MultiMeasurementCombiner-loss"

    # noinspection PyTypeChecker
    def __init__(self) -> None:
        self.df_map: Dict[str, pd.DataFrame] = {}
        self.df: pd.DataFrame = None
        self.error_intervals: List[ErrorInterval] = None

    def add_dataframe(self, family: str, df: pd.DataFrame) -> None:
        assert self.df is None
        assert self.error_intervals is None
        assert family not in self.df_map
        self.df_map[family] = df

    def combine(self) -> None:
        assert len(self.df_map) > 0
        assert self.df is None
        assert self.error_intervals is None

        df_map: Dict[str, pd.DataFrame] = {}
        for family, df in self.df_map.items():
            df_map[family] = df.copy(deep=True)
            df_map[family]["family"] = family

        concatenated_df: pd.DataFrame = pd.concat(objs=df_map.values(), ignore_index=True)
        concatenated_df.sort_values(by="x", inplace=True, ignore_index=True)
        concatenated_df.set_index(keys="x", drop=True, inplace=True)
        pivoted_concatenated_df: pd.DataFrame = concatenated_df.pivot(columns="family", values="y2")
        self.df = pivoted_concatenated_df

    def resample(self, rule: str) -> None:
        """
        Resample to simulate the sensor's capability, independent of the frequency of events.
        The rule (e.g., '5S', '1min', etc.) represents the minimum time interval that must elapse
        before the sensor can produce a new event. After resampling,
        only the last set of measurements within each interval is preserved, and the rest are discarded.
        """
        assert self.df is not None
        assert self.error_intervals is None

        df: pd.DataFrame = self.df.copy(deep=True)
        agg: Dict[str, str] = {column: "last" for column in df.columns.tolist()}
        df_resampled: pd.DataFrame = df.resample(rule=rule).agg(agg)
        df_resampled.dropna(how="all", inplace=True)
        self.df = df_resampled

    def set_loss(
        self,
        time_between_errors_distribution: DistributionType,
        time_between_errors_distribution_seed: int,
        error_duration_distribution: DistributionType,
        error_duration_distribution_seed: int,
    ) -> None:
        assert self.df is not None
        assert self.error_intervals is None

        assert time_between_errors_distribution.seed_name == self.LOSS_SEED_NAME
        assert error_duration_distribution.seed_name == self.LOSS_SEED_NAME

        _RC.init_seed(name=self.LOSS_SEED_NAME, seed=time_between_errors_distribution_seed)
        _RC.init_seed(name=self.LOSS_SEED_NAME, seed=error_duration_distribution_seed)
        # TODO I think it's better to create to generators!

        df: pd.DataFrame = self.df.copy(deep=True)
        df["loss"] = 0

        duration: float = (df.index[-1] - df.index[0]).total_seconds()
        error_intervals: List[ErrorInterval] = generate_errors_intervals(
            ttp_between_errors_distribution=time_between_errors_distribution,
            error_ttp_distribution=error_duration_distribution,
            ttp=duration,
        )

        x0: pd.Timestamp = df.index[0]
        for error_interval in error_intervals:
            start = x0 + pd.Timedelta(seconds=error_interval.start)
            end = x0 + pd.Timedelta(seconds=error_interval.end)
            df.loc[((df.index >= start) & (df.index <= end)), "loss"] = 1

        self.df = df
        self.error_intervals = error_intervals

    def set_no_loss(self) -> None:
        assert self.df is not None
        assert self.error_intervals is None

        df: pd.DataFrame = self.df.copy(deep=True)
        df["loss"] = 0

        self.df = df
        self.error_intervals = []

    def set_loss_metrics(self) -> None:
        assert self.df is not None
        assert self.error_intervals is not None

        assert self.df is not None and isinstance(
            self.df.index, pd.DatetimeIndex
        ), "DataFrame index must be DatetimeIndex."
        assert self.error_intervals is not None and all(
            isinstance(interval, ErrorInterval) for interval in self.error_intervals
        ), "Error intervals must be a list of ErrorInterval."

        df: pd.DataFrame = self.df
        error_intervals: List[ErrorInterval] = self.error_intervals

        # --------------------------------------------------

        # duration: float = 0.0
        # uptime: float = 0.0
        downtime: float = 0.0
        errors: int = 0
        mean_error_duration: float = 0.0

        # --------------------------------------------------

        duration: float = (df.index[-1] - df.index[0]).total_seconds()

        # --------------------------------------------------

        x0: pd.Timestamp = df.index[0]
        for error_interval in error_intervals:
            start = x0 + pd.Timedelta(seconds=error_interval.start)
            end = x0 + pd.Timedelta(seconds=error_interval.end)
            duration_ = (end - start).total_seconds()

            downtime = downtime + duration_
            errors = errors + 1
            mean_error_duration = mean_error_duration + duration_

        if errors > 0:
            mean_error_duration = mean_error_duration / errors

        # --------------------------------------------------

        t: np.ndarray = np.arange(start=0, stop=duration)
        uptime = duration - downtime
        error_rate = downtime / duration
        # The probability that the sensor will operate without failure for a time t.
        reliability: np.ndarray = np.exp(-error_rate * t)

        tick_count: int = len(df)
        tick_with_loss_count: int = len(df.query("loss == 1"))
        tick_with_loss_pct: float = 1.0 - ((tick_count - tick_with_loss_count) / tick_count)

        metrics: Dict = {
            "tick_count": tick_count,
            "tick_with_loss_count": tick_with_loss_count,
            "tick_with_loss_pct": tick_with_loss_pct,
            "duration": duration,
            "uptime": uptime,
            "downtime": downtime,
            "error_rate": error_rate,
            "mean_error_duration": mean_error_duration,
            "t": t.tolist(),
            "reliability": reliability.tolist(),
        }

        # --------------------------------------------------

        df.attrs["loss_metrics"] = metrics
        self.df = df


# ####################################################################################################
# Composer
# ####################################################################################################


MEASUREMENT_UNIT_BY_MEASUREMENT_NAME: Dict[str, str] = {
    "temperature": "CELSIUS",
    "humidity": "PERCENTAGE",
}


@dataclass
class Frequency:
    distribution: DistributionType
    seed: int


@dataclass
class Interactions:
    distributions: List[DistributionType]
    seed: int


@dataclass
class Measurement:
    name: str
    unit: str
    sample: Union[str, np.ndarray]
    frequency: Frequency
    start: pd.Timestamp
    timezone: Optional[str]
    interactions: Interactions


@dataclass
class Sensor:
    name: str
    measurements: List[Measurement]
    frequency: Optional[str]


@dataclass
class Loss:
    time_between_errors_distribution: DistributionType
    error_duration_distribution: DistributionType


@dataclass
class Iteration:
    name: str
    loss_seed_by_sensor: Dict[str, int]
    loss_seed_fallback: int


@dataclass
class Variation:
    name: str
    loss_by_sensor: Dict[str, Loss]
    iterations: List[Iteration]


@dataclass
class AverageCalculationCompositeTransformationParametersSetsSpace:
    # TODO Consider renaming the CT to TimeWindowedBasicStatisticsCompositeTransformation
    physical_quantity_list: List[str]
    time_window_size_list: List[int]
    number_of_contributing_sensors_list: List[int]
    ignore_completeness_filtering_list: List[bool]
    fabrication_past_events_steps_behind_list: List[int]
    fabrication_forecasting_steps_ahead_list: List[int]
    # TODO Θεωρώ ότι όταν το past events είναι μεγαλύτερο από το forecasting, πρέπει να απενεργοποιώ το forecasting.
    #  Τουλάχιστον αν κρατήσω τη λογική ότι πρώτα κάνω past και μετά forecasting.

    def __post_init__(self) -> None:
        for physical_quantity in self.physical_quantity_list:
            assert physical_quantity in ["TEMPERATURE", "HUMIDITY"]
        self.time_window_size_list = sorted(self.time_window_size_list)
        self.number_of_contributing_sensors_list = sorted(self.number_of_contributing_sensors_list)
        self.ignore_completeness_filtering_list = sorted(self.ignore_completeness_filtering_list)
        self.fabrication_past_events_steps_behind_list = sorted(self.fabrication_past_events_steps_behind_list)
        self.fabrication_forecasting_steps_ahead_list = sorted(self.fabrication_forecasting_steps_ahead_list)

    def to_composite_transformation_parameters_set(self) -> List[Dict]:
        physical_quantity_list: List[str] = self.physical_quantity_list
        time_window_size_list: List[int] = self.time_window_size_list
        number_of_contributing_sensors_list: List[int] = self.number_of_contributing_sensors_list
        ignore_completeness_filtering_list: List[bool] = self.ignore_completeness_filtering_list
        fabrication_past_events_steps_behind: List[int] = self.fabrication_past_events_steps_behind_list
        fabrication_forecasting_steps_ahead: List[int] = self.fabrication_forecasting_steps_ahead_list

        return generate_average_calculation_parameters_sets(
            physical_quantity_list=physical_quantity_list,
            time_window_size=time_window_size_list,
            number_of_contributing_sensors=number_of_contributing_sensors_list,
            ignore_completeness_filtering_list=ignore_completeness_filtering_list,
            fabrication_past_events_steps_behind=fabrication_past_events_steps_behind,
            fabrication_forecasting_steps_ahead=fabrication_forecasting_steps_ahead,
        )


@dataclass
class Simulation:
    name: str
    sensors: List[Sensor]
    variations: List[Variation]
    average_ct_ps_space: AverageCalculationCompositeTransformationParametersSetsSpace

    def process(self, base_directory: str) -> None:
        baseline: Variation = Variation(
            name="baseline-0",
            loss_by_sensor={},
            iterations=[Iteration(name="iteration-0", loss_seed_by_sensor={}, loss_seed_fallback=SEED)],
        )

        simulation_variation_iteration_list: List[Dict[str, str]] = []

        for variation_index, variation in enumerate([baseline] + self.variations):
            if variation_index != 0:
                if "baseline" in variation.name.lower():
                    raise Exception(
                        f"variation with name `{variation.name}` is invalid "
                        f"because it contains the substring `baseline`!"
                    )

            for iteration in variation.iterations:
                simulation_variation_iteration_list.append(
                    {
                        "simulationName": self.name,
                        "variationName": variation.name,
                        "iterationName": iteration.name,
                    }
                )

                svi_input_dir: str = os.path.join(
                    base_directory, self.name, variation.name, iteration.name, "_system", "input"
                )
                os.makedirs(svi_input_dir, exist_ok=True)

                svi_output_dir: str = os.path.join(
                    base_directory, self.name, variation.name, iteration.name, "_system", "output"
                )
                os.makedirs(svi_output_dir, exist_ok=True)

                df_by_sensor: Dict[str, pd.DataFrame] = {}
                seen_measurement_name_list: List[str] = []

                for sensor in self.sensors:
                    sensor_name: str = sensor.name

                    df_map: Dict[str, pd.DataFrame] = {}

                    for measurement in sensor.measurements:
                        measurement_name: str = measurement.name
                        y: Union[str, np.ndarray] = measurement.sample
                        frequency: Frequency = measurement.frequency
                        start: pd.Timestamp = measurement.start
                        interactions_distributions: List[DistributionType] = measurement.interactions.distributions
                        interactions_distributions_seed: int = measurement.interactions.seed

                        if isinstance(y, str):
                            y = np.random.uniform(low=20, high=30, size=100)  # TODO load from file.
                        elif isinstance(y, np.ndarray):
                            pass
                        else:
                            raise ValueError(f"y type {type(y)} is not one of [str, np.ndarray]!")

                        if measurement_name not in seen_measurement_name_list:
                            seen_measurement_name_list.append(measurement_name)

                        m_gen: MeasurementBasicGenerator = MeasurementBasicGenerator()
                        m_gen.set_y(y=y)
                        m_gen.set_x(
                            distribution=frequency.distribution,
                            seed=frequency.seed,
                            start=start,
                            timezone=None,
                        )
                        m_gen.construct_dataframe()
                        m_gen.set_interactions(
                            distributions=interactions_distributions,
                            seed=interactions_distributions_seed,
                        )

                        df: pd.DataFrame = m_gen.df

                        df.to_excel(os.path.join(svi_input_dir, f"{sensor_name}-{measurement_name}-df.xlsx"))
                        df.to_parquet(os.path.join(svi_input_dir, f"{sensor_name}-{measurement_name}-df.parquet"))
                        df.to_json(
                            os.path.join(svi_input_dir, f"{sensor_name}-{measurement_name}-df.json"), orient="records"
                        )
                        df.to_csv(os.path.join(svi_input_dir, f"{sensor_name}-{measurement_name}-df.csv"))

                        df_map[measurement_name] = df

                    combiner: MultiMeasurementCombiner = MultiMeasurementCombiner()
                    for family, df in df_map.items():
                        combiner.add_dataframe(family=family, df=df)
                    combiner.combine()

                    if sensor.frequency is not None:
                        combiner.resample(rule=sensor.frequency)

                    loss: Optional[Loss] = None
                    if sensor_name in variation.loss_by_sensor:
                        loss = variation.loss_by_sensor[sensor_name]

                    if loss is None:
                        _logger.warning(
                            f"sensor_name {sensor_name} not in loss_by_sensor of variation: {variation.name}."
                        )
                        combiner.set_no_loss()
                        combiner.set_loss_metrics()
                    else:
                        time_between_errors_distribution_seed: int = iteration.loss_seed_fallback
                        error_duration_distribution_seed: int = iteration.loss_seed_fallback

                        if sensor_name in iteration.loss_seed_by_sensor:
                            time_between_errors_distribution_seed = iteration.loss_seed_by_sensor[sensor_name]
                            error_duration_distribution_seed = iteration.loss_seed_by_sensor[sensor_name]
                        else:
                            _logger.warning(
                                f"sensor_name {sensor_name} not in loss_seed_by_sensor of iteration: {iteration.name}. "
                                f"Using fallback: {iteration.loss_seed_fallback}"
                            )

                        time_between_errors_distribution: DistributionType = loss.time_between_errors_distribution
                        time_between_errors_distribution_seed: int = time_between_errors_distribution_seed
                        error_duration_distribution: DistributionType = loss.error_duration_distribution
                        error_duration_distribution_seed: int = error_duration_distribution_seed

                        combiner.set_loss(
                            time_between_errors_distribution=time_between_errors_distribution,
                            time_between_errors_distribution_seed=time_between_errors_distribution_seed,
                            error_duration_distribution=error_duration_distribution,
                            error_duration_distribution_seed=error_duration_distribution_seed,
                        )
                        combiner.set_loss_metrics()

                    df: pd.DataFrame = combiner.df

                    df.to_excel(os.path.join(svi_input_dir, f"{sensor_name}-df.xlsx"))
                    df.to_parquet(os.path.join(svi_input_dir, f"{sensor_name}-df.parquet"))
                    df.to_json(os.path.join(svi_input_dir, f"{sensor_name}-df.json"), orient="records")
                    df.to_csv(os.path.join(svi_input_dir, f"{sensor_name}-df.csv"))

                    df_by_sensor[sensor_name] = df

                # Merge all Sensors dataframes.
                # --------------------------------------------------

                for key in df_by_sensor.keys():
                    df_by_sensor[key]["sensor"] = key
                    df_by_sensor[key]["x"] = df_by_sensor[key].index.copy(deep=True)
                    df_by_sensor[key]["x"].reset_index(drop=True, inplace=True)

                df: pd.DataFrame = pd.concat(objs=df_by_sensor.values(), ignore_index=True)
                df.sort_values(by="x", inplace=True, ignore_index=True)
                df.set_index(keys="x", drop=True, inplace=True)
                df["ds"] = df.index.copy(deep=True)
                df["timestamp"] = df["ds"].apply(lambda x: int(x.timestamp() * 1000))

                df.to_excel(os.path.join(svi_input_dir, f"df.xlsx"))
                df.to_parquet(os.path.join(svi_input_dir, f"df.parquet"))
                df.to_json(os.path.join(svi_input_dir, f"df.json"), orient="records")
                df.to_csv(os.path.join(svi_input_dir, f"df.csv"))

                events: List[Dict] = []
                rows: List[Dict] = df.to_dict(orient="records")
                for row in rows:
                    if row["loss"] > 0:
                        continue

                    measurements = []
                    for seen_measurement_name in seen_measurement_name_list:
                        if seen_measurement_name not in row:
                            continue
                        value = row[seen_measurement_name]
                        if pd.isna(value) is True:
                            continue
                        measurements.append(
                            {
                                "name": seen_measurement_name,
                                "value": {
                                    "double": value,
                                },
                                "unit": {"string": MEASUREMENT_UNIT_BY_MEASUREMENT_NAME[seen_measurement_name]},
                            }
                        )

                    if len(measurements) <= 0:
                        continue

                    events.append(
                        {
                            "sensorId": row["sensor"],
                            "measurements": measurements,
                            "timestamps": {
                                "defaultTimestamp": {"long": row["timestamp"]},
                                "timestamps": {
                                    "sensed": {"long": row["timestamp"]},
                                },
                            },
                            "identifiers": {
                                "clientSideId": {
                                    "string": uuid.uuid4().__str__(),
                                },
                                "correlationIds": {},
                            },
                            "additional": {
                                "simulation_name": {"string": self.name},
                                "variation_name": {"string": variation.name},
                                "iteration_name": {"string": iteration.name},
                                "ds": {"string": row["ds"].__str__()},
                            },
                        }
                    )

                json_string = json.dumps(obj=events, indent=4, allow_nan=False, sort_keys=False)
                with open(os.path.join(svi_input_dir, "SensorTelemetryRawEventIBO.json"), "w") as outfile:
                    outfile.write(json_string)

        # Persistence: Prerequisites
        # --------------------------------------------------

        os.makedirs(os.path.join(base_directory, self.name, "_system"), exist_ok=True)

        # Persistence: Simulation Variation Iteration (list)
        # --------------------------------------------------

        _to_json(
            obj=simulation_variation_iteration_list,
            directory=os.path.join(base_directory, self.name, "_system"),
            file_name="SimulationVariationIterationList.json",
        )

        # Persistence: Composite Transformations (list)
        # --------------------------------------------------

        ct_ps_list: List[Dict] = self.average_ct_ps_space.to_composite_transformation_parameters_set()
        _to_json(
            obj=ct_ps_list,
            directory=os.path.join(base_directory, self.name, "_system"),
            file_name="AverageCalculationCompositeTransformationParameters.json",
        )


# ####################################################################################################
# Evaluation
# ####################################################################################################


def _complex_event_uid(ibo: Dict, simulation_name: str, variation_name: str, iteration_name: str) -> str:
    ct_ps_id: str = ibo["compositeTransformationParametersIdentifier"]
    start_timestamp: int = ibo["startTimestamp"]
    end_timestamp: int = ibo["endTimestamp"]
    return f"{simulation_name}/{variation_name}/{iteration_name}/{ct_ps_id}/{start_timestamp}/{end_timestamp}"


def _extract_iot_event_value(ibo: Dict, sensor_id: str) -> Optional[float]:
    try:
        return round(ibo["events"][sensor_id]["measurement"]["value"]["double"], 2)
    except KeyError:
        return None


def _extract_iot_event_is_fabricated(ibo: Dict, sensor_id: str) -> Optional[float]:
    try:
        return 1 if "eventFabricationMethod" in ibo["events"][sensor_id]["additional"] else 0
    except KeyError:
        return 0


def _to_complex_event(ibo: Dict, simulation_name: str, variation_name: str, iteration_name: str) -> Dict:
    """
    IBO to Complex Event. IBO is also a Complex Event. However, it has too much information.
    This function creates a representation of a Complex Event for convenient processing and interpretation.
    """

    ct_ps_id_str: str = ibo["compositeTransformationParametersIdentifier"]
    ct_ps_id_obj: CompositeTransformationParameterID = parse_ctp_id(ctp_id=ct_ps_id_str)

    uid: str = _complex_event_uid(
        ibo=ibo, simulation_name=simulation_name, variation_name=variation_name, iteration_name=iteration_name
    )
    ground_truth_counterpart_uid: str = _complex_event_uid(
        ibo=ibo, simulation_name=simulation_name, variation_name="baseline-0", iteration_name="iteration-0"
    )

    naive_count: int = ibo["additional"]["eventFabricationNaiveCount"]["long"]
    ses_count: int = ibo["additional"]["eventFabricationSESCount"]["long"]
    fabricated_events_count: int = naive_count + ses_count
    fabricated_events_duration: int = ibo["additional"]["eventFabricationDuration"]["long"]

    event: Dict[str, Any] = {
        # IDs and taxonomy: Simulation-Variation-Iteration ----------
        "simulation_name": simulation_name,
        "variation_name": variation_name,
        "iteration_name": iteration_name,
        # TODO Keep one timestamp for ID and sorting and joining...
        # IDs and taxonomy: Composite Transformation ----------
        "naive_max_distance_param": ct_ps_id_obj.fabrication_past_events_steps_behind,
        "expon_max_distance_param": ct_ps_id_obj.fabrication_forecasting_steps_ahead,
        "expon_max_distance_actual": ct_ps_id_obj.fabrication_past_events_steps_behind + ct_ps_id_obj.fabrication_forecasting_steps_ahead,
        # IDs and taxonomy: Time Window ----------
        "start_timestamp": ibo["startTimestamp"],
        "start_dt": str(pd.Timestamp(ibo["startTimestamp"], unit="ms", tz="UTC")),
        "end_timestamp": ibo["endTimestamp"],
        "end_dt": str(pd.Timestamp(ibo["endTimestamp"], unit="ms", tz="UTC")),
        # Business: Average Calculation ----------
        "value": round(ibo["average"]["value"]["double"], 2),
        "value_before": round(ibo["additional"]["averageValueBeforeEventFabrication"]["double"], 2),
        "real": -1.0,  # We do not have the value yet.
        "value_ef": -1.0,  # We do not have the value yet.
        "value_ef_real": -1.0,  # We do not have the value yet.
        # Quality (after fabrication if performed) ----------
        "completeness": ibo["qualityProperties"]["metrics"]["completeness1"]["double"],
        "completeness_before": ibo["additional"]["completeness1BeforeEventFabrication"]["double"],
        "accuracy": -1.0,  # We do not have the value yet.
        "accuracy_before": -1.0,  # We do not have the value yet.
        "accuracy_ef": -1.0,  # We do not have the value yet.
        "timeliness1": ibo["qualityProperties"]["metrics"]["timeliness1"]["double"],
        "timeliness1_before": ibo["additional"]["timeliness1BeforeEventFabrication"]["double"],
        "timeliness2": ibo["qualityProperties"]["metrics"]["timeliness2"]["double"],
        "timeliness2_before": ibo["additional"]["timeliness2BeforeEventFabrication"]["double"],
        # Event Fabrication ----------
        "naive_count": naive_count,
        "ses_count": ses_count,
        "fabricated_events_count": fabricated_events_count,
        # Benchmarking ----------
        "fabricated_events_duration": fabricated_events_duration,
        # IoT Events / Sensors / Event Fabrication ----------
        **{f"sensor-{i}": _extract_iot_event_value(ibo, f"sensor-{i}") for i in range(1, 7)},
        **{f"sensor-{i}-ef": _extract_iot_event_is_fabricated(ibo, f"sensor-{i}") for i in range(1, 7)},
        **{f"sensor-{i}-real": -1.0 for i in range(1, 7)},  # We do not have the value yet.
        # System-Specifics (they dropped after processing) ----------
        "uid": uid,
        "is_baseline": "baseline" in variation_name.lower(),
        "ground_truth_counterpart_uid": ground_truth_counterpart_uid,
    }

    # Mean of values of fabricated events
    # --------------------------------------------------

    value_ef: float = 0.0
    total: int = 0
    for i in range(1, 7):
        is_fabricated: bool = event[f"sensor-{i}-ef"] > 0
        if is_fabricated is True:
            value_ef = value_ef + event[f"sensor-{i}"]
            total = total + 1
    if total > 0:
        value_ef = value_ef / total
        value_ef = round(value_ef, 2)
    else:
        value_ef = None

    event["value_ef"] = value_ef

    # --------------------------------------------------

    return event


def _update_ground_truth(df: pd.DataFrame) -> pd.DataFrame:
    def _update_ground_truth_apply_func_factory(column: str) -> Callable:
        def _update_ground_truth_apply_func(row):
            counterpart_row = df[df["uid"] == row["ground_truth_counterpart_uid"]]
            assert len(counterpart_row) == 1, "There should be exactly one match for each uid."
            if not counterpart_row.empty:
                counterpart_value = counterpart_row[column].iloc[0]
                return counterpart_value
            else:
                return None

        return _update_ground_truth_apply_func

    _apply_func = _update_ground_truth_apply_func_factory(column="value")
    df["real"] = df.apply(lambda row: _apply_func(row), axis=1)

    # TODO Detect these places and make them generic (to support a sensor naming convention and dynamic min/max)
    for i in range(1, 7):
        _apply_func = _update_ground_truth_apply_func_factory(column=f"sensor-{i}")
        df[f"sensor-{i}-real"] = df.apply(lambda row: _apply_func(row), axis=1)

    def _mean_value_of_fabricated_events_apply_func(row):
        value: Optional[float] = 0.0
        total: int = 0
        for i_ in range(1, 7):
            if row[f"sensor-{i_}-ef"] > 0:
                value = value + row[f"sensor-{i_}-real"]
                total = total + 1
        if total > 0:
            value = value / total
            value = round(value, 2)
        else:
            value = None
        return value

    df["value_ef_real"] = df.apply(lambda row: _mean_value_of_fabricated_events_apply_func(row), axis=1)

    return df


def _update_accuracy(df: pd.DataFrame) -> pd.DataFrame:
    def _update_accuracy_apply_func_factory(column_value: str, column_real: str) -> Callable:
        def _update_accuracy_apply_func(row):
            value_ = row[column_value]
            real_ = row[column_real]
            if real_ == 0:
                value_ = value_ + EPS
                real_ = real_ + EPS
            return 1.0 - (abs(value_ - real_) / real_)

        return _update_accuracy_apply_func

    _apply_func = _update_accuracy_apply_func_factory(column_value="value", column_real="real")
    df["accuracy"] = df.apply(lambda row: _apply_func(row), axis=1)

    _apply_func = _update_accuracy_apply_func_factory(column_value="value_before", column_real="real")
    df["accuracy_before"] = df.apply(lambda row: _apply_func(row), axis=1)

    _apply_func = _update_accuracy_apply_func_factory(column_value="value_ef", column_real="value_ef_real")
    df["accuracy_ef"] = df.apply(lambda row: _apply_func(row), axis=1)

    return df


def _multiply_percentage_and_round(df: pd.DataFrame, columns: List[str], round_n_digits: int) -> pd.DataFrame:
    for column in columns:
        df[column] = df[column] * 100
        df[column] = df[column].round(round_n_digits)
    return df


def perform_evaluation(directory: str, simulation_name) -> None:
    simulation_directory: str = os.path.join(directory, simulation_name)

    complex_event_list: List[Dict[str, Any]] = []

    svi_list: List[Dict] = _from_json(
        path_to_file=os.path.join(simulation_directory, "_system", "SimulationVariationIterationList.json")
    )
    for svi in svi_list:
        simulation_name_: str = svi["simulationName"]
        assert simulation_name_ == simulation_name
        variation_name: str = svi["variationName"]
        iteration_name: str = svi["iterationName"]

        output_directory: str = os.path.join(
            directory, simulation_name, variation_name, iteration_name, "_system", "output"
        )

        # Read all output files (i.e., files with extension `.json`).
        # These file contains 0 or more complex events.
        # --------------------------------------------------

        output_file_name_list: List[str] = [f for f in os.listdir(output_directory) if f.endswith(".json")]
        for output_file_name in output_file_name_list:
            path_to_output_file: str = os.path.join(output_directory, output_file_name)
            with open(path_to_output_file, "r") as file:
                ibo_list: List[Dict[str, Any]] = json.load(file)
                for ibo in ibo_list:
                    complex_event: Dict[str, Any] = _to_complex_event(
                        ibo=ibo,
                        simulation_name=simulation_name,
                        variation_name=variation_name,
                        iteration_name=iteration_name,
                    )
                    complex_event_list.append(complex_event)

    complex_event_df: pd.DataFrame = pd.DataFrame(data=complex_event_list)
    _persist_dataframe(df=complex_event_df, directory=simulation_directory, file_name="complex-event-out")
    complex_event_df = _update_ground_truth(df=complex_event_df)
    complex_event_df = _update_accuracy(df=complex_event_df)
    complex_event_df = _multiply_percentage_and_round(
        df=complex_event_df,
        columns=[
            "completeness",
            "timeliness1",
            "timeliness2",
            "accuracy",
            "completeness_before",
            "timeliness1_before",
            "timeliness2_before",
            "accuracy_before",
            "accuracy_ef",
        ],
        round_n_digits=2,
    )
    _persist_dataframe(df=complex_event_df, directory=simulation_directory, file_name="complex-event-eval")


def perform_evaluation_aggregation(directory: str, simulation_name) -> None:
    raise NotImplementedError  # TODO Implement.
