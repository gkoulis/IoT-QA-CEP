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

Author: Dimitris Gkoulis
Created at: Wednesday 04 October 2023
Modified at: Saturday 04 November 2023
"""

import json
import logging
import os
import random
import re
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from iotvm_extensions.examples.average_calculation_parameters_sets import generate_average_calculation_parameters_sets

_logger = logging.getLogger(__name__)


# ####################################################################################################
# Random Context.
# ####################################################################################################


def _to_json(obj, directory: str, file_name: str) -> None:
    json_object = json.dumps(obj, indent=4, allow_nan=False, sort_keys=False)
    path_to_file: str = os.path.join(directory, file_name)
    # assert os.path.exists(path_to_file) is False
    with open(path_to_file, "w") as outfile:
        outfile.write(json_object)


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
            iterations=[Iteration(name="iteration-0", loss_seed_by_sensor={}, loss_seed_fallback=DEFAULT_SEED)],
        )

        simulation_variation_iteration_list: List[Dict[str, str]] = []

        for variation in [baseline] + self.variations:
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
                df["timestamp"] = df['ds'].apply(lambda x: int(x.timestamp() * 1000))

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
# Tests and Examples
# ####################################################################################################


# TODO Create library with ready to use objects.
# TODO Move to project constants or even better to library.
DEFAULT_SEED: int = 42


def _example1() -> None:
    # project_directory: str = "/Users/gkoulis/projects/dgk-phd-monorepo/src/iotvm-extensions"
    project_directory: str = "/home/dgk/projects/PhD/dgk-phd-monorepo/src/iotvm-extensions"
    base_directory: str = os.path.join(project_directory, "local_data", "simulation1-EXAMPLE")
    path_to_dataset: str = os.path.join(project_directory, "datasets", "dataset-1-slice-9-13.csv")
    sample1_df: pd.DataFrame = pd.read_csv(path_to_dataset, delimiter="\t")
    sample: np.ndarray = sample1_df["value"].values

    timezone: Optional[str] = None  # "Europe/Athens"

    timestamp: pd.Timestamp = pd.Timestamp(
        year=2024,
        month=3,
        day=9,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tz=timezone,
        unit="sec",  # sec for simplicity, ns for precision
    )

    FREQ_DIST_CONST1: DistributionType = ConstantDistribution(
        seed_name=MeasurementBasicGenerator.X_SEED_NAME, loc=t_min_to_sec(5.0), size=None
    )
    FREQ_CONST1: Frequency = Frequency(distribution=FREQ_DIST_CONST1, seed=DEFAULT_SEED)

    LOSS1: Loss = Loss(
        time_between_errors_distribution=ExponentialDistribution(
            seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(17.0), size=None
        ),
        error_duration_distribution=ExponentialDistribution(
            seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(33.0), size=None
        ),
    )
    LOSS2: Loss = Loss(
        time_between_errors_distribution=ExponentialDistribution(
            seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(157.0), size=None
        ),
        error_duration_distribution=ExponentialDistribution(
            seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME, scale=t_min_to_sec(20.0), size=None
        ),
    )

    # TODO Make sure it produces the same results as before!!!!!!!!
    simulation: Simulation = Simulation(
        name="simulation-1",
        sensors=[
            Sensor(
                name="sensor-1",
                measurements=[
                    Measurement(
                        name="temperature",
                        unit="celsius",
                        sample=sample,
                        frequency=FREQ_CONST1,
                        start=timestamp,
                        timezone=timezone,
                        interactions=Interactions(
                            distributions=[
                                ConstantDistribution(
                                    seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                    loc=1.0,
                                ),
                                NormalDistribution(
                                    seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                    loc=0.5,
                                    scale=0.2,
                                    size=None,
                                ),
                            ],
                            seed=DEFAULT_SEED,
                        ),
                    ),
                ],
                frequency=None,
            ),
            Sensor(
                name="sensor-2",
                measurements=[
                    Measurement(
                        name="temperature",
                        unit="celsius",
                        sample=sample,
                        frequency=FREQ_CONST1,
                        start=timestamp,
                        timezone=timezone,
                        interactions=Interactions(
                            distributions=[
                                ConstantDistribution(
                                    seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                    loc=-1.0,
                                    size=None,
                                ),
                                NormalDistribution(
                                    seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME,
                                    loc=0.5,
                                    scale=0.1,
                                    size=None,
                                ),
                            ],
                            seed=DEFAULT_SEED,
                        ),
                    ),
                ],
                frequency=None,
            ),
        ],
        variations=[
            Variation(
                name="variation-1",
                loss_by_sensor={
                    "sensor-1": LOSS1,
                    "sensor-2": LOSS2,
                },
                iterations=[
                    Iteration(
                        name="iteration-1",
                        loss_seed_by_sensor={
                            "sensor-1": DEFAULT_SEED,
                            "sensor-2": DEFAULT_SEED,
                        },
                        loss_seed_fallback=DEFAULT_SEED,
                    ),
                ],
            )
        ],
        average_ct_ps_space=AverageCalculationCompositeTransformationParametersSetsSpace(
            physical_quantity_list=["TEMPERATURE"],
            time_window_size_list=[5],
            # number_of_contributing_sensors_list=[2, 4, 6],
            number_of_contributing_sensors_list=[2],  # TODO Temporary.
            ignore_completeness_filtering_list=[False],
            # fabrication_past_events_steps_behind_list=[2, 4, 6],
            # fabrication_forecasting_steps_ahead_list=[2, 4, 6],
            fabrication_past_events_steps_behind_list=[0],  # TODO Temporary.
            fabrication_forecasting_steps_ahead_list=[0],  # TODO Temporary.
        ),
    )
    # TODO Do not allow if directory already exists!
    simulation.process(base_directory=base_directory)


def run_example() -> None:
    # TODO Remove and move to examples.py
    _example1()
