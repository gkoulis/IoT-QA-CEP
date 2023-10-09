"""
Sensor synthetic data generator [EXPERIMENTAL].

Abbreviation: `ttp` time, tick or period.

Author: Dimitris Gkoulis
Created at: Wednesday 04 October 2023
Modified at: Tuesday 10 October 2023
"""

import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

matplotlib.use(backend="Qt5Agg")


# ####################################################################################################
# Private Context.
# ####################################################################################################


class _Context:
    def __init__(self) -> None:
        self.np_rng: np.random.Generator | None = None
        self.py_rng: random.Random | None = None

    def set_seed(self, seed: Optional[int]) -> None:
        self.np_rng = np.random.default_rng(seed=seed)
        self.py_rng = random.Random(x=seed)


_C: _Context = _Context()


# ####################################################################################################
# Distributions
# ####################################################################################################


@dataclass
class NormalDistribution:
    loc: float
    scale: float
    size: Optional[int] = None

    def random(self, size: Optional[int]) -> Any:
        return _C.np_rng.normal(loc=self.loc, scale=self.scale, size=size)

    def random2(self) -> Any:
        return self.random(size=self.size)


class ConstantDistribution(NormalDistribution):
    def __init__(self, loc: float, size: Optional[int] = None) -> None:
        super().__init__(loc=loc, scale=0, size=size)


@dataclass
class ExponentialDistribution:
    scale: float
    size: Optional[int] = None

    def random(self, size: Optional[int]) -> Any:
        return _C.np_rng.exponential(scale=self.scale, size=size)

    def random2(self) -> None:
        return self.random(size=self.size)


DistributionType = Union[
    NormalDistribution, ConstantDistribution, ExponentialDistribution
]


# ####################################################################################################
# Timestamps (relative).
# ####################################################################################################


def generate_relative_timestamps(distribution: DistributionType, duration: float):
    """

    ...

    Parameters
    ----------

    distribution : DistributionType
    duration : float
        the total duration.

    Returns
    -------

    np.ndarray
        a numpy array with the timestamps.
    """
    times = [0]
    while times[-1] < duration:
        interval: float = distribution.random(size=None)
        times.append(times[-1] + interval)
    # Removing the last time which exceeds `duration`.
    return np.array(times[:-1])


# ####################################################################################################
# Errors.
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
# Up-sampling.
# ####################################################################################################


def perform_up_sampling(
    df: pd.DataFrame, distribution: DistributionType
) -> pd.DataFrame:
    df.set_index(
        keys=df["relative_timestamp"].values,
        drop=True,
        inplace=True,
        verify_integrity=True,
    )

    duration = df["relative_timestamp"].iloc[-1] - df["relative_timestamp"].iloc[0]
    duration = float(duration)

    relative_timestamps = [float(df["relative_timestamp"].iloc[0])]
    while relative_timestamps[-1] < duration:
        interval: float = distribution.random(size=None)
        relative_timestamps.append(relative_timestamps[-1] + interval)
    relative_timestamps = relative_timestamps[:-1]

    new_df: pd.DataFrame = df.reindex(
        df.index.union(pd.Index(relative_timestamps))
    ).interpolate(method="linear")
    new_df["relative_timestamp"] = new_df.index.values
    new_df.reset_index(drop=True, inplace=True)

    return new_df


# ####################################################################################################
# Utilities.
# ####################################################################################################


def get_change(current, previous):
    if current == previous:
        return 100.0
    try:
        return (abs(current - previous) / previous) * 100.0
    except ZeroDivisionError:
        return 0


def generate_random_booleans(size: int, probability: float) -> List[bool]:
    data: List[bool] = []
    for _ in range(0, size):
        data.append(_C.py_rng.random() < probability)
    return data


def build_macros(df: pd.DataFrame) -> List[str]:
    macros: List[str] = []
    for idx, data in df.iterrows():
        str1: str = "fail_push" if data["error"] > 0 else "push"
        value: float = data["noisy_value"]
        value = round(value, 2)
        macro: str = f"{str1}(T(exact({value})))"
        macros.append(macro)
    return macros


# ####################################################################################################
# Logic.
# ####################################################################################################


def generate_synthetic_data_by_example(
    sample: np.ndarray,
    frequency_distribution: DistributionType,
    up_sampling_distribution: Optional[DistributionType],
    noise_distributions: List[DistributionType],
    ttp_between_errors_distribution: DistributionType,
    error_ttp_distribution: DistributionType,
    seed: Optional[int],
    display_plot: bool,
    display_log: bool,
) -> pd.DataFrame:
    """
    TODO Docs.

    sample με συνεχείς τιμές και ένα distribution το οποίο θα αναθέσει timestamps σε αυτές τις συνεχείς τιμές.
    Επιπροσθέτως, ένα offset για να "ρυθμίσει" τις χρονικές στιγμές ώστε να μην είναι ίδιες για κάθε sensor.
    Ιδιαίτερα χρήσιμο σε περίπτωση που έχουμε σταθερά intervals.
    Από αυτό το sample, προκύπτει ένα dataframe.
    Επιλογή: up-sampling για αύξηση των observations.
    Επιλογή: down-sampling για μείωση των observations.
    Επιλογή: errors.
    Επιλογή: noise
    TODO future: signal manipulation (independent of this method)
    """

    # Validations.
    # --------------------------------------------------

    assert len(sample.shape) == 1

    # Seed.
    # --------------------------------------------------

    _C.set_seed(seed=seed)

    # Initializations.
    # --------------------------------------------------

    duration: float = 0.0
    uptime: float = 0.0
    downtime: float = 0.0
    errors: int = 0
    mean_error_duration: float = 0.0

    # Generate relative timestamps for the provided sample.
    # --------------------------------------------------

    relative_timestamps: np.ndarray = frequency_distribution.random(size=len(sample))
    relative_timestamps = np.cumsum(relative_timestamps)
    relative_timestamps = relative_timestamps[:-1]
    relative_timestamps = np.append([0], relative_timestamps)

    # Create the dataframe.
    # --------------------------------------------------

    df1: pd.DataFrame = pd.DataFrame(
        data={
            "relative_timestamp": relative_timestamps,
            "value": sample,
            "error": 0,
        }
    )

    if display_plot is True:
        df1.plot(x="relative_timestamp", y="value")
        plt.show()

    # Perform down-sampling to decrease observations.
    # --------------------------------------------------

    # @future implementation.

    # Perform up-sampling to increase observations.
    # --------------------------------------------------

    if up_sampling_distribution is not None:
        df2: pd.DataFrame = perform_up_sampling(
            df=df1.copy(deep=True),
            # scale: mean interval (in minutes)
            distribution=ExponentialDistribution(scale=5),
            # distribution=NormalDistribution(loc=1, scale=0),  # Constant.
        )

    # Select dataframe.
    # --------------------------------------------------

    if up_sampling_distribution is None:
        df: pd.DataFrame = df1
    else:
        # noinspection PyUnboundLocalVariable
        df: pd.DataFrame = df2

    # Noise.
    # --------------------------------------------------

    noise_series_list = []
    for noise_distribution in noise_distributions:
        noise_series = noise_distribution.random(size=len(df))
        noise_series_list.append(noise_series)

    df["noisy_value"] = df["value"].values
    for noise_series in noise_series_list:
        df["noisy_value"] = df["noisy_value"] + noise_series

    # Switch [EXPERIMENTAL].
    # --------------------------------------------------

    # i = df["relative_timestamp"] > 500.0
    # df.loc[i, "noisy_value"] = df.loc[i, "noisy_value"] + 3.369

    # Generate error intervals.
    # --------------------------------------------------

    duration = df["relative_timestamp"].iloc[-1] - df["relative_timestamp"].iloc[0]
    duration = float(duration)
    error_intervals: List[ErrorInterval] = generate_errors_intervals(
        ttp_between_errors_distribution=ttp_between_errors_distribution,
        error_ttp_distribution=error_ttp_distribution,
        # error_ttp_distribution=ConstantDistribution(loc=10),
        ttp=duration,
    )

    # Add errors intervals.
    # --------------------------------------------------

    df_first_dt = float(df["relative_timestamp"].iloc[0])
    df["error"] = 0  # Reset.
    for error_interval in error_intervals:
        start = df_first_dt + error_interval.start
        end = df_first_dt + error_interval.end
        df.loc[
            ((df["relative_timestamp"] >= start) & (df["relative_timestamp"] <= end)),
            "error",
        ] = 1

        downtime = downtime + (end - start)
        errors = errors + 1
        mean_error_duration = mean_error_duration + (end - start)

    if errors > 0:
        mean_error_duration = mean_error_duration / errors

    # Availability + Reliability.
    # --------------------------------------------------

    t = df["relative_timestamp"].values
    uptime = duration - downtime
    error_rate = downtime / duration
    # The probability that the sensor will operate without failure for a time t.
    reliability = np.exp(-error_rate * t)

    if display_log is True:
        print("Metrics:")
        print("duration             : ", duration)
        print("uptime               : ", uptime)
        print("downtime             : ", downtime)
        print("error rate           : ", error_rate)
        print("mean error duration  : ", mean_error_duration)
        # print("reliability : ", reliability)
        print("\n")

    if display_plot is True:
        plt.plot(t, reliability)
        plt.xlabel("(Relative) Time (t)")
        plt.ylabel("Reliability R(t)")
        plt.title("Reliability Function over Time")
        plt.grid(True)
        plt.show()

    # Plotting.
    # --------------------------------------------------

    if display_plot is True:
        errors_df = df.query("error == 1")

        plt.figure(figsize=(12, 5))

        plt.plot(df["relative_timestamp"], df["value"], label="Value", color="blue")
        plt.plot(
            df["relative_timestamp"],
            df["noisy_value"],
            label="Value (noise)",
            color="purple",
        )

        # plt.scatter(errors_df["relative_timestamp"], errors_df["value"], label="Error", marker='x', color="red")
        plt.scatter(
            errors_df["relative_timestamp"],
            errors_df["noisy_value"],
            label="Error",
            marker="x",
            color="red",
        )

        # plt.plot(df1["relative_timestamp"], df1["value"], "o-", label="original", color="blue")
        # plt.plot(df2["relative_timestamp"], df2["value"], ".-", label="up-sampled", color="red")

        max_noisy_value = df["noisy_value"].max()

        plt.fill_between(
            df["relative_timestamp"],
            0,
            max_noisy_value + (max_noisy_value * 0.01),
            where=(df["error"] > 0),
            label="Error",
            color="pink",
            alpha=0.5,
        )

        plt.legend()
        plt.title("Sensor 1")
        plt.xlabel("Timestamp")
        plt.ylabel("Value")
        plt.grid(True)
        plt.show()

    # Result.
    # --------------------------------------------------

    # TODO store metrics in df.attributes

    return df


# ####################################################################################################
# API.
# ####################################################################################################


def run_example_20231009() -> None:
    """
    TODO add to docs: use cases. For example, to generate an excel with macros,
        the frequency must be constant and the same for all sensors.
    TODO Benchmarking.
    TODO decouple this script from the previous implementation.
    TODO REFACTOR sensor simulation to not rely on schedule_df. Use contracts and BO data types.
    """

    # Sample 1.
    # --------------------------------------------------

    sample1_df: pd.DataFrame = pd.read_csv(
        "dataset-1.csv", delimiter="\t", header=0, index_col=False
    )
    sample1_df["timestamp"] = pd.to_datetime(sample1_df["timestamp"])

    dt1 = pd.to_datetime("2021-01-17 09:00:00")
    dt2 = pd.to_datetime("2021-01-17 13:00:00")
    sample1_df = sample1_df[
        ((sample1_df["timestamp"] >= dt1) & (sample1_df["timestamp"] <= dt2))
    ].copy(deep=True)

    # Sensor 1.
    # --------------------------------------------------

    s1_df: pd.DataFrame = generate_synthetic_data_by_example(
        sample=sample1_df["value"].values,
        frequency_distribution=ConstantDistribution(loc=5),
        up_sampling_distribution=ExponentialDistribution(scale=5),
        noise_distributions=[
            ConstantDistribution(loc=2),
            NormalDistribution(loc=0.5, scale=0.2),
        ],
        ttp_between_errors_distribution=ExponentialDistribution(scale=100),
        error_ttp_distribution=ExponentialDistribution(scale=10.5),
        seed=42,
        display_plot=False,
        display_log=False,
    )
    macros: List[str] = build_macros(df=s1_df)
    s1_df["macro"] = macros

    print(s1_df)


def run_example_20231010() -> None:
    # Sample 1.
    # --------------------------------------------------

    sample1_df: pd.DataFrame = pd.read_csv(
        "dataset-1.csv", delimiter="\t", header=0, index_col=False
    )
    sample1_df["timestamp"] = pd.to_datetime(sample1_df["timestamp"])

    dt1 = pd.to_datetime("2021-01-17 09:00:00")
    dt2 = pd.to_datetime("2021-01-17 13:00:00")
    sample1_df = sample1_df[
        ((sample1_df["timestamp"] >= dt1) & (sample1_df["timestamp"] <= dt2))
    ].copy(deep=True)

    # Sensors parameters.
    # --------------------------------------------------

    sample: np.ndarray = sample1_df["value"].values
    frequency_distribution: DistributionType = ConstantDistribution(loc=5)

    dataframe_by_sensor_id: Dict[str, pd.DataFrame] = {}
    sensors_parameters: Dict[str, Dict] = {
        "sensor-1": dict(
            sample=sample,
            frequency_distribution=frequency_distribution,
            up_sampling_distribution=None,
            noise_distributions=[
                ConstantDistribution(loc=1),
                NormalDistribution(loc=0.5, scale=0.1),
            ],
            ttp_between_errors_distribution=ExponentialDistribution(scale=100),
            error_ttp_distribution=ExponentialDistribution(scale=10.5),
            seed=1_000,
            display_plot=False,
            display_log=False,
        ),
        "sensor-2": dict(
            sample=sample,
            frequency_distribution=frequency_distribution,
            up_sampling_distribution=None,
            noise_distributions=[
                ConstantDistribution(loc=-1),
                NormalDistribution(loc=0.5, scale=0.1),
            ],
            ttp_between_errors_distribution=NormalDistribution(loc=50, scale=50),
            error_ttp_distribution=NormalDistribution(loc=20.0, scale=5.5),
            seed=10_000,
            display_plot=False,
            display_log=False,
        ),
        "sensor-3": dict(
            sample=sample,
            frequency_distribution=frequency_distribution,
            up_sampling_distribution=None,
            noise_distributions=[
                NormalDistribution(loc=0.0, scale=2.0),
            ],
            ttp_between_errors_distribution=ExponentialDistribution(scale=100),
            error_ttp_distribution=ExponentialDistribution(scale=10.5),
            seed=100_000,
            display_plot=False,
            display_log=False,
        ),
        "sensor-4": dict(
            sample=sample,
            frequency_distribution=frequency_distribution,
            up_sampling_distribution=None,
            noise_distributions=[
                ConstantDistribution(loc=4),
                NormalDistribution(loc=0.0, scale=2.0),
            ],
            ttp_between_errors_distribution=ExponentialDistribution(scale=100),
            error_ttp_distribution=ExponentialDistribution(scale=10.5),
            seed=1_000_000,
            display_plot=False,
            display_log=False,
        ),
        "sensor-5": dict(
            sample=sample,
            frequency_distribution=frequency_distribution,
            up_sampling_distribution=None,
            noise_distributions=[
                ConstantDistribution(loc=-0.5),
                NormalDistribution(loc=0.2, scale=0.1),
            ],
            ttp_between_errors_distribution=ExponentialDistribution(scale=20),
            error_ttp_distribution=ExponentialDistribution(scale=20.5),
            seed=10_000_000,
            display_plot=False,
            display_log=False,
        ),
        "sensor-6": dict(
            sample=sample,
            frequency_distribution=frequency_distribution,
            up_sampling_distribution=None,
            noise_distributions=[
                ConstantDistribution(loc=0.5),
                NormalDistribution(loc=0.2, scale=0.1),
            ],
            ttp_between_errors_distribution=ConstantDistribution(loc=50),
            error_ttp_distribution=ConstantDistribution(loc=20),
            seed=100_000_000,
            display_plot=False,
            display_log=False,
        ),
    }

    # Synthetic data generation (multiple).
    # --------------------------------------------------

    for sensor_id, sensor_parameters in sensors_parameters.items():
        df: pd.DataFrame = generate_synthetic_data_by_example(**sensor_parameters)
        macros: List[str] = build_macros(df=df)
        df["macro"] = macros
        dataframe_by_sensor_id[sensor_id] = df

    # TODO dataframe have the same frequency and size. In any other case, group_by is required!

    df_data: Dict[str, Any] = {"recurring_window": 0}
    for sensor_id, sensor_df in dataframe_by_sensor_id.items():
        df_data[sensor_id] = sensor_df["macro"].values

    df: pd.DataFrame = pd.DataFrame(data=df_data)
    df["recurring_window"] = df.index.values + 1

    df.to_excel("macros-generated.xlsx", sheet_name="data", index=False)

    # Plotting.
    # --------------------------------------------------

    plt.figure(figsize=(12, 5))

    for sensor_id, sensor_df in dataframe_by_sensor_id.items():
        plt.plot(
            sensor_df["relative_timestamp"],
            sensor_df["noisy_value"],
            label=f"{sensor_id}",
        )

        errors_df = sensor_df.query("error == 1")

        plt.scatter(
            errors_df["relative_timestamp"],
            errors_df["noisy_value"],
            label=f"{sensor_id} error",
            marker="x",
            color="red",
        )

    plt.legend()
    plt.title("Sensor 1")
    plt.xlabel("Timestamp")
    plt.ylabel("Value")
    plt.grid(True)
    plt.show()
