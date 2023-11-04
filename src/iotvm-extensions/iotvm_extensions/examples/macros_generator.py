"""
Sensor synthetic data generator [EXPERIMENTAL].

Abbreviation: `ttp` time, tick or period.

Author: Dimitris Gkoulis
Created at: Wednesday 04 October 2023
Modified at: Saturday 04 November 2023
"""

import copy
import json
import os
import pprint
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import optuna
import pandas as pd
import seaborn as sns

matplotlib.use(backend="Qt5Agg")


# ####################################################################################################
# Private Constants.
# ####################################################################################################


_DPI: int = 300


# ####################################################################################################
# Private Context.
# ####################################################################################################


class _Context:
    def __init__(self) -> None:
        self.np_rng_map: Dict[str, np.random.Generator] = {}
        self.py_rng_map: Dict[str, random.Random] = {}
        self._set_default()

    def _set_default(self) -> None:
        self.set_seed(name="default", seed=42)

    def set_seed(self, name: str, seed: Optional[int]) -> None:
        self.np_rng_map[name] = np.random.default_rng(seed=seed)
        self.py_rng_map[name] = random.Random(x=seed)


_C: _Context = _Context()


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
        return _C.np_rng_map[self.seed_name].normal(loc=self.loc, scale=self.scale, size=size)

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
        return _C.np_rng_map[self.seed_name].exponential(scale=self.scale, size=size)

    def random2(self) -> None:
        return self.random(size=self.size)


DistributionType = Union[
    NormalDistribution, ConstantDistribution, ExponentialDistribution
]


def _obj_to_distribution(seed_name: str, obj: Dict) -> DistributionType:
    assert obj is not None
    assert type(obj) == dict

    if obj["type"] == "constant":
        return ConstantDistribution(
            seed_name=seed_name,
            loc=obj["loc"]
        )

    elif obj["type"] == "normal":
        return NormalDistribution(
            seed_name=seed_name,
            loc=obj["loc"],
            scale=obj["scale"],
        )

    elif obj["type"] == "exponential":
        return ExponentialDistribution(
            seed_name=seed_name,
            scale=obj["scale"]
        )

    raise ValueError("invalid type!")


def _any_to_distribution(seed_name: str, obj: Union[List[Dict], Dict, None]) -> Union[List[DistributionType], DistributionType, None]:
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


def _store_dataframe_to_excel(df: pd.DataFrame, directory: str, file_name: str) -> None:
    path_to_file: str = os.path.join(directory, file_name)
    assert os.path.exists(path_to_file) is False
    df.to_excel(path_to_file, sheet_name="data", index=False)


def _store_dataframe_attrs_to_json(df: pd.DataFrame, directory: str, file_name: str) -> None:
    json_object = json.dumps(df.attrs, indent=4)
    path_to_file: str = os.path.join(directory, file_name)
    assert os.path.exists(path_to_file) is False
    with open(path_to_file, "w") as outfile:
        outfile.write(json_object)


# ####################################################################################################
# Visualization (outputs of `generate_synthetic_data_by_example`).
# ####################################################################################################


def plot_generated_synthetic_data_by_example(df: pd.DataFrame, path_to_dir: str, display_errors: bool) -> None:
    sensor_id: str = df.attrs["sensor_id"]

    # Reliability
    # --------------------------------------------------

    t = df.attrs["errors_metrics"]["t"]
    reliability = df.attrs["errors_metrics"]["reliability"]
    plt.plot(t, reliability)
    plt.xlabel("(Relative) Time (t)")
    plt.ylabel("Reliability R(t)")
    plt.title("Reliability Function over Time")
    plt.grid(True)
    plt.savefig(os.path.join(path_to_dir, f"{sensor_id}-reliability-function-over-time.png"), dpi=_DPI)
    plt.close("all")

    # Data with errors.
    # --------------------------------------------------

    plt.figure(figsize=(18, 10))
    plt.plot(df["relative_timestamp"], df["value"], label="original sample", color="black", linestyle="dashed", linewidth=0.5)
    plt.plot(
        df["relative_timestamp"].values,
        df["noisy_value"].values,
        label=f"{sensor_id} generated value",
        color="blue",
    )

    if display_errors is True:
        errors_df = df.query("error == 1")
        plt.scatter(
            errors_df["relative_timestamp"].values,
            errors_df["noisy_value"].values,
            label="Error",
            marker="x",
            color="red",
        )
        max_noisy_value = df["noisy_value"].max()
        min_noisy_value = df["noisy_value"].min()
        plt.fill_between(
            df["relative_timestamp"].values,
            min_noisy_value - (min_noisy_value * 0.01),
            max_noisy_value + (max_noisy_value * 0.01),
            where=(df["error"] > 0),
            label="Error",
            color="pink",
            alpha=0.5,
            )

    plt.legend(loc="best", frameon=False)
    plt.title(f"{sensor_id} generated synthetic data")
    plt.xlabel("Timestamp")
    plt.ylabel("Value")
    plt.grid(True)
    plt.savefig(os.path.join(path_to_dir, f"{sensor_id}-generated-synthetic-data-{display_errors}.png"), dpi=_DPI)
    plt.close("all")


def plot_multiple_generated_synthetic_data_by_example(df_list: List[pd.DataFrame], path_to_dir: str, display_errors: bool) -> None:
    plt.figure(figsize=(18, 10))

    color_palette = sns.color_palette("deep", len(df_list))

    for idx, df in enumerate(df_list):
        sensor_id: str = df.attrs["sensor_id"]

        plt.plot(
            df["relative_timestamp"].values,
            df["noisy_value"].values,
            label=sensor_id,
            color=color_palette[idx],
        )

        if display_errors is True:
            errors_df = df.query("error == 1")
            plt.scatter(
                errors_df["relative_timestamp"].values,
                errors_df["noisy_value"].values,
                label=f"{sensor_id} error",
                marker="x",
                color="red",
            )

    plt.legend(loc="best", frameon=False)
    plt.title("all sensors generated synthetic data")
    plt.xlabel("Timestamp")
    plt.ylabel("Value")
    plt.grid(True)
    plt.savefig(os.path.join(path_to_dir, f"all-sensors-generated-synthetic-data-{display_errors}.png"), dpi=_DPI)
    plt.close("all")


def plot_time_windows_style1() -> None:
    # TODO Implement.

    x = [1, 2, 3, 4, 5]
    ground_truth = [12.5, 12.5, 13, 12, 11]
    data_by_sensor = {
        "sensor-1": [12, 13, 14, 13, 12],
        "sensor-2": [11, 12, 13, 14, 13],
        "sensor-3": [15, 14, 13, 14, 15],
    }
    colors_by_sensor = {
        "sensor-1": [12, 13, 14, 13, 12],
        "sensor-2": [11, 12, 13, 14, 13],
        "sensor-3": [15, 14, 13, 14, 15],
    }
    errors_count = [0, 1, 0, 1, 2]

    plt.scatter(x, [1, 1, 1, 1, 1], s=100, c=["red", "green", "green", "green", "red"], marker="v")
    plt.scatter(x, [2, 2, 2, 2, 2], s=100, c=["red", "red", "green", "red", "red"], marker="v")
    plt.yticks([1, 2], ["sensor-1", "sensor-2"])
    plt.vlines([0.5, 1.5, 2.5, 3.5, 4.5, 5.5], ymin=0, ymax=6, colors="gray", alpha=0.1)
    plt.hlines([0.5, 1.5, 2.5, 3.5, 4.5, 5.5], xmin=0, xmax=6, colors="gray", alpha=0.1)
    plt.annotate(text="12.4", xy=(1., 1.1),)
    plt.annotate(text="13.4", xy=(2., 1.1))
    plt.annotate(text="16.4", xy=(3., 1.1))
    plt.annotate(text="15.4", xy=(4., 1.1))
    plt.annotate(text="12.4", xy=(5., 1.1))
    plt.show()


# ####################################################################################################
# Logic.
# ####################################################################################################


def generate_synthetic_data_by_example(
    sensor_id: str,
    sample: np.ndarray,
    frequency_distribution: DistributionType,
    up_sampling_distribution: Optional[DistributionType],
    noise_distributions: List[DistributionType],
    ttp_between_errors_distribution: DistributionType,
    error_ttp_distribution: DistributionType,
    frequency_distribution_seed: Optional[int],
    up_sampling_distribution_seed: Optional[int],
    noise_distributions_seed: Optional[int],
    errors_distributions_seed: Optional[int],
) -> pd.DataFrame:
    """
    sample με συνεχείς τιμές και ένα distribution το οποίο θα αναθέσει timestamps σε αυτές τις συνεχείς τιμές.
    Επιπροσθέτως, ένα offset για να "ρυθμίσει" τις χρονικές στιγμές ώστε να μην είναι ίδιες για κάθε sensor.
    Ιδιαίτερα χρήσιμο σε περίπτωση που έχουμε σταθερά intervals.
    Από αυτό το sample, προκύπτει ένα dataframe.
    Επιλογή: up-sampling για αύξηση των observations.
    Επιλογή: down-sampling για μείωση των observations.
    Επιλογή: errors.
    Επιλογή: noise

    TODO Future Implementations:
        - Docs.
        - Signal manipulation (independent like mockseries)
    """

    # Validations.
    # --------------------------------------------------

    assert len(sample.shape) == 1

    # Seed.
    # --------------------------------------------------

    _C.set_seed(name="frequency_distribution", seed=frequency_distribution_seed)
    _C.set_seed(name="up_sampling_distribution", seed=up_sampling_distribution_seed)
    _C.set_seed(name="noise_distributions", seed=noise_distributions_seed)
    _C.set_seed(name="errors_distributions", seed=errors_distributions_seed)

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

    # Perform down-sampling to decrease observations.
    # --------------------------------------------------

    # @future implementation.

    # Perform up-sampling to increase observations.
    # --------------------------------------------------

    # ΠΡΟΣΩΡΙΝΟ!
    assert up_sampling_distribution is None
    if up_sampling_distribution is not None:
        df2: pd.DataFrame = perform_up_sampling(
            df=df1.copy(deep=True),
            # scale: mean interval (in minutes)
            distribution=up_sampling_distribution,
            # distribution=ExponentialDistribution(scale=5),  # Exponential
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

    tick_count: int = len(df)
    tick_with_error_count: int = len(df.query("error == 1"))
    tick_with_error_pct: float = 1. - ((tick_count - tick_with_error_count) / tick_count)

    errors_metrics: Dict = {
        "tick_count": tick_count,
        "tick_with_error_count": tick_with_error_count,
        "tick_with_error_pct": tick_with_error_pct,
        "duration": duration,
        "uptime": uptime,
        "downtime": downtime,
        "error_rate": error_rate,
        "mean_error_duration": mean_error_duration,
        "t": list(t),
        "reliability": list(reliability),
    }

    # Result.
    # --------------------------------------------------

    df.attrs["sensor_id"] = sensor_id
    df.attrs["errors_metrics"] = errors_metrics

    return df


# ####################################################################################################
# API.
# ####################################################################################################


def optimize_distribution_parameters_v1(dataset_path_to_file: str) -> None:

    # Sample 1.
    # --------------------------------------------------

    sample1_df: pd.DataFrame = pd.read_csv(
        dataset_path_to_file, delimiter="\t", header=0, index_col=False
    )
    sample1_df["timestamp"] = pd.to_datetime(sample1_df["timestamp"])

    # Objective Target.
    # --------------------------------------------------

    target_tick_with_error_pct: float = 0.105

    # Objective Constants.
    # --------------------------------------------------

    sensor_id: str = "sensor-1"
    sample: np.ndarray = sample1_df["value"].values
    # TODO Κανονικά από Configuration...
    frequency_distribution: DistributionType = ConstantDistribution(seed_name="default", loc=5.0)
    up_sampling_distribution: Optional[DistributionType] = None
    noise_distributions: List[DistributionType] = []
    frequency_distribution_seed: Optional[int] = 42
    up_sampling_distribution_seed: Optional[int] = 42
    noise_distributions_seed: Optional[int] = 42

    # Optimization Objective.
    # --------------------------------------------------

    def _objective1(trial: optuna.Trial) -> float:
        exp_dist_scale1 = trial.suggest_float(name="exp_dist_scale1", low=1.0, high=200.0, step=1.0)
        exp_dist_scale2 = trial.suggest_float(name="exp_dist_scale2", low=1.0, high=50.0, step=1.0)
        errors_distributions_seed = trial.suggest_int("seed", low=1, high=1_000_000_000)

        dataframe: pd.DataFrame = generate_synthetic_data_by_example(
            sensor_id=sensor_id,
            sample=sample,
            frequency_distribution=frequency_distribution,
            up_sampling_distribution=up_sampling_distribution,
            noise_distributions=noise_distributions,
            ttp_between_errors_distribution=ExponentialDistribution(
                seed_name="errors_distributions",
                scale=exp_dist_scale1
            ),
            error_ttp_distribution=ExponentialDistribution(
                seed_name="errors_distributions",
                scale=exp_dist_scale2
            ),
            frequency_distribution_seed=frequency_distribution_seed,
            up_sampling_distribution_seed=up_sampling_distribution_seed,
            noise_distributions_seed=noise_distributions_seed,
            errors_distributions_seed=errors_distributions_seed,
        )

        trial.set_user_attr("errors_metrics", dataframe.attrs["errors_metrics"])

        tick_with_error_pct: float = dataframe.attrs["errors_metrics"]["tick_with_error_pct"]
        abs_distance = abs(target_tick_with_error_pct - tick_with_error_pct)

        return abs_distance

    def _early_stopping_cb(study_, trial_) -> None:
        if trial_.value == 0:
            study_.stop()

    # Optimization.
    # --------------------------------------------------

    study: optuna.Study = optuna.create_study()
    study.optimize(_objective1, n_trials=100, callbacks=[_early_stopping_cb])

    pprint.pprint(study.best_params, sort_dicts=False, indent=2)
    errors_metrics = copy.deepcopy(study.best_trial.user_attrs["errors_metrics"])
    del errors_metrics["t"]
    del errors_metrics["reliability"]
    pprint.pprint(errors_metrics, sort_dicts=False, indent=2)


def generate_macros_multiple_sensors(dataset_path_to_file: str, sensors_parameters: Dict[str, Dict], path_to_experiment_input_directory: str) -> None:

    # Sample 1.
    # --------------------------------------------------

    sample1_df: pd.DataFrame = pd.read_csv(
        dataset_path_to_file, delimiter="\t", header=0, index_col=False
    )
    sample1_df["timestamp"] = pd.to_datetime(sample1_df["timestamp"])

    samples: Dict[str, np.ndarray] = {
        "sample1": sample1_df["value"].values,
    }

    # Sensors parameters.
    # --------------------------------------------------

    parameters_by_sensor_id: Dict[str, Dict] = copy.deepcopy(sensors_parameters)
    del sensors_parameters
    for sensor_id in parameters_by_sensor_id.keys():
        parameters_by_sensor_id[sensor_id]["sensor_id"] = sensor_id

        sample_name: str = parameters_by_sensor_id[sensor_id]["sample_name"]
        parameters_by_sensor_id[sensor_id]["sample"] = samples[sample_name]
        del parameters_by_sensor_id[sensor_id]["sample_name"]

        parameters_by_sensor_id[sensor_id]["frequency_distribution"] = _any_to_distribution(seed_name="frequency_distribution", obj=parameters_by_sensor_id[sensor_id]["frequency_distribution"])
        parameters_by_sensor_id[sensor_id]["up_sampling_distribution"] = _any_to_distribution(seed_name="up_sampling_distribution", obj=parameters_by_sensor_id[sensor_id]["up_sampling_distribution"])
        parameters_by_sensor_id[sensor_id]["noise_distributions"] = _any_to_distribution(seed_name="noise_distributions", obj=parameters_by_sensor_id[sensor_id]["noise_distributions"])
        parameters_by_sensor_id[sensor_id]["ttp_between_errors_distribution"] = _any_to_distribution(seed_name="errors_distributions", obj=parameters_by_sensor_id[sensor_id]["ttp_between_errors_distribution"])
        parameters_by_sensor_id[sensor_id]["error_ttp_distribution"] = _any_to_distribution(seed_name="errors_distributions", obj=parameters_by_sensor_id[sensor_id]["error_ttp_distribution"])

    # Synthetic data generation (multiple).
    # --------------------------------------------------

    dataframe_by_sensor_id: Dict[str, pd.DataFrame] = {}
    for sensor_id, params in parameters_by_sensor_id.items():
        df: pd.DataFrame = generate_synthetic_data_by_example(**params)
        macros: List[str] = build_macros(df=df)
        df["macro"] = macros
        dataframe_by_sensor_id[sensor_id] = df

    # Validation.
    # NOTICE 04 Nov 2023: Κανονικά αυτό δε θα έπρεπε να υπάρχει.
    # Όμως, για το PhD, και το paper στο simulations,
    # έχουμε σταθερό time window με μόνο μία τιμή.
    # Επίσης, όλα τα δείγματα έχουν το ίδιο μέγεθος.
    # Όταν αφαιρεθεί αυτό θα πρέπει να κάποιο τρόπο να τα ενώσω σε ένα.
    # --------------------------------------------------

    temp_len = None
    temp_values = None
    for temp_df in dataframe_by_sensor_id.values():
        if temp_len is None:
            temp_len = len(temp_df)
        if temp_values is None:
            temp_values = temp_df["relative_timestamp"].to_list()

        assert temp_len == len(temp_df)
        assert temp_values == temp_df["relative_timestamp"].to_list()

    # Macros spreadsheet.
    # --------------------------------------------------

    df_data: Dict[str, Any] = {"recurring_window": 0}

    for sensor_id, sensor_df in dataframe_by_sensor_id.items():
        df_data[sensor_id] = sensor_df["macro"].values

    for sensor_id, sensor_df in dataframe_by_sensor_id.items():
        df_data[f"comment__{sensor_id}__value"] = sensor_df["noisy_value"].values

    for sensor_id, sensor_df in dataframe_by_sensor_id.items():
        df_data[f"comment__{sensor_id}__error"] = sensor_df["error"].values

    df: pd.DataFrame = pd.DataFrame(data=df_data)
    df["recurring_window"] = df.index.values + 1

    sensor_id_list: List[str] = list(dataframe_by_sensor_id.keys())
    sensor_id_list_length: int = len(sensor_id_list)

    def _avg_apply_func(row) -> float:
        _sum = 0.0
        for _sensor_id in sensor_id_list:
            _sum = _sum + row[f"comment__{_sensor_id}__value"]
        return _sum / sensor_id_list_length

    df["comment__average_value"] = df.apply(func=_avg_apply_func, axis=1)

    def _errors_count_apply_func(row) -> float:
        _count: int = 0
        for _sensor_id in sensor_id_list:
            if row[f"comment__{_sensor_id}__error"] > 0:
                _count = _count + 1
        return _count

    df["comment__fail_count"] = df.apply(func=_errors_count_apply_func, axis=1)
    df["comment__success_count"] = sensor_id_list_length - df["comment__fail_count"]

    for sensor_id in sensor_id_list:
        del df[f"comment__{sensor_id}__value"]
        del df[f"comment__{sensor_id}__error"]

    # Results.
    # --------------------------------------------------

    _store_dataframe_to_excel(df=df, directory=path_to_experiment_input_directory, file_name="macros_generated.xlsx")
    df.to_pickle(os.path.join(path_to_experiment_input_directory, "macros_generated.pkl"))

    for sensor_id, temp_df in dataframe_by_sensor_id.items():
        _store_dataframe_to_excel(df=temp_df, directory=path_to_experiment_input_directory, file_name=f"generated-synthetic-data-{sensor_id}-dataframe.xlsx")
        _store_dataframe_attrs_to_json(df=temp_df, directory=path_to_experiment_input_directory, file_name=f"generated-synthetic-data-{sensor_id}-attributes.json")

    path_to_dir: str = os.path.join(path_to_experiment_input_directory, "figures")
    os.makedirs(path_to_dir, exist_ok=True)
    for temp_df in dataframe_by_sensor_id.values():
        plot_generated_synthetic_data_by_example(df=temp_df, path_to_dir=path_to_dir, display_errors=False)
        plot_generated_synthetic_data_by_example(df=temp_df, path_to_dir=path_to_dir, display_errors=True)

    plot_multiple_generated_synthetic_data_by_example(df_list=list(dataframe_by_sensor_id.values()), path_to_dir=path_to_dir, display_errors=False)
    plot_multiple_generated_synthetic_data_by_example(df_list=list(dataframe_by_sensor_id.values()), path_to_dir=path_to_dir, display_errors=True)
