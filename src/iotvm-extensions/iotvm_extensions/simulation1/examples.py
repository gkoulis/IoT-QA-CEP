"""
Author: Dimitris Gkoulis
Created at: Sunday 10 March 2024
"""

import os
from pathlib import Path
from typing import Optional
import logging

import numpy as np
import pandas as pd

from ._base import (
    t_min_to_sec,
    DistributionType,
    ConstantDistribution,
    NormalDistribution,
    ExponentialDistribution,
    MeasurementBasicGenerator,
    MultiMeasurementCombiner,
    Simulation,
    AverageCalculationCompositeTransformationParametersSetsSpace,
    perform_evaluation,
)
from ._presets import Presets
from ._visualization import plot_synthetic_time_series, plot_synthetic_time_series_loss_metrics

_logger = logging.getLogger(__name__)


def generator_and_combiner_example() -> None:
    temperature_arr: np.ndarray = np.random.uniform(low=20, high=30, size=100)
    humidity_arr: np.ndarray = np.random.uniform(low=10, high=90, size=100)

    # --------------------------------------------------

    timezone: Optional[str] = None
    start: pd.Timestamp = pd.Timestamp(
        year=2024,
        month=3,
        day=9,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tz=timezone,
        unit="ns",
    )

    frequency_distribution: DistributionType = NormalDistribution(
        seed_name=MeasurementBasicGenerator.X_SEED_NAME, loc=t_min_to_sec(10.0), scale=t_min_to_sec(1.0), size=None
    )
    frequency_seed: int = 42

    # --------------------------------------------------

    g1: MeasurementBasicGenerator = MeasurementBasicGenerator()
    g1.set_y(y=temperature_arr)
    g1.set_x(distribution=frequency_distribution, seed=frequency_seed, start=start, timezone=timezone)
    g1.construct_dataframe()
    g1.set_interactions(
        distributions=[
            ConstantDistribution(seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME, loc=5.0, size=None),
            NormalDistribution(
                seed_name=MeasurementBasicGenerator.INTERACTIONS_SEED_NAME, loc=0.0, scale=15.0, size=None
            ),
        ],
        seed=42,
    )

    # --------------------------------------------------

    g2: MeasurementBasicGenerator = MeasurementBasicGenerator()
    g2.set_y(y=humidity_arr)
    g2.set_x(distribution=frequency_distribution, seed=frequency_seed, start=start, timezone=timezone)
    g2.construct_dataframe()
    # g2.set_interactions(distributions=[], seed=42)

    # --------------------------------------------------

    c1: MultiMeasurementCombiner = MultiMeasurementCombiner()
    c1.add_dataframe(family="temperature", df=g1.df)
    c1.add_dataframe(family="humidity", df=g2.df)
    c1.combine()
    # c1.resample(rule="7min")
    c1.set_loss(
        time_between_errors_distribution=ExponentialDistribution(
            seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME,
            scale=t_min_to_sec(90.0),
            size=None,
        ),
        time_between_errors_distribution_seed=42,
        error_duration_distribution=ExponentialDistribution(
            seed_name=MultiMeasurementCombiner.LOSS_SEED_NAME,
            scale=t_min_to_sec(30.0),
            size=None,
        ),
        error_duration_distribution_seed=42,
    )
    c1.set_loss_metrics()

    # Visualization
    # --------------------------------------------------

    df: pd.DataFrame = c1.df
    df.attrs["sensor_id"] = "sensor-1"  # TODO Temporary
    plot_synthetic_time_series(
        df=df,
        column1="temperature",
        display_loss=True,
        show=True,
        path_to_dir=None,
        dpi=300,
    )
    plot_synthetic_time_series(
        df=df,
        column1="humidity",
        display_loss=True,
        show=True,
        path_to_dir=None,
        dpi=300,
    )
    plot_synthetic_time_series_loss_metrics(df=df, show=True, path_to_dir=None, dpi=300)


def setup_example() -> None:
    base_directory: str = (
        Path(__file__).resolve().parent.parent.parent.parent.joinpath("iotvm-local-data", "simulations").__str__()
    )

    datasets_directory: str = Path(__file__).resolve().parent.parent.parent.joinpath("datasets").__str__()
    path_to_dataset: str = os.path.join(datasets_directory, "dataset-1-slice-9-13.csv")
    sample1_df: pd.DataFrame = pd.read_csv(path_to_dataset, delimiter="\t")
    sample: np.ndarray = sample1_df["value"].values

    timezone: Optional[str] = None  # "Europe/Athens"

    # The first timestamp of the dataset we use.
    # 2021-01-17 09:00:00
    start: pd.Timestamp = pd.Timestamp(
        year=2021,
        month=1,
        day=17,
        hour=9,
        minute=0,
        second=0,
        microsecond=0,
        tz=timezone,
        unit="sec",  # sec for simplicity, ns for precision
    )
    start_from_dataset: pd.Timestamp = pd.Timestamp(sample1_df["timestamp"].iloc[0])
    assert start == start_from_dataset

    presets: Presets = Presets()
    presets.sample = sample
    presets.start = start
    presets.timezone = timezone
    presets.prepare()

    simulation: Simulation = Simulation(
        name="simulation-1",
        sensors=presets.PAPER_SENSORS,
        variations=[
            presets.variation("variation-1", 1, 2, 3, 4, 5, 6),
            presets.variation("variation-2", 1, 2, 6, 4, 5, 3),
            presets.variation("variation-3", 1, 4, 2, 3, 5, 6),
            presets.variation("variation-4", 1, 2, 5, 4, 3, 2),
            presets.variation("variation-5", 4, 5, 1, 2, 6, 4),
            presets.variation("variation-6", 4, 4, 2, 2, 4, 4),
            presets.variation("variation-7", 1, 2, 3, 1, 2, 3),
        ],
        average_ct_ps_space=AverageCalculationCompositeTransformationParametersSetsSpace(
            physical_quantity_list=["TEMPERATURE"],
            time_window_size_list=[5],  # TODO always pass string... PT5M
            number_of_contributing_sensors_list=[2, 4, 6],
            ignore_completeness_filtering_list=[False],
            fabrication_past_events_steps_behind_list=[0, 2, 4, 6],
            fabrication_forecasting_steps_ahead_list=[0, 2, 4, 6],
        ),
    )

    if simulation.check_directory_existence(base_directory=base_directory) is True:
        _logger.warning(f"Directory for simulation `{simulation.name}` already exists! Aborting...")
        return

    simulation.process(base_directory=base_directory)


def evaluation_example() -> None:
    base_directory: str = (
        Path(__file__).resolve().parent.parent.parent.parent.joinpath("iotvm-local-data", "simulations").__str__()
    )
    simulation_name: str = "simulation-1"

    perform_evaluation(directory=base_directory, simulation_name=simulation_name)
