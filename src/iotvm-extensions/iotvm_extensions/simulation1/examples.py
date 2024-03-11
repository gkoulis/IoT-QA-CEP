"""
Author: Dimitris Gkoulis
Created at: Sunday 10 March 2024
"""

from typing import Optional
import pandas as pd
import numpy as np

from ._base import (
    t_min_to_sec,
    DistributionType,
    ConstantDistribution,
    NormalDistribution,
    ExponentialDistribution,
    MeasurementBasicGenerator,
    MultiMeasurementCombiner,
)
from ._visualization import plot_synthetic_time_series, plot_synthetic_time_series_loss_metrics


def example1() -> None:
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
