"""
Author: Dimitris Gkoulis
Created at: Wednesday 10 April 2024
"""

import os
from pathlib import Path
from typing import Dict, List, Optional
from matplotlib.ticker import (MultipleLocator, AutoMinorLocator)

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def heuristic_modeling_helpers_example() -> None:
    base_directory: str = (
        Path(__file__).resolve().parent.parent.parent.parent.joinpath("iotvm-local-data", "simulations").__str__()
    )
    simulation_name: str = "simulation-1"
    path_to_parquet: str = os.path.join(base_directory, simulation_name, "complex-event-eval.parquet")

    # --------------------------------------------------

    df: pd.DataFrame = pd.read_parquet(path_to_parquet)
    df = df.query("is_baseline == 1 and min_num_of_sensors == 6 and naive_max_distance_param == 0 and expon_max_distance_param == 0")

    # Plot.
    # --------------------------------------------------

    color_palette = sns.color_palette("deep", 6)

    fig, ax1 = plt.subplots()
    fig.set_size_inches(20.5, 10.5)

    for i in range(1, 7):
        column: str = f"sensor-{i}-real"
        ax1.plot(
            df["time_window_index"],
            df[column],
            "o-",
            label=f"sensor-{i}",
            color=color_palette[i - 1],
            # linestyle="solid",
            linewidth=1.0,
        )

    # TODO auto.
    y_min: float = 8.5
    y_max: float = 33.0
    # ax1.vlines(
    #     np.arange(0.5, len(df), 1.0),
    #     ymin=y_min,
    #     ymax=y_max,
    #     colors="gray",
    #     alpha=0.2,
    #     linewidth=0.8,
    #     linestyles="dashed",
    # )
    ax1.set_xticks(df["time_window_index"])

    # TODO auto.
    x_min: float = 0 - 1
    x_max: float = 50 + 1
    # ax1.hlines(
    #     np.arange(y_min, y_max, 0.5),
    #     xmin=x_min,
    #     xmax=x_max,
    #     colors="gray",
    #     alpha=0.2,
    #     linewidth=0.8,
    #     linestyles="dashed",
    # )
    ax1.yaxis.set_major_locator(MultipleLocator(0.5))

    ax1.set_xlabel("Time Window Index")
    ax1.set_ylabel("Average Air Temperature")
    ax1.grid(True)
    ax1.legend(loc="upper left", ncols=3)

    plt.show()
    plt.close("all")
