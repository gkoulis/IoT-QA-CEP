"""
Author: Dimitris Gkoulis
Created at: Wednesday 04 October 2023
"""

import os
from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_synthetic_time_series(
    df: pd.DataFrame,
    column1: str,
    display_loss: bool = True,
    show: bool = True,
    path_to_dir: Optional[str] = None,
    dpi: int = 300,
) -> None:
    sensor_id: str = df.attrs["sensor_id"]

    df = df[df[column1].notna()]

    plt.figure(figsize=(18, 10))
    plt.plot(
        df.index,
        df[column1],
        label=f"{column1}",
        color="blue",
        linestyle="solid",
        linewidth=1.0,
    )

    if display_loss is True:
        errors_df = df.query("loss == 1")
        plt.scatter(errors_df.index, errors_df[column1], label="Loss", marker="x", color="red")
        max_value = df[column1].max()
        min_value = df[column1].min()
        plt.fill_between(
            df.index,
            min_value - (min_value * 0.01),
            max_value + (max_value * 0.01),
            where=(df["loss"] > 0),
            label="Loss",
            color="pink",
            alpha=0.5,
        )

    plt.legend(loc="best", frameon=False)
    plt.title(f"Sensor '{sensor_id}' Synthetic Time-Series")
    plt.xlabel("Timestamp")
    plt.ylabel("Value")
    plt.grid(True)
    if path_to_dir is not None:
        plt.savefig(
            os.path.join(path_to_dir, f"{sensor_id}-synthetic-time-series-{display_loss}.png"),
            dpi=dpi,
        )
    if show is True:
        plt.show()
    plt.close("all")


def plot_synthetic_time_series_loss_metrics(
    df: pd.DataFrame,
    show: bool = True,
    path_to_dir: Optional[str] = None,
    dpi: int = 300,
) -> None:
    sensor_id: str = df.attrs["sensor_id"]
    metrics: Dict = df.attrs["loss_metrics"]
    tick_count: int = metrics["tick_count"]
    tick_with_loss_count: int = metrics["tick_with_loss_count"]
    tick_with_loss_pct: int = metrics["tick_with_loss_pct"]
    error_rate: float = metrics["error_rate"]
    t = metrics["t"]
    reliability = metrics["reliability"]

    plt.figure(figsize=(18, 10))
    plt.plot(t, reliability)
    plt.xlabel("(Relative) Time (t)")
    plt.ylabel("Reliability R(t)")
    plt.title(
        f"Sensor '{sensor_id}' Reliability Function over Time"
        f"\n"
        f"(The error rate of {round(error_rate * 100, 2)}% "
        f"results in the loss of {tick_with_loss_count} out of {tick_count} values"
        f", i.e., {round(tick_with_loss_pct * 100, 2)}%)"
    )
    plt.grid(True)
    if path_to_dir is not None:
        plt.savefig(
            os.path.join(path_to_dir, f"{sensor_id}-reliability-function-over-time.png"),
            dpi=dpi,
        )
    if show is True:
        plt.show()
    plt.close("all")


def plot_multiple_synthetic_time_series(
    df_list: List[pd.DataFrame],
    show: bool = True,
    display_loss: bool = True,
    path_to_dir: Optional[str] = None,
    dpi: int = 300,
) -> None:
    plt.figure(figsize=(18, 10))

    color_palette = sns.color_palette("deep", len(df_list))

    for idx, df in enumerate(df_list):
        sensor_id: str = df.attrs["sensor_id"]

        plt.plot(
            df["x"],
            df["y2"],
            label=sensor_id,
            color=color_palette[idx],
        )

        if display_loss is True:
            loss_df: pd.DataFrame = df.query("loss == 1")
            plt.scatter(
                loss_df["x"],
                loss_df["y1"],
                label=f"{sensor_id} loss",
                marker="x",
                color="red",
            )

    plt.legend(loc="best", frameon=False)
    plt.title(f"Synthetic Time-Series ({len(df_list)} sensors)")
    plt.xlabel("Timestamp")
    plt.ylabel("Value")
    plt.grid(True)
    if path_to_dir is not None:
        plt.savefig(
            os.path.join(path_to_dir, f"all-sensors-generated-synthetic-data-{display_loss}.png"),
            dpi=dpi,
        )
    if show is True:
        plt.show()
    plt.close("all")
