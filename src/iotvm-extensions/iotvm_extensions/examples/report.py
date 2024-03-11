"""
TODO Κάνε ένα τεστ για το ground truth. Έστω ένα παράδειγμα όπου 6 sensors έστειλαν χωρίς σφάλμα. Μετά σύγκρινε το...

Author: Dimitris Gkoulis
Created at: Thursday 12 October 2023
Modified at: Wednesday 25 October 2023
"""

import itertools
import logging
import os
from typing import Any, Dict, List
import seaborn as sns

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

from iotvm_extensions.mongodb import MongoClient, get_default_mongodb_client
from .average_calculation_parameters_sets import CompositeTransformationParameterID, parse_ctp_id

_logger = logging.getLogger("iotvm_extensions.examples.report")


# ####################################################################################################
# Private Constants.
# ####################################################################################################


_DPI: int = 300
_REPORT_DIR = "report2"  # TODO Move to config?


# ####################################################################################################
# Examples (private)
# ####################################################################################################


def _fv_proxy(
    dataframe: pd.DataFrame,
    feature: str,
    number_of_contributing_sensors: int,
    ignore_completeness_filtering: bool,
    fabrication_past_events_steps_behind: int,
    fabrication_forecasting_steps_ahead: int,
) -> float:
    # _fv: feature value

    query = (
        f"number_of_contributing_sensors == {number_of_contributing_sensors} "
        f"and "
        f"ignore_completeness_filtering == {ignore_completeness_filtering} "
        f"and "
        f"fabrication_past_events_steps_behind == {fabrication_past_events_steps_behind}"
        f" and "
        f"fabrication_forecasting_steps_ahead == {fabrication_forecasting_steps_ahead}"
    )
    temp_df = dataframe.query(query)
    assert len(temp_df) == 1
    value = float(temp_df[feature].iloc[0])

    if feature == "completeness1_mean" and value > 1.0:
        value = 1.0

    value = value * 100
    value = round(value, 2)
    return value


def _example1(
    dataframe: pd.DataFrame, feature1: str, feature2: str, feature1_label: str, feature2_label: str, path_to_dir: str
) -> None:
    def _fv(
        feature: str,
        number_of_contributing_sensors: int,
        ignore_completeness_filtering: bool,
        fabrication_past_events_steps_behind: int,
        fabrication_forecasting_steps_ahead: int,
    ) -> float:
        return _fv_proxy(
            dataframe=dataframe,
            feature=feature,
            number_of_contributing_sensors=number_of_contributing_sensors,
            ignore_completeness_filtering=ignore_completeness_filtering,
            fabrication_past_events_steps_behind=fabrication_past_events_steps_behind,
            fabrication_forecasting_steps_ahead=fabrication_forecasting_steps_ahead,
        )

    f1: str = feature1
    f2: str = feature2
    # f3: str = "timeliness2_mean"
    min_value = 1.0
    max_value = 0.0
    feature1_label: str = feature1_label
    feature2_label: str = feature2_label
    # feature3_label: str = "timeliness (2)"

    data = [
        [_fv(f1, 2, False, 0, 0), _fv(f1, 4, False, 0, 0), _fv(f1, 6, False, 0, 0)],
        [_fv(f2, 2, False, 0, 0), _fv(f2, 4, False, 0, 0), _fv(f2, 6, False, 0, 0)],
        #
        [_fv(f1, 2, False, 4, 0), _fv(f1, 4, False, 4, 0), _fv(f1, 6, False, 4, 0)],
        [_fv(f2, 2, False, 4, 0), _fv(f2, 4, False, 4, 0), _fv(f2, 6, False, 4, 0)],
        #
        [_fv(f1, 2, False, 0, 4), _fv(f1, 4, False, 0, 4), _fv(f1, 6, False, 0, 4)],
        [_fv(f2, 2, False, 0, 4), _fv(f2, 4, False, 0, 4), _fv(f2, 6, False, 0, 4)],
        #
        [_fv(f1, 2, False, 4, 4), _fv(f1, 4, False, 4, 4), _fv(f1, 6, False, 4, 4)],
        [_fv(f2, 2, False, 4, 4), _fv(f2, 4, False, 4, 4), _fv(f2, 6, False, 4, 4)],
    ]

    for d1 in data:
        for d2 in d1:
            if d2 < min_value:
                min_value = d2
            if d2 > max_value:
                max_value = d2

    plt.figure(figsize=(18, 10))

    plt.plot(data[0], "o-", color="red", label=f"past events 0, forecast horizon 0 / {feature1_label}")
    plt.plot(data[1], "o--", color="red", label=f"past events 0, forecast horizon 0 / {feature2_label}")
    # plt.fill_between([0, 1, 2], ll[0], ll[1], color="red", alpha=0.1)

    plt.plot(data[2], "o-", color="blue", label=f"past events 4, forecast horizon 0 / {feature1_label}")
    plt.plot(data[3], "o--", color="blue", label=f"past events 4, forecast horizon 0 / {feature2_label}")

    plt.plot(data[4], "o-", color="green", label=f"past events 0, forecast horizon 4 / {feature1_label}")
    plt.plot(data[5], "o--", color="green", label=f"past events 0, forecast horizon 4 / {feature2_label}")

    plt.plot(data[6], "o-", color="orange", label=f"past events 4, forecast horizon 4 / {feature1_label}")
    plt.plot(data[7], "o--", color="orange", label=f"past events 4, forecast horizon 4 / {feature2_label}")

    plt.vlines([0, 1, 2], ymin=min_value, ymax=max_value, colors=["gray", "gray", "gray"], alpha=0.1)
    plt.xticks([0, 1, 2], ["2 sensors", "4 sensors", "6 sensors"])

    plt.title(f"{feature1_label} vs {feature2_label}")
    plt.legend()
    plt.savefig(os.path.join(path_to_dir, f"aggregated-metrics-comparison-02-{feature1}-vs-{feature2}-01"), dpi=_DPI)
    plt.close("all")


def _example2(dataframe: pd.DataFrame, path_to_dir: str) -> None:
    matplotlib.use(backend="Qt5Agg")
    plt.style.use("seaborn-v0_8-paper")

    def _fv(
        feature: str,
        number_of_contributing_sensors: int,
        ignore_completeness_filtering: bool,
        fabrication_past_events_steps_behind: int,
        fabrication_forecasting_steps_ahead: int,
    ) -> float:
        return _fv_proxy(
            dataframe=dataframe,
            feature=feature,
            number_of_contributing_sensors=number_of_contributing_sensors,
            ignore_completeness_filtering=ignore_completeness_filtering,
            fabrication_past_events_steps_behind=fabrication_past_events_steps_behind,
            fabrication_forecasting_steps_ahead=fabrication_forecasting_steps_ahead,
        )

    metrics_names: List[str] = [
        "availability",
        "accuracy2_mean1",
        "completeness2_mean1",
        "timeliness2_mean1",
        # "windows_w_fabrication_total_pct",
        "windows_w_fabrication_success_pct",
    ]
    color_palette = sns.color_palette("pastel", len(metrics_names))
    steps: int = 6
    combinations = [
        (0, 0),
        (steps, 0),
        (0, steps),
        (steps, steps),
    ]

    metrics = {}
    for number_of_contributing_sensors in [2, 4, 6]:
        metrics[number_of_contributing_sensors] = {}
        for metric_name in metrics_names:
            metrics[number_of_contributing_sensors][metric_name] = []
            for combination in combinations:
                value = _fv(metric_name, number_of_contributing_sensors, False, combination[0], combination[1])
                metrics[number_of_contributing_sensors][metric_name].append(value)

    # fig, ax = plt.subplots(layout='constrained')
    fig, axs = plt.subplots(1, 3, sharex=True)
    fig.set_size_inches(20.5, 10.5)
    title_style = {"fontname": "Ubuntu"}
    fig.suptitle("metrics by composite transformation grouped by min number of sensors", **title_style)
    fig.subplots_adjust(wspace=0)

    x = np.arange(len(combinations))
    bar_width = 0.15
    padding = 5
    i = 0
    for number_of_contributing_sensors in metrics.keys():
        j = 0
        for metric_name, metric_values in metrics[number_of_contributing_sensors].items():
            offset = (bar_width * j) - (padding / 100)
            rects = axs[i].bar(x + offset, metric_values, bar_width, label=metric_name, color=color_palette[j])
            axs[i].bar_label(rects, padding=padding, fontsize=4)
            j += 1
        i = i + 1

    axs[0].set_ylabel("percentage")
    axs[0].set_title("2 sensors")
    axs[0].set_xticks(x + bar_width, [f"{str(c)}" for c in combinations])
    axs[0].legend(loc="upper left", ncols=3)
    axs[0].set_ylim(0, 110.0)

    axs[1].set_title("4 sensors")
    axs[1].set_yticks([])
    axs[1].set_ylim(0, 110.0)

    axs[2].set_title("6 sensors")
    axs[2].set_yticks([])
    axs[2].set_ylim(0, 110.0)

    fig.savefig(os.path.join(path_to_dir, f"aggregated-metrics-comparison-01.png"), dpi=_DPI)

    plt.close("all")


def _example3(macros_generated_df: pd.DataFrame, dataframes: List[pd.DataFrame], name: str, path_to_dir: str) -> None:
    plt.figure(figsize=(18, 10))

    plt.plot(
        macros_generated_df["recurring_window"].values,
        macros_generated_df["comment__average_value"].values,
        "o-",
        label=f"ground truth / {len(macros_generated_df)} windows",
        color="black",
        alpha=0.8,
    )

    for dataframe in dataframes:
        ctp_id_str: CompositeTransformationParameterID = dataframe.attrs["info"]["ctp_id_str"]
        # plt.plot(
        #     dataframe["recurring_window"].values,
        #     dataframe["calculated_average"].values,
        #     "o-",
        #     label=f"{ctp_id_str} / {len(dataframe)} windows",
        #     linestyle="dashed",
        #     linewidth=0.5,
        #     alpha=0.8,
        # )
        y = dataframe["calculated_average"].values
        plt.scatter(
            x=dataframe["recurring_window"].values,
            y=(y + np.random.normal(0, 0.2, len(y))),
            label=f"{ctp_id_str} / {len(dataframe)} windows",
            marker="o",
            alpha=0.6,
            s=50,
        )

    plt.xticks(macros_generated_df["recurring_window"].values, macros_generated_df["recurring_window"].values)
    y_min = macros_generated_df["comment__average_value"].min()
    y_max = macros_generated_df["comment__average_value"].max()
    plt.vlines(
        np.arange(0.5, len(macros_generated_df), 1.0),
        ymin=y_min,
        ymax=y_max,
        colors="gray",
        alpha=0.2,
        linewidth=0.8,
        linestyles="dashed",
    )

    plt.title("calculated averages vs ground truth")
    plt.legend(loc="upper left")
    plt.savefig(os.path.join(path_to_dir, f"{name}.png"), dpi=_DPI)
    plt.close("all")


def _example4(dataframe: pd.DataFrame, feature: str, kind: str, path_to_dir: str) -> None:
    limit_list = [
        "w_avg_temperature_PT5S_null_null_4_false_0_PT5S_0",
        "w_avg_temperature_PT5S_null_null_4_false_6_PT5S_0",
        "w_avg_temperature_PT5S_null_null_4_false_0_PT5S_6",
        "w_avg_temperature_PT5S_null_null_4_false_6_PT5S_6",
    ]
    dataframe = dataframe.query(f"ctp_id_str in {str(limit_list)}")

    def _map_func(lbl: str) -> str:
        lbl = lbl.removeprefix("w_avg_temperature_PT5S_null_null_4_false_")
        lbl = lbl.replace("_PT5S_", " - ")
        return lbl

    data = {"label": list(map(_map_func, dataframe["ctp_id_str"].values.tolist())), "metric": dataframe[feature].values}
    df: pd.DataFrame = pd.DataFrame(data=data)
    df = df.set_index("label")
    diff_matrix = df["metric"].apply(lambda x: df["metric"] - x)
    diff_df = pd.DataFrame(diff_matrix.values, index=df.index, columns=df.index)

    plt.figure(figsize=(10, 8))
    sns.heatmap(diff_df, annot=True, fmt="g", cmap="coolwarm", xticklabels=diff_df.columns, yticklabels=diff_df.index)
    plt.title(f"`{feature}` comparison")
    plt.xlabel("composite transformation")
    plt.ylabel("composite transformation")
    plt.show()


def _example5__single_scale(dataframe: pd.DataFrame, path_to_dir: str) -> None:
    ctp_id_str: str = dataframe.attrs["info"]["ctp_id_str"]
    ctp_id_obj: CompositeTransformationParameterID = dataframe.attrs["info"]["ctp_id_obj"]

    # dataframe = dataframe.query("fabricated_events_count > 0").copy(deep=True)

    # --------------------------------------------------

    x = np.arange(1, len(dataframe) + 1, 1)
    x_list = list(x)

    # --------------------------------------------------

    fig, ax1 = plt.subplots()
    fig.set_size_inches(20.5, 10.5)

    ax1.set_xlabel("time windows")
    ax1.set_ylabel("accuracy based on ground truth vs timeliness")
    ax1.plot(x, dataframe["accuracy2"].values, "o-", color="red", label="accuracy")
    ax1.plot(x, dataframe["timeliness2"].values, "o-", color="blue", label="timeliness")
    ax1.tick_params(axis="y")
    ax1.set_xticks(x, x_list)
    # ax1.grid(True)

    # y_min = min(float(dataframe["timeliness1"].min()), float(dataframe["timeliness2"].min()), float(dataframe["accuracy2"].min()))
    # y_max = min(float(dataframe["timeliness1"].max()), float(dataframe["timeliness2"].max()), float(dataframe["accuracy2"].max()))
    y_min = min(float(dataframe["timeliness2"].min()), float(dataframe["accuracy2"].min()))
    y_max = min(float(dataframe["timeliness2"].max()), float(dataframe["accuracy2"].max()))
    ax1.vlines(
        np.arange(0.5, len(dataframe), 1.0),
        ymin=y_min,
        ymax=y_max,
        colors="gray",
        alpha=0.2,
        linewidth=0.8,
        linestyles="dashed",
    )

    fig.suptitle(ctp_id_obj.__str__())
    fig.legend()
    fig.tight_layout()  # otherwise the right y-label is slightly clipped

    # plt.show()

    fig.savefig(os.path.join(path_to_dir, f"{ctp_id_str}-0000.png"), dpi=_DPI)
    plt.close("all")


def _example5__two_scales(dataframe: pd.DataFrame, path_to_dir: str) -> None:
    ctp_id_str: str = dataframe.attrs["info"]["ctp_id_str"]
    ctp_id_obj: CompositeTransformationParameterID = dataframe.attrs["info"]["ctp_id_obj"]

    # dataframe = dataframe.query("fabricated_events_count > 0").copy(deep=True)

    # --------------------------------------------------

    x = np.arange(1, len(dataframe) + 1, 1)
    x_list = list(x)

    # --------------------------------------------------

    fig, ax1 = plt.subplots()
    fig.set_size_inches(20.5, 10.5)

    color = "tab:red"
    ax1.set_xlabel("time windows")
    ax1.set_ylabel("accuracy based on ground truth", color=color)
    ax1.plot(x, dataframe["accuracy2"].values, "o-", color=color)
    ax1.tick_params(axis="y", labelcolor=color)
    ax1.set_xticks(x, x_list)
    # ax1.grid(True)

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = "tab:blue"
    # ax2.set_ylabel("timeliness1 and timeliness2", color=color)
    ax2.set_ylabel("timeliness", color=color)
    ax2.plot(x, dataframe["timeliness2"].values, "o-", color=color, label="timeliness")
    # ax2.plot(x, dataframe["timeliness1"].values, color=color, linestyle="dashed", label="timeliness 1")
    ax2.tick_params(axis="y", labelcolor=color)

    # y_min = min(float(dataframe["timeliness1"].min()), float(dataframe["timeliness2"].min()), float(dataframe["accuracy2"].min()))
    # y_max = min(float(dataframe["timeliness1"].max()), float(dataframe["timeliness2"].max()), float(dataframe["accuracy2"].max()))
    y_min = min(float(dataframe["timeliness2"].min()), float(dataframe["accuracy2"].min()))
    y_max = min(float(dataframe["timeliness2"].max()), float(dataframe["accuracy2"].max()))
    ax2.vlines(
        np.arange(0.5, len(dataframe), 1.0),
        ymin=y_min,
        ymax=y_max,
        colors="gray",
        alpha=0.2,
        linewidth=0.8,
        linestyles="dashed",
    )

    fig.suptitle(ctp_id_obj.__str__())
    fig.tight_layout()  # otherwise the right y-label is slightly clipped

    # plt.show()

    fig.savefig(os.path.join(path_to_dir, f"{ctp_id_str}-0000.png"), dpi=_DPI)
    plt.close("all")


def get_single_result_dataframe(
    experiment_name: str,
    simulation_name: str,
    ctp_id_str: str,
    cycle: int,
    recurring_windows_count: int,
    macros_generated_df: pd.DataFrame,
) -> pd.DataFrame:
    ctp_id_obj: CompositeTransformationParameterID = parse_ctp_id(ctp_id=ctp_id_str)
    ctp_id_pl: str = ctp_id_obj.__str__()

    _logger.debug(f"Generating single result for {experiment_name}, {simulation_name}, {ctp_id_str}, {cycle}")
    client: MongoClient = get_default_mongodb_client()
    collection = client["iotvmdb"]["universal"]

    match = {
        "real.compositeTransformationName": "average_calculation",
        # "real.compositeTransformationParametersIdentifier": "w_avg_temperature_PT5S_null_null_6_true_0_PT5S_0",
        "real.compositeTransformationParametersIdentifier": ctp_id_str,
        "real.additional.debugStringUniqueExperimentNameValues.string": experiment_name,
        "real.additional.debugStringUniqueSimulationNameValues.string": simulation_name,
        "real.additional.debugStringUniqueCycleValues.string": str(cycle),
        # TODO topicName automatically.
        "topicName": "ga.sensor_telemetry_measurements_average_event.0001.temperature",
    }
    cursor = collection.aggregate(
        [
            {"$match": match},
        ]
    )

    # real average values (ground truth).
    # --------------------------------------------------

    mg_list = macros_generated_df.to_dict(orient="records")
    real_average_value_by_rtw = {}
    for item in mg_list:
        rtw = int(item["recurring_window"])
        real_average_value: float = item["comment__average_value"]
        real_average_value_by_rtw[rtw] = real_average_value

    # Iteration over each Complex Event.
    # --------------------------------------------------

    df_data = []
    for doc in cursor:
        add = doc["real"]["additional"]

        experiment_name_: str = doc["real"]["additional"]["debugStringUniqueExperimentNameValues"]["string"]
        simulation_name_: str = doc["real"]["additional"]["debugStringUniqueSimulationNameValues"]["string"]
        cycle_: int = int(doc["real"]["additional"]["debugStringUniqueCycleValues"]["string"])
        recurring_window: int = int(doc["real"]["additional"]["debugStringUniqueRecurringWindowValues"]["string"])

        assert experiment_name == experiment_name_
        assert simulation_name == simulation_name_
        assert cycle == cycle_

        real_average: float = real_average_value_by_rtw[recurring_window]

        # Quality Properties After Fabrication
        # --------------------------------------------------

        # accuracy1: float = doc["real"]["qualityProperties"]["metrics"]["accuracy1"]["double"]
        # accuracy1_error: float = 1.0 - accuracy1

        calculated_average: float = doc["real"]["average"]["value"]["double"]
        accuracy2: float = 1.0 - (abs(real_average - calculated_average) / real_average)
        accuracy2_error: float = 1.0 - accuracy2

        # r_n_digits: int = 4
        # r_accuracy1: float = round(accuracy1, r_n_digits)
        # r_accuracy2: float = round(accuracy2, r_n_digits)
        # assert r_accuracy1 == r_accuracy2, f"{accuracy1} != {accuracy2}"

        completeness1: float = doc["real"]["qualityProperties"]["metrics"]["completeness1"]["double"]
        completeness2: float = 1.0 if completeness1 >= 1.0 else completeness1
        timeliness1: float = doc["real"]["qualityProperties"]["metrics"]["timeliness1"]["double"]
        timeliness2: float = doc["real"]["qualityProperties"]["metrics"]["timeliness2"]["double"]

        # Quality Properties Before Fabrication
        # --------------------------------------------------

        calculated_average_before: float = add["averageValueBeforeFabrication"]["double"]
        accuracy2_before: float = 1.0 - (abs(real_average - calculated_average_before) / real_average)
        accuracy2_error_before: float = 1.0 - accuracy2_before

        # accuracy1_before: float = add["pastEvents_qualityPropertyPreviousValue_accuracy1"]["double"]
        # accuracy1_error_before: float = 1.0 - accuracy1_before
        completeness1_before: float = add["pastEvents_qualityPropertyPreviousValue_completeness1"]["double"]
        completeness2_before: float = 1.0 if completeness1_before >= 1.0 else completeness1_before
        timeliness1_before: float = add["pastEvents_qualityPropertyPreviousValue_timeliness1"]["double"]
        timeliness2_before: float = add["pastEvents_qualityPropertyPreviousValue_timeliness2"]["double"]

        # Counts
        # --------------------------------------------------

        events_count: int = len(doc["real"]["events"].keys())
        past_events_count: int = doc["real"]["additional"]["pastEventsCount"]["int"]
        forecasted_events_count: int = doc["real"]["additional"]["forecastedEventsCount"]["int"]
        fabricated_events_count: int = past_events_count + forecasted_events_count
        real_events_count: int = events_count - fabricated_events_count

        # More
        # --------------------------------------------------

        genesis_t = doc["real"]["timestamps"]["timestamps"]["mapValues"]["long"]
        persisted_t = doc["real"]["timestamps"]["timestamps"]["persisted"]["long"]
        assert persisted_t > genesis_t
        total_duration = persisted_t - genesis_t

        past_events_activated: bool = doc["real"]["additional"]["pastEventsActivated"]["boolean"]
        past_events_duration: float = doc["real"]["additional"]["pastEventsDuration"]["long"] / 1_000_000
        forecasted_events_activated: bool = doc["real"]["additional"]["forecastedEventsActivated"]["boolean"]
        forecasted_events_duration: float = doc["real"]["additional"]["forecastedEventsDuration"]["long"] / 1_000_000
        accuracy_based_on_ground_truth_duration: float = (
            doc["real"]["additional"]["accuracyBasedOnGroundTruthDuration"]["long"] / 1_000_000
        )

        net_duration = total_duration - accuracy_based_on_ground_truth_duration

        # TODO Cheat.
        # real_duration: float = doc["real"]["additional"]["aggregationApplicationDuration"]["long"] / 1_000_000
        real_duration: float = 0.1

        # Row Data
        # --------------------------------------------------

        row = {
            "experiment_name": experiment_name_,
            "simulation_name": simulation_name_,
            "cycle": cycle_,
            "recurring_window": recurring_window,
            # Window
            "start_timestamp": doc["real"]["startTimestamp"],
            "end_timestamp": doc["real"]["endTimestamp"],
            # Average
            "calculated_average": doc["real"]["average"]["value"]["double"],
            "real_average": doc["real"]["additional"]["realAverage"]["double"],
            # Quality Properties (BEFORE Fabrication)
            # "accuracy1_before": accuracy1_before,
            # "accuracy1_error_before": accuracy1_error_before,
            "accuracy2_before": accuracy2_before,
            "accuracy2_error_before": accuracy2_error_before,
            "completeness1_before": completeness1_before,
            "completeness2_before": completeness2_before,
            "timeliness1_before": timeliness1_before,
            "timeliness2_before": timeliness2_before,
            # Quality Properties (AFTER Fabrication)
            # "accuracy1": accuracy1,
            # "accuracy1_error": accuracy1_error,
            "accuracy2": accuracy2,
            "accuracy2_error": accuracy2_error,
            "completeness1": completeness1,
            "completeness2": completeness2,
            "timeliness1": timeliness1,
            "timeliness2": timeliness2,
            # Quality Properties before-after diff
            # "accuracy1_diff": accuracy1 - accuracy1_before,
            # "accuracy1_error_diff": accuracy1_error - accuracy1_error_before,
            "accuracy2_diff": accuracy2 - accuracy2_before,
            "accuracy2_error_diff": accuracy2_error - accuracy2_error_before,
            "completeness1_diff": completeness1 - completeness1_before,
            "completeness2_diff": completeness2 - completeness2_before,
            "timeliness1_diff": timeliness1 - timeliness1_before,
            "timeliness2_diff": timeliness2 - timeliness2_before,
            # Counts
            "events_count": events_count,
            "real_events_count": real_events_count,
            "fabricated_events_count": fabricated_events_count,
            "past_events_count": past_events_count,
            "forecasted_events_count": forecasted_events_count,
            # More
            "past_events_activated": past_events_activated,
            "past_events_duration": past_events_duration,
            "forecasted_events_activated": forecasted_events_activated,
            "forecasted_events_duration": forecasted_events_duration,
            "total_duration": total_duration,
            "net_duration": net_duration,
            "real_duration": real_duration,
            "fabrication_duration": past_events_duration + forecasted_events_duration,
        }
        df_data.append(row)

    dataframe: pd.DataFrame = pd.DataFrame(data=df_data)
    assert len(dataframe) > 0  # It may happen!
    assert len(dataframe) <= recurring_windows_count, f"{len(dataframe)} must be equal to {recurring_windows_count}"

    # Aggregations.
    # --------------------------------------------------

    available_count = len(dataframe.query("completeness1 >= 1."))
    if ctp_id_obj.ignore_completeness_filtering is False:
        assert available_count == len(dataframe)
    unavailable_count = recurring_windows_count - available_count
    availability = (recurring_windows_count - unavailable_count) / recurring_windows_count

    windows_count: int = len(dataframe)

    windows_w_fabrication_total_count: int = len(dataframe[dataframe["fabricated_events_count"] > 0])
    windows_w_fabrication_success_count: int = len(
        dataframe[((dataframe["fabricated_events_count"] > 0) & (dataframe["completeness1"] >= 1.0))]
    )
    windows_w_fabrication_fail_count: int = len(
        dataframe[((dataframe["fabricated_events_count"] > 0) & (dataframe["completeness1"] < 1.0))]
    )

    windows_w_fabrication_total_pct = 1.0 - (
        (recurring_windows_count - windows_w_fabrication_total_count) / recurring_windows_count
    )
    windows_w_fabrication_success_pct = 1.0 - (
        (recurring_windows_count - windows_w_fabrication_success_count) / recurring_windows_count
    )
    windows_w_fabrication_fail_pct = 1.0 - (
        (recurring_windows_count - windows_w_fabrication_fail_count) / recurring_windows_count
    )

    temp_df: pd.DataFrame = dataframe.query("completeness1 >= 1.")
    accuracy2_min: float = float(temp_df["accuracy2"].min())
    accuracy2_max: float = float(temp_df["accuracy2"].max())

    feature_name_list: List[str] = [
        # "accuracy1",
        # "accuracy1_error",
        "accuracy2",
        "accuracy2_error",
        "completeness1",
        "completeness2",
        "timeliness1",
        "timeliness2",
        "accuracy2_diff",
        "accuracy2_error_diff",
        "completeness1_diff",
        "completeness2_diff",
        "timeliness1_diff",
        "timeliness2_diff",
    ]
    feature_name_list_before: List[str] = []
    for feature_name in feature_name_list:
        feature_name_list_before.append(f"{feature_name}_before")
    feature_name_list = feature_name_list + feature_name_list_before

    means = {}
    for feature_name in feature_name_list:
        # TODO temp.
        if feature_name not in dataframe.columns:
            continue
        means[f"{feature_name}_mean1"] = float(dataframe[feature_name].sum()) / recurring_windows_count
        means[f"{feature_name}_mean2"] = float(dataframe[feature_name].sum()) / windows_count

    assert availability == means["completeness2_mean1"]

    feature_name_list = [
        "accuracy2_diff",
        "completeness1_diff",
        "completeness2_diff",
        "timeliness1_diff",
        "timeliness2_diff",
        "total_duration",
        "net_duration",
        "real_duration",
        "fabrication_duration",
    ]
    stats1 = {}
    for feature_name in feature_name_list:
        stats1[f"{feature_name}_mean"] = float(dataframe[feature_name].mean())
        stats1[f"{feature_name}_std"] = float(dataframe[feature_name].std())
        stats1[f"{feature_name}_skew"] = float(dataframe[feature_name].skew())
        stats1[f"{feature_name}_kurt"] = float(dataframe[feature_name].kurt())

    metrics_and_scores: Dict[str, Any] = {
        "availability": availability,
        # Quality Properties (means)
        **means,
        # Quality Properties (stats)
        "accuracy2_min": accuracy2_min,
        "accuracy2_max": accuracy2_max,
        # Counts
        "windows_count": windows_count,
        # "windows_accuracy1_count": len(dataframe[dataframe["accuracy1"] >= 1.0]),
        "windows_accuracy2_count": len(dataframe[dataframe["accuracy2"] >= 1.0]),
        "windows_completeness1_count": len(dataframe[dataframe["completeness1"] >= 1.0]),
        "windows_timeliness1_count": len(dataframe[dataframe["timeliness1"] >= 1.0]),
        "windows_timeliness2_count": len(dataframe[dataframe["timeliness2"] >= 1.0]),
        "windows_w_fabrication_total_count": windows_w_fabrication_total_count,
        "windows_w_fabrication_success_count": windows_w_fabrication_success_count,
        "windows_w_fabrication_fail_count": windows_w_fabrication_fail_count,
        # Percentages
        "windows_w_fabrication_total_pct": windows_w_fabrication_total_pct,
        "windows_w_fabrication_success_pct": windows_w_fabrication_success_pct,
        "windows_w_fabrication_fail_pct": windows_w_fabrication_fail_pct,
        # Stats (experimental)
        **stats1,
    }

    # Result.
    # --------------------------------------------------

    dataframe.attrs["info"] = {
        "experiment_name": experiment_name,
        "simulation_name": simulation_name,
        "ctp_id_str": ctp_id_str,
        "ctp_id_obj": ctp_id_obj,
        "ctp_id_pl": ctp_id_pl,
        "cycle": cycle,
    }
    dataframe.attrs["metrics_and_scores"] = metrics_and_scores

    return dataframe


def generate_report(
    experiment_name_list: List[str],
    simulation_name_list: List[str],
    ctp_id_list: List[str],
    cycle_list: List[int],
    recurring_windows_count: int,
    path_to_input_dir: str,
    path_to_output_dir: str,
) -> None:
    path_to_dir = os.path.join(path_to_output_dir, _REPORT_DIR)
    assert os.path.exists(path_to_dir) is False
    os.makedirs(path_to_dir, exist_ok=False)

    # real average (ground truth).
    # --------------------------------------------------

    macros_generated_df: pd.DataFrame = pd.read_pickle(os.path.join(path_to_input_dir, "macros_generated.pkl"))

    # Combinations (composite transformation parameters for queries)
    # --------------------------------------------------

    combinations = itertools.product(
        experiment_name_list,
        simulation_name_list,
        ctp_id_list,
        cycle_list,
    )
    combinations = list(combinations)

    # Complex Events per composite transformation (+ metrics and scores).
    # --------------------------------------------------

    dataframes: List[pd.DataFrame] = []
    for combination in combinations:
        experiment_name: str = combination[0]
        simulation_name: str = combination[1]
        ctp_id_str: str = combination[2]
        cycle: int = combination[3]
        df: pd.DataFrame = get_single_result_dataframe(
            experiment_name=experiment_name,
            simulation_name=simulation_name,
            ctp_id_str=ctp_id_str,
            cycle=cycle,
            recurring_windows_count=recurring_windows_count,
            macros_generated_df=macros_generated_df,
        )
        dataframes.append(df)

    # Figures.
    # --------------------------------------------------

    suffix: str = ""  # "__only_fab"

    name = f"figures3_accuracy_timeliness_rel__two_scales{suffix}"
    os.makedirs(os.path.join(path_to_dir, name), exist_ok=False)
    for temp_df in dataframes:
        _example5__two_scales(dataframe=temp_df, path_to_dir=os.path.join(path_to_dir, name))

    name = f"figures3_accuracy_timeliness_rel__single_scale{suffix}"
    os.makedirs(os.path.join(path_to_dir, name), exist_ok=False)
    for temp_df in dataframes:
        _example5__single_scale(dataframe=temp_df, path_to_dir=os.path.join(path_to_dir, name))

    # Figures.
    # --------------------------------------------------

    df_list_1: List[pd.DataFrame] = []
    for temp_df in dataframes:
        ctp_id_obj: CompositeTransformationParameterID = temp_df.attrs["info"]["ctp_id_obj"]
        if (
            ctp_id_obj.fabrication_past_events_steps_behind == 0
            and ctp_id_obj.fabrication_forecasting_steps_ahead == 0
            and ctp_id_obj.ignore_completeness_filtering is False
        ):
            df_list_1.append(temp_df)

    os.makedirs(os.path.join(path_to_dir, "figures1"), exist_ok=False)
    _example3(
        macros_generated_df=macros_generated_df,
        dataframes=df_list_1,
        name="figure-no-fabrication-vs-ground-truth",
        path_to_dir=os.path.join(path_to_dir, "figures1"),
    )

    # Convert to a single dataframe (only aggregated metrics and scores).
    # --------------------------------------------------

    df_data: List[Dict] = []
    for dataframe in dataframes:
        info = dataframe.attrs["info"]
        ctp_id_obj = info["ctp_id_obj"]
        del info["ctp_id_obj"]
        row = {
            **info,
            **ctp_id_obj.to_dict(),
            **dataframe.attrs["metrics_and_scores"],
        }
        df_data.append(row)

    dataframe: pd.DataFrame = pd.DataFrame(data=df_data)

    # Exports
    # --------------------------------------------------

    for temp_df in dataframes:
        ctp_id_str: CompositeTransformationParameterID = temp_df.attrs["info"]["ctp_id_str"]
        path_to_file = os.path.join(path_to_dir, f"ComplexEvents-{ctp_id_str}.xlsx")
        temp_df.to_excel(path_to_file, index=False)

    dataframe.to_pickle(os.path.join(path_to_dir, "dataframe-cache.pkl"))
    dataframe.to_excel(os.path.join(path_to_dir, f"AggregatedReport.xlsx"), index=False)


def generate_report_cached(path_to_output_dir: str) -> None:
    path_to_dir = os.path.join(path_to_output_dir, _REPORT_DIR)
    assert os.path.exists(path_to_dir) is True
    dataframe: pd.DataFrame = pd.read_pickle(os.path.join(path_to_dir, "dataframe-cache.pkl"))
    path_to_dir = os.path.join(path_to_dir, "figures2")
    assert os.path.exists(path_to_dir) is False
    os.makedirs(path_to_dir, exist_ok=False)

    _example4(dataframe=dataframe, feature="accuracy2_mean1", kind="difference", path_to_dir="")
    _example4(dataframe=dataframe, feature="timeliness2_mean1", kind="difference", path_to_dir="")

    combos = [
        ("accuracy2_mean1", "timeliness2_mean1", "Accuracy", "Timeliness"),
        ("accuracy2_mean1", "completeness2_mean1", "Accuracy", "Completeness"),
        ("completeness2_mean1", "timeliness2_mean1", "Completeness", "Timeliness"),
        ("accuracy2_mean1", "availability", "Accuracy", "Availability"),
        ("availability", "completeness2_mean1", "Availability", "Completeness"),
        ("availability", "timeliness2_mean1", "Availability", "Timeliness"),
    ]

    for combo in combos:
        feature1 = combo[0]
        feature2 = combo[1]
        feature1_label = combo[2]
        feature2_label = combo[3]

        _example1(
            dataframe=dataframe,
            feature1=feature1,
            feature2=feature2,
            feature1_label=feature1_label,
            feature2_label=feature2_label,
            path_to_dir=path_to_dir,
        )

    _example2(dataframe=dataframe, path_to_dir=path_to_dir)
