import logging

import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import pairwise_distances

_logger = logging.getLogger("iotvm_extensions.examples.report")


# ####################################################################################################
# Report.
# ####################################################################################################


def generate_report() -> None:
    """
    TODO For each one.
    TODO baseline - accuracy2 mean (and add to excel)
    :return:
    """

    baseline = 0.984087794

    ctp_id_list = [
        "w_avg_temperature_PT5S_null_null_4_false_0_PT5S_0",
        "w_avg_temperature_PT5S_null_null_4_false_0_PT5S_2",
        "w_avg_temperature_PT5S_null_null_4_false_0_PT5S_4",
        "w_avg_temperature_PT5S_null_null_4_false_0_PT5S_6",
        "w_avg_temperature_PT5S_null_null_4_false_2_PT5S_0",
        "w_avg_temperature_PT5S_null_null_4_false_2_PT5S_2",
        "w_avg_temperature_PT5S_null_null_4_false_2_PT5S_4",
        "w_avg_temperature_PT5S_null_null_4_false_2_PT5S_6",
        "w_avg_temperature_PT5S_null_null_4_false_4_PT5S_0",
        "w_avg_temperature_PT5S_null_null_4_false_4_PT5S_2",
        "w_avg_temperature_PT5S_null_null_4_false_4_PT5S_4",
        "w_avg_temperature_PT5S_null_null_4_false_4_PT5S_6",
        "w_avg_temperature_PT5S_null_null_4_false_6_PT5S_0",
        "w_avg_temperature_PT5S_null_null_4_false_6_PT5S_2",
        "w_avg_temperature_PT5S_null_null_4_false_6_PT5S_4",
        "w_avg_temperature_PT5S_null_null_4_false_6_PT5S_6",
    ]
    ctp_id_shortened_list = []
    for ctp_id in ctp_id_list:
        temp = ctp_id
        temp = temp.removeprefix("w_avg_temperature_PT5S_null_null_4_false_")
        temp = temp.replace("_PT5S_", "/")
        ctp_id_shortened_list.append(temp)

    accuracy2_mean2 = [
        0.984087794,
        0.9753425562,
        0.9766337434,
        0.9776996573,
        0.9803180937,
        0.9759154363,
        0.976726057,
        0.9769740687,
        0.9819033227,
        0.977578238,
        0.9777751188,
        0.9777751188,
        0.9830651537,
        0.9788462034,
        0.9788462034,
        0.9788462034,
    ]

    accuracy2_mean1 = [
        0.3542716058,
        0.6632329382,
        0.8008396696,
        0.9190376779,
        0.5097654087,
        0.8002506578,
        0.9181224936,
        0.9769740687,
        0.6284181265,
        0.9189235437,
        0.9777751188,
        0.9777751188,
        0.7667908199,
        0.9788462034,
        0.9788462034,
        0.9788462034,
    ]

    for accc in accuracy2_mean2:
        # print(baseline - accc)
        pass

    accuracy2_mean2_diff = []
    for acc in accuracy2_mean2:
        accuracy2_mean2_diff.append(baseline - acc)

    timeliness1_mean2 = [
        1.0,
        0.8382352941,
        0.7865853659,
        0.7659574468,
        0.9134615385,
        0.7865853659,
        0.7659574468,
        0.765,
        0.8515625,
        0.7659574468,
        0.765,
        0.765,
        0.8012820513,
        0.765,
        0.765,
        0.765,
    ]

    timeliness2_mean2 = [
        1.0,
        0.8382352941,
        0.7865853659,
        0.7659574468,
        0.9375,
        0.8109756098,
        0.789893617,
        0.7875,
        0.919921875,
        0.8311170213,
        0.82625,
        0.82625,
        0.9006410256,
        0.8566666667,
        0.8566666667,
        0.8566666667,
    ]

    acc_arr = np.array(accuracy2_mean2, dtype=np.float32).reshape(1, -1)
    tim2_arr = np.array(timeliness2_mean2, dtype=np.float32).reshape(1, -1)

    euclidean = pairwise_distances(acc_arr, tim2_arr, metric="euclidean")
    correlation = pairwise_distances(acc_arr, tim2_arr, metric="correlation")
    print(euclidean, ", ", correlation)

    # --------------------------------------------------

    x = np.arange(1, len(ctp_id_list) + 1, 1)

    # --------------------------------------------------

    fig, ax1 = plt.subplots()

    color = "tab:red"
    ax1.set_xlabel("composite transformations")
    ax1.set_ylabel("mean accuracy over available", color=color)
    ax1.plot(x, accuracy2_mean2, color=color)
    ax1.tick_params(axis="y", labelcolor=color)
    ax1.set_xticks(x, ctp_id_shortened_list)

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = "tab:blue"
    ax2.set_ylabel("timeliness means over available", color=color)
    ax2.plot(x, timeliness2_mean2, color=color, label="timeliness 2")
    ax2.plot(x, timeliness1_mean2, color=color, linestyle="dashed", label="timeliness 1")
    ax2.tick_params(axis="y", labelcolor=color)

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()

    # Diff
    # --------------------------------------------------

    fig, ax1 = plt.subplots()

    color = "tab:red"
    ax1.set_xlabel("composite transformations")
    ax1.set_ylabel("baseline (0.984) - mean accuracy over available", color=color)
    ax1.plot(x, accuracy2_mean2_diff, color=color)
    ax1.tick_params(axis="y", labelcolor=color)
    ax1.set_xticks(x, ctp_id_shortened_list)

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

    color = "tab:blue"
    ax2.set_ylabel("timeliness means over available", color=color)
    ax2.plot(x, timeliness2_mean2, color=color, label="timeliness 2")
    ax2.plot(x, timeliness1_mean2, color=color, linestyle="dashed", label="timeliness 1")
    ax2.tick_params(axis="y", labelcolor=color)

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()


def generate_report2(ctp_id_str: str) -> None:
    pass
