from sklearn.metrics import accuracy_score, r2_score


def run_experiment1_0001() -> None:
    y_pred = [0, 1, 0, 1, 0, 1]
    y_true = [1, 1, 1, 1, 1, 1]
    as1 = accuracy_score(y_true=y_true, y_pred=y_pred)
    as2 = accuracy_score(y_true=y_true, y_pred=y_pred, normalize=False)

    print(as1)
    print(as2)


def run_experiment1_0002() -> None:
    y_pred = [0.3, 1, 0.6, 1, 0.9, 1]
    y_true = [1, 1, 1, 1, 1, 1]

    as1 = r2_score(y_true=y_true, y_pred=y_pred)

    print(as1)
