import matplotlib

from iotvm_extensions.mongodb import initialize_default_mongodb_client


def set_up_prerequisites() -> None:
    initialize_default_mongodb_client()
    matplotlib.use(backend="Qt5Agg")
