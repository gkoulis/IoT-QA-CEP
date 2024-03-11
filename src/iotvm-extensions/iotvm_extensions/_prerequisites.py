import matplotlib

from iotvm_extensions.helpers import initialize_server_client_local_facade
from iotvm_extensions.mongodb import initialize_default_mongodb_client


def set_up_prerequisites() -> None:
    initialize_default_mongodb_client()
    initialize_server_client_local_facade()
    matplotlib.use(backend="Qt5Agg")
