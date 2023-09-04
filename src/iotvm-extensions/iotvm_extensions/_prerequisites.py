from iotvm_extensions.mongodb import initialize_default_mongodb_client
from iotvm_extensions.helpers import initialize_server_client_local_facade


def set_up_prerequisites() -> None:
    initialize_default_mongodb_client()
    initialize_server_client_local_facade()
