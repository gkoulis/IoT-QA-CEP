def get_service_name(service_class) -> str:
    return service_class.__name__.split(".")[-1]
