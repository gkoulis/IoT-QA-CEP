import json
import dataclasses
import os
import re
from typing import Dict, List
import fire
import shutil

from iotvm_extensions.examples.average_calculation_parameters_sets import (
    generate_average_calculation_parameters_sets,
    generate_composite_transformation_parameters_ids,
)
from iotvm_extensions.examples.configuration import (
    EXPERIMENT_NAME,
    EXPERIMENT_INPUT_DIRECTORY,
    EXPERIMENT_OUTPUT_DIRECTORY,
    PHYSICAL_QUANTITY,
    TIME_WINDOW_SIZE_LIST,
    NUMBER_OF_CONTRIBUTING_SENSORS_LIST,
    IGNORE_COMPLETENESS_FILTERING_LIST,
    FABRICATION_PAST_EVENTS_STEPS_BEHIND_LIST,
    FABRICATION_FORECASTING_STEPS_AHEAD_LIST,
    SENSOR_ID_LIST,
    PHYSICAL_QUANTITY_LOWER,
    TOPIC_NAME,
    FREQUENCY_IN_SECONDS_LIST,
    MG__DATASET_PATH_TO_FILE,
    MG__PARAMETERS_BY_SENSOR_ID,
    MG__EXPECTED_RECURRING_TIME_WINDOWS,
    REPORT__EXPERIMENT_NAME_LIST,
    REPORT__SIMULATION_NAME_LIST,
    REPORT__CYCLE_LIST,
)
from iotvm_extensions.examples.fabrication_forecasting import (
    run_fabrication_forecasting_example,
)
from iotvm_extensions.examples.macros_generator import (
    optimize_distribution_parameters_v1,
    generate_macros_multiple_sensors,
)
from iotvm_extensions.examples.report import generate_report, generate_report_cached
from iotvm_extensions.examples.sensor_simulation import (
    run_sensor_simulation_example,
)
from iotvm_extensions.examples.server_client import run_client_example
from iotvm_extensions.fabrication_forecasting.base import disable_statsmodels_convergence_warnings
from iotvm_extensions.fabrication_forecasting.spec import (
    FabricationForecastingService,
)
from iotvm_extensions.fabrication_forecasting.spec.ttypes import (
    ForecastScope,
    ForecastRequest,
)
from iotvm_extensions.examples.report_mara import generate_report as generate_report_mara
from iotvm_extensions.mongodb import MongoClient, get_default_mongodb_client
from iotvm_extensions.server_client.client import ClientsFactory
from iotvm_extensions.server_client.server import start_server
from ._logging import set_up_logging
from ._prerequisites import set_up_prerequisites


# noinspection PyMethodMayBeStatic
class CLI:
    def start_server(self) -> None:
        disable_statsmodels_convergence_warnings()
        start_server()

    def ensure_forecasters(self) -> None:
        factory: ClientsFactory = ClientsFactory()
        factory.register_service(service_class=FabricationForecastingService)

        service: FabricationForecastingService = factory.get_service(
            service_name="FabricationForecastingService"
        )
        assert service is not None

        sensor_id_list: List[str] = SENSOR_ID_LIST
        frequency_in_seconds_list: List[int] = FREQUENCY_IN_SECONDS_LIST
        physical_quantity_lower: str = PHYSICAL_QUANTITY_LOWER
        topic_name: str = TOPIC_NAME

        for sensor_id in sensor_id_list:
            for frequency_in_seconds in frequency_in_seconds_list:
                scope = ForecastScope(
                    sensorId=sensor_id,
                    physicalQuantity=physical_quantity_lower,
                    topicName=topic_name,
                    frequencyInSeconds=frequency_in_seconds,
                )
                service.ensure(scope=scope)
                request = ForecastRequest(
                    startTimestamp=0, endTimestamp=1, stepsAhead=10, comment="none"
                )
                # service.forecast(scope=scope, request=request)  # TODO Just to make sure... add try-except.

    def delete_all_mongodb_documents(self) -> None:
        client: MongoClient = get_default_mongodb_client()
        collections_names: List[str] = ["recorded_sensor_data", "universal"]
        for collection_name in collections_names:
            collection = client["iotvmdb"][collection_name]
            collection.delete_many({})

    def run_fabrication_forecasting_example(self) -> None:
        run_fabrication_forecasting_example()

    def run_sensor_simulation_example(self) -> None:
        run_sensor_simulation_example(experiment_name=EXPERIMENT_NAME, path_to_dir=EXPERIMENT_INPUT_DIRECTORY)

    def run_client_example(self) -> None:
        run_client_example()

    def optimize_distribution_parameters_v1(self) -> None:
        optimize_distribution_parameters_v1(
            dataset_path_to_file=MG__DATASET_PATH_TO_FILE,
        )

    def generate_macros_multiple_sensors(self) -> None:
        generate_macros_multiple_sensors(
            dataset_path_to_file=MG__DATASET_PATH_TO_FILE,
            sensors_parameters=MG__PARAMETERS_BY_SENSOR_ID,
            path_to_experiment_input_directory=EXPERIMENT_INPUT_DIRECTORY
        )

    def generate_average_calculation_parameters_sets_json(self) -> None:
        data: List[Dict] = generate_average_calculation_parameters_sets(
            physical_quantity=PHYSICAL_QUANTITY,
            time_window_size=TIME_WINDOW_SIZE_LIST,
            number_of_contributing_sensors=NUMBER_OF_CONTRIBUTING_SENSORS_LIST,
            ignore_completeness_filtering_list=IGNORE_COMPLETENESS_FILTERING_LIST,
            fabrication_past_events=FABRICATION_PAST_EVENTS_STEPS_BEHIND_LIST,
            fabrication_forecasting=FABRICATION_FORECASTING_STEPS_AHEAD_LIST,
        )
        # print(pd.DataFrame(data=data))
        print(
            f"Creating average-calculation-parameters-sets.json ({len(data)} parameters sets)"
        )

        json_object = json.dumps(data, indent=4)
        path_to_file: str = os.path.join(EXPERIMENT_INPUT_DIRECTORY, "average-calculation-parameters-sets.json")
        assert os.path.exists(path_to_file) is False
        with open(path_to_file, "w") as outfile:
            outfile.write(json_object)

    def persist_configuration(self) -> None:
        import iotvm_extensions.examples.configuration as config

        data = {}
        for key in config.__dict__.keys():
            if bool(re.match("^[A-Z][_A-Z0-9]*$", key)) is True:
                data[key] = config.__dict__[key]

        print(
            f"Creating configuration.json ({len(data)} key-value pairs)"
        )

        class EnhancedJSONEncoder(json.JSONEncoder):
            def default(self, o):
                if dataclasses.is_dataclass(o):
                    return dataclasses.asdict(o)
                return super().default(o)

        json_object = json.dumps(data, indent=4, cls=EnhancedJSONEncoder)
        path_to_file: str = os.path.join(EXPERIMENT_INPUT_DIRECTORY, "configuration.json")
        assert os.path.exists(path_to_file) is False
        with open(path_to_file, "w") as outfile:
            outfile.write(json_object)

        shutil.copy(config.CONFIGURATION_SCRIPT_FILE, os.path.join(EXPERIMENT_INPUT_DIRECTORY, f"configuration-{config.CONFIGURATION_SCRIPT_NAME}.py"))

    def generate_report(self) -> None:
        physical_quantity: str = PHYSICAL_QUANTITY
        time_window_size_list: List[int] = TIME_WINDOW_SIZE_LIST
        number_of_contributing_sensors_list: List[int] = NUMBER_OF_CONTRIBUTING_SENSORS_LIST
        ignore_completeness_filtering_list: List[bool] = IGNORE_COMPLETENESS_FILTERING_LIST
        fabrication_past_events_steps_behind: List[int] = FABRICATION_PAST_EVENTS_STEPS_BEHIND_LIST
        fabrication_forecasting_steps_ahead: List[int] = FABRICATION_FORECASTING_STEPS_AHEAD_LIST

        ctp_id_list: List[str] = generate_composite_transformation_parameters_ids(
            physical_quantity=physical_quantity,
            time_window_size=time_window_size_list,
            number_of_contributing_sensors=number_of_contributing_sensors_list,
            ignore_completeness_filtering_list=ignore_completeness_filtering_list,
            fabrication_past_events_steps_behind=fabrication_past_events_steps_behind,
            fabrication_forecasting_steps_ahead=fabrication_forecasting_steps_ahead,
        )
        # ctp_id_list: List[str] = ["w_avg_temperature_PT5S_null_null_2_true_0_PT5S_0"]
        experiment_name_list: List[str] = REPORT__EXPERIMENT_NAME_LIST
        simulation_name_list: List[str] = REPORT__SIMULATION_NAME_LIST
        cycle_list: List[int] = REPORT__CYCLE_LIST
        recurring_windows_count: int = MG__EXPECTED_RECURRING_TIME_WINDOWS

        generate_report(
            experiment_name_list=experiment_name_list,
            simulation_name_list=simulation_name_list,
            ctp_id_list=ctp_id_list,
            cycle_list=cycle_list,
            recurring_windows_count=recurring_windows_count,
            path_to_input_dir=EXPERIMENT_INPUT_DIRECTORY,
            path_to_output_dir=EXPERIMENT_OUTPUT_DIRECTORY,
        )

    def generate_report_cached(self) -> None:
        generate_report_cached(path_to_output_dir=EXPERIMENT_OUTPUT_DIRECTORY)

    def generate_report_mara(self) -> None:
        generate_report_mara()


if __name__ == "__main__":
    set_up_logging()
    set_up_prerequisites()
    fire.Fire(CLI)
