Compile iotvm-gateway

Start docker  

Delete volumes if necessary  
Delete Kafka Streams local data

```
docker compose -f docker-compose.yml up -d
docker compose -f iotvm-docker/docker-compose.yml up -d
docker compose -f ../iotvm-docker/docker-compose.yml up -d
```

Parameterize iotvm_extensions.examples.configuration._YOUR_FILE  
Change imports in `iotvm_extensions.examples.configuration.__init__.py`  

python -m iotvm_extensions.examples_cli generate_macros_multiple_sensors  
python -m iotvm_extensions.examples_cli generate_average_calculation_parameters_sets_json  
python -m iotvm_extensions.examples_cli persist_configuration  

Copy `average-calculation-parameters-sets.json` from iotvm_extensions/local_data/experiments/YOUR_EXPERIMENT/input/average-calculation-parameters-sets.json to `iotvm-eventengine/src/main/resources/average-calculation-parameters-sets.json`

(Re)Compile event engine
```
mvn clean package
```

When performing simulations, run with enabled assertions ```java -ea -jar TARGET.jar```

```
java -jar target/gateway-1.0.0-PROTOTYPE.1.jar server configuration.yml
python -m iotvm_extensions.examples_cli start_server
java -jar target/eventengine-1.0.0-PROTOTYPE.1.jar
java -ea -jar target/eventengine-1.0.0-PROTOTYPE.1.jar
python -m iotvm_extensions.examples_cli ensure_forecasters  # If has data.
python -m iotvm_extensions.examples_cli run_sensor_simulation_example
python -m iotvm_extensions.examples_cli ensure_forecasters
```

Check the console after `python -m iotvm_extensions.examples_cli ensure_forecasters`
Copy `simulation_name` value and change the value of `REPORT__SIMULATION_NAME_LIST` in your configuration.

python -m iotvm_extensions.examples_cli generate_report  
python -m iotvm_extensions.examples_cli generate_report_cached  
