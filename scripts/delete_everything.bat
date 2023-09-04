F:
cd \projects\PhD\DGk-PhD-Monorepo

docker-compose -f .\src\iotvm-docker\docker-compose.yml down

docker volume rm iotvm-docker_iotvm-kafka-data
docker volume rm iotvm-docker_iotvm-kafka-secrets
docker volume rm iotvm-docker_iotvm-zookeeper-secrets
docker volume rm iotvm-docker_iotvm-zookeeper-data
docker volume rm iotvm-docker_iotvm-zookeeper-logs
docker volume rm iotvm-docker_iotvm-schemaregistry-secrets
docker volume rm iotvm-docker_iotvm-mongodb-data-db
docker volume rm iotvm-docker_iotvm-mongodb-data-configdb

del /s /f /q F:\tmp\*

pause
