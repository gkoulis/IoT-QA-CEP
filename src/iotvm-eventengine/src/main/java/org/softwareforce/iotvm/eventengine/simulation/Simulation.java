package org.softwareforce.iotvm.eventengine.simulation;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.cep.SimpleCompositeTransformationFactoriesManager;
import org.softwareforce.iotvm.eventengine.cep.ct.AverageCalculationCompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.AverageCalculationCompositeTransformationParameters;
import org.softwareforce.iotvm.eventengine.cep.ct.CompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.IngestionCompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.IngestionCompositeTransformationParameters;
import org.softwareforce.iotvm.eventengine.cep.ct.SplittingCompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.SplittingCompositeTransformationParameters;
import org.softwareforce.iotvm.eventengine.configuration.KafkaConfiguration;
import org.softwareforce.iotvm.eventengine.configuration.PersistenceConfiguration;
import org.softwareforce.iotvm.eventengine.persistence.*;
import org.softwareforce.iotvm.eventengine.utilities.GeneralUtilities;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * Class for executing a simulation.
 *
 * @author Dimitris Gkoulis
 * @createdAt Tuesday 12 March 2024
 */
public class Simulation {

  private static final Logger LOGGER = LoggerFactory.getLogger(Simulation.class);

  private final String baseDirectory;
  private final String simulationName;
  private final IBOPersistenceServiceType iboPersistenceServiceType;

  public Simulation(
      String baseDirectory,
      final String simulationName,
      final IBOPersistenceServiceType iboPersistenceServiceType) {
    this.baseDirectory = baseDirectory;
    this.simulationName = simulationName;
    this.iboPersistenceServiceType = iboPersistenceServiceType;
  }

  public List<SimulationVariationIteration> loadSimulationVariationIterationList() {
    return GeneralUtilities.loadListOfInstancesFromJSON(
            Path.of(
                    this.baseDirectory,
                    this.simulationName,
                    "_system",
                    "SimulationVariationIterationList.json")
                .toString(),
            SimulationVariationIterationJsonNode.class)
        .orElseThrow()
        .stream()
        .map(
            (simulationVariationIterationJsonNode) ->
                new SimulationVariationIteration(
                    this.baseDirectory,
                    simulationVariationIterationJsonNode.simulationName,
                    simulationVariationIterationJsonNode.variationName,
                    simulationVariationIterationJsonNode.iterationName))
        .toList();
  }

  public List<AverageCalculationCompositeTransformationParameters>
      loadCompositeTransformationParameterSetList() {
    return GeneralUtilities.loadListOfInstancesFromJSON(
            Path.of(
                    this.baseDirectory,
                    this.simulationName,
                    "_system",
                    "AverageCalculationCompositeTransformationParameters.json")
                .toString(),
            AverageCalculationCompositeTransformationParametersJsonNode.class)
        .orElseThrow()
        .stream()
        .map(
            (jsonNode) ->
                new AverageCalculationCompositeTransformationParameters(
                    PhysicalQuantity.valueOf(jsonNode.physicalQuantity),
                    // TODO It's better to always pass seconds - No pass ISO string! Period.
                    Duration.ofMinutes(jsonNode.timeWindowSize),
                    null,
                    null,
                    jsonNode.minimumNumberOfContributingSensors,
                    jsonNode.ignoreCompletenessFiltering,
                    jsonNode.pastWindowsLookup,
                    null,
                    jsonNode.futureWindowsLookup))
        .toList();
  }

  private void executeSimulationVariationIteration(
      final SimulationVariationIteration simulationVariationIteration) throws IOException {
    final String simulationName = simulationVariationIteration.getSimulationName();
    final String variationName = simulationVariationIteration.getVariationName();
    final String iterationName = simulationVariationIteration.getIterationName();

    final Path kafkaStreamsStateDirectoryPath =
        Path.of(
            this.baseDirectory,
            simulationName,
            variationName,
            iterationName,
            "_system",
            "kafka-streams");

    // Cleanings
    // --------------------------------------------------

    GeneralUtilities.deleteDirectorySafely(kafkaStreamsStateDirectoryPath);

    // Dependencies
    // --------------------------------------------------

    final IBOPersistenceService iboPersistenceService;
    switch (this.iboPersistenceServiceType) {
      case NO_OPS -> iboPersistenceService = new IBOPersistenceServiceNoOpsImpl();
      case BASE -> iboPersistenceService = new IBOPersistenceServiceBaseImpl();
      case MONGODB -> iboPersistenceService =
          new IBOPersistenceServiceMongoImpl(
              new PersistenceConfiguration(
                  "mongodb://localhost:27017/?readPreference=primary&appname=IoTVM_EventEngine&ssl=false",
                  "iotvmdb"));
      default -> throw new IllegalStateException(
          "Invalid IBOPersistenceServiceType: " + this.iboPersistenceServiceType);
    }

    // Composite Transformations Parameters
    // --------------------------------------------------

    final IngestionCompositeTransformationParameters ingestionParameters =
        new IngestionCompositeTransformationParameters();
    final SplittingCompositeTransformationParameters splittingParameters =
        new SplittingCompositeTransformationParameters();
    final List<AverageCalculationCompositeTransformationParameters>
        averageCalculationParametersList = this.loadCompositeTransformationParameterSetList();

    // Composite Transformations Factories
    // --------------------------------------------------

    final CompositeTransformationFactory ingestionCTF =
        new IngestionCompositeTransformationFactory(ingestionParameters, iboPersistenceService);
    final CompositeTransformationFactory splittingCTF =
        new SplittingCompositeTransformationFactory(splittingParameters, iboPersistenceService);

    final List<CompositeTransformationFactory> averageCalculationCTFs = new ArrayList<>();
    for (final AverageCalculationCompositeTransformationParameters parametersSet :
        averageCalculationParametersList) {
      final CompositeTransformationFactory averageCalculation =
          new AverageCalculationCompositeTransformationFactory(
              parametersSet, iboPersistenceService);

      averageCalculationCTFs.add(averageCalculation);
    }

    // Composite Transformations Factories Manager
    // --------------------------------------------------

    final SimpleCompositeTransformationFactoriesManager manager =
        SimpleCompositeTransformationFactoriesManager.newInstance()
            .withCompositeTransformationFactory(ingestionCTF)
            .withCompositeTransformationFactory(splittingCTF);

    for (final CompositeTransformationFactory ctf : averageCalculationCTFs) {
      manager.withCompositeTransformationFactory(ctf);
    }

    // Kafka Streams Configuration
    // --------------------------------------------------

    final KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    final Properties kafkaStreamsProperties = kafkaConfiguration.getKafkaStreamsProperties();
    kafkaStreamsProperties.put("state.dir", kafkaStreamsStateDirectoryPath.toString());

    // Kafka Streams
    // --------------------------------------------------

    final StreamsBuilder streamsBuilder = manager.build();
    final Topology topology = streamsBuilder.build();
    final TopologyTestDriver topologyTestDriver =
        new TopologyTestDriver(topology, kafkaStreamsProperties);

    // SerDe
    // --------------------------------------------------

    final Serde<String> STRING_SERDE = Constants.STRING_SERDE;
    final Serde<SensorTelemetryRawEventIBO> SENSOR_TELEMETRY_RAW_EVENT_IBO_SERDE =
        Constants.SENSOR_TELEMETRY_RAW_EVENT_IBO_SERDE;

    // Input Topics
    // --------------------------------------------------

    final TestInputTopic<String, SensorTelemetryRawEventIBO> SENSOR_TELEMETRY_RAW_EVENT_TIT =
        topologyTestDriver.createInputTopic(
            Constants.SENSOR_TELEMETRY_RAW_EVENT_TOPIC,
            STRING_SERDE.serializer(),
            SENSOR_TELEMETRY_RAW_EVENT_IBO_SERDE.serializer());

    // Output Topics
    // --------------------------------------------------

    // final TestOutputTopic<String, SensorTelemetryMeasurementsAverageEventIBO>
    // SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOT = topologyTestDriver.createOutputTopic();

    // They are stored automatically to MongoDB.

    // Simulation: Ingestion
    // --------------------------------------------------

    final List<SensorTelemetryRawEventIBO> sensorTelemetryRawEventIBOList =
        simulationVariationIteration.loadSensorTelemetryRawEventIBOList();

    for (final SensorTelemetryRawEventIBO event : sensorTelemetryRawEventIBOList) {
      SENSOR_TELEMETRY_RAW_EVENT_TIT.pipeInput("universal", event);
    }

    // Simulation: Splitting
    // --------------------------------------------------

    // Runs automatically! Just declare all input and output topics.

    // Simulation: Average Calculation
    // --------------------------------------------------

    // Runs automatically! Just declare all input and output topics.

    // Output
    // --------------------------------------------------

    // final Set<String> producedTopicNames = topologyTestDriver.producedTopicNames();
    for (final AverageCalculationCompositeTransformationParameters parametersSet :
        averageCalculationParametersList) {
      final String topicName =
          Constants.getSensorTelemetryMeasurementsAverageEventTopic(
              parametersSet.getPhysicalQuantity(), parametersSet.getUniqueIdentifier());

      // if (!producedTopicNames.contains(topicName)) {}

      final TestOutputTopic<String, SensorTelemetryMeasurementsAverageEventIBO> testOutputTopic =
          topologyTestDriver.createOutputTopic(
              topicName,
              Constants.STRING_SERDE.deserializer(),
              Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE.deserializer());

      final List<SensorTelemetryMeasurementsAverageEventIBO> eventList =
          testOutputTopic.readKeyValuesToList().stream().map(i -> i.value).toList();

      // final String fileName = topicName + ".json";
      // For simplicity, we use only the parameters unique identifier.
      String fileName = parametersSet.getUniqueIdentifier() + ".json";
      fileName =
          fileName.replaceFirst(AverageCalculationCompositeTransformationParameters.ID_PREFIX, "");
      final String pathToFile =
          Path.of(
                  baseDirectory,
                  simulationName,
                  variationName,
                  iterationName,
                  "_system",
                  "output",
                  fileName)
              .toString();
      GeneralUtilities.writeInstancesOfSpecificRecordAsJson(eventList, pathToFile);
    }

    // Kafka Streams
    // --------------------------------------------------

    topologyTestDriver.close();

    // Cleanings
    // --------------------------------------------------

    GeneralUtilities.deleteDirectorySafely(kafkaStreamsStateDirectoryPath);
  }

  public void execute() {
    final List<SimulationVariationIteration> simulationVariationIterationList =
        this.loadSimulationVariationIterationList();
    for (final SimulationVariationIteration simulationVariationIteration :
        simulationVariationIterationList) {
      try {
        this.executeSimulationVariationIteration(simulationVariationIteration);
      } catch (IOException ex) {
        LOGGER.error(
            "Execution of SimulationVariationIteration: {} failed!",
            simulationVariationIteration,
            ex);
      }
    }
  }
}
