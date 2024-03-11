package org.softwareforce.iotvm.eventengine;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.cep.SimpleCompositeTransformationFactoriesManager;
import org.softwareforce.iotvm.eventengine.cep.ct.*;
import org.softwareforce.iotvm.eventengine.configuration.ApplicationConfiguration;
import org.softwareforce.iotvm.eventengine.configuration.KafkaConfiguration;
import org.softwareforce.iotvm.eventengine.configuration.PersistenceConfiguration;
import org.softwareforce.iotvm.eventengine.extensions.FabricationForecastingServiceAdapter;
import org.softwareforce.iotvm.eventengine.extensions.SensingRecordingServiceAdapter;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceServiceImpl;
import org.softwareforce.iotvm.shared.event.*;

public class EventEngineApplicationSimulation {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EventEngineApplicationSimulation.class);

  private static List<SensorTelemetryRawEventIBO> getSensorTelemetryRawEventIBOList(
      Instant timestamp, int size) {
    final List<SensorTelemetryRawEventIBO> list = new ArrayList<>();

    final Random rand = new Random(42);

    for (int i = 0; i < size; i++) {
      timestamp = timestamp.plusSeconds(1L); // TODO add random jitter.
      final long timestampEpochMilli = timestamp.toEpochMilli();

      final int sensorIdNum = rand.nextInt(6) + 1;

      double temperature = rand.nextDouble() * (30 - 20) + 20;
      temperature = (double) (Math.round(temperature * 100.0) / 100.0);

      final Map<String, Long> timestamps = new HashMap<>();
      timestamps.put(Constants.SENSED, timestampEpochMilli);

      list.add(
          SensorTelemetryRawEventIBO.newBuilder()
              .setSensorId("sensor-" + sensorIdNum)
              .setMeasurements(
                  List.of(
                      MeasurementIBO.newBuilder()
                          .setName(PhysicalQuantity.TEMPERATURE.getName())
                          .setValue(temperature)
                          .setUnit(PhysicalQuantity.TEMPERATURE.getUnit())
                          .build()))
              .setTimestamps(
                  TimestampsIBO.newBuilder()
                      .setDefaultTimestamp(timestampEpochMilli) // TODO It will become null...
                      .setTimestamps(timestamps)
                      .build())
              .setIdentifiers(
                  IdentifiersIBO.newBuilder()
                      .setClientSideId(UUID.randomUUID().toString())
                      .setCorrelationIds(new HashMap<>())
                      .build())
              .setAdditional(new HashMap<>())
              .build());
    }

    return list;
  }

  private static List<AverageCalculationCompositeTransformationParameters>
      loadAverageCalculationParametersSets() {
    try (InputStream in =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("average-calculation-parameters-sets.json")) {
      final ObjectMapper mapper = new ObjectMapper();
      final List<AverageCalculationCompositeTransformationParametersJsonNode> items =
          mapper.readValue(
              in,
              mapper
                  .getTypeFactory()
                  .constructCollectionType(
                      List.class,
                      AverageCalculationCompositeTransformationParametersJsonNode.class));
      return items.stream()
          .map(AverageCalculationCompositeTransformationParametersJsonNode::toParameters)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    new EventEngineApplicationSimulation().run1();
  }

  public void run1() {
    long startTime = System.nanoTime();

    ApplicationConfiguration.getInstance().load();

    // TODO Do not forget to delete kafka-streams directory... IMPORTANT!!!!!!!

    // Dependencies
    // --------------------------------------------------

    final IBOPersistenceServiceImpl iboPersistenceServiceImpl =
        new IBOPersistenceServiceImpl(
            new PersistenceConfiguration(
                "mongodb://localhost:27017/?readPreference=primary&appname=IoTVM_EventEngine&ssl=false",
                "iotvmdb"));
    final FabricationForecastingServiceAdapter fabricationForecastingServiceAdapter =
        new FabricationForecastingServiceAdapter();
    final SensingRecordingServiceAdapter sensingRecordingServiceAdapter =
        new SensingRecordingServiceAdapter();

    // Composite Transformations Parameters
    // --------------------------------------------------

    final IngestionCompositeTransformationParameters ingestionParameters =
        new IngestionCompositeTransformationParameters();
    final SplittingCompositeTransformationParameters splittingParameters =
        new SplittingCompositeTransformationParameters();
    final List<AverageCalculationCompositeTransformationParameters>
        averageCalculationParametersList = loadAverageCalculationParametersSets();
    //    final AverageCalculationCompositeTransformationParameters averageCalculationParameters =
    //        new AverageCalculationCompositeTransformationParameters(
    //            PhysicalQuantity.TEMPERATURE, Duration.ofSeconds(5), null, null, 1, true, 10,
    // null, 0);

    // Composite Transformations Factories
    // --------------------------------------------------

    final CompositeTransformationFactory ingestionCTF =
        new IngestionCompositeTransformationFactory(ingestionParameters, iboPersistenceServiceImpl);
    final CompositeTransformationFactory splittingCTF =
        new SplittingCompositeTransformationFactory(splittingParameters, iboPersistenceServiceImpl);
    //    final CompositeTransformationFactory averageCalculationCTF =
    //        new AverageCalculationCompositeTransformationFactory(
    //            averageCalculationParameters,
    //            iboPersistenceServiceImpl,
    //            fabricationForecastingServiceAdapter,
    //            sensingRecordingServiceAdapter);

    final List<CompositeTransformationFactory> averageCalculationCTFs = new ArrayList<>();
    for (final AverageCalculationCompositeTransformationParameters parametersSet :
        averageCalculationParametersList) {
      final CompositeTransformationFactory averageCalculation =
          new AverageCalculationCompositeTransformationFactory(
              parametersSet,
              iboPersistenceServiceImpl,
              fabricationForecastingServiceAdapter,
              sensingRecordingServiceAdapter);

      averageCalculationCTFs.add(averageCalculation);
    }

    // Composite Transformations Factories Manager
    // --------------------------------------------------

    final SimpleCompositeTransformationFactoriesManager manager =
        SimpleCompositeTransformationFactoriesManager.newInstance()
            .withCompositeTransformationFactory(ingestionCTF)
            .withCompositeTransformationFactory(splittingCTF)
        // .withCompositeTransformationFactory(averageCalculationCTF)
        ;
    // TODO You can add a custom CTF to record input and output and organize them in FS in
    // real-time!

    for (final CompositeTransformationFactory ctf : averageCalculationCTFs) {
      manager.withCompositeTransformationFactory(ctf);
    }

    // Kafka Streams Configuration
    // --------------------------------------------------

    final KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    final Properties kafkaStreamsProperties = kafkaConfiguration.getKafkaStreamsProperties();

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
    final Serde<SensorTelemetryEventIBO> SENSOR_TELEMETRY_EVENT_IBO_SERDE =
        Constants.SENSOR_TELEMETRY_EVENT_IBO_SERDE;
    final Serde<SensorTelemetryMeasurementEventIBO> SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE =
        Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE;
    final Serde<SensorTelemetryMeasurementsAverageEventIBO>
        SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE =
            Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE;

    // Input Topics
    // --------------------------------------------------

    final TestInputTopic<String, SensorTelemetryRawEventIBO> SENSOR_TELEMETRY_RAW_EVENT_TIT =
        topologyTestDriver.createInputTopic(
            Constants.SENSOR_TELEMETRY_RAW_EVENT_TOPIC,
            STRING_SERDE.serializer(),
            SENSOR_TELEMETRY_RAW_EVENT_IBO_SERDE.serializer());

    //    final TestInputTopic<String, SensorTelemetryEventIBO> SENSOR_TELEMETRY_EVENT_TIT =
    //        topologyTestDriver.createInputTopic(
    //            Constants.SENSOR_TELEMETRY_EVENT_TOPIC,
    //            STRING_SERDE.serializer(),
    //            SENSOR_TELEMETRY_EVENT_IBO_SERDE.serializer());
    //
    //    final Map<String, TestInputTopic<String, SensorTelemetryMeasurementEventIBO>>
    // SENSOR_TELEMETRY_MEASUREMENT_EVENT_TIT_MAP = new HashMap<>();
    //
    //
    // SENSOR_TELEMETRY_MEASUREMENT_EVENT_TIT_MAP.put(Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC, topologyTestDriver.createInputTopic(
    //        Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC,
    //        STRING_SERDE.serializer(),
    //        SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE.serializer()));
    //
    //    for (final PhysicalQuantity physicalQuantity : PhysicalQuantity.values()) {
    //      final String topic =
    // Constants.getSensorTelemetryMeasurementEventTopic(physicalQuantity);
    //      SENSOR_TELEMETRY_MEASUREMENT_EVENT_TIT_MAP.put(topic,
    // topologyTestDriver.createInputTopic(
    //          topic,
    //          STRING_SERDE.serializer(),
    //          SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE.serializer()));
    //    }

    // Output Topics
    // --------------------------------------------------

    //    final TestOutputTopic<String, SensorTelemetryEventIBO> SENSOR_TELEMETRY_EVENT_TOT =
    //        topologyTestDriver.createOutputTopic(
    //            Constants.SENSOR_TELEMETRY_EVENT_TOPIC,
    //            STRING_SERDE.deserializer(),
    //            SENSOR_TELEMETRY_EVENT_IBO_SERDE.deserializer());
    //
    //    final TestOutputTopic<String, SensorTelemetryMeasurementEventIBO>
    // SENSOR_TELEMETRY_MEASUREMENT_EVENT_TEMPERATURE_TOT =
    //        topologyTestDriver.createOutputTopic(
    //            Constants.getSensorTelemetryMeasurementEventTopic(PhysicalQuantity.TEMPERATURE),
    //            STRING_SERDE.deserializer(),
    //            SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE.deserializer());
    //

    //    final String testTopic =
    // Constants.getSensorTelemetryMeasurementsAverageEventTopic(PhysicalQuantity.TEMPERATURE,
    // averageCalculationParameters.getUniqueIdentifier());
    //    final TestOutputTopic<String, SensorTelemetryMeasurementsAverageEventIBO>
    //        SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOT =
    //            topologyTestDriver.createOutputTopic(
    //                testTopic,
    //                STRING_SERDE.deserializer(),
    //                SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE.deserializer());

    // Simulation: Ingestion
    // --------------------------------------------------

    final List<SensorTelemetryRawEventIBO> sensorTelemetryRawEventIBOList =
        getSensorTelemetryRawEventIBOList(Instant.now(), 100);

    for (final SensorTelemetryRawEventIBO event : sensorTelemetryRawEventIBOList) {
      SENSOR_TELEMETRY_RAW_EVENT_TIT.pipeInput("universal", event);
    }

    // topologyTestDriver.advanceWallClockTime(Duration.ofMinutes(6));

    System.out.println(topologyTestDriver.producedTopicNames());

    // Simulation: Splitting
    // --------------------------------------------------

    // Runs automatically! Just declare all input and output topics.

    // Simulation: Average Calculation
    // --------------------------------------------------

    // Runs automatically! Just declare all input and output topics.

    // Kafka Streams
    // --------------------------------------------------

    topologyTestDriver.close();

    long endTime = System.nanoTime();
    long diff = endTime - startTime;
    System.out.println(diff);

    // TODO Delete files...?
  }
}
