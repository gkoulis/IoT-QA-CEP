package org.softwareforce.iotvm.eventengine;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.cep.SimpleCompositeTransformationFactoriesManager;
import org.softwareforce.iotvm.eventengine.cep.ct.AverageCalculationCompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.AverageCalculationCompositeTransformationParameters;
import org.softwareforce.iotvm.eventengine.cep.ct.AverageCalculationMergingCompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.AverageCalculationMergingCompositeTransformationParameters;
import org.softwareforce.iotvm.eventengine.cep.ct.CompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.IngestionCompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.IngestionCompositeTransformationParameters;
import org.softwareforce.iotvm.eventengine.cep.ct.SplittingCompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.SplittingCompositeTransformationParameters;
import org.softwareforce.iotvm.eventengine.configuration.KafkaConfiguration;
import org.softwareforce.iotvm.eventengine.configuration.PersistenceConfiguration;
import org.softwareforce.iotvm.eventengine.experimental.SensorTelemetryMeasurementEventForecastingService;
import org.softwareforce.iotvm.eventengine.kafka.KafkaAdminService;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceServiceImpl;

/**
 * Event Engine Application.
 *
 * @author Dimitris Gkoulis
 */
public class EventEngineApplication {

  public static void main(String[] args) {
    new EventEngineApplication().run();
  }

  public void run() {
    final KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
    final Admin kafkaAdmin = kafkaConfiguration.getKafkaAdmin();
    final Properties kafkaStreamsProperties = kafkaConfiguration.getKafkaStreamsProperties();

    final KafkaAdminService kafkaAdminService = new KafkaAdminService(kafkaAdmin);

    final IngestionCompositeTransformationParameters ingestionParameters =
        new IngestionCompositeTransformationParameters();
    final SplittingCompositeTransformationParameters splittingParameters =
        new SplittingCompositeTransformationParameters();
    final AverageCalculationCompositeTransformationParameters averageCalculationParameters =
        new AverageCalculationCompositeTransformationParameters(
            PhysicalQuantity.TEMPERATURE, Duration.ofSeconds(30), null, null, 4, true, 1);
    final AverageCalculationMergingCompositeTransformationParameters
        averageCalculationMergingParameters =
            new AverageCalculationMergingCompositeTransformationParameters(
                List.of(averageCalculationParameters));

    final List<String> topicNameList =
        List.of(
            Constants.SENSOR_TELEMETRY_RAW_EVENT_TOPIC,
            Constants.SENSOR_TELEMETRY_EVENT_TOPIC,
            Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC,
            Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOPIC,
            // Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC
            Constants.getSensorTelemetryMeasurementEventTopic(PhysicalQuantity.TEMPERATURE),
            Constants.getSensorTelemetryMeasurementEventTopic(PhysicalQuantity.HUMIDITY),
            // Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOPIC
            Constants.getSensorTelemetryMeasurementsAverageEventTopic(PhysicalQuantity.TEMPERATURE),
            Constants.getSensorTelemetryMeasurementsAverageEventTopic(PhysicalQuantity.HUMIDITY),
            Constants.getSensorTelemetryMeasurementsAverageEventTopic(
                PhysicalQuantity.TEMPERATURE, averageCalculationParameters.getUniqueIdentifier()),
            Constants.getSensorTelemetryMeasurementsAverageEventTopic(
                PhysicalQuantity.HUMIDITY, averageCalculationParameters.getUniqueIdentifier()));
    final List<NewTopic> newTopicList =
        kafkaAdminService.convertTopicNamesToNewTopics(topicNameList, 1, 1);
    kafkaAdminService.createTopics(newTopicList);

    final IBOPersistenceServiceImpl iboPersistenceServiceImpl =
        new IBOPersistenceServiceImpl(
            new PersistenceConfiguration(
                "mongodb://localhost:27017/?readPreference=primary&appname=IoTVM_EventEngine&ssl=false",
                "iotvmdb"));

    final SensorTelemetryMeasurementEventForecastingService
        sensorTelemetryMeasurementEventForecastingService =
            new SensorTelemetryMeasurementEventForecastingService();

    final CompositeTransformationFactory ingestion =
        new IngestionCompositeTransformationFactory(ingestionParameters, iboPersistenceServiceImpl);
    final CompositeTransformationFactory splitting =
        new SplittingCompositeTransformationFactory(splittingParameters, iboPersistenceServiceImpl);
    final CompositeTransformationFactory averageCalculation =
        new AverageCalculationCompositeTransformationFactory(
            averageCalculationParameters,
            iboPersistenceServiceImpl,
            sensorTelemetryMeasurementEventForecastingService);
    final CompositeTransformationFactory averageCalculationMerging =
        new AverageCalculationMergingCompositeTransformationFactory(
            averageCalculationMergingParameters, iboPersistenceServiceImpl);

    final SimpleCompositeTransformationFactoriesManager manager =
        SimpleCompositeTransformationFactoriesManager.newInstance()
            .withCompositeTransformationFactory(ingestion)
            .withCompositeTransformationFactory(splitting)
            .withCompositeTransformationFactory(averageCalculation)
            .withCompositeTransformationFactory(averageCalculationMerging);

    final StreamsBuilder streamsBuilder = manager.build();
    final KafkaStreams kafkaStreams =
        new KafkaStreams(streamsBuilder.build(), kafkaStreamsProperties);

    // Always (and unconditionally) clean local state prior to starting the processing topology.
    // We opt for this unconditional call here because this will make it easier for you to play
    // around with the example
    // when resetting the application for doing a re-run (via the Application Reset Tool,
    // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
    //
    // The drawback of cleaning up local state prior is that your app must rebuilt its local state
    // from scratch, which
    // will take time and will require reading all the state-relevant data from the Kafka cluster
    // over the network.
    // Thus, in a production scenario you typically do not want to clean up always as we do here but
    // rather only when it
    // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line
    // flag for your app).
    // See `ApplicationResetExample.java` for a production-like example.
    kafkaStreams.cleanUp(); // TODO Optional.

    kafkaStreams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams.
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
  }
}
