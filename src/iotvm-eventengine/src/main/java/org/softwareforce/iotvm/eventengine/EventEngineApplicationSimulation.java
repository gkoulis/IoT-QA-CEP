package org.softwareforce.iotvm.eventengine;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.softwareforce.iotvm.eventengine.cep.ct.AverageCalculationCompositeTransformationParametersJsonNode;
import org.softwareforce.iotvm.eventengine.cep.ct.CompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.configuration.ApplicationConfiguration;
import org.softwareforce.iotvm.eventengine.configuration.KafkaConfiguration;
import org.softwareforce.iotvm.eventengine.configuration.PersistenceConfiguration;
import org.softwareforce.iotvm.eventengine.extensions.FabricationForecastingServiceAdapter;
import org.softwareforce.iotvm.eventengine.extensions.SensingRecordingServiceAdapter;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceServiceImpl;
import org.softwareforce.iotvm.shared.event.*;

import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class EventEngineApplicationSimulation {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventEngineApplicationSimulation.class);

    private static List<AverageCalculationCompositeTransformationParameters> loadAverageCalculationParametersSets() {
        try (InputStream in = Thread.currentThread()
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

    public SensorTelemetryMeasurementEventIBO test1(long defaultTimestamp) {
        SensorTelemetryMeasurementEventIBO event = SensorTelemetryMeasurementEventIBO
                .newBuilder()
                .setSensorId("sensor-1")
                .setMeasurement(MeasurementIBO.newBuilder().setName(PhysicalQuantity.TEMPERATURE.getName()).setValue(20D).setUnit(PhysicalQuantity.TEMPERATURE.getUnit()).build())
                .setTimestamps(TimestampsIBO.newBuilder().setDefaultTimestamp(defaultTimestamp).setTimestamps(new HashMap<>()).build())
                .setIdentifiers(IdentifiersIBO.newBuilder().setClientSideId(null).setCorrelationIds(new HashMap<>()).build())
                .setAdditional(new HashMap<>())
                .build();
        return event;
    }

    public SensorTelemetryRawEventIBO getRawEvent() {
        return SensorTelemetryRawEventIBO.newBuilder()
                .setSensorId("sensor-1")
                .setMeasurements(List.of(MeasurementIBO.newBuilder().setName(PhysicalQuantity.TEMPERATURE.getName()).setValue(10.0D).setUnit(PhysicalQuantity.TEMPERATURE.getUnit()).build()))
                .setTimestamps(TimestampsIBO.newBuilder().setDefaultTimestamp(null).setTimestamps(new HashMap<>()).build())
                .setIdentifiers(IdentifiersIBO.newBuilder().setClientSideId(null).setCorrelationIds(new HashMap<>()).build())
                .setAdditional(new HashMap<>())
                .build();
    }

    public static void main(String[] args) {
        new EventEngineApplicationSimulation().run3();
    }

    public void run3() {
        ApplicationConfiguration.getInstance().load();

        // TODO Do not forget to delete kafka-streams directory...

        final KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
        final Properties kafkaStreamsProperties = kafkaConfiguration.getKafkaStreamsProperties();

        final IBOPersistenceServiceImpl iboPersistenceServiceImpl =
                new IBOPersistenceServiceImpl(
                        new PersistenceConfiguration(
                                "mongodb://localhost:27017/?readPreference=primary&appname=IoTVM_EventEngine&ssl=false", "iotvmdb"));

        final AverageCalculationCompositeTransformationParameters parameters = new AverageCalculationCompositeTransformationParameters(
                PhysicalQuantity.TEMPERATURE,
                Duration.ofSeconds(5),
                null,
                null,
                1,
                true,
                10,
                null,
                0
        );

        final FabricationForecastingServiceAdapter fabricationForecastingServiceAdapter = new FabricationForecastingServiceAdapter();
        final SensingRecordingServiceAdapter sensingRecordingServiceAdapter = new SensingRecordingServiceAdapter();
        final CompositeTransformationFactory averageCalculationCTF = new AverageCalculationCompositeTransformationFactory(parameters, iboPersistenceServiceImpl, fabricationForecastingServiceAdapter, sensingRecordingServiceAdapter);

        final SimpleCompositeTransformationFactoriesManager manager = SimpleCompositeTransformationFactoriesManager.newInstance().withCompositeTransformationFactory(averageCalculationCTF);
        final StreamsBuilder streamsBuilder = manager.build();
        final Topology topology = streamsBuilder.build();
        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, kafkaStreamsProperties);

        final PhysicalQuantity physicalQuantity = PhysicalQuantity.TEMPERATURE;
        final String inputTopicName = Constants.getSensorTelemetryMeasurementEventTopic(physicalQuantity);
        // final String outputTopicName = Constants.getSensorTelemetryMeasurementsAverageEventTopic(physicalQuantity);
        final String outputTopicName = "ga.sensor_telemetry_measurements_average_event.0001.temperature.w_avg_temperature_PT5S_null_null_1_true_10_PT5S_10"; // TODO Auto.

        final Serde<String> stringSerde = Constants.STRING_SERDE;
        final Serde<SensorTelemetryMeasurementEventIBO> sensorTelemetryMeasurementEventIBOSerde = Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE;
        final Serde<SensorTelemetryMeasurementsAverageEventIBO> sensorTelemetryMeasurementsAverageEventIBOSerde = Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE;
        final TestInputTopic<String, SensorTelemetryMeasurementEventIBO> inputTopic = testDriver.createInputTopic(inputTopicName, stringSerde.serializer(), sensorTelemetryMeasurementEventIBOSerde.serializer());
        final TestOutputTopic<String, SensorTelemetryMeasurementsAverageEventIBO> outputTopic = testDriver.createOutputTopic(outputTopicName, stringSerde.deserializer(), sensorTelemetryMeasurementsAverageEventIBOSerde.deserializer());

        inputTopic.pipeInput("universal", this.test1(0L));
        // inputTopic.advanceTime(Duration.ofSeconds(6));
        inputTopic.pipeInput("universal", this.test1(1000000L));

        System.out.println(outputTopic.readKeyValue());
        System.out.println(testDriver.getAllStateStores().keySet());

        testDriver.close();
    }
}
