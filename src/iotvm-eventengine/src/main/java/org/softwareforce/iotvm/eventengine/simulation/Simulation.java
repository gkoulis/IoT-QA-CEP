package org.softwareforce.iotvm.eventengine.simulation;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.AverageCalculationCompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.AverageCalculationCompositeTransformationParameters;
import org.softwareforce.iotvm.eventengine.cep.ct.CompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.IngestionCompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.IngestionCompositeTransformationParameters;
import org.softwareforce.iotvm.eventengine.cep.ct.SplittingCompositeTransformationFactory;
import org.softwareforce.iotvm.eventengine.cep.ct.SplittingCompositeTransformationParameters;
import org.softwareforce.iotvm.eventengine.utilities.GeneralUtilities;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.kafka.common.serialization.Serde;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.SimpleCompositeTransformationFactoriesManager;
import org.softwareforce.iotvm.eventengine.configuration.KafkaConfiguration;
import org.softwareforce.iotvm.eventengine.configuration.PersistenceConfiguration;
import org.softwareforce.iotvm.eventengine.extensions.FabricationForecastingServiceAdapter;
import org.softwareforce.iotvm.eventengine.extensions.SensingRecordingServiceAdapter;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceServiceImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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

    public Simulation(String baseDirectory, final String simulationName) {
        this.baseDirectory = baseDirectory;
        this.simulationName = simulationName;
    }

    public List<SimulationVariationIteration> loadSimulationVariationIterationList() {
        return GeneralUtilities.loadListOfInstancesFromJSON(
                Path.of(this.baseDirectory, this.simulationName, "_system", "SimulationVariationIterationList.json").toString(),
                SimulationVariationIteration.class
        ).orElseThrow();
    }

    public List<AverageCalculationCompositeTransformationParameters> loadCompositeTransformationParameterSetList() {
        return GeneralUtilities.loadListOfInstancesFromJSON(
                Path.of(this.baseDirectory, this.simulationName, "_system", "AverageCalculationCompositeTransformationParameters.json").toString(),
                AverageCalculationCompositeTransformationParameters.class
        ).orElseThrow();
    }

    private void executeSimulationVariationIteration(final SimulationVariationIteration simulationVariationIteration) throws IOException {
        final Path kafkaStreamsStateDirectoryPath = Path.of(this.baseDirectory, this.simulationName, "_system");

        // Cleanings
        // --------------------------------------------------

        GeneralUtilities.deleteDirectorySafely(kafkaStreamsStateDirectoryPath);

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
                averageCalculationParametersList = this.loadCompositeTransformationParameterSetList();

        // Composite Transformations Factories
        // --------------------------------------------------

        final CompositeTransformationFactory ingestionCTF =
                new IngestionCompositeTransformationFactory(ingestionParameters, iboPersistenceServiceImpl);
        final CompositeTransformationFactory splittingCTF =
                new SplittingCompositeTransformationFactory(splittingParameters, iboPersistenceServiceImpl);

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
                        .withCompositeTransformationFactory(splittingCTF);

        for (final CompositeTransformationFactory ctf : averageCalculationCTFs) {
            manager.withCompositeTransformationFactory(ctf);
        }

        // Kafka Streams Configuration
        // --------------------------------------------------

        final KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
        final Properties kafkaStreamsProperties = kafkaConfiguration.getKafkaStreamsProperties();
        // TODO create dir if does not exist? Or it is not necessary?
        kafkaStreamsProperties.put("state.dir", kafkaStreamsStateDirectoryPath.toString());

        // Kafka Streams
        // --------------------------------------------------

        final StreamsBuilder streamsBuilder = manager.build();
        final Topology topology = streamsBuilder.build();
        final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, kafkaStreamsProperties);

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

        // They are stored automatically to MongoDB.

        // Simulation: Ingestion
        // --------------------------------------------------

        final List<SensorTelemetryRawEventIBO> sensorTelemetryRawEventIBOList = simulationVariationIteration.loadSensorTelemetryRawEventIBOList();

        for (final SensorTelemetryRawEventIBO event : sensorTelemetryRawEventIBOList) {
            SENSOR_TELEMETRY_RAW_EVENT_TIT.pipeInput("universal", event);
        }

        // Simulation: Splitting
        // --------------------------------------------------

        // Runs automatically! Just declare all input and output topics.

        // Simulation: Average Calculation
        // --------------------------------------------------

        // Runs automatically! Just declare all input and output topics.

        // Kafka Streams
        // --------------------------------------------------

        topologyTestDriver.close();

        // Cleanings
        // --------------------------------------------------

        GeneralUtilities.deleteDirectorySafely(kafkaStreamsStateDirectoryPath);
    }

    public void execute() {
        final List<SimulationVariationIteration> simulationVariationIterationList = this.loadSimulationVariationIterationList();
        for (final SimulationVariationIteration simulationVariationIteration : simulationVariationIterationList) {
            try {
                this.executeSimulationVariationIteration(simulationVariationIteration);
            } catch (IOException ex) {
                LOGGER.error("Execution of SimulationVariationIteration: {} failed!", simulationVariationIteration, ex);
            }
        }
    }
}
