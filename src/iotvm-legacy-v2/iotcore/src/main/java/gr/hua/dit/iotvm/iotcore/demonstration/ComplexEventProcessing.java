package gr.hua.dit.iotvm.iotcore.demonstration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gr.hua.dit.iotvm.iotcore.config.Constants;
import gr.hua.dit.iotvm.iotcore.config.Constants.MeasurementType;
import gr.hua.dit.iotvm.iotcore.demonstration.util.PreProcessingProcessor;
import gr.hua.dit.iotvm.iotcore.demonstration.util.SensorTelemetryEventTimestampExtractor;
import gr.hua.dit.iotvm.iotcore.demonstration.util.SensorTelemetryMeasurementEventTimestampExtractor;
import gr.hua.dit.iotvm.library.event.model.AverageMeasurementValueAggregate;
import gr.hua.dit.iotvm.library.event.model.SensorTelemetryEvent;
import gr.hua.dit.iotvm.library.event.model.SensorTelemetryEventMeasurement;
import gr.hua.dit.iotvm.library.event.model.SensorTelemetryMeasurementEvent;
import gr.hua.dit.iotvm.library.event.model.WindowedAverageMeasurementValueEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * Processor for Complex Event Processing based on Kafka Streams.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 12 January 2023
 * @lastModifiedAt Monday 16 January 2023
 * @since 1.0.0-PROTOTYPE.1
 */
@SuppressWarnings({"Convert2Diamond", "Convert2Lambda"})
@Component
public class ComplexEventProcessing {

    // TODO Add AtomicInteger(s) to count windows when hopping time window is enabled.

    private static final Logger logger = LoggerFactory.getLogger(ComplexEventProcessing.class);

    /**
     * Counter for debugging purposes.
     */
    private final AtomicInteger counter = new AtomicInteger(0);

    private static final List<WindowedAverageMeasurementValueCalculationParametersSet> PARAMETERS_SETS =
            new ArrayList<>();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private static final JsonDeserializer<SensorTelemetryEvent> SENSOR_TELEMETRY_EVENT_JSON_DESERIALIZER =
            new JsonDeserializer<SensorTelemetryEvent>();
    private static final JsonSerializer<SensorTelemetryEvent> SENSOR_TELEMETRY_EVENT_JSON_SERIALIZER =
            new JsonSerializer<SensorTelemetryEvent>();
    private static final Serde<SensorTelemetryEvent> SENSOR_TELEMETRY_EVENT_SERDE =
            Serdes.serdeFrom(SENSOR_TELEMETRY_EVENT_JSON_SERIALIZER, SENSOR_TELEMETRY_EVENT_JSON_DESERIALIZER);

    private static final JsonDeserializer<SensorTelemetryMeasurementEvent>
            SENSOR_TELEMETRY_MEASUREMENT_EVENT_JSON_DESERIALIZER =
                    new JsonDeserializer<SensorTelemetryMeasurementEvent>();
    private static final JsonSerializer<SensorTelemetryMeasurementEvent>
            SENSOR_TELEMETRY_MEASUREMENT_EVENT_JSON_SERIALIZER = new JsonSerializer<SensorTelemetryMeasurementEvent>();
    private static final Serde<SensorTelemetryMeasurementEvent> SENSOR_TELEMETRY_MEASUREMENT_EVENT_SERDE =
            Serdes.serdeFrom(
                    SENSOR_TELEMETRY_MEASUREMENT_EVENT_JSON_SERIALIZER,
                    SENSOR_TELEMETRY_MEASUREMENT_EVENT_JSON_DESERIALIZER);

    private static final JsonDeserializer<AverageMeasurementValueAggregate>
            AVERAGE_MEASUREMENT_VALUE_AGGREGATE_JSON_DESERIALIZER =
                    new JsonDeserializer<AverageMeasurementValueAggregate>();
    private static final JsonSerializer<AverageMeasurementValueAggregate>
            AVERAGE_MEASUREMENT_VALUE_AGGREGATE_JSON_SERIALIZER =
                    new JsonSerializer<AverageMeasurementValueAggregate>();
    private static final Serde<AverageMeasurementValueAggregate> AVERAGE_MEASUREMENT_VALUE_AGGREGATE_SERDE =
            Serdes.serdeFrom(
                    AVERAGE_MEASUREMENT_VALUE_AGGREGATE_JSON_SERIALIZER,
                    AVERAGE_MEASUREMENT_VALUE_AGGREGATE_JSON_DESERIALIZER);

    private static final JsonDeserializer<WindowedAverageMeasurementValueEvent>
            WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_JSON_DESERIALIZER =
                    new JsonDeserializer<WindowedAverageMeasurementValueEvent>();
    private static final JsonSerializer<WindowedAverageMeasurementValueEvent>
            WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_JSON_SERIALIZER =
                    new JsonSerializer<WindowedAverageMeasurementValueEvent>();
    private static final Serde<WindowedAverageMeasurementValueEvent> WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_SERDE =
            Serdes.serdeFrom(
                    WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_JSON_SERIALIZER,
                    WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_JSON_DESERIALIZER);

    private static final List<String> SENSORS_NAMES = new ArrayList<>();

    static {
        PARAMETERS_SETS.add(new WindowedAverageMeasurementValueCalculationParametersSet(
                MeasurementType.TEMPERATURE, Duration.ofMinutes(5), Duration.ofMinutes(1), 1));
        PARAMETERS_SETS.add(new WindowedAverageMeasurementValueCalculationParametersSet(
                MeasurementType.TEMPERATURE, Duration.ofMinutes(5), Duration.ofMinutes(1), 4));
        PARAMETERS_SETS.add(new WindowedAverageMeasurementValueCalculationParametersSet(
                MeasurementType.TEMPERATURE, Duration.ofMinutes(5), Duration.ofMinutes(1), 6));
        PARAMETERS_SETS.add(new WindowedAverageMeasurementValueCalculationParametersSet(
                MeasurementType.HUMIDITY, Duration.ofMinutes(5), Duration.ofMinutes(1), 4));

        Assert.isTrue(
                PARAMETERS_SETS.stream()
                                .map(WindowedAverageMeasurementValueCalculationParametersSet::getIdentifier)
                                .distinct()
                                .count()
                        == PARAMETERS_SETS.size(),
                "WindowedAverageMeasurementValueCalculationParametersSet elements are not unique!");

        SENSOR_TELEMETRY_EVENT_JSON_DESERIALIZER.addTrustedPackages("*");
        SENSOR_TELEMETRY_MEASUREMENT_EVENT_JSON_DESERIALIZER.addTrustedPackages("*");
        AVERAGE_MEASUREMENT_VALUE_AGGREGATE_JSON_DESERIALIZER.addTrustedPackages("*");
        WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_JSON_DESERIALIZER.addTrustedPackages("*");

        // TODO It should be dynamic.
        // TODO Apply these names to generator.
        SENSORS_NAMES.add("sensor-1");
        SENSORS_NAMES.add("sensor-2");
        SENSORS_NAMES.add("sensor-3");
        SENSORS_NAMES.add("sensor-4");
        SENSORS_NAMES.add("sensor-5");
        SENSORS_NAMES.add("sensor-6");
    }

    private final KafkaTemplate<String, Object> kafkaTemplate;

    /* ------------ Constructors ------------ */

    public ComplexEventProcessing(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /* ------------ Private ------------ */

    private static String averageMeasurementValueAggregateToJsonString(
            AverageMeasurementValueAggregate averageMeasurementValueAggregate) {
        try {
            return OBJECT_MAPPER.writeValueAsString(averageMeasurementValueAggregate);
        } catch (JsonProcessingException ex) {
            logger.error("Failed to convert AverageMeasurementValueAggregate to JSON!", ex);
            return null;
        }
    }

    private static AverageMeasurementValueAggregate jsonStringToAverageMeasurementValueAggregate(String json) {
        try {
            return OBJECT_MAPPER.readValue(json, AverageMeasurementValueAggregate.class);
        } catch (Exception ex) {
            logger.error("Failed to convert JSON to AverageMeasurementValueAggregate!", ex);
            return null;
        }
    }

    private void produceEventToFabricateEvent(String name) {
        final String topicName = "trigger_for_fabricated_sensor_telemetry_measurement_event_0001";
        this.kafkaTemplate.send(topicName, name);
    }

    /* ------------ CEP Implementation ------------ */

    /**
     * Creates ingestion topology.
     *
     * @param streamsBuilder the {@link StreamsBuilder} for topology building.
     * @see SensorTelemetryEvent
     */
    private void buildIngestionTopology(StreamsBuilder streamsBuilder) {
        final String inputTopicName = Constants.SENSOR_TELEMETRY_EVENT_TOPIC;
        Consumed<String, SensorTelemetryEvent> stringSensorTelemetryEventConsumed = Consumed.with(
                        STRING_SERDE, SENSOR_TELEMETRY_EVENT_SERDE)
                .withTimestampExtractor(new SensorTelemetryEventTimestampExtractor());

        final String outputTopicName = Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC;
        Produced<String, SensorTelemetryMeasurementEvent> stringSensorTelemetryMeasurementEventProduced =
                Produced.with(STRING_SERDE, SENSOR_TELEMETRY_MEASUREMENT_EVENT_SERDE);

        streamsBuilder.stream(inputTopicName, stringSensorTelemetryEventConsumed)
                .process(PreProcessingProcessor::new)
                .flatMapValues((v) -> {
                    final String nodeId = v.getNodeId();
                    final String nodeGroupId = v.getNodeGroupId();
                    final Long timestamp = v.getTimestamp();
                    // final Map<String, Object> extraData1 = v.getExtraData();

                    final List<SensorTelemetryMeasurementEvent> sensorTelemetryMeasurementEventList =
                            new ArrayList<>(v.getMeasurements().size());

                    for (final SensorTelemetryEventMeasurement sensorTelemetryEventMeasurement : v.getMeasurements()) {
                        final String name = sensorTelemetryEventMeasurement.getName();
                        final Double value = sensorTelemetryEventMeasurement.getValue();
                        final String unit = sensorTelemetryEventMeasurement.getUnit();
                        final Map<String, Object> extraData2 = new HashMap<>();

                        final SensorTelemetryMeasurementEvent sensorTelemetryMeasurementEvent =
                                new SensorTelemetryMeasurementEvent(
                                        name, value, unit, nodeId, nodeGroupId, timestamp, extraData2);

                        sensorTelemetryMeasurementEventList.add(sensorTelemetryMeasurementEvent);
                    }

                    return sensorTelemetryMeasurementEventList;
                })
                // .selectKey((k, v) -> v.getNodeId())
                .selectKey((k, v) -> Constants.ANY_NODE)
                .to(outputTopicName, stringSensorTelemetryMeasurementEventProduced);
    }

    /**
     * Creates ingestion topologies for each provided {@link MeasurementType}.
     *
     * @param streamsBuilder the {@link StreamsBuilder} for topology building.
     * @param measurementTypes a {@link List} of {@link MeasurementType}.
     * @see SensorTelemetryMeasurementEvent
     * @see #buildIngestionTopologyForMeasurementTypesDynamically(StreamsBuilder)
     */
    private void buildIngestionTopologyForMeasurementTypes(
            StreamsBuilder streamsBuilder, List<MeasurementType> measurementTypes) {
        final String inputTopicName = Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC;
        final Consumed<String, SensorTelemetryMeasurementEvent> stringSensorTelemetryMeasurementEventConsumed =
                Consumed.with(STRING_SERDE, SENSOR_TELEMETRY_MEASUREMENT_EVENT_SERDE);

        final Produced<String, SensorTelemetryMeasurementEvent> stringSensorTelemetryMeasurementEventProduced =
                Produced.with(STRING_SERDE, SENSOR_TELEMETRY_MEASUREMENT_EVENT_SERDE);

        for (final MeasurementType measurementType : measurementTypes) {
            final String outputTopicName = measurementType.getSensorTelemetryMeasurementEventTopic();

            streamsBuilder.stream(inputTopicName, stringSensorTelemetryMeasurementEventConsumed)
                    .filter((k, v) -> measurementType.getName().equals(v.getName()))
                    .to(outputTopicName, stringSensorTelemetryMeasurementEventProduced);
        }
    }

    /**
     * Creates ingestion topology for each {@link MeasurementType} of records (dynamically).
     *
     * @param streamsBuilder the {@link StreamsBuilder} for topology building.
     * @see #buildIngestionTopologyForMeasurementTypes(StreamsBuilder, List)
     */
    private void buildIngestionTopologyForMeasurementTypesDynamically(StreamsBuilder streamsBuilder) {
        // TODO https://developer.confluent.io/tutorials/dynamic-output-topic/confluent.html

        final String inputTopicName = Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC;
        final Consumed<String, SensorTelemetryMeasurementEvent> stringSensorTelemetryMeasurementEventConsumed =
                Consumed.with(STRING_SERDE, SENSOR_TELEMETRY_MEASUREMENT_EVENT_SERDE);

        final Produced<String, SensorTelemetryMeasurementEvent> stringSensorTelemetryMeasurementEventProduced =
                Produced.with(STRING_SERDE, SENSOR_TELEMETRY_MEASUREMENT_EVENT_SERDE);

        final TopicNameExtractor<String, SensorTelemetryMeasurementEvent> outputTopicNameExtractor =
                (key, value, recordContext) -> {
                    final String name = value.getName().toLowerCase(Locale.ROOT).strip();
                    final String outputTopicName =
                            MeasurementType.valueOf(name).getSensorTelemetryMeasurementEventTopic();
                    // TODO DLQ
                    return outputTopicName;
                };

        streamsBuilder.stream(inputTopicName, stringSensorTelemetryMeasurementEventConsumed)
                .to(outputTopicNameExtractor, stringSensorTelemetryMeasurementEventProduced);
    }

    /**
     * Creates CEP topology for the parametric Scenario 2 as described in the research paper.
     *
     * @param streamsBuilder the {@link StreamsBuilder} for topology building.
     * @param parametersSet the object with the parameters for parameterizing the windowed average calculation logic.
     * @future add names to processing nodes?
     */
    private void buildScenario2ComplexEventProcessingTopology(
            StreamsBuilder streamsBuilder, WindowedAverageMeasurementValueCalculationParametersSet parametersSet) {
        final String microServiceId = parametersSet.getIdentifier();

        final String inputTopicName = parametersSet.getMeasurementType().getSensorTelemetryMeasurementEventTopic();
        final String outputTopicName = parametersSet
                .getMeasurementType()
                .getWindowedAverageMeasurementValueEventTopicWithSuffix(microServiceId);

        // Hopping Time Window (also referred as Sliding Time Window by many platforms).
        final Duration timeWindowDuration = parametersSet.getTimeWindowDuration();
        final Duration timeWindowAdvance = parametersSet.getTimeWindowAdvance();
        TimeWindows windowDefinition = TimeWindows.ofSizeWithNoGrace(timeWindowDuration);
        if (timeWindowAdvance != null) windowDefinition = windowDefinition.advanceBy(timeWindowAdvance);

        // Sliding Time Window (the real one).
        // https://developer.confluent.io/tutorials/sliding-windows/kstreams.html
        // SlidingWindows windowDefinition = SlidingWindows.ofTimeDifferenceWithNoGrace(timeWindowDuration);

        streamsBuilder.stream(
                        inputTopicName,
                        Consumed.with(STRING_SERDE, SENSOR_TELEMETRY_MEASUREMENT_EVENT_SERDE)
                                .withTimestampExtractor(new SensorTelemetryMeasurementEventTimestampExtractor()))
                .groupByKey(Grouped.with(STRING_SERDE, SENSOR_TELEMETRY_MEASUREMENT_EVENT_SERDE))
                .windowedBy(windowDefinition)
                // .emitStrategy(EmitStrategy.onWindowClose())
                // .emitStrategy(EmitStrategy.onWindowUpdate())
                .aggregate(
                        // For each window: initialize the aggregate.
                        // The aggregate is a class that collects the last measurement
                        // from each sensor and calculates the average air temperature.
                        new Initializer<String>() {
                            @Override
                            public String apply() {
                                logger.debug("aggregate.Initialization.apply()");
                                final AverageMeasurementValueAggregate aggregate1 =
                                        new AverageMeasurementValueAggregate();
                                return averageMeasurementValueAggregateToJsonString(aggregate1);
                            }
                        },
                        new Aggregator<String, SensorTelemetryMeasurementEvent, String>() {

                            // Each time a new event occurs in the window, process it.
                            // Processing:
                            //  (1) check if the event is the sensor's last event and add it to the kv map.
                            //  (2) re-calculate the air temperature average based on the kv entries in the map.
                            @Override
                            public String apply(String key, SensorTelemetryMeasurementEvent event, String aggregate) {
                                logger.debug(
                                        "aggregate.Aggregator.apply({}, {}, {})",
                                        event.getNodeId(),
                                        event.getValue(),
                                        Instant.ofEpochMilli(event.getTimestamp()));
                                final AverageMeasurementValueAggregate aggregate2 =
                                        jsonStringToAverageMeasurementValueAggregate(aggregate);
                                //noinspection ConstantConditions
                                aggregate2.addSensorTelemetryMeasurementEvent(event);

                                // TODO Parameter: enabled/disabled.
                                // TODO move out of this topology?
                                for (String name : SENSORS_NAMES) {
                                    if (!aggregate2.isSensorIncluded(name)) {
                                        produceEventToFabricateEvent(name);
                                    }
                                }

                                return averageMeasurementValueAggregateToJsonString(aggregate2);
                            }
                        },
                        // After each processing, store the aggregate for future use.
                        // Aggregate, which holds the state, is materialized,
                        // i.e., stored in a window store for future use by the next event to occur.
                        Materialized.<String, String, WindowStore<Bytes, byte[]>>as(String.format(
                                        "aggregation-window-store-%s", microServiceId.toLowerCase(Locale.ENGLISH)))
                                .withCachingDisabled()
                                .withKeySerde(STRING_SERDE)
                                .withValueSerde(STRING_SERDE))
                // Use `suppress` when EmitStrategy.onWindowUpdate() is active.
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .mapValues((k, v) -> jsonStringToAverageMeasurementValueAggregate(v))
                .toStream()
                .mapValues((k, v) -> new WindowedAverageMeasurementValueEvent(
                        microServiceId,
                        k.window().startTime().toEpochMilli(),
                        k.window().endTime().toEpochMilli(),
                        parametersSet.getMeasurementType().getName(),
                        parametersSet.getMeasurementType().getUnit(),
                        v.getSensorTelemetryMeasurementEvents(),
                        v.getAverage(),
                        v.getTimestamp(),
                        timeWindowDuration.getSeconds(),
                        (timeWindowAdvance == null ? 0L : timeWindowAdvance.getSeconds()),
                        v.getSensorTelemetryMeasurementEvents().size(),
                        parametersSet.getMinimumNumberOfContributingNodes()))
                // TODO Implement Filter for checking `getMinimumNumberOfContributingNodes`? No!
                .selectKey((k, v) -> Constants.ANY_NODE)
                .to(outputTopicName, Produced.with(STRING_SERDE, WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_SERDE));
    }

    /**
     * Builds the merge topology 1 (micro-service to measurement type) of each Scenario 2 parameters set.
     *
     * @param streamsBuilder the {@link StreamsBuilder} for topology building.
     * @param parametersSets a {@link List} with the {@link WindowedAverageMeasurementValueCalculationParametersSet} instances.
     */
    private void buildScenario2MergeTopology1(
            StreamsBuilder streamsBuilder,
            List<WindowedAverageMeasurementValueCalculationParametersSet> parametersSets) {
        final Consumed<String, WindowedAverageMeasurementValueEvent> consumed =
                Consumed.with(STRING_SERDE, WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_SERDE);
        final Produced<String, WindowedAverageMeasurementValueEvent> produced =
                Produced.with(STRING_SERDE, WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_SERDE);

        for (final WindowedAverageMeasurementValueCalculationParametersSet parametersSet : parametersSets) {
            final String microServiceId = parametersSet.getIdentifier();
            final String inputTopicName = parametersSet
                    .getMeasurementType()
                    .getWindowedAverageMeasurementValueEventTopicWithSuffix(microServiceId);
            final String outputTopicName =
                    parametersSet.getMeasurementType().getWindowedAverageMeasurementValueEventTopic();

            streamsBuilder.stream(inputTopicName, consumed).to(outputTopicName, produced);
        }
    }

    /**
     * Builds the merge topology 2 (measurement type to all) of each Scenario 2 parameters set.
     *
     * @param streamsBuilder the {@link StreamsBuilder} for topology building.
     * @param measurementTypes a {@link List} with the {@link MeasurementType} instances.
     */
    private void buildScenario2MergeTopology2(StreamsBuilder streamsBuilder, List<MeasurementType> measurementTypes) {
        final Consumed<String, WindowedAverageMeasurementValueEvent> consumed =
                Consumed.with(STRING_SERDE, WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_SERDE);
        final Produced<String, WindowedAverageMeasurementValueEvent> produced =
                Produced.with(STRING_SERDE, WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_SERDE);

        for (final MeasurementType measurementType : measurementTypes) {
            final String inputTopicName = measurementType.getWindowedAverageMeasurementValueEventTopic();
            final String outputTopicName = Constants.WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_TOPIC;

            streamsBuilder.stream(inputTopicName, consumed).to(outputTopicName, produced);
        }
    }

    private void buildDebugTopology1(StreamsBuilder streamsBuilder) {
        final String inputTopicName = Constants.WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_TOPIC;

        final Consumed<String, WindowedAverageMeasurementValueEvent> consumed =
                Consumed.with(STRING_SERDE, WINDOWED_AVERAGE_MEASUREMENT_VALUE_EVENT_SERDE);

        streamsBuilder.stream(inputTopicName, consumed).foreach((k, v) -> {
            System.out.println(v.describe());
            // System.out.println(v.toString());
        });
    }

    /* ------------ Beans ------------ */

    /**
     * Creates topics for each {@link WindowedAverageMeasurementValueCalculationParametersSet} instance.
     *
     * @return {@link NewTopics} instance used by Kafka admin to create the corresponding topics.
     */
    @Bean
    private NewTopics newKafkaTopicsForScenario2() {
        final List<String> topicNameList = new ArrayList<>();

        for (final WindowedAverageMeasurementValueCalculationParametersSet parametersSet : PARAMETERS_SETS) {
            final String microServiceId = parametersSet.getIdentifier();
            final String topicName = parametersSet
                    .getMeasurementType()
                    .getWindowedAverageMeasurementValueEventTopicWithSuffix(microServiceId);
            topicNameList.add(topicName);
        }

        return new NewTopics(topicNameList.stream()
                .map((name) -> TopicBuilder.name(name).partitions(1).replicas(1).build())
                .toArray(NewTopic[]::new));
    }

    @Autowired
    public void build(StreamsBuilder streamsBuilder) {
        final List<WindowedAverageMeasurementValueCalculationParametersSet> parametersSets = PARAMETERS_SETS;
        final List<MeasurementType> measurementTypes = parametersSets.stream()
                .map(WindowedAverageMeasurementValueCalculationParametersSet::getMeasurementType)
                .distinct()
                .collect(Collectors.toList());

        this.buildIngestionTopology(streamsBuilder);
        // this.buildIngestionTopologyForMeasurementTypes(streamsBuilder, measurementTypes);
        this.buildIngestionTopologyForMeasurementTypesDynamically(streamsBuilder);
        for (final WindowedAverageMeasurementValueCalculationParametersSet parametersSet : PARAMETERS_SETS) {
            buildScenario2ComplexEventProcessingTopology(streamsBuilder, parametersSet);
        }
        this.buildScenario2MergeTopology1(streamsBuilder, parametersSets);
        this.buildScenario2MergeTopology2(streamsBuilder, measurementTypes);

        this.buildDebugTopology1(streamsBuilder);

        final Topology topology = streamsBuilder.build();
        logger.info("topology.describe:\n{}", topology.describe());
    }

    /* ------------ Class ------------ */

    /**
     * Parameters set for building a topology for windowed calculation of average value from a minimum allowed number of contributing nodes.
     */
    public static final class WindowedAverageMeasurementValueCalculationParametersSet {

        /**
         * The {@link MeasurementType} to ingest and process.
         */
        private final MeasurementType measurementType;

        /**
         * The duration of the time window.
         */
        private final Duration timeWindowDuration;

        /**
         * The advance duration of the time window. If it's {@code null} a normal time window will be created, otherwise a hopping time window will be created.
         */
        @Nullable
        private final Duration timeWindowAdvance;

        /**
         * The minimum allowed number of contributing nodes to the calculation of the average value in the specified time window.
         */
        private final int minimumNumberOfContributingNodes;

        /* ------------ Constructors ------------ */

        public WindowedAverageMeasurementValueCalculationParametersSet(
                MeasurementType measurementType,
                Duration timeWindowDuration,
                @Nullable Duration timeWindowAdvance,
                int minimumNumberOfContributingNodes) {
            this.measurementType = measurementType;
            this.timeWindowDuration = timeWindowDuration;
            this.timeWindowAdvance = timeWindowAdvance;
            this.minimumNumberOfContributingNodes = minimumNumberOfContributingNodes;
        }

        /* ------------ Getters ------------ */

        public MeasurementType getMeasurementType() {
            return measurementType;
        }

        public Duration getTimeWindowDuration() {
            return timeWindowDuration;
        }

        @Nullable
        public Duration getTimeWindowAdvance() {
            return timeWindowAdvance;
        }

        public int getMinimumNumberOfContributingNodes() {
            return minimumNumberOfContributingNodes;
        }

        /* ------------ Getters (special) ------------ */

        public String getIdentifier() {
            final StringBuffer sb = new StringBuffer("w_avg_");
            sb.append(this.measurementType.getName());
            sb.append("_");
            sb.append(this.timeWindowDuration.toSeconds());
            sb.append("_");
            sb.append(this.timeWindowAdvance == null ? "null" : this.timeWindowAdvance.getSeconds());
            sb.append("_");
            sb.append(this.minimumNumberOfContributingNodes);
            return sb.toString();
        }

        /* ------------ Overrides ------------ */

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WindowedAverageMeasurementValueCalculationParametersSet that =
                    (WindowedAverageMeasurementValueCalculationParametersSet) o;
            return minimumNumberOfContributingNodes == that.minimumNumberOfContributingNodes
                    && measurementType == that.measurementType
                    && Objects.equals(timeWindowDuration, that.timeWindowDuration)
                    && Objects.equals(timeWindowAdvance, that.timeWindowAdvance);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    measurementType, timeWindowDuration, timeWindowAdvance, minimumNumberOfContributingNodes);
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("WindowedAverageMeasurementValueCalculationParametersSet{");
            sb.append("measurementType=").append(measurementType);
            sb.append(", timeWindowDuration=").append(timeWindowDuration);
            sb.append(", timeWindowAdvance=").append(timeWindowAdvance);
            sb.append(", minimumNumberOfContributingNodes=").append(minimumNumberOfContributingNodes);
            sb.append('}');
            return sb.toString();
        }
    }
}
