package org.softwareforce.iotvm.eventengine.cep.ct;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.CalculationUtils;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.FixedSizeTimeWindowSpec;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.ValidNonNullTimestampExtractor;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.averagecalculation.AggregatorImpl;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.averagecalculation.InitializerImpl;
import org.softwareforce.iotvm.eventengine.cep.fabrication.EventFabricationMapper;
import org.softwareforce.iotvm.eventengine.cep.fabrication.EventFabricationMethod;
import org.softwareforce.iotvm.eventengine.cep.fabrication.EventFabricationService;
import org.softwareforce.iotvm.eventengine.cep.fabrication.InputEvent;
import org.softwareforce.iotvm.eventengine.cep.fabrication.OutputEvent;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceService;
import org.softwareforce.iotvm.shared.event.IdentifiersIBO;
import org.softwareforce.iotvm.shared.event.QualityPropertiesIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageAggregateIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.TimestampsIBO;

/**
 * Composite Transformation for average calculation.
 *
 * <p>TODO Future Implementations:
 *
 * <ul>
 *   <li>Topics names, IDs, etc, are just too big for Windows file system (260 chars). Add to
 *       limitations
 *   <li>Check if {@code TimestampExtractorUtil} works well and how it affects the ordering of the
 *       events. For instance, a utility that checks if all timestamps in {@code TimestampsIBO}
 *       belong to the same time-window would be extremely useful.
 *   <li>I should add contextual information to {@code additional} map like registered sensors and
 *       more.
 *   <li>Create custom Kafka Streams processor for event fabrication (inside fabrication package).
 *   <li>Do we need a {@code StreamTimeTickEnforcer}?
 * </ul>
 *
 * @author Dimitris Gkoulis
 */
public class AverageCalculationCompositeTransformationFactory
    extends CompositeTransformationFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AverageCalculationCompositeTransformationFactory.class);
  private static final String NAME = "average_calculation";

  private final AverageCalculationCompositeTransformationParameters parameters;
  private final FixedSizeTimeWindowSpec fixedSizeTimeWindowSpec;
  private final IBOPersistenceService iboPersistenceService;
  private final EventFabricationService eventFabricationService;

  /* ------------ Constructors ------------ */

  public AverageCalculationCompositeTransformationFactory(
      AverageCalculationCompositeTransformationParameters parameters,
      IBOPersistenceService iboPersistenceService) {
    this.parameters = parameters;
    this.fixedSizeTimeWindowSpec =
        new FixedSizeTimeWindowSpec(
            this.parameters.getTimeWindowSize(),
            this.parameters.getTimeWindowGrace(),
            this.parameters.getTimeWindowAdvance());
    this.iboPersistenceService = iboPersistenceService;
    this.eventFabricationService =
        new EventFabricationService(
            this.parameters.getTimeWindowSize().toMillis(), new HashSet<>(Constants.SENSOR_IDS));
  }

  /* ------------ Internals ------------ */

  private static QualityPropertiesIBO calculateAndGetQualityPropertiesIBO(
      final long timeWindowStartTimestampMs,
      final long timeWindowEndTimestampMs,
      final long timeWindowSizeMs,
      final int minimumNumberOfContributingSensors,
      final int fabricationPastEventsStepsBehind,
      final int fabricationForecastingStepsAhead,
      final List<SensorTelemetryMeasurementEventIBO> eventIBOList) {
    final double completeness1 =
        CalculationUtils.calculateCompleteness1(minimumNumberOfContributingSensors, eventIBOList);
    final double timeliness1 =
        CalculationUtils.calculateTimeliness1(
            timeWindowStartTimestampMs, timeWindowEndTimestampMs, eventIBOList);

    final Map<String, Long> maxDistances = new HashMap<>();
    maxDistances.put(EventFabricationMethod.NAIVE.name(), (long) fabricationPastEventsStepsBehind);
    maxDistances.put(
        EventFabricationMethod.SIMPLE_EXPONENTIAL_SMOOTHING.name(),
        (long) fabricationForecastingStepsAhead);
    maxDistances.put(
        EventFabricationMethod.EXPONENTIAL_SMOOTHING_WITH_LINEAR_TREND.name(),
        (long) fabricationForecastingStepsAhead);
    final long defaultMaxDistance = 0;

    final Map<String, Double> alphas = new HashMap<>();
    alphas.put(EventFabricationMethod.NAIVE.name(), 0.8);
    alphas.put(EventFabricationMethod.SIMPLE_EXPONENTIAL_SMOOTHING.name(), 0.9);
    alphas.put(EventFabricationMethod.EXPONENTIAL_SMOOTHING_WITH_LINEAR_TREND.name(), 0.9);
    final double defaultAlpha = 0.0;

    for (final EventFabricationMethod method : EventFabricationMethod.values()) {
      Preconditions.checkState(maxDistances.containsKey(method.name()));
      Preconditions.checkState(alphas.containsKey(method.name()));
    }

    final double timeliness2 =
        CalculationUtils.calculateTimeliness(
            timeWindowStartTimestampMs,
            timeWindowEndTimestampMs,
            timeWindowSizeMs,
            maxDistances,
            defaultMaxDistance,
            alphas,
            defaultAlpha,
            eventIBOList);

    final Map<String, Double> metrics = new HashMap<>();

    metrics.put("completeness1", completeness1);
    metrics.put("timeliness1", timeliness1);
    metrics.put("timeliness2", timeliness2);

    return QualityPropertiesIBO.newBuilder().setMetrics(metrics).build();
  }

  /* ------------ Implementation ------------ */

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public CompositeTransformationParameters getParameters() {
    return this.parameters;
  }

  @Override
  public StreamsBuilder build(StreamsBuilder streamsBuilder) {
    LOGGER.debug(
        "Request to build {} {} composite transformation",
        this.getUniqueIdentifier(),
        this.getName());

    final PhysicalQuantity physicalQuantity = this.parameters.getPhysicalQuantity();
    final String parametersUniqueIdentifier = this.parameters.getUniqueIdentifier();

    final String inputTopicName =
        Constants.getSensorTelemetryMeasurementEventTopic(physicalQuantity);
    final String outputTopicName =
        Constants.getSensorTelemetryMeasurementsAverageEventTopic(
            physicalQuantity, parametersUniqueIdentifier);

    final TimeWindows timeWindowsDefinition = this.fixedSizeTimeWindowSpec.getTimeWindows();

    // After each processing, store the aggregate for future use.
    // Aggregate, which holds the state, is materialized,
    // i.e., stored in a window store for future use by the next event to occur.
    final Materialized<
            String, SensorTelemetryMeasurementsAverageAggregateIBO, WindowStore<Bytes, byte[]>>
        materialized =
            Materialized
                .<String, SensorTelemetryMeasurementsAverageAggregateIBO,
                    WindowStore<Bytes, byte[]>>
                    as(String.format("aggregation-window-store-%s", parametersUniqueIdentifier))
                .withCachingEnabled()
                // .withCachingDisabled()  # TODO Check.
                .withKeySerde(Constants.STRING_SERDE)
                .withValueSerde(
                    Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_AGGREGATE_IBO_SERDE);

    streamsBuilder.stream(
            inputTopicName,
            Consumed.with(
                    Constants.STRING_SERDE, Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE)
                .withTimestampExtractor(new ValidNonNullTimestampExtractor()))
        // @future select key: greenhouse ID -> then groupByKey
        .groupByKey(
            Grouped.with(
                Constants.STRING_SERDE, Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE))
        .windowedBy(timeWindowsDefinition)
        // .emitStrategy(EmitStrategy.onWindowClose())
        // .emitStrategy(EmitStrategy.onWindowUpdate())
        // TODO try without named (materialized)
        .aggregate(new InitializerImpl(physicalQuantity), new AggregatorImpl(), materialized)
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .toStream()
        .mapValues(
            (key, value) -> {

              // Timestamps.
              // --------------------------------------------------

              final Long now = Instant.now().toEpochMilli();
              final Map<String, Long> timestamps = new HashMap<>();
              timestamps.put("mapValues", now);
              timestamps.put("timeWindowStartTimestamp", key.window().startTime().toEpochMilli());
              timestamps.put("timeWindowEndTimestamp", key.window().endTime().toEpochMilli());
              timestamps.put(
                  "aggregationInitialization",
                  value.getTimestamps().getTimestamps().get("aggregationInitialization"));
              timestamps.put(
                  "firstAggregationApplication",
                  value.getTimestamps().getTimestamps().get("firstAggregationApplication"));
              timestamps.put(
                  "lastAggregationApplication",
                  value.getTimestamps().getTimestamps().get("lastAggregationApplication"));
              final TimestampsIBO timestampsIBO =
                  TimestampsIBO.newBuilder()
                      .setDefaultTimestamp(now)
                      .setTimestamps(timestamps)
                      .build();

              // Quality Properties.
              // --------------------------------------------------

              final QualityPropertiesIBO qualityPropertiesIBO =
                  calculateAndGetQualityPropertiesIBO(
                      key.window().start(),
                      key.window().end(),
                      this.fixedSizeTimeWindowSpec.getTimeWindowSize().toMillis(),
                      this.parameters.getMinimumNumberOfContributingSensors(),
                      this.parameters.getPastWindowsLookup(),
                      this.parameters.getFutureWindowsLookupAlternative(),
                      value.getEvents().values().stream().toList());

              // Additional: values before event fabrication.
              // --------------------------------------------------

              final Map<String, Object> additional = value.getAdditional();
              additional.put("averageValueBeforeEventFabrication", value.getAverage().getValue());
              for (final String metricKey : qualityPropertiesIBO.getMetrics().keySet()) {
                additional.put(
                    metricKey + "BeforeEventFabrication",
                    qualityPropertiesIBO.getMetrics().get(metricKey));
              }

              // Complex Event instantiation.
              // --------------------------------------------------

              return SensorTelemetryMeasurementsAverageEventIBO.newBuilder()
                  .setCompositeTransformationName(this.getName())
                  .setCompositeTransformationIdentifier(this.getUniqueIdentifier())
                  .setCompositeTransformationParametersIdentifier(
                      this.parameters.getUniqueIdentifier())
                  .setStartTimestamp(key.window().startTime().toEpochMilli())
                  .setEndTimestamp(key.window().endTime().toEpochMilli())
                  .setAverage(value.getAverage())
                  .setEvents(value.getEvents())
                  .setQualityProperties(qualityPropertiesIBO)
                  .setTimestamps(timestampsIBO)
                  .setIdentifiers(
                      IdentifiersIBO.newBuilder()
                          .setClientSideId(UUID.randomUUID().toString())
                          .setCorrelationIds(new HashMap<>())
                          .build())
                  .setAdditional(additional)
                  .build();
            })
        .mapValues(
            (key, value) -> {
              final Map<String, Object> additional = value.getAdditional();
              additional.put("eventFabricationDuration", 0L);
              additional.put("eventFabricationNaiveCount", 0L);
              additional.put("eventFabricationSESCount", 0L);
              additional.put("eventFabricationESLTCount", 0L);
              additional.put("eventFabricationCount", 0L);

              // Update Time-Series structures.
              // --------------------------------------------------

              final List<InputEvent> inputEventList = new ArrayList<>();
              for (final SensorTelemetryMeasurementEventIBO ibo : value.getEvents().values()) {
                this.eventFabricationService.updateTimeWindowedTimeSeries(
                    ibo.getSensorId(),
                    ibo.getMeasurement().getValue(),
                    ibo.getTimestamps().getDefaultTimestamp());
                inputEventList.add(
                    new InputEvent(
                        ibo.getSensorId(),
                        ibo.getMeasurement().getValue(),
                        ibo.getTimestamps().getDefaultTimestamp()));
              }

              // Check completeness.
              // --------------------------------------------------

              final Double completeness1 =
                  value.getQualityProperties().getMetrics().get("completeness1");
              Preconditions.checkState(completeness1 != null, "completeness1 cannot be null!");
              if (completeness1 >= 1) {
                return value;
              }
              if (this.parameters.getPastWindowsLookup() == 0
                  && this.parameters.getFutureWindowsLookup() == 0) {
                return value;
              }

              // Perform event fabrication.
              // --------------------------------------------------

              final long startNs = System.nanoTime();
              final Set<OutputEvent> outputEventSet =
                  this.eventFabricationService.performEventFabrication(
                      inputEventList,
                      key.window().start(),
                      this.parameters.getMinimumNumberOfContributingSensors(),
                      this.parameters.getPastWindowsLookup(),
                      this.parameters.getFutureWindowsLookup());
              final long endNs = System.nanoTime();
              final long durationNs = endNs - startNs;

              long count = 0L;
              long naiveCount = 0L;
              long sesCount = 0L;
              long esltCount = 0L;

              // Add fabricated events to complex event.
              // --------------------------------------------------

              for (final OutputEvent outputEvent : outputEventSet) {
                final SensorTelemetryMeasurementEventIBO sensorTelemetryMeasurementEventIBO =
                    EventFabricationMapper.map(outputEvent, this.parameters.getPhysicalQuantity());
                assert !value
                    .getEvents()
                    .containsKey(sensorTelemetryMeasurementEventIBO.getSensorId());

                value
                    .getEvents()
                    .put(
                        sensorTelemetryMeasurementEventIBO.getSensorId(),
                        sensorTelemetryMeasurementEventIBO);

                count++;
                if (EventFabricationMethod.NAIVE.equals(outputEvent.getMethod())) {
                  naiveCount++;
                } else if (EventFabricationMethod.SIMPLE_EXPONENTIAL_SMOOTHING.equals(
                    outputEvent.getMethod())) {
                  sesCount++;
                } else if (EventFabricationMethod.EXPONENTIAL_SMOOTHING_WITH_LINEAR_TREND.equals(
                    outputEvent.getMethod())) {
                  esltCount++;
                }
              }

              // Re-calculate average.
              // --------------------------------------------------

              value
                  .getAverage()
                  .setValue(
                      CalculationUtils.calculateAverage(
                              value.getEvents().values().stream().toList())
                          .orElse((double) Short.MIN_VALUE));

              // Re-calculate quality properties.
              // --------------------------------------------------

              final QualityPropertiesIBO qualityPropertiesIBO =
                  calculateAndGetQualityPropertiesIBO(
                      key.window().start(),
                      key.window().end(),
                      this.fixedSizeTimeWindowSpec.getTimeWindowSize().toMillis(),
                      parameters.getMinimumNumberOfContributingSensors(),
                      parameters.getPastWindowsLookup(),
                      parameters.getFutureWindowsLookupAlternative(),
                      value.getEvents().values().stream().toList());
              value.setQualityProperties(qualityPropertiesIBO);

              // Update additional.
              // --------------------------------------------------

              additional.put("eventFabricationDuration", durationNs);
              additional.put("eventFabricationNaiveCount", naiveCount);
              additional.put("eventFabricationSESCount", sesCount);
              additional.put("eventFabricationESLTCount", esltCount);
              additional.put("eventFabricationCount", count);

              value.setAdditional(additional);

              return value;
            })
        .filter(
            (key, value) -> {
              if (this.parameters.isIgnoreCompletenessFiltering()) {
                return true;
              }
              final Double completeness1 =
                  value.getQualityProperties().getMetrics().get("completeness1");
              Preconditions.checkState(completeness1 != null, "completeness1 cannot be null!");
              return completeness1 >= 1;
            })
        // Persistence.
        .mapValues((value) -> this.iboPersistenceService.insert(outputTopicName, value))
        // @future Key can be the greenhouse ID.
        .selectKey((k, v) -> Constants.ANY_SENSOR)
        .to(
            outputTopicName,
            Produced.with(
                Constants.STRING_SERDE,
                Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE));

    return streamsBuilder;
  }
}
