package org.softwareforce.iotvm.eventengine.cep.ct;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.FixedSizeTimeWindowSpec;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.FixedSizeTimeWindowSpec.PastFixedSizeTimeWindowsLimits;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.ValidNonNullTimestampExtractor;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.averagecalculation.AggregatorImpl;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.averagecalculation.InitializerImpl;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceServiceImpl;
import org.softwareforce.iotvm.shared.event.IdentifiersIBO;
import org.softwareforce.iotvm.shared.event.QualityPropertiesIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageAggregateIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.TimestampsIBO;

/**
 * Composite Transformation for average calculation.
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
  private final IBOPersistenceServiceImpl iboPersistenceService;

  /* ------------ Constructors ------------ */

  public AverageCalculationCompositeTransformationFactory(
      AverageCalculationCompositeTransformationParameters parameters,
      IBOPersistenceServiceImpl iboPersistenceService) {
    this.parameters = parameters;
    this.fixedSizeTimeWindowSpec =
        new FixedSizeTimeWindowSpec(
            this.parameters.getTimeWindowSize(),
            this.parameters.getTimeWindowGrace(),
            this.parameters.getTimeWindowAdvance());
    this.iboPersistenceService = iboPersistenceService;
  }

  /* ------------ Quality Properties Calculators ------------ */

  @SuppressWarnings("RedundantCast")
  private double calculateCompleteness(List<SensorTelemetryMeasurementEventIBO> eventList) {
    double real = (double) this.parameters.getMinimumNumberOfContributingSensors();
    double expected = (double) eventList.size();
    return 1d - ((real - expected) / real);
  }

  @SuppressWarnings("RedundantCast")
  private double calculateTimeliness(
      Long startTimestamp, Long endTimestamp, List<SensorTelemetryMeasurementEventIBO> eventList) {
    if (eventList.isEmpty()) {
      return 0.0d;
    }
    double real = (double) eventList.size();
    double timely = 0.0D;
    for (final SensorTelemetryMeasurementEventIBO event : eventList) {
      if (event.getTimestamps().getDefaultTimestamp() >= startTimestamp
          && event.getTimestamps().getDefaultTimestamp() <= endTimestamp) {
        timely = timely + 1D;
      }
    }
    return 1d - ((real - timely) / real);
  }

  /* ------------ Internals ------------ */

  private StreamsBuilder buildReporting(final String topicName, StreamsBuilder streamsBuilder) {
    streamsBuilder.stream(
            topicName,
            Consumed.with(
                    Constants.STRING_SERDE,
                    Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE)
                .withTimestampExtractor(new ValidNonNullTimestampExtractor()))
        .foreach(
            (key, value) -> {
              double lagInSeconds =
                  (Instant.now().toEpochMilli() - value.getEndTimestamp()) / 1000D;
              LOGGER.info(
                  """
                  Mapping SensorTelemetryMeasurementsAverageAggregateIBO to SensorTelemetryMeasurementsAverageEventIBO
                    {} - {}
                    {} - {}
                    {} {}
                    {} sensors : {}
                    lag : {}
                    completeness : {}
                    timeliness   : {}
                  """,
                  Instant.ofEpochMilli(value.getStartTimestamp()),
                  Instant.ofEpochMilli(value.getEndTimestamp()),
                  value.getStartTimestamp(),
                  value.getEndTimestamp(),
                  value.getAverage().getName(),
                  value.getAverage().getValue(),
                  value.getEvents().size(),
                  value.getEvents().values().stream()
                      .map(SensorTelemetryMeasurementEventIBO::getSensorId)
                      .collect(Collectors.joining(", ")),
                  lagInSeconds,
                  value.getQualityProperties().getCompleteness(),
                  value.getQualityProperties().getTimeliness());
            });

    return streamsBuilder;
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
        "Request to provision {} {} composite transformation",
        this.getUniqueIdentifier(),
        this.getName());

    final PhysicalQuantity physicalQuantity = this.parameters.getPhysicalQuantity();
    final String parametersUniqueIdentifier = this.parameters.getUniqueIdentifier();
    // TODO I need a short version of identifiers for directories
    //  because windows does not allow more than 260 chars.

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
                // .withCachingDisabled()
                .withKeySerde(Constants.STRING_SERDE)
                .withValueSerde(
                    Constants.SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_AGGREGATE_IBO_SERDE);

    streamsBuilder.stream(
            inputTopicName,
            Consumed.with(
                    Constants.STRING_SERDE, Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE)
                .withTimestampExtractor(new ValidNonNullTimestampExtractor()))
        .groupByKey(
            Grouped.with(
                Constants.STRING_SERDE, Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE))
        .windowedBy(timeWindowsDefinition)
        // .emitStrategy(EmitStrategy.onWindowClose())
        // .emitStrategy(EmitStrategy.onWindowUpdate())
        .aggregate(new InitializerImpl(physicalQuantity), new AggregatorImpl(), materialized)
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .toStream()
        .mapValues(
            (key, value) -> {
              double lagInSeconds =
                  (Instant.now().toEpochMilli() - key.window().endTime().toEpochMilli()) / 1000D;
              LOGGER.trace(
                  """
                  Mapping SensorTelemetryMeasurementsAverageAggregateIBO to SensorTelemetryMeasurementsAverageEventIBO
                    {} - {}
                    {} - {}
                    {} {}
                    {} sensors : {}
                    lag : {}
                  """,
                  key.window().startTime(),
                  key.window().endTime(),
                  key.window().start(),
                  key.window().end(),
                  value.getAverage().getName(),
                  value.getAverage().getValue(),
                  value.getEvents().size(),
                  value.getEvents().values().stream()
                      .map(SensorTelemetryMeasurementEventIBO::getSensorId)
                      .collect(Collectors.joining(", ")),
                  lagInSeconds);

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

              final double completeness =
                  this.calculateCompleteness(value.getEvents().values().stream().toList());
              final double timeliness =
                  this.calculateTimeliness(
                      key.window().startTime().toEpochMilli(),
                      key.window().endTime().toEpochMilli(),
                      value.getEvents().values().stream().toList());
              final QualityPropertiesIBO qualityPropertiesIBO =
                  QualityPropertiesIBO.newBuilder()
                      .setCompleteness(completeness)
                      .setTimeliness(timeliness)
                      .build();

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
                  .setAdditional(value.getAdditional())
                  .build();
            })
        .mapValues(
            (key, value) -> {
              value.getAdditional().put("softSensingActivated", false);

              if (value.getQualityProperties().getCompleteness() >= 1) {
                return value;
              }

              final int pastWindowsLookup = this.parameters.getPastWindowsLookup();

              if (pastWindowsLookup <= 0) {
                return value;
              }

              value.getAdditional().put("softSensingActivated", true);

              final List<String> missingSensorIds = new ArrayList<>();

              for (final String sensorId : Constants.SENSOR_IDS) {
                if (!value.getEvents().containsKey(sensorId)) {
                  missingSensorIds.add(sensorId);
                }
              }

              // TODO Make sure that 10 millis is safe to subtract.
              final long timestamp =
                  key.window().endTime().minus(10, ChronoUnit.MILLIS).toEpochMilli();
              final PastFixedSizeTimeWindowsLimits pastFixedSizeTimeWindowsLimits =
                  this.fixedSizeTimeWindowSpec.calculatePastWindowsLimits(
                      timestamp, pastWindowsLookup);
              final long startTimestamp = pastFixedSizeTimeWindowsLimits.getStart().toEpochMilli();
              final long endTimestamp = pastFixedSizeTimeWindowsLimits.getEnd().toEpochMilli();

              for (final String sensorId : missingSensorIds) {
                final Optional<SensorTelemetryMeasurementEventIBO> result =
                    this.iboPersistenceService.findPastSensorTelemetryMeasurementEventIBO(
                        physicalQuantity, sensorId, startTimestamp, endTimestamp);
                if (result.isEmpty()) {
                  LOGGER.trace(
                      "Could not find {} SensorTelemetryMeasurementEventIBO for sensor with ID : {}"
                          + " between {} and {}",
                      physicalQuantity.name(),
                      sensorId,
                      startTimestamp,
                      endTimestamp);
                  continue;
                }
                value.getEvents().put(sensorId, result.get());
              }

              value
                  .getAdditional()
                  .put("completenessReal", value.getQualityProperties().getCompleteness());
              value
                  .getAdditional()
                  .put("timelinessReal", value.getQualityProperties().getTimeliness());

              final double completeness =
                  this.calculateCompleteness(value.getEvents().values().stream().toList());
              final double timeliness =
                  this.calculateTimeliness(
                      key.window().startTime().toEpochMilli(),
                      key.window().endTime().toEpochMilli(),
                      value.getEvents().values().stream().toList());

              value.getQualityProperties().setCompleteness(completeness);
              value.getQualityProperties().setTimeliness(timeliness);

              return value;
            })
        .filter(
            (key, value) -> {
              if (this.parameters.isIgnoreCompletenessFiltering()) {
                return true;
              }
              return value.getQualityProperties().getCompleteness() >= 1;
            })
        .mapValues((value) -> this.iboPersistenceService.saveAlt(outputTopicName, value))
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
