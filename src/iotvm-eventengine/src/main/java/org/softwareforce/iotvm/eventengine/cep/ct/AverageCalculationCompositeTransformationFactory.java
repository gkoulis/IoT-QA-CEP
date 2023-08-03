package org.softwareforce.iotvm.eventengine.cep.ct;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.FabricationValueMapperWithKey;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.FixedSizeTimeWindowSpec;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.FixedSizeTimeWindowSpec.PastFixedSizeTimeWindowsLimits;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.ValidNonNullTimestampExtractor;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.averagecalculation.AggregatorImpl;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.averagecalculation.InitializerImpl;
import org.softwareforce.iotvm.eventengine.experimental.SensorTelemetryMeasurementEventForecastingService;
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
  private final SensorTelemetryMeasurementEventForecastingService
      sensorTelemetryMeasurementEventForecastingService;

  /* ------------ Constructors ------------ */

  public AverageCalculationCompositeTransformationFactory(
      AverageCalculationCompositeTransformationParameters parameters,
      IBOPersistenceServiceImpl iboPersistenceService,
      SensorTelemetryMeasurementEventForecastingService
          sensorTelemetryMeasurementEventForecastingService) {
    this.parameters = parameters;
    this.fixedSizeTimeWindowSpec =
        new FixedSizeTimeWindowSpec(
            this.parameters.getTimeWindowSize(),
            this.parameters.getTimeWindowGrace(),
            this.parameters.getTimeWindowAdvance());
    this.iboPersistenceService = iboPersistenceService;
    this.sensorTelemetryMeasurementEventForecastingService =
        sensorTelemetryMeasurementEventForecastingService;
  }

  /* ------------ Quality Properties Calculators ------------ */

  // TODO Move to utils (CalculationUtils)
  private Optional<Double> calculateAverage(List<SensorTelemetryMeasurementEventIBO> eventList) {
    final int size = eventList.size();
    if (size == 0) {
      return Optional.empty();
    }
    final double sum = eventList.stream().mapToDouble(i -> i.getMeasurement().getValue()).sum();
    final double average = sum / size;
    return Optional.of(average);
  }

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

  @SuppressWarnings("RedundantCast")
  private double calculateTimelinessAlt(
      Long startTimestamp, Long endTimestamp, List<SensorTelemetryMeasurementEventIBO> eventList) {

    // TODO -1 or +1?
    final long diff = (endTimestamp - startTimestamp) + 1;
    final List<Long> changePoints = new ArrayList<>();
    for (int i = 1; i <= parameters.getPastWindowsLookup(); i++) {
      changePoints.add(startTimestamp - (diff * i));
    }

    // TODO Remove.
    System.out.println(diff);
    System.out.println(changePoints);

    if (eventList.isEmpty()) {
      return 0.0d;
    }
    double real = (double) eventList.size();
    double timely = 0.0D;
    for (final SensorTelemetryMeasurementEventIBO event : eventList) {
      // TODO γίνεται και πιο απλό -> βάλε το τωρινό time window στα changepoints (με index 0).
      if (event.getTimestamps().getDefaultTimestamp() >= startTimestamp
          && event.getTimestamps().getDefaultTimestamp() <= endTimestamp) {
        timely = timely + 1D;
      } else {
        double temp = 0D;
        int index = 1;
        for (final long changePoint : changePoints) {
          // TODO Remove.
          System.out.println("CHECKING CHANGEPOINT : " + changePoint + " (for " + event.getTimestamps().getDefaultTimestamp() + ")");
          if (event.getTimestamps().getDefaultTimestamp() < changePoint) {
            index++;
            continue;
          }
          // TODO Remove.
          System.out.println("CHANGEPOINT DETECTED: " + changePoint);
          temp = 1D - ((1D / parameters.getPastWindowsLookup()) * index);
          // TODO Remove.
          System.out.println("RESULT: " + temp);
          break;
        }
        timely = timely + temp;
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
        // @future select key: greenhouse ID -> then groupByKey
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
              final double timelinessAtl =
                  this.calculateTimelinessAlt(
                      key.window().startTime().toEpochMilli(),
                      key.window().endTime().toEpochMilli(),
                      value.getEvents().values().stream().toList());
              final QualityPropertiesIBO qualityPropertiesIBO =
                  QualityPropertiesIBO.newBuilder()
                      .setCompleteness(completeness)
                      .setTimeliness(timeliness)
                      .build();

              final Map<String, Object> additional = value.getAdditional();
              // TODO Add to quality properties!
              // TODO (and refactor e.g. store previous value before fabrication etc.)
              additional.put("timelinessAlt", timelinessAtl);

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
        // Fabrication: past events (name: "pastEvents").
        .mapValues(
            new FabricationValueMapperWithKey() {
              @Override
              public String name() {
                return "pastEvents";
              }

              @Override
              public boolean shouldApply(
                  Windowed<String> readOnlyKey, SensorTelemetryMeasurementsAverageEventIBO value) {
                if (value.getQualityProperties().getCompleteness() >= 1) {
                  return false;
                }
                if (parameters.getPastWindowsLookup() <= 0) {
                  return false;
                }
                //noinspection RedundantIfStatement
                if (value.getEvents().isEmpty()) {
                  return false;
                }
                return true;
              }

              @Override
              public SensorTelemetryMeasurementsAverageEventIBO applyReal(
                  Windowed<String> key, SensorTelemetryMeasurementsAverageEventIBO value) {
                Preconditions.checkState(
                    value.getEvents().size() < parameters.getMinimumNumberOfContributingSensors(),
                    "events.size must be less than minimumNumberOfContributingSensors");

                // Get missing sensors IDs.
                final List<String> missingSensorIds = new ArrayList<>();
                for (final String sensorId : Constants.SENSOR_IDS) {
                  if (!value.getEvents().containsKey(sensorId)) {
                    missingSensorIds.add(sensorId);
                  }
                }
                Preconditions.checkState(
                    missingSensorIds.size() > 0, "missingSensorIds.size must be greater than zero");

                // How many events required to get completeness of 1?
                final int missing = missingSensorIds.size();
                final int required =
                    parameters.getMinimumNumberOfContributingSensors() - value.getEvents().size();
                int found = 0;
                int notFound = 0;
                int count = 0;

                // TODO Make sure that 10 millis is safe to subtract.
                // Get range start and end points to search for past events.
                final long timestamp =
                    key.window().endTime().minus(10, ChronoUnit.MILLIS).toEpochMilli();
                final PastFixedSizeTimeWindowsLimits pastFixedSizeTimeWindowsLimits =
                    fixedSizeTimeWindowSpec.calculatePastWindowsLimits(
                        timestamp, parameters.getPastWindowsLookup());
                final long startTimestamp =
                    pastFixedSizeTimeWindowsLimits.getStart().toEpochMilli();
                final long endTimestamp = pastFixedSizeTimeWindowsLimits.getEnd().toEpochMilli();

                // Find past events.
                // This list may contain more events than necessary.
                // This list may also contain fewer events than required.
                final List<SensorTelemetryMeasurementEventIBO> pastEventList = new ArrayList<>();
                for (final String sensorId : missingSensorIds) {
                  final Optional<SensorTelemetryMeasurementEventIBO> pastEventOptional =
                      iboPersistenceService.findPastSensorTelemetryMeasurementEventIBO(
                          physicalQuantity, sensorId, startTimestamp, endTimestamp);

                  if (pastEventOptional.isEmpty()) {
                    LOGGER.trace(
                        "Could not find past version of {} SensorTelemetryMeasurementEventIBO for"
                            + " sensor with ID : {} between {} and {}",
                        physicalQuantity.name(),
                        sensorId,
                        startTimestamp,
                        endTimestamp);
                    notFound++;
                    continue;
                  }

                  found++;
                  pastEventList.add(pastEventOptional.get());
                }

                // Validate pasts events list.
                final long totalPastEventListCount = pastEventList.size();
                final long uniquePastEventListCount =
                    pastEventList.stream()
                        .map(SensorTelemetryMeasurementEventIBO::getSensorId)
                        .distinct()
                        .count();
                Preconditions.checkState(
                    totalPastEventListCount == uniquePastEventListCount,
                    "totalPastEventListCount must be equal to uniquePastEventListCount");

                // Sort past events list (defaultTimestamp, desc).
                pastEventList.sort(
                    Comparator.comparing(o -> o.getTimestamps().getDefaultTimestamp()));
                Collections.reverse(pastEventList);

                // Set missing values.
                for (final SensorTelemetryMeasurementEventIBO pastEvent : pastEventList) {
                  if (value.getEvents().containsKey(pastEvent.getSensorId())) {
                    throw new IllegalStateException(
                        "events Map contains v sensorId "
                            + pastEvent.getSensorId()
                            + " key which is unexpected!");
                  }
                  value.getEvents().put(pastEvent.getSensorId(), pastEvent);
                  count++;

                  // TODO Parameter to override this.
                  if (count >= required) {
                    break;
                  }
                }

                // Validate counters.
                Preconditions.checkState(
                    (notFound + found) == missing,
                    "sum of notFound and found must be equal to missing");
                Preconditions.checkState(
                    found == pastEventList.size(), "found must be equal to pastEventList.size");
                Preconditions.checkState(
                    count <= required, "count must be less than or equal to required");

                // Update additional data.
                value.getAdditional().put(this.name() + "Missing", missing);
                value.getAdditional().put(this.name() + "Required", required);
                value.getAdditional().put(this.name() + "Found", found);
                value.getAdditional().put(this.name() + "NotFound", notFound);
                value.getAdditional().put(this.name() + "Count", count);

                // Calculate average.
                value
                    .getAverage()
                    .setValue(
                        calculateAverage(value.getEvents().values().stream().toList())
                            .orElse((double) Short.MIN_VALUE));

                // Update quality properties.
                final double completeness =
                    calculateCompleteness(value.getEvents().values().stream().toList());
                final double timeliness =
                    calculateTimeliness(
                        key.window().startTime().toEpochMilli(),
                        key.window().endTime().toEpochMilli(),
                        value.getEvents().values().stream().toList());
                final double timelinessAlt =
                    calculateTimelinessAlt(
                        key.window().startTime().toEpochMilli(),
                        key.window().endTime().toEpochMilli(),
                        value.getEvents().values().stream().toList());

                value.getQualityProperties().setCompleteness(completeness);
                value.getQualityProperties().setTimeliness(timeliness);
                // TODO add to quality properties.
                value.getAdditional().put("timelinessAlt", timelinessAlt);

                return value;
              }
            })
        // Fabrication: forecasted events (name: "forecastedEvents").
        .mapValues(
            new FabricationValueMapperWithKey() {
              @Override
              public String name() {
                return "forecastedEvents";
              }

              @Override
              public boolean shouldApply(
                  Windowed<String> readOnlyKey, SensorTelemetryMeasurementsAverageEventIBO value) {
                if (value.getQualityProperties().getCompleteness() >= 1) {
                  return false;
                }
                //noinspection RedundantIfStatement
                if (value.getEvents().isEmpty()) {
                  return false;
                }
                return true;
              }

              @Override
              public SensorTelemetryMeasurementsAverageEventIBO applyReal(
                  Windowed<String> key, SensorTelemetryMeasurementsAverageEventIBO value) {
                Preconditions.checkState(
                    value.getEvents().size() < parameters.getMinimumNumberOfContributingSensors(),
                    "events.size must be less than minimumNumberOfContributingSensors");

                // Get missing sensors IDs.
                final List<String> missingSensorIds = new ArrayList<>();
                for (final String sensorId : Constants.SENSOR_IDS) {
                  if (!value.getEvents().containsKey(sensorId)) {
                    missingSensorIds.add(sensorId);
                  }
                }
                Preconditions.checkState(
                    missingSensorIds.size() > 0, "missingSensorIds.size must be greater than zero");

                // How many events required to get completeness of 1?
                final int missing = missingSensorIds.size();
                final int required =
                    parameters.getMinimumNumberOfContributingSensors() - value.getEvents().size();
                int found = 0;
                int notFound = 0;
                int count = 0;

                // Get range start and end points to search for past events.
                final long startTimestamp = key.window().start();
                final long endTimestamp = key.window().end();
                final long timeWindowSize = parameters.getTimeWindowSize().toMillis();

                // Find forecasted events.
                // This list may contain more events than necessary.
                // This list may also contain fewer events than required.
                final List<SensorTelemetryMeasurementEventIBO> forecastedEventsList =
                    new ArrayList<>();
                for (final String sensorId : missingSensorIds) {
                  final Optional<SensorTelemetryMeasurementEventIBO> pastEventOptional =
                      sensorTelemetryMeasurementEventForecastingService.forecast(
                          physicalQuantity,
                          sensorId,
                          inputTopicName,
                          parameters.getTimeWindowSize().toString(),
                          startTimestamp,
                          endTimestamp,
                          timeWindowSize);

                  if (pastEventOptional.isEmpty()) {
                    LOGGER.trace(
                        "Could not find forecasted version of {} SensorTelemetryMeasurementEventIBO"
                            + " for sensor with ID : {} between {} and {} with time window size {}",
                        physicalQuantity.name(),
                        sensorId,
                        startTimestamp,
                        endTimestamp,
                        timeWindowSize);
                    notFound++;
                    continue;
                  }

                  found++;
                  forecastedEventsList.add(pastEventOptional.get());
                }

                // Validate pasts events list.
                final long totalPastEventListCount = forecastedEventsList.size();
                final long uniquePastEventListCount =
                    forecastedEventsList.stream()
                        .map(SensorTelemetryMeasurementEventIBO::getSensorId)
                        .distinct()
                        .count();
                Preconditions.checkState(
                    totalPastEventListCount == uniquePastEventListCount,
                    "totalPastEventListCount must be equal to uniquePastEventListCount");

                // Sort past events list (metric, direction).
                // TODO Check if metric exists?
                // TODO add a valid metric!
                final String metric = "accuracy";
                final boolean descending = false;
                forecastedEventsList.sort(
                    Comparator.comparing(o -> (Double) o.getAdditional().get(metric)));
                if (descending) {
                  Collections.reverse(forecastedEventsList);
                }

                // Set missing values.
                for (final SensorTelemetryMeasurementEventIBO pastEvent : forecastedEventsList) {
                  if (value.getEvents().containsKey(pastEvent.getSensorId())) {
                    throw new IllegalStateException(
                        "events Map contains v sensorId "
                            + pastEvent.getSensorId()
                            + " key which is unexpected!");
                  }
                  value.getEvents().put(pastEvent.getSensorId(), pastEvent);
                  count++;

                  // TODO Parameter to override this.
                  if (count >= required) {
                    break;
                  }
                }

                // Validate counters.
                Preconditions.checkState(
                    (notFound + found) == missing,
                    "sum of notFound and found must be equal to missing");
                Preconditions.checkState(
                    found == forecastedEventsList.size(),
                    "found must be equal to forecastedEventsList.size");
                Preconditions.checkState(
                    count <= required, "count must be less than or equal to required");

                // Calculate average.
                value
                    .getAverage()
                    .setValue(
                        calculateAverage(value.getEvents().values().stream().toList())
                            .orElse((double) Short.MIN_VALUE));

                // Update additional data.
                value.getAdditional().put(this.name() + "Missing", missing);
                value.getAdditional().put(this.name() + "Required", required);
                value.getAdditional().put(this.name() + "Found", found);
                value.getAdditional().put(this.name() + "NotFound", notFound);
                value.getAdditional().put(this.name() + "Count", count);

                // Update quality properties.
                final double completeness =
                    calculateCompleteness(value.getEvents().values().stream().toList());
                final double timeliness =
                    calculateTimeliness(
                        key.window().startTime().toEpochMilli(),
                        key.window().endTime().toEpochMilli(),
                        value.getEvents().values().stream().toList());
                final double timelinessAlt =
                    calculateTimelinessAlt(
                        key.window().startTime().toEpochMilli(),
                        key.window().endTime().toEpochMilli(),
                        value.getEvents().values().stream().toList());

                value.getQualityProperties().setCompleteness(completeness);
                value.getQualityProperties().setTimeliness(timeliness);
                // TODO Add to quality properties.
                value.getAdditional().put("timelinessAlt", timelinessAlt);

                return value;
              }
            })
        // TODO accuracy based on ground-truth.
        .mapValues((key, value) -> value)
        .filter(
            (key, value) -> {
              if (this.parameters.isIgnoreCompletenessFiltering()) {
                return true;
              }
              return value.getQualityProperties().getCompleteness() >= 1;
            })
        // Persistence.
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
