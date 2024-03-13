package org.softwareforce.iotvm.eventengine.cep.ct;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
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
import org.softwareforce.iotvm.eventengine.cep.CalculationUtils;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.FabricationValueMapperWithKey;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.FixedSizeTimeWindowSpec;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.FixedSizeTimeWindowSpec.PastFixedSizeTimeWindowsLimits;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.ValidNonNullTimestampExtractor;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.averagecalculation.AggregatorImpl;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.averagecalculation.InitializerImpl;
import org.softwareforce.iotvm.eventengine.cep.fabrication.EventFabricationService;
import org.softwareforce.iotvm.eventengine.cep.fabrication.InputEvent;
import org.softwareforce.iotvm.eventengine.cep.fabrication.OutputEvent;
import org.softwareforce.iotvm.eventengine.extensions.FabricationForecastingServiceAdapter;
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
  private final FabricationForecastingServiceAdapter fabricationForecastingServiceAdapter;
  private final EventFabricationService eventFabricationService;

  /* ------------ Constructors ------------ */

  public AverageCalculationCompositeTransformationFactory(
      AverageCalculationCompositeTransformationParameters parameters,
      IBOPersistenceServiceImpl iboPersistenceService,
      FabricationForecastingServiceAdapter fabricationForecastingServiceAdapter) {
    this.parameters = parameters;
    this.fixedSizeTimeWindowSpec =
        new FixedSizeTimeWindowSpec(
            this.parameters.getTimeWindowSize(),
            this.parameters.getTimeWindowGrace(),
            this.parameters.getTimeWindowAdvance());
    this.iboPersistenceService = iboPersistenceService;
    this.fabricationForecastingServiceAdapter = fabricationForecastingServiceAdapter;
    this.eventFabricationService = new EventFabricationService(this.parameters.getTimeWindowSize().toMillis(), new HashSet<>(Constants.SENSOR_IDS));
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
                    completeness1 : {}
                    timeliness1   : {}
                    timeliness2   : {}
                    accuracy1     : {}
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
                  value.getQualityProperties().getMetrics().get("completeness1"),
                  value.getQualityProperties().getMetrics().get("timeliness1"),
                  value.getQualityProperties().getMetrics().get("timeliness2"),
                  value.getQualityProperties().getMetrics().get("accuracy1"));
            });

    return streamsBuilder;
  }

  private static QualityPropertiesIBO calculateAndGetQualityPropertiesIBO(
      final long windowStartTimestamp,
      final long windowEndTimestamp,
      final int minimumNumberOfContributingSensors,
      final int fabricationPastEventsStepsBehind,
      final int fabricationForecastingStepsAhead,
      final List<SensorTelemetryMeasurementEventIBO> eventIBOList) {
    final double completeness1 =
        CalculationUtils.calculateCompleteness1(minimumNumberOfContributingSensors, eventIBOList);
    final double timeliness1 =
        CalculationUtils.calculateTimeliness1(
            windowStartTimestamp, windowEndTimestamp, eventIBOList);
    final double timeliness2 =
        CalculationUtils.calculateTimeliness2(
            windowStartTimestamp,
            windowEndTimestamp,
            fabricationPastEventsStepsBehind,
            eventIBOList);

    final double accuracy1 = 0.0D;

    final Map<String, Double> metrics = new HashMap<>();

    metrics.put("completeness1", completeness1);
    metrics.put("timeliness1", timeliness1);
    metrics.put("timeliness2", timeliness2);
    metrics.put("accuracy1", accuracy1);

    final QualityPropertiesIBO qualityPropertiesIBO =
        QualityPropertiesIBO.newBuilder().setMetrics(metrics).build();

    return qualityPropertiesIBO;
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
        .aggregate(new InitializerImpl(physicalQuantity), new AggregatorImpl(), materialized)
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .toStream()
        // TODO time-related feature και debug/inspection -> ένα stage που να ελέγχει αν τα
        // timestamps που βρίσκονται στο TimestampExtractorUtil ανήκουν ΌΛΑ στο ίδιο παράθυρο. Αν
        // όχι, τότε υπάρχει ασυμφωνία που πρέπει να αποτυπωθεί στο composite event
        // TODO context stage: π.χ. ποιοι sensors είναι registered? Βάλε τους μέσα στο additional.
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

              final QualityPropertiesIBO qualityPropertiesIBO =
                  calculateAndGetQualityPropertiesIBO(
                      key.window().start(),
                      key.window().end(),
                      this.parameters.getMinimumNumberOfContributingSensors(),
                      this.parameters.getPastWindowsLookup(),
                      this.parameters.getFutureWindowsLookupAlternative(),
                      value.getEvents().values().stream().toList());

              // Sensor Simulation inspection and debugging.
              final List<SensorTelemetryMeasurementEventIBO> eventIBOList =
                  value.getEvents().values().stream().toList();
              // TODO Εδώ αποδεικνύεται πώς ο client μπορεί να διαλύσει όλο το σύστημα. MapUtils για
              // να παίρνω σωστά τις τιμές...
              final String debugStringUniqueExperimentNameValues =
                  eventIBOList.stream()
                      .map(e -> (String) e.getAdditional().get("experiment_name"))
                      .filter(Objects::nonNull)
                      .distinct()
                      .sorted()
                      .collect(Collectors.joining(", "));
              final String debugStringUniqueSimulationNameValues =
                  eventIBOList.stream()
                      .map(e -> (String) e.getAdditional().get("simulation_name"))
                      .filter(Objects::nonNull)
                      .distinct()
                      .sorted()
                      .collect(Collectors.joining(", "));
              final String debugStringUniqueCycleValues =
                  eventIBOList.stream()
                      .map(e -> (String) e.getAdditional().get("cycle"))
                      .filter(Objects::nonNull)
                      .distinct()
                      .sorted()
                      .collect(Collectors.joining(", "));
              final String debugStringUniqueRecurringWindowValues =
                  eventIBOList.stream()
                      .map(e -> (String) e.getAdditional().get("recurring_window"))
                      .filter(Objects::nonNull)
                      .distinct()
                      .sorted()
                      .collect(Collectors.joining(", "));

              final Map<String, Object> additional = value.getAdditional();
              additional.put(
                  "debugStringUniqueExperimentNameValues", debugStringUniqueExperimentNameValues);
              additional.put(
                  "debugStringUniqueSimulationNameValues", debugStringUniqueSimulationNameValues);
              additional.put("debugStringUniqueCycleValues", debugStringUniqueCycleValues);
              additional.put(
                  "debugStringUniqueRecurringWindowValues", debugStringUniqueRecurringWindowValues);

              additional.put("averageValueBeforeFabrication", value.getAverage().getValue());
              for (final String metricKey : qualityPropertiesIBO.getMetrics().keySet()) {
                additional.put(
                    metricKey + "BeforeFabrication",
                    qualityPropertiesIBO.getMetrics().get(metricKey));
              }

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

        .mapValues((key, value) -> {
          final List<InputEvent> inputEventList = new ArrayList<>();
          for (final SensorTelemetryMeasurementEventIBO ibo : value.getEvents().values()) {
            this.eventFabricationService.updateTimeWindowedTimeSeries(ibo.getSensorId(), ibo.getMeasurement().getValue(), ibo.getTimestamps().getDefaultTimestamp());
            inputEventList.add(new InputEvent(ibo.getSensorId(), ibo.getMeasurement().getValue(), ibo.getTimestamps().getDefaultTimestamp()));
          }
          // TODO Check completeness...
          final Set<OutputEvent> outputEventSet = this.eventFabricationService.performEventFabrication(inputEventList, key.window().start(), this.parameters.getMinimumNumberOfContributingSensors(), this.parameters.getPastWindowsLookup(), this.parameters.getFutureWindowsLookup());
          if (!outputEventSet.isEmpty()) {
            System.out.println(outputEventSet.size()); // TODO Remove.
            for (final OutputEvent o : outputEventSet) {
              System.out.println(o);
            }
            System.out.println("\n");
          }
          // TODO Continue from here.
          return value;
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
                final Double completeness1 =
                    value.getQualityProperties().getMetrics().get("completeness1");
                Preconditions.checkState(completeness1 != null, "completeness1 cannot be null!");
                if (completeness1 >= 1) {
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

                // @PHD_DOCS -10 millis is safe to subtract because the window size cannot be less
                // than 1 second.

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
                // TODO Prefer null, not Short.MIN_VALUE
                value
                    .getAverage()
                    .setValue(
                        CalculationUtils.calculateAverage(
                                value.getEvents().values().stream().toList())
                            .orElse((double) Short.MIN_VALUE));

                final QualityPropertiesIBO qualityPropertiesIBO =
                    calculateAndGetQualityPropertiesIBO(
                        key.window().start(),
                        key.window().end(),
                        parameters.getMinimumNumberOfContributingSensors(),
                        parameters.getPastWindowsLookup(),
                        parameters.getFutureWindowsLookupAlternative(),
                        value.getEvents().values().stream().toList());
                value.setQualityProperties(qualityPropertiesIBO);

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
                final Double completeness1 =
                    value.getQualityProperties().getMetrics().get("completeness1");
                Preconditions.checkState(completeness1 != null, "completeness1 cannot be null!");
                if (completeness1 >= 1) {
                  return false;
                }
                if (parameters.getFutureWindowsLookupAlternative() <= 0) {
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
                  final Optional<SensorTelemetryMeasurementEventIBO> forecastedEventOptional =
                      fabricationForecastingServiceAdapter.forecast(
                          physicalQuantity,
                          sensorId,
                          inputTopicName,
                          parameters.getForecastingWindowSize().toSeconds(),
                          startTimestamp,
                          endTimestamp,
                          parameters.getFutureWindowsLookupAlternative());

                  if (forecastedEventOptional.isEmpty()) {
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
                  forecastedEventsList.add(forecastedEventOptional.get());
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

                // Sort fabricated events. I want to select the most recent.
                // If they have the same time diff, sort by (dataset) completeness.
                final Comparator<SensorTelemetryMeasurementEventIBO> c1 =
                    Comparator.comparingDouble(
                        o ->
                            (Double)
                                o.getAdditional().get("forecastResponseMetricsTimeDifference"));
                final Comparator<SensorTelemetryMeasurementEventIBO> c2 =
                    Comparator.comparingDouble(
                        o -> (Double) o.getAdditional().get("forecastResponseMetricsCompleteness"));

                forecastedEventsList.sort(c2);
                Collections.reverse(forecastedEventsList);
                forecastedEventsList.sort(c1);

                // Debug.
                // Uncomment to debug.
                /*
                final String debugString1 =
                    forecastedEventsList.stream()
                        .map(
                            o ->
                                o.getSensorId()
                                    + ": "
                                    + o.getAdditional().get("forecastResponseMetricsTimeDifference")
                                    + ", "
                                    + o.getAdditional().get("forecastResponseMetricsCompleteness"))
                        .collect(Collectors.joining("\n"));
                LOGGER.debug("DEBUG : \n{}", debugString1);
                */

                // @bug (August 2023) Κάπου μου είχε δείξει completeness 1.
                // Μετά από μερικά δευτερόλεπτα, είναι αδύνατον να έχω completeness 1.

                // Set missing values.
                for (final SensorTelemetryMeasurementEventIBO forecastedEvent :
                    forecastedEventsList) {
                  if (value.getEvents().containsKey(forecastedEvent.getSensorId())) {
                    throw new IllegalStateException(
                        "events Map contains v sensorId "
                            + forecastedEvent.getSensorId()
                            + " key which is unexpected!");
                  }
                  value.getEvents().put(forecastedEvent.getSensorId(), forecastedEvent);
                  count++;

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
                        CalculationUtils.calculateAverage(
                                value.getEvents().values().stream().toList())
                            .orElse((double) Short.MIN_VALUE));

                // Update additional data.
                value.getAdditional().put(this.name() + "Missing", missing);
                value.getAdditional().put(this.name() + "Required", required);
                value.getAdditional().put(this.name() + "Found", found);
                value.getAdditional().put(this.name() + "NotFound", notFound);
                value.getAdditional().put(this.name() + "Count", count);

                final QualityPropertiesIBO qualityPropertiesIBO =
                    calculateAndGetQualityPropertiesIBO(
                        key.window().start(),
                        key.window().end(),
                        parameters.getMinimumNumberOfContributingSensors(),
                        parameters.getPastWindowsLookup(),
                        parameters.getFutureWindowsLookupAlternative(),
                        value.getEvents().values().stream().toList());
                value.setQualityProperties(qualityPropertiesIBO);

                return value;
              }
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
