package org.softwareforce.iotvm.eventengine.cep.ct;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.eventengine.cep.ct.specifics.ValidNonNullTimestampExtractor;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceServiceImpl;
import org.softwareforce.iotvm.shared.event.IdentifiersIBO;
import org.softwareforce.iotvm.shared.event.MeasurementIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.TimestampsIBO;

/**
 * Composite Transformation for splitting.
 *
 * @author Dimitris Gkoulis
 */
public final class SplittingCompositeTransformationFactory extends CompositeTransformationFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SplittingCompositeTransformationFactory.class);
  private static final String NAME = "splitting";

  private final SplittingCompositeTransformationParameters parameters;
  private final IBOPersistenceServiceImpl iboPersistenceService;

  /* ------------ Constructors ------------ */

  public SplittingCompositeTransformationFactory(
      SplittingCompositeTransformationParameters parameters,
      IBOPersistenceServiceImpl iboPersistenceService) {
    this.parameters = parameters;
    this.iboPersistenceService = iboPersistenceService;
  }

  /* ------------ Internal ------------ */

  private String getTopicName(SensorTelemetryMeasurementEventIBO ibo) {
    final String name = ibo.getMeasurement().getName().toUpperCase(Locale.ROOT).strip();
    final PhysicalQuantity physicalQuantity = PhysicalQuantity.valueOf(name);
    return Constants.getSensorTelemetryMeasurementEventTopic(physicalQuantity);
  }

  private StreamsBuilder buildSubTopology1(StreamsBuilder streamsBuilder) {
    final String inputTopicName = Constants.SENSOR_TELEMETRY_EVENT_TOPIC;
    final Consumed<String, SensorTelemetryEventIBO> consumedWith =
        Consumed.with(Constants.STRING_SERDE, Constants.SENSOR_TELEMETRY_EVENT_IBO_SERDE)
            .withTimestampExtractor(new ValidNonNullTimestampExtractor());

    final String outputTopicName = Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC;
    final Produced<String, SensorTelemetryMeasurementEventIBO> producedWith =
        Produced.with(
            Constants.STRING_SERDE, Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE);

    streamsBuilder.stream(inputTopicName, consumedWith)
        .flatMapValues(
            (value) -> {
              final long epochMilli = Instant.now().toEpochMilli();

              final List<SensorTelemetryMeasurementEventIBO> sensorTelemetryMeasurementEventList =
                  new ArrayList<>(value.getMeasurements().size());

              for (final MeasurementIBO measurementIBO : value.getMeasurements()) {
                final long epochMilliReal = Instant.now().toEpochMilli();

                final String sensorId = value.getSensorId();

                final Long defaultTimestamp = value.getTimestamps().getDefaultTimestamp();
                final Map<String, Long> timestamps =
                    new HashMap<>(value.getTimestamps().getTimestamps());
                timestamps.put(Constants.SPLITTED_L1, epochMilli);
                timestamps.put(Constants.SPLITTED_L1_REAL, epochMilliReal);

                final TimestampsIBO timestampsIBO =
                    TimestampsIBO.newBuilder()
                        .setDefaultTimestamp(defaultTimestamp)
                        .setTimestamps(timestamps)
                        .build();

                final String clientSideId = UUID.randomUUID().toString();
                final Map<String, String> correlationIds =
                    new HashMap<>(value.getIdentifiers().getCorrelationIds());
                correlationIds.put(
                    Constants.keyForSensorTelemetryEventIBO(Constants.CLIENT_SIDE_ID),
                    value.getIdentifiers().getClientSideId());
                correlationIds.put(
                    Constants.keyForSensorTelemetryEventIBO(Constants.PERSISTENCE_ID),
                    value.getIdentifiers().getCorrelationIds().get(Constants.PERSISTENCE_ID));

                final IdentifiersIBO identifiersIBO =
                    IdentifiersIBO.newBuilder()
                        .setClientSideId(clientSideId)
                        .setCorrelationIds(correlationIds)
                        .build();

                final Map<String, Object> additional = new HashMap<>(value.getAdditional());

                final SensorTelemetryMeasurementEventIBO sensorTelemetryMeasurementEventIBO =
                    SensorTelemetryMeasurementEventIBO.newBuilder()
                        .setSensorId(sensorId)
                        .setMeasurement(measurementIBO)
                        .setTimestamps(timestampsIBO)
                        .setIdentifiers(identifiersIBO)
                        .setAdditional(additional)
                        .build();

                sensorTelemetryMeasurementEventList.add(sensorTelemetryMeasurementEventIBO);
              }

              return sensorTelemetryMeasurementEventList;
            })
        .mapValues((value) -> this.iboPersistenceService.saveAlt(outputTopicName, value))
        .selectKey((k, v) -> Constants.ANY_SENSOR)
        .to(outputTopicName, producedWith);

    return streamsBuilder;
  }

  private StreamsBuilder buildSubTopology2(StreamsBuilder streamsBuilder) {
    final String inputTopicName = Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC;
    final Consumed<String, SensorTelemetryMeasurementEventIBO> consumedWith =
        Consumed.with(
                Constants.STRING_SERDE, Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE)
            .withTimestampExtractor(new ValidNonNullTimestampExtractor());

    final Produced<String, SensorTelemetryMeasurementEventIBO> producedWith =
        Produced.with(
            Constants.STRING_SERDE, Constants.SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE);

    final TopicNameExtractor<String, SensorTelemetryMeasurementEventIBO> outputTopicNameExtractor =
        (key, value, recordContext) -> {
          LOGGER.trace(
              "extracting topic for {} measurement of sensor {} @ {}",
              value.getMeasurement().getName(),
              value.getSensorId(),
              value.getTimestamps().getDefaultTimestamp());
          return this.getTopicName(value);
        };

    streamsBuilder.stream(inputTopicName, consumedWith)
        .filter(
            (key, value) -> {
              LOGGER.trace(
                  "filtering {} measurement of sensor {} @ {}",
                  value.getMeasurement().getName(),
                  value.getSensorId(),
                  value.getTimestamps().getDefaultTimestamp());
              final String physicalQuantityName =
                  value.getMeasurement().getName().toUpperCase(Locale.ROOT).strip();
              // @future Convert camel-case to snake-case and then to upper case,
              // i.e., a smart function that can detect the physical quantity.
              // Use the same function in `outputTopicNameExtractor`.
              try {
                PhysicalQuantity.valueOf(physicalQuantityName);
                return true;
              } catch (IllegalArgumentException ex) {
                LOGGER.warn("Found non-supported PhysicalQuantity : {}", physicalQuantityName);
                return false;
              }
            })
        .mapValues(
            (value) -> {
              value
                  .getTimestamps()
                  .getTimestamps()
                  .put(Constants.SPLITTED_L2, Instant.now().toEpochMilli());
              return this.iboPersistenceService.saveAlt(this.getTopicName(value), value);
            })
        .selectKey((k, v) -> Constants.ANY_SENSOR)
        .to(outputTopicNameExtractor, producedWith);

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

  @SuppressWarnings({"ReassignedVariable", "DataFlowIssue"})
  @Override
  public StreamsBuilder build(StreamsBuilder streamsBuilder) {
    LOGGER.debug(
        "Request to build {} {} composite transformation",
        this.getUniqueIdentifier(),
        this.getName());

    streamsBuilder = this.buildSubTopology1(streamsBuilder);
    streamsBuilder = this.buildSubTopology2(streamsBuilder);

    return streamsBuilder;
  }
}
