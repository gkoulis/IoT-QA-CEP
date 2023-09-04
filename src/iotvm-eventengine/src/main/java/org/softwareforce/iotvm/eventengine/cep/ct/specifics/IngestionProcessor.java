package org.softwareforce.iotvm.eventengine.cep.ct.specifics;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.util.TimestampExtractorUtil;
import org.softwareforce.iotvm.eventengine.persistence.IBOPersistenceServiceImpl;
import org.softwareforce.iotvm.shared.event.IdentifiersIBO;
import org.softwareforce.iotvm.shared.event.MeasurementIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;
import org.softwareforce.iotvm.shared.event.TimestampsIBO;

public class IngestionProcessor
    implements Processor<String, SensorTelemetryRawEventIBO, String, SensorTelemetryEventIBO> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionProcessor.class);

  private ProcessorContext<String, SensorTelemetryEventIBO> context;
  private final IBOPersistenceServiceImpl iboPersistenceService;
  private final String inputTopicName;
  private final String outputTopicName;

  /* ------------ Constructors ------------ */

  public IngestionProcessor(
      IBOPersistenceServiceImpl iboPersistenceService,
      String inputTopicName,
      String outputTopicName) {
    this.iboPersistenceService = iboPersistenceService;
    this.inputTopicName = inputTopicName;
    this.outputTopicName = outputTopicName;
  }

  /* ------------ Internals ------------ */

  /**
   * Notes:
   *
   * <ul>
   *   <li>must not mutate {@link SensorTelemetryRawEventIBO}
   *   <li>must copy data from {@link SensorTelemetryRawEventIBO}
   * </ul>
   *
   * @param sensorTelemetryRawEventIBO the {@link SensorTelemetryRawEventIBO} instance to map.
   * @return the {@link SensorTelemetryEventIBO} instance.
   */
  private SensorTelemetryEventIBO mapValue(SensorTelemetryRawEventIBO sensorTelemetryRawEventIBO) {
    final String sensorId = sensorTelemetryRawEventIBO.getSensorId();

    final List<MeasurementIBO> measurementIBOList =
        sensorTelemetryRawEventIBO.getMeasurements().stream()
            .filter(Objects::nonNull)
            .distinct()
            .toList();

    final Map<String, Long> timestamps =
        new HashMap<>(sensorTelemetryRawEventIBO.getTimestamps().getTimestamps());
    timestamps.put(Constants.INGESTED, Instant.now().toEpochMilli());

    final TimestampsIBO timestampsIBO =
        TimestampsIBO.newBuilder()
            // This will force selecting timestamp again in a later stage (inside this processor).
            .setDefaultTimestamp(null)
            .setTimestamps(timestamps)
            .build();

    final Map<String, String> correlationIds = new HashMap<>();
    correlationIds.put(
        Constants.keyForSensorTelemetryRawEventIBO(Constants.CLIENT_SIDE_ID),
        sensorTelemetryRawEventIBO.getIdentifiers().getClientSideId());
    correlationIds.put(
        Constants.keyForSensorTelemetryRawEventIBO(Constants.PERSISTENCE_ID),
        sensorTelemetryRawEventIBO
            .getIdentifiers()
            .getCorrelationIds()
            .get(Constants.PERSISTENCE_ID));

    final IdentifiersIBO identifiersIBO =
        IdentifiersIBO.newBuilder()
            .setClientSideId(UUID.randomUUID().toString())
            .setCorrelationIds(correlationIds)
            .build();

    final Map<String, Object> additional =
        new HashMap<>(sensorTelemetryRawEventIBO.getAdditional());

    return SensorTelemetryEventIBO.newBuilder()
        .setSensorId(sensorId)
        .setMeasurements(measurementIBOList)
        .setTimestamps(timestampsIBO)
        .setIdentifiers(identifiersIBO)
        .setAdditional(additional)
        .build();
  }

  private SensorTelemetryEventIBO selectTimestamp(
      SensorTelemetryEventIBO sensorTelemetryEventIBO, Long recordTimestamp) {
    if (sensorTelemetryEventIBO.getTimestamps().getDefaultTimestamp() != null) {
      return sensorTelemetryEventIBO;
    }
    final long timestamp =
        TimestampExtractorUtil.get(
            sensorTelemetryEventIBO.getTimestamps(), List.of(recordTimestamp));
    sensorTelemetryEventIBO.getTimestamps().setDefaultTimestamp(timestamp);
    return sensorTelemetryEventIBO;
  }

  /* ------------ Interface Implementation ------------ */

  @Override
  public void init(ProcessorContext<String, SensorTelemetryEventIBO> context) {
    Processor.super.init(context);
    this.context = context;
  }

  @SuppressWarnings("DataFlowIssue")
  @Override
  public void process(Record<String, SensorTelemetryRawEventIBO> record) {
    LOGGER.trace(
        "processing SensorTelemetryRawEventIBO {} @ {}",
        record.value().getSensorId(),
        record.value().getTimestamps().getDefaultTimestamp());

    SensorTelemetryRawEventIBO sensorTelemetryRawEventIBO = record.value();
    SensorTelemetryEventIBO sensorTelemetryEventIBO;

    sensorTelemetryRawEventIBO =
        this.iboPersistenceService.saveAlt(this.inputTopicName, sensorTelemetryRawEventIBO);
    sensorTelemetryEventIBO = this.mapValue(sensorTelemetryRawEventIBO);
    sensorTelemetryEventIBO
        .getTimestamps()
        .getTimestamps()
        .put(Constants.RECORD_TIMESTAMP, record.timestamp());
    sensorTelemetryEventIBO = this.selectTimestamp(sensorTelemetryEventIBO, record.timestamp());
    sensorTelemetryEventIBO =
        this.iboPersistenceService.saveAlt(this.outputTopicName, sensorTelemetryEventIBO);

    final Record<String, SensorTelemetryEventIBO> newRecord =
        new Record<>(
            Constants.ANY_SENSOR,
            sensorTelemetryEventIBO,
            sensorTelemetryEventIBO.getTimestamps().getDefaultTimestamp());

    Header[] headersArray = record.headers().toArray();
    if (headersArray.length > 0) {
      LOGGER.warn(
          "Record<String, SensorTelemetryRawEventIBO> has {} unregistered headers : {}",
          headersArray.length,
          Arrays.stream(headersArray).map(Header::key).collect(Collectors.joining(", ")));
    }

    this.context.forward(newRecord);
  }

  @Override
  public void close() {
    Processor.super.close();
  }
}
