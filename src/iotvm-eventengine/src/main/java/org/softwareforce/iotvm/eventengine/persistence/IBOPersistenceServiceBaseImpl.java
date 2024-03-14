package org.softwareforce.iotvm.eventengine.persistence;

import java.time.Instant;
import javax.annotation.Nullable;
import org.bson.types.ObjectId;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.persistence.model.PersistedIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * Base implementation of {@link IBOPersistenceService}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 14 March 2024
 */
public class IBOPersistenceServiceBaseImpl implements IBOPersistenceService {

  /* ------------ Constructors ------------ */

  public IBOPersistenceServiceBaseImpl() {}

  /* ------------ Interface Implementation ------------ */

  private PersistedIBO _insert(
      @Nullable ObjectId objectId,
      final String topicName,
      SensorTelemetryRawEventIBO sensorTelemetryRawEventIBO) {
    return null;
  }

  private PersistedIBO _insert(
      @Nullable ObjectId objectId,
      final String topicName,
      SensorTelemetryEventIBO sensorTelemetryEventIBO) {
    return null;
  }

  private PersistedIBO _insert(
      @Nullable ObjectId objectId,
      final String topicName,
      SensorTelemetryMeasurementEventIBO sensorTelemetryMeasurementEventIBO) {
    return null;
  }

  private PersistedIBO _insert(
      @Nullable ObjectId objectId,
      final String topicName,
      SensorTelemetryMeasurementsAverageEventIBO sensorTelemetryMeasurementsAverageEventIBO) {
    return null;
  }

  /* ------------ Interface Implementation ------------ */

  @Override
  public SensorTelemetryRawEventIBO insert(
      final String topicName, SensorTelemetryRawEventIBO value) {
    value.getTimestamps().getTimestamps().put(Constants.PERSISTED, Instant.now().toEpochMilli());

    final ObjectId objectId = ObjectId.get();
    value
        .getIdentifiers()
        .getCorrelationIds()
        .put(Constants.PERSISTENCE_ID, objectId.toHexString());

    this._insert(objectId, topicName, value);

    return value;
  }

  @Override
  public SensorTelemetryEventIBO insert(final String topicName, SensorTelemetryEventIBO value) {
    value.getTimestamps().getTimestamps().put(Constants.PERSISTED, Instant.now().toEpochMilli());

    final ObjectId objectId = ObjectId.get();
    value
        .getIdentifiers()
        .getCorrelationIds()
        .put(Constants.PERSISTENCE_ID, objectId.toHexString());

    this._insert(objectId, topicName, value);

    return value;
  }

  @Override
  public SensorTelemetryMeasurementEventIBO insert(
      final String topicName, SensorTelemetryMeasurementEventIBO value) {
    value.getTimestamps().getTimestamps().put(Constants.PERSISTED, Instant.now().toEpochMilli());

    final ObjectId objectId = ObjectId.get();
    value
        .getIdentifiers()
        .getCorrelationIds()
        .put(Constants.PERSISTENCE_ID, objectId.toHexString());

    this._insert(objectId, topicName, value);

    return value;
  }

  @Override
  public SensorTelemetryMeasurementsAverageEventIBO insert(
      final String topicName, SensorTelemetryMeasurementsAverageEventIBO value) {
    value.getTimestamps().getTimestamps().put(Constants.PERSISTED, Instant.now().toEpochMilli());

    final ObjectId objectId = ObjectId.get();
    value
        .getIdentifiers()
        .getCorrelationIds()
        .put(Constants.PERSISTENCE_ID, objectId.toHexString());

    this._insert(objectId, topicName, value);

    return value;
  }
}
