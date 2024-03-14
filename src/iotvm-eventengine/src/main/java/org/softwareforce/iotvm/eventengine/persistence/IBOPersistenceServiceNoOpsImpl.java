package org.softwareforce.iotvm.eventengine.persistence;

import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * No-ops implementation of {@link IBOPersistenceService}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 14 March 2024
 */
public class IBOPersistenceServiceNoOpsImpl implements IBOPersistenceService {

  /* ------------ Constructors ------------ */

  public IBOPersistenceServiceNoOpsImpl() {}

  /* ------------ Interface Implementation ------------ */

  @Override
  public SensorTelemetryRawEventIBO insert(String topicName, SensorTelemetryRawEventIBO value) {
    return value;
  }

  @Override
  public SensorTelemetryEventIBO insert(String topicName, SensorTelemetryEventIBO value) {
    return value;
  }

  @Override
  public SensorTelemetryMeasurementEventIBO insert(
      String topicName, SensorTelemetryMeasurementEventIBO value) {
    return value;
  }

  @Override
  public SensorTelemetryMeasurementsAverageEventIBO insert(
      String topicName, SensorTelemetryMeasurementsAverageEventIBO value) {
    return value;
  }
}
