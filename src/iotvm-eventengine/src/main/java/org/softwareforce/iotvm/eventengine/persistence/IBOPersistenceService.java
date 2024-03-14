package org.softwareforce.iotvm.eventengine.persistence;

import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * Service for managing persistence of IBO entities.
 *
 * @author Dimitris Gkoulis
 */
public interface IBOPersistenceService {

  SensorTelemetryRawEventIBO insert(final String topicName, SensorTelemetryRawEventIBO value);

  SensorTelemetryEventIBO insert(final String topicName, SensorTelemetryEventIBO value);

  SensorTelemetryMeasurementEventIBO insert(
      final String topicName, SensorTelemetryMeasurementEventIBO value);

  SensorTelemetryMeasurementsAverageEventIBO insert(
      final String topicName, SensorTelemetryMeasurementsAverageEventIBO value);
}
