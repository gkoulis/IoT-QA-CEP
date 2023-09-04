package org.softwareforce.iotvm.gateway.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.softwareforce.iotvm.gateway.configuration.Constants;

/**
 * A sensor telemetry event with multiple measurements.
 *
 * @author Dimitris Gkoulis
 */
public final class SensorTelemetryEvent implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private final List<SensorTelemetryEventMeasurement> measurements;
  private final String sensorId;
  private final Long timestamp;
  private final Map<String, Object> additional;

  /* ------------ Constructors ------------ */

  /** Constructor for Jackson. */
  public SensorTelemetryEvent() {
    this.measurements = new ArrayList<>();
    this.sensorId = Constants.UNDEFINED;
    this.timestamp = 0L;
    this.additional = new HashMap<>();
  }

  public SensorTelemetryEvent(
      List<SensorTelemetryEventMeasurement> measurements,
      String sensorId,
      Long timestamp,
      Map<String, Object> additional) {
    this.measurements = measurements;
    this.sensorId = sensorId;
    this.timestamp = timestamp;
    this.additional = additional;
  }

  /* ------------ Getters ------------ */

  public List<SensorTelemetryEventMeasurement> getMeasurements() {
    return measurements;
  }

  public String getSensorId() {
    return sensorId;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public Map<String, Object> getAdditional() {
    return additional;
  }

  /* ------------ Overrides ------------ */

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("SensorTelemetryEvent{");
    sb.append("measurements=").append(measurements);
    sb.append(", sensorId='").append(sensorId).append('\'');
    sb.append(", timestamp=").append(timestamp);
    sb.append(", additional=").append(additional);
    sb.append('}');
    return sb.toString();
  }
}
