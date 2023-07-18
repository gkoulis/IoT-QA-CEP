package org.softwareforce.iotvm.gateway.service.model;

import jakarta.validation.constraints.NotNull;
import java.io.Serial;
import java.io.Serializable;
import org.softwareforce.iotvm.gateway.model.SensorTelemetryEvent;

/**
 * Request object for pushing a {@link SensorTelemetryEvent}.
 *
 * @author Dimitris Gkoulis
 */
public class PushSensorTelemetryEventRequest implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  @NotNull private SensorTelemetryEvent sensorTelemetryEvent;
  @NotNull private Long receivedTimestamp;

  /* ------------ Constructors ------------ */

  public PushSensorTelemetryEventRequest() {
    this.sensorTelemetryEvent = null;
    this.receivedTimestamp = null;
  }

  public PushSensorTelemetryEventRequest(
      SensorTelemetryEvent sensorTelemetryEvent, Long receivedTimestamp) {
    this.sensorTelemetryEvent = sensorTelemetryEvent;
    this.receivedTimestamp = receivedTimestamp;
  }

  /* ------------ Getters and Setters ------------ */

  public SensorTelemetryEvent getSensorTelemetryEvent() {
    return sensorTelemetryEvent;
  }

  public void setSensorTelemetryEvent(SensorTelemetryEvent sensorTelemetryEvent) {
    this.sensorTelemetryEvent = sensorTelemetryEvent;
  }

  public Long getReceivedTimestamp() {
    return receivedTimestamp;
  }

  public void setReceivedTimestamp(Long receivedTimestamp) {
    this.receivedTimestamp = receivedTimestamp;
  }

  /* ------------ Overrides ------------ */

  @Override
  public String toString() {
    return "PushSensorTelemetryEventRequest{"
        + "sensorTelemetryEvent="
        + sensorTelemetryEvent
        + ", receivedTimestamp="
        + receivedTimestamp
        + '}';
  }
}
