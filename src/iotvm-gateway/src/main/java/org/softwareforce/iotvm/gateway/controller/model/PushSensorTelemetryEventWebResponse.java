package org.softwareforce.iotvm.gateway.controller.model;

import jakarta.validation.constraints.NotNull;
import java.io.Serial;
import java.io.Serializable;
import org.softwareforce.iotvm.gateway.model.SensorTelemetryEvent;

/**
 * Web Response object after pushing a {@link SensorTelemetryEvent}.
 *
 * @author Dimitris Gkoulis
 */
public class PushSensorTelemetryEventWebResponse implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  @NotNull private SensorTelemetryEvent sensorTelemetryEvent;

  /* ------------ Constructors ------------ */

  public PushSensorTelemetryEventWebResponse() {
    this.sensorTelemetryEvent = null;
  }

  public PushSensorTelemetryEventWebResponse(SensorTelemetryEvent sensorTelemetryEvent) {
    this.sensorTelemetryEvent = sensorTelemetryEvent;
  }

  /* ------------ Getters and Setters ------------ */

  public SensorTelemetryEvent getSensorTelemetryEvent() {
    return sensorTelemetryEvent;
  }

  public void setSensorTelemetryEvent(SensorTelemetryEvent sensorTelemetryEvent) {
    this.sensorTelemetryEvent = sensorTelemetryEvent;
  }

  /* ------------ Overrides ------------ */

  @Override
  public String toString() {
    return "PushSensorTelemetryEventWebResponse{"
        + "sensorTelemetryEvent="
        + sensorTelemetryEvent
        + '}';
  }
}
