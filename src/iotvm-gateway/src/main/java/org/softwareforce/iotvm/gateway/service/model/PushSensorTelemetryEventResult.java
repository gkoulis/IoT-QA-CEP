package org.softwareforce.iotvm.gateway.service.model;

import java.io.Serial;
import java.io.Serializable;
import org.softwareforce.iotvm.gateway.model.SensorTelemetryEvent;

/**
 * Result object after pushing a {@link SensorTelemetryEvent}.
 *
 * @author Dimitris Gkoulis
 */
public class PushSensorTelemetryEventResult implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private SensorTelemetryEvent sensorTelemetryEvent;

  /* ------------ Constructors ------------ */

  public PushSensorTelemetryEventResult() {
    this.sensorTelemetryEvent = null;
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
    final StringBuffer sb = new StringBuffer("PushSensorTelemetryEventResult{");
    sb.append("sensorTelemetryEvent=").append(sensorTelemetryEvent);
    sb.append('}');
    return sb.toString();
  }
}
