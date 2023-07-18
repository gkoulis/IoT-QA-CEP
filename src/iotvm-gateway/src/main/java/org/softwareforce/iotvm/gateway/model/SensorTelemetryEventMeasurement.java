package org.softwareforce.iotvm.gateway.model;

import java.io.Serial;
import java.io.Serializable;
import org.softwareforce.iotvm.gateway.configuration.Constants;

/**
 * A measurement of a {@link SensorTelemetryEvent}.
 *
 * @author Dimitris Gkoulis
 */
public final class SensorTelemetryEventMeasurement implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private final String name;
  private final Double value;
  private final String unit;

  /* ------------ Constructors ------------ */

  /** Constructor for Jackson. */
  public SensorTelemetryEventMeasurement() {
    this.name = Constants.UNDEFINED;
    this.value = 0D;
    this.unit = Constants.UNDEFINED;
  }

  public SensorTelemetryEventMeasurement(String name, Double value, String unit) {
    this.name = name;
    this.value = value;
    this.unit = unit;
  }

  /* ------------ Getters ------------ */

  public String getName() {
    return name;
  }

  public Double getValue() {
    return value;
  }

  public String getUnit() {
    return unit;
  }

  /* ------------ Overrides ------------ */

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("SensorTelemetryEventMeasurement{");
    sb.append("name='").append(name).append('\'');
    sb.append(", value=").append(value);
    sb.append(", unit='").append(unit).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
