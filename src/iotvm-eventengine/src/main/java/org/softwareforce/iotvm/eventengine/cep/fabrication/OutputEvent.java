package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.Objects;

/**
 * An output event from the event fabrication service.
 *
 * <p>The use of the instances in collections dictates the existence of only one {@link OutputEvent}
 * per sensor. The {@link #equals(Object)} is also implemented with this in mind, i.e., the equality
 * is checked using only the {@link #sensorId}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Wednesday 13 March 2024
 */
public class OutputEvent {

  /** The ID of the sensor. */
  private final String sensorId;

  /** The value of the fabricated event. */
  private final Double value;

  /** The event fabrication method used to impute value. */
  private final EventFabricationMethod method;

  /**
   * The relative distance from the current time-window, i.e., the time-window in which the missing
   * event belongs to.
   */
  private final long distance;

  /**
   * The timestamp (milliseconds) of the fabricated event which is equal to the start of the current
   * time-window, i.e., the time-window in which the missing event belongs to.
   */
  private final long timestampMs;

  /* ------------ Constructors ------------ */

  public OutputEvent(
      String sensorId,
      Double value,
      EventFabricationMethod method,
      long distance,
      long timestampMs) {
    this.sensorId = sensorId;
    this.value = value;
    this.method = method;
    this.distance = distance;
    this.timestampMs = timestampMs;
  }

  /* ------------ Getters ------------ */

  public String getSensorId() {
    return this.sensorId;
  }

  public Double getValue() {
    return this.value;
  }

  public EventFabricationMethod getMethod() {
    return this.method;
  }

  public long getDistance() {
    return this.distance;
  }

  public long getTimestampMs() {
    return this.timestampMs;
  }

  /* ------------ Overrides ------------ */

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OutputEvent that = (OutputEvent) o;
    return Objects.equals(this.sensorId, that.sensorId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.sensorId);
  }

  @Override
  public String toString() {
    return "OutputEvent{"
        + "sensorId='"
        + this.sensorId
        + '\''
        + ", value="
        + this.value
        + ", method='"
        + this.method
        + '\''
        + ", distance="
        + this.distance
        + ", timestampMs="
        + this.timestampMs
        + '}';
  }
}
