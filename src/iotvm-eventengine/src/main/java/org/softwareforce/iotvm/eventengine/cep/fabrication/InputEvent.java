package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.Objects;

/**
 * An input event to the event fabrication service.
 *
 * <p>The use of the instances in collections dictates the existence of only one {@link InputEvent}
 * per sensor. The {@link #equals(Object)} is also implemented with this in mind, i.e., the equality
 * is checked using only the {@link #sensorId}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Wednesday 13 March 2024
 */
public class InputEvent {

  /** The ID of the sensor. */
  private final String sensorId;

  /** The value of the event. */
  private final double value;

  /** The timestamp (ms) of the event. */
  private final long timestampMs;

  // TODO Should we use the time window? Check also the equals...
  //  and write docs that only one per sensor is
  // required........................!!!!!!!!!!!!!!!!!!!!!!!!1

  /* ------------ Constructors ------------ */

  public InputEvent(final String sensorId, final double value, final long timestampMs) {
    this.sensorId = sensorId;
    this.value = value;
    this.timestampMs = timestampMs;
  }

  /* ------------ Getters ------------ */

  public String getSensorId() {
    return sensorId;
  }

  public double getValue() {
    return value;
  }

  public long getTimestampMs() {
    return timestampMs;
  }

  /* ------------ Overrides ------------ */

  @Override
  public String toString() {
    return "InputEvent{"
        + "sensorId='"
        + sensorId
        + '\''
        + ", value="
        + value
        + ", timestampMs="
        + timestampMs
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InputEvent that = (InputEvent) o;
    return Objects.equals(sensorId, that.sensorId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sensorId);
  }
}
