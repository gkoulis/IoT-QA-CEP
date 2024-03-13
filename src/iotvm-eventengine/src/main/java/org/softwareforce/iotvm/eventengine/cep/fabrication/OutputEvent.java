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

  /** */
  private final String sensorId;

  /** */
  private final Double value;

  /** */
  private final String method; // TODO enum.

  /** */
  private final long distance;

  /** */
  private final long timestampMs;

  public OutputEvent(
      String sensorId, Double value, String method, long distance, long timestampMs) {
    this.sensorId = sensorId;
    this.value = value;
    this.method = method;
    this.distance = distance;
    this.timestampMs = timestampMs;
  }

  public String getSensorId() {
    return sensorId;
  }

  public Double getValue() {
    return value;
  }

  public String getMethod() {
    return method;
  }

  public long getDistance() {
    return distance;
  }

  public long getTimestampMs() {
    return timestampMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OutputEvent that = (OutputEvent) o;
    return Objects.equals(sensorId, that.sensorId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sensorId);
  }

  @Override
  public String toString() {
    return "OutputEvent{" +
        "sensorId='" + sensorId + '\'' +
        ", value=" + value +
        ", method='" + method + '\'' +
        ", distance=" + distance +
        ", timestampMs=" + timestampMs +
        '}';
  }
}
