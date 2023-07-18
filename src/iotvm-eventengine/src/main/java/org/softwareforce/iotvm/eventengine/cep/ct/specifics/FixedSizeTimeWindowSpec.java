package org.softwareforce.iotvm.eventengine.cep.ct.specifics;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

/**
 * Fixed-size time window specification for composite transformations that perform windowing
 * operations.
 *
 * @author Dimitris Gkoulis
 */
@SuppressWarnings("FieldCanBeLocal")
public class FixedSizeTimeWindowSpec {

  /** The size of the time window. */
  private final Duration timeWindowSize;

  /** The grace period of the time window. */
  @Nullable private final Duration timeWindowGrace;

  /**
   * The advance duration of the time window. If it's {@code null} a normal time window will be
   * created, otherwise a hopping time window will be created.
   */
  @Nullable private final Duration timeWindowAdvance;

  /** The Kafka Streams {@link TimeWindows} definition. */
  private final TimeWindows timeWindows;

  /* ------------ Constructors ------------ */

  public FixedSizeTimeWindowSpec(
      Duration timeWindowSize,
      @Nullable Duration timeWindowGrace,
      @Nullable Duration timeWindowAdvance) {
    this.timeWindowSize = timeWindowSize;
    this.timeWindowGrace = timeWindowGrace;
    this.timeWindowAdvance = timeWindowAdvance;
    validate(this.timeWindowSize, this.timeWindowGrace, this.timeWindowAdvance);
    this.timeWindows = build(this.timeWindowSize, this.timeWindowGrace, this.timeWindowAdvance);
  }

  /* ------------ Getters ------------ */

  public Duration getTimeWindowSize() {
    return timeWindowSize;
  }

  @Nullable
  public Duration getTimeWindowGrace() {
    return timeWindowGrace;
  }

  @Nullable
  public Duration getTimeWindowAdvance() {
    return timeWindowAdvance;
  }

  public TimeWindows getTimeWindows() {
    return timeWindows;
  }

  /* ------------ Internals ------------ */

  protected static void validate(
      Duration timeWindowSize,
      @Nullable Duration timeWindowGrace,
      @Nullable Duration timeWindowAdvance) {
    if (timeWindowSize == null) {
      throw new IllegalArgumentException("timeWindowSize cannot be null!");
    }
    if (timeWindowSize.isZero() || timeWindowSize.isNegative()) {
      throw new IllegalArgumentException("timeWindowSize cannot be zero or negative!");
    }
    if (timeWindowGrace != null) {
      if (timeWindowGrace.isZero() || timeWindowGrace.isNegative()) {
        throw new IllegalArgumentException("timeWindowGrace cannot be zero or negative!");
      }
    }
    if (timeWindowAdvance != null) {
      if (timeWindowAdvance.isZero() || timeWindowAdvance.isNegative()) {
        throw new IllegalArgumentException("timeWindowAdvance cannot be zero or negative!");
      }
      if (timeWindowAdvance.toMillis() > timeWindowSize.toMillis()) {
        throw new IllegalArgumentException(
            "timeWindowAdvance cannot be greater than timeWindowSize!");
      }
      if (timeWindowAdvance.toMillis() == timeWindowSize.toMillis()) {
        throw new IllegalArgumentException(
            "timeWindowAdvance cannot be equal to timeWindowSize! Set timeWindowAdvance to null to"
                + " enable a tumbling window!");
      }
    }
  }

  protected static TimeWindows build(
      Duration timeWindowSize,
      @Nullable Duration timeWindowGrace,
      @Nullable Duration timeWindowAdvance) {
    TimeWindows timeWindows;

    if (timeWindowGrace == null) {
      timeWindows = TimeWindows.ofSizeWithNoGrace(timeWindowSize);
    } else {
      timeWindows = TimeWindows.ofSizeAndGrace(timeWindowSize, timeWindowGrace);
    }

    if (timeWindowAdvance != null) {
      timeWindows = timeWindows.advanceBy(timeWindowAdvance);
    }

    return timeWindows;
  }

  /* ------------ Utilities ------------ */

  public Map<Long, TimeWindow> windowsFor(long timestamp) {
    return this.timeWindows.windowsFor(timestamp);
  }

  /**
   * Calculates the time range corresponding to the number of previous time windows in which past
   * records will be searched.
   *
   * <p>The calculation method is as follows:
   *
   * <ul>
   *   <li>Find the matching time windows based on the current timestamp ({@link TimeWindow}). It is
   *       recalled that in case of time window hopping, a timestamp can belong to more than one
   *       window (without this being necessary).
   *   <li>From the matching time windows, choose the most recent one (based on its key, i.e., the
   *       start time).
   *   <li>Using the {@code startTime} of the latest {@link TimeWindow}, calculate the bounds.
   *   <li>{@link PastFixedSizeTimeWindowsLimits#start} is obtained by subtracting from the most
   *       recent time window the total duration of previous time windows (i.e., time window
   *       duration * past windows).
   *   <li>{@link PastFixedSizeTimeWindowsLimits#end} is the {@code startTime} of the most recent
   *       time window.
   * </ul>
   *
   * <p>Logical query for selecting past events:
   *
   * <pre>{@code startTimestamp <= event.timestamp < endTimestamp}</pre>
   *
   * @param timestamp the current timestamp (epoch milli).
   * @param windows the number of past windows. The current window is not included.
   * @return an {@link PastFixedSizeTimeWindowsLimits} instances with the start and end timestamps.
   */
  public PastFixedSizeTimeWindowsLimits calculatePastWindowsLimits(long timestamp, long windows) {
    if (windows <= 0) {
      throw new IllegalArgumentException("windows must greater than zero!");
    }
    final Map<Long, TimeWindow> timeWindowMap = this.windowsFor(timestamp);
    final long maxTimestamp =
        timeWindowMap.keySet().stream()
            .mapToLong(i -> i)
            .max()
            .orElseThrow(NoSuchElementException::new);
    final TimeWindow timeWindow = timeWindowMap.get(maxTimestamp);
    final Instant start =
        timeWindow.startTime().minus(this.timeWindowSize.toMillis() * windows, ChronoUnit.MILLIS);
    final Instant end = timeWindow.startTime();
    return new PastFixedSizeTimeWindowsLimits(start, end);
  }

  public static final class PastFixedSizeTimeWindowsLimits {

    private final Instant start;
    private final Instant end;

    private PastFixedSizeTimeWindowsLimits(Instant start, Instant end) {
      this.start = start;
      this.end = end;
    }

    public Instant getStart() {
      return start;
    }

    public Instant getEnd() {
      return end;
    }

    @Override
    public String toString() {
      return "PastFixedSizeTimeWindowsLimits{" + "start=" + start + ", end=" + end + '}';
    }
  }
}
