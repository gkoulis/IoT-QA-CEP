package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Time-Windowed Time-Series.
 *
 * <p>TODO στο fabrication ίσως δεν έχει νόημα το forecasting να είναι μικρότερο από το past
 * event...
 *
 * <p>Time-Windows are:
 *
 * <ul>
 *   <li>Left boundary inclusive: This means that if a timestamp falls exactly at the beginning of a
 *       time window, it is considered part of that time window.
 *   <li>Right boundary exclusive: This means that if a timestamp is exactly on what would be the
 *       "end" of a time window, it actually belongs to the next time window.
 * </ul>
 *
 * @author Dimitris Gkoulis
 * @createdAt Wednesday 13 March 2024
 */
public class TimeWindowedTimeSeries {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimeWindowedTimeSeries.class);

  /** The name of the instance for convenience. */
  private final String name;

  /** The size of the time-window in milliseconds. */
  private final long timeWindowSizeMs;

  private final SortedMap<TimeWindow, Point> points;

  /* ------------ Constructors ------------ */

  public TimeWindowedTimeSeries(final String name, final long timeWindowSizeMs) {
    this.name = name;
    this.timeWindowSizeMs = timeWindowSizeMs;
    //noinspection Convert2Lambda,Convert2Diamond
    this.points =
        new TreeMap<>(
            new Comparator<TimeWindow>() {

              @Override
              public int compare(TimeWindow tw1, TimeWindow tw2) {
                return Long.compare(tw1.getStartTimestampMs(), tw2.getStartTimestampMs());
              }
            });
  }

  /* ------------ Access ------------ */

  /**
   * Adds a new {@link Point} if it does not already exist, or updates an existing {@link Point} if
   * the new {@code timestampMs} is greater than or equal to the existing {@code timestampMs}. This
   * method is particularly useful for Complex Event Processing, where real-time requirements
   * dictate the addition and modification of the time series as new data comes in.
   *
   * @param timestampMs the timestamp (ms) of the data point.
   * @param value the value of the data point.
   */
  public void update(long timestampMs, double value) {
    final TimeWindow timeWindow = new TimeWindow(timestampMs, this.timeWindowSizeMs);

    // TODO Reject if belongs to past / older time windows? (silent or exception)

    if (!this.points.containsKey(timeWindow)) {
      final Point point = new Point(timestampMs, this.timeWindowSizeMs, value);
      assert point.timeWindow.equals(timeWindow);
      this.points.put(timeWindow, point);
      return;
    }

    final Point existingPoint = this.points.get(timeWindow);
    assert existingPoint != null; // TODO Do the assertions work?
    assert existingPoint.timeWindow.equals(timeWindow);

    if (timestampMs >= existingPoint.timestampMs) {
      existingPoint.update(timestampMs, value);
    }
  }

  /**
   * Handles missing points @TODO Optimize for real-time ops (continue from the last imputation
   * point)
   */
  public boolean handleMissingPoints() {
    if (this.points.isEmpty()) {
      return false;
    }
    if (this.points.size() == 1) {
      return false;
    }

    // Fast check if contains missing points.
    // -------------------------------------------------------

    // TODO Implement.

    // -------------------------------------------------------

    final TimeWindow firstTw = this.points.firstKey();
    final TimeWindow lastTw = this.points.lastKey();
    if (firstTw.equals(lastTw)) {
      throw new IllegalStateException("firstTw cannot be equal to lastTw");
    }
    if (this.points.get(firstTw).isImputed()) {
      throw new IllegalStateException("firstTw points to a Point which is imputed!");
    }
    if (this.points.get(lastTw).isImputed()) {
      throw new IllegalStateException("lastTw points to a Point which is imputed!");
    }

    // -------------------------------------------------------

    final List<TimeWindow> indexList = new ArrayList<>();
    final List<Double> valueList = new ArrayList<>();
    final List<Boolean> imputedList = new ArrayList<>();

    // -------------------------------------------------------

    TimeWindow currentTw = this.points.firstKey();
    while (true) {
      if (!this.points.containsKey(currentTw)) {
        this.points.put(
            currentTw,
            new Point(currentTw.startTimestampMs, this.timeWindowSizeMs, Double.NaN, true));
      }

      final Point currentPoint = this.points.get(currentTw);
      indexList.add(currentPoint.getTimeWindow());
      valueList.add(currentPoint.getValue());
      imputedList.add(currentPoint.isImputed());

      if (currentTw.equals(lastTw)) {
        break;
      }

      currentTw = currentTw.getNext();
    }

    // -------------------------------------------------------

    double[] series = valueList.stream().mapToDouble(Double::doubleValue).toArray();

    for (int i = 0; i < series.length; i++) {
      if (!Double.isNaN(series[i])) {
        continue;
      }

      // Find indices of the previous and next non-missing values.
      int prev = i - 1;
      int next = i + 1;
      while (prev >= 0 && Double.isNaN(series[prev])) prev--;
      while (next < series.length && Double.isNaN(series[next])) next++;

      // Linear interpolation if both previous and next exist.
      if (prev >= 0 && next < series.length) {
        series[i] = series[prev] + (series[next] - series[prev]) * (i - prev) / (next - prev);
      } else {
        // Handle edge cases or throw an error.
        // NOTE: Highly unexpected because
        // we have strong validations in the start of the method.
        series[i] = 0; // TODO Placeholder, consider better handling
      }
    }

    // -------------------------------------------------------

    for (int i = 0; i < series.length; i++) {
      final TimeWindow timeWindow = indexList.get(i);
      final double value = series[i];
      final boolean imputed = imputedList.get(i);

      if (Double.isNaN(series[i])) {
        throw new IllegalStateException("series[" + i + "] is NaN!");
      }

      if (imputed) {
        if (!this.points.get(timeWindow).imputed) {
          throw new IllegalStateException(
              "Point in TimeWindow " + timeWindow + " is not imputed but it should be!");
        }

        this.points.get(timeWindow).value = value;
      } else {
        if (this.points.get(timeWindow).imputed) {
          throw new IllegalStateException(
              "Point in TimeWindow " + timeWindow + " is imputed but it should not be!");
        }
      }
    }

    return true;
  }

  public SortedMap<TimeWindow, Point> getPoints() {
    return this.points; // TODO Remove this functions because it is mutable!!!!!
  }

  /* ------------ Overrides ------------ */

  @Override
  public String toString() {
    return name
        + " time-series ("
        + this.timeWindowSizeMs
        + " ms) with "
        + this.points.size()
        + " data points";
  }

  /* ------------ Time Window Utilities ------------ */

  public static long calculateTimeWindowIndex(long timestampMs, long timeWindowSizeMs) {
    return timestampMs / timeWindowSizeMs;
  }

  /** Calculates the time-window start timestamp (ms) (inclusive). */
  public static long calculateTimeWindowStartTimestampMs(long timestampMs, long timeWindowSizeMs) {
    return (timestampMs / timeWindowSizeMs) * timeWindowSizeMs;
  }

  /** Calculates the time-window end timestamp (ms) (exclusive). */
  public static long calculateTimeWindowEndTimestampMs(long timestampMs, long timeWindowSizeMs) {
    return ((timestampMs / timeWindowSizeMs) + 1) * timeWindowSizeMs - 1;
  }

  /* ------------ Nested Class ------------ */

  /** A Time-Window. */
  public static final class TimeWindow {

    /** The size (ms) of the time-window. */
    private final long sizeMs;

    /** The index of the time-window. */
    private final long index;

    /** The start timestamp (ms) of the time-windows. */
    private final long startTimestampMs;

    /** The end timestamp (ms) of the time-window. */
    private final long endTimestampMs;

    /**
     * @param timestampMs the timestamp (ms) for which to create the time-window for.
     * @param sizeMs the size (ms) of the time-window.
     */
    public TimeWindow(final long timestampMs, final long sizeMs) {
      this.sizeMs = sizeMs;
      this.index = calculateTimeWindowIndex(timestampMs, this.sizeMs);
      this.startTimestampMs = calculateTimeWindowStartTimestampMs(timestampMs, this.sizeMs);
      this.endTimestampMs = calculateTimeWindowEndTimestampMs(timestampMs, this.sizeMs);
    }

    public long getSizeMs() {
      return this.sizeMs;
    }

    public long getIndex() {
      return this.index;
    }

    public long getStartTimestampMs() {
      return this.startTimestampMs;
    }

    public long getEndTimestampMs() {
      return this.endTimestampMs;
    }

    public long distanceFrom(final TimeWindow timeWindow) {
      return this.getIndex() - timeWindow.getIndex();
    }

    public TimeWindow getNext() {
      // TODO Okay? add test.
      return new TimeWindow(this.endTimestampMs + 1, this.sizeMs);
    }

    public TimeWindow getPrevious() {
      // TODO Okay? add test.
      return new TimeWindow(this.startTimestampMs - this.sizeMs, this.sizeMs);
    }

    @Override
    public String toString() {
      return "TimeWindow{"
          + "sizeMs="
          + sizeMs
          + ", index="
          + index
          + ", startTimestampMs="
          + startTimestampMs
          + ", endTimestampMs="
          + endTimestampMs
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TimeWindow that = (TimeWindow) o;
      return sizeMs == that.sizeMs
          && index == that.index
          && startTimestampMs == that.startTimestampMs
          && endTimestampMs == that.endTimestampMs;
    }

    @Override
    public int hashCode() {
      return Objects.hash(sizeMs, index, startTimestampMs, endTimestampMs);
    }
  }

  /** A Point. */
  public static final class Point {

    /** The {@link TimeWindow} in which the {@link Point} belongs to. */
    private final TimeWindow timeWindow;

    /** The timestamp (ms) of the data. */
    private long timestampMs;

    /** The value of the data. */
    private double value;

    /** A flag that declares if the value (and the timestamp) of the data is imputed. */
    public boolean imputed;

    /* ------------ Constructors ------------ */

    public Point(final long timestampMs, final long timeWindowSizeMs, final double value) {
      this(timestampMs, timeWindowSizeMs, value, false);
    }

    public Point(
        final long timestampMs,
        final long timeWindowSizeMs,
        final double value,
        final boolean imputed) {
      this.timeWindow = new TimeWindow(timestampMs, timeWindowSizeMs);
      this.timestampMs = timestampMs;
      this.value = value;
      this.imputed = imputed;
    }

    /* ------------ Getters ------------ */

    public TimeWindow getTimeWindow() {
      return this.timeWindow;
    }

    public long getTimestampMs() {
      return this.timestampMs;
    }

    public double getValue() {
      return this.value;
    }

    public boolean isImputed() {
      return this.imputed;
    }

    /* ------------ Access ------------ */

    public void update(final long timestampMs, final double value) {
      this.timestampMs = timestampMs;
      this.value = value;
    }

    /* ------------ Overrides ------------ */

    @Override
    public String toString() {
      return "Point{"
          + "timeWindow="
          + this.timeWindow
          + ", timestampMs="
          + this.timestampMs
          + ", value="
          + this.value
          + ", imputed="
          + this.imputed
          + '}';
    }
  }
}
