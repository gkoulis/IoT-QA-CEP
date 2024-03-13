package org.softwareforce.iotvm.eventengine.cep.fabrication;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link TimeWindowedTimeSeries}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Wednesday 13 March 2024
 */
public class TimeWindowedTimeSeriesTests {
  private static long p(final String timestamp) {
    return Instant.parse(timestamp).toEpochMilli();
  }

  @Test
  public void testTimeWindowPreviousAndNext() {
    long timeWindowSizeMs = Duration.ofMinutes(5).toMillis();
    long ds1 = p("2024-01-01T10:05:00.00Z");
    TimeWindowedTimeSeries.TimeWindow tw =
        new TimeWindowedTimeSeries.TimeWindow(ds1, timeWindowSizeMs);

    assertEquals(tw.distanceFrom(tw.getPrevious()), 1);
    assertEquals(tw.distanceFrom(tw.getNext()), -1);

    System.out.println(tw);
    System.out.println(tw.getPrevious());
    System.out.println(tw.getNext());
  }

  @Test
  public void testTimeWindowedTimeSeriesUpdate() {
    TimeWindowedTimeSeries ts =
        new TimeWindowedTimeSeries("sensor-1", Duration.ofMinutes(5).toMillis());

    long ds1 = 0L;

    ds1 = p("2024-01-01T10:01:00.00Z");
    ts.update(ds1, 15.0D);
    assertEquals(ts.getPoints().get(ts.getPoints().lastKey()).getValue(), 15.0D);
    assertEquals(ts.getPoints().get(ts.getPoints().lastKey()).getTimestampMs(), ds1);
    assertEquals(ts.getPoints().lastKey().distanceFrom(ts.getPoints().firstKey()), 0L);

    // Older than the current. Must not update.
    ts.update(p("2024-01-01T10:00:55.00Z"), 14.0D);
    assertEquals(ts.getPoints().get(ts.getPoints().lastKey()).getValue(), 15.0D);
    assertEquals(ts.getPoints().get(ts.getPoints().lastKey()).getTimestampMs(), ds1);
    assertEquals(ts.getPoints().lastKey().distanceFrom(ts.getPoints().firstKey()), 0L);

    // Newer than the current. Must update.
    ds1 = p("2024-01-01T10:02:00.00Z");
    ts.update(ds1, 16.0D);
    assertEquals(ts.getPoints().get(ts.getPoints().lastKey()).getValue(), 16.0D);
    assertEquals(ts.getPoints().get(ts.getPoints().lastKey()).getTimestampMs(), ds1);
    assertEquals(ts.getPoints().lastKey().distanceFrom(ts.getPoints().firstKey()), 0L);

    // 3 time-windows later. Must update.
    ds1 = p("2024-01-01T10:15:00.00Z");
    ts.update(ds1, 20.0D);
    assertEquals(ts.getPoints().get(ts.getPoints().lastKey()).getValue(), 20.0D);
    assertEquals(ts.getPoints().get(ts.getPoints().lastKey()).getTimestampMs(), ds1);
    assertEquals(ts.getPoints().lastKey().distanceFrom(ts.getPoints().firstKey()), 3L);

    System.out.println(ts.toString());
    System.out.println(ts.getPoints());
  }

  @Test
  public void testHandlingOfMissingPoints1() {
    /*
    1  - 10:00:00 = first (real)
    2  - 10:05:00 = must impute
    3  - 10:10:00 = must impute
    4  - 10:15:00 = must impute
    5  - 10:20:00 = must impute
    6  - 10:25:00 = must impute
    7  - 10:30:00 = last (real)
    */

    final TimeWindowedTimeSeries ts =
        new TimeWindowedTimeSeries("sensor-1", Duration.ofMinutes(5).toMillis());

    long ds1 = p("2024-01-01T10:00:00.00Z");
    ts.update(ds1, 15.0D);

    assertEquals(1, ts.getPoints().size());
    assertEquals(0, ts.getPoints().lastKey().distanceFrom(ts.getPoints().firstKey()));
    assertFalse(ts.handleMissingPoints());

    long ds2 = p("2024-01-01T10:30:00.00Z");
    ts.update(ds2, 30.0D);

    assertEquals(2, ts.getPoints().size());
    assertEquals(6, ts.getPoints().lastKey().distanceFrom(ts.getPoints().firstKey()));
    assertTrue(ts.handleMissingPoints());

    assertEquals(7, ts.getPoints().size());

    for (final TimeWindowedTimeSeries.Point point : ts.getPoints().values()) {
      System.out.print(point.isImputed());
      System.out.print(" ");
      System.out.print(point.getValue());
      System.out.print(" @ ");
      System.out.print(point.getTimeWindow().getStartTimestampMs());
      System.out.print(" (");
      System.out.print(point.getTimeWindow().getIndex());
      System.out.print(")");
      System.out.print("\n");
    }
  }

  @Test
  public void testHandlingOfMissingPoints2() {
    /*
    1  - 10:00:00 = first (real)
    2  - 10:05:00 = must impute
    3  - 10:10:00 = must impute
    4  - 10:15:00 = must impute
    5  - 10:20:00 = must impute
    6  - 10:25:00 = must impute
    7  - 10:30:00 = middle (real)
    8  - 10:35:00 = must impute
    9  - 10:40:00 = must impute
    10 - 10:45:00 = must impute
    11 - 10:50:00 = must impute
    12 - 10:55:00 = must impute
    13 - 11:00:00 = last (real)
    */

    // TODO Complete the test.
  }
}
