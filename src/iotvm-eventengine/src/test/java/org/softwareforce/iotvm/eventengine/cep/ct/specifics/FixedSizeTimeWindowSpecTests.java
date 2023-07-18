package org.softwareforce.iotvm.eventengine.cep.ct.specifics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for validating the behavior for fixed size time windows.
 *
 * @author Dimitris Gkoulis
 */
public class FixedSizeTimeWindowSpecTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(FixedSizeTimeWindowSpecTests.class);

  private final Instant TIMESTAMP1 = Instant.parse("2023-01-01T10:10:30Z");

  /* ------------ Internals ------------ */

  private void testPredictableNumberOfMatchingWindows(FixedSizeTimeWindowSpec spec, int expected) {
    final long seconds =
        Duration.between(TIMESTAMP1, TIMESTAMP1.plus(Duration.ofDays(1))).toSeconds();
    for (int second = 1; second <= seconds; second++) {
      final long timestamp = TIMESTAMP1.plus(Duration.ofSeconds(1)).toEpochMilli();
      assertEquals(expected, spec.windowsFor(timestamp).size());
    }
  }

  /* ------------ Tests ------------ */

  @Test
  void windowWithoutAdvanceBy_minutes() {
    final long timeWindowSizeMinutes = 1;
    final Duration timeWindowSize = Duration.ofMinutes(timeWindowSizeMinutes);
    final Duration timeWindowGrace = null;
    final Duration timeWindowAdvance = null;

    final FixedSizeTimeWindowSpec spec =
        new FixedSizeTimeWindowSpec(timeWindowSize, timeWindowGrace, timeWindowAdvance);

    final Map<Long, TimeWindow> timeWindowMap = spec.windowsFor(TIMESTAMP1.toEpochMilli());
    assertEquals(1, timeWindowMap.size());

    final TimeWindow timeWindow = timeWindowMap.values().stream().toList().get(0);
    assertEquals(timeWindow.startTime().toString(), "2023-01-01T10:10:00Z");
    assertEquals(timeWindow.endTime().toString(), "2023-01-01T10:11:00Z");

    final int pastTimeWindowsLimit = 2;

    final Instant limitStart1 =
        timeWindow
            .startTime()
            .minus(timeWindowSizeMinutes * pastTimeWindowsLimit, ChronoUnit.MINUTES);
    final Instant limitStart2 =
        timeWindow
            .startTime()
            .minus(timeWindowSize.toMillis() * pastTimeWindowsLimit, ChronoUnit.MILLIS);
    assertEquals(limitStart1, limitStart2);

    /*
    LOGGER.info(
        "{} - {} --- {}",
        timeWindow.startTime(),
        timeWindow.endTime(),
        timeWindow.startTime().minus(1, ChronoUnit.MINUTES));
    */

    this.testPredictableNumberOfMatchingWindows(spec, 1);
  }

  @Test
  void windowWithAdvanceBy_minutes() {
    final long timeWindowSizeMinutes = 5;
    final Duration timeWindowSize = Duration.ofMinutes(timeWindowSizeMinutes);
    final Duration timeWindowGrace = null;
    final Duration timeWindowAdvance = Duration.ofMinutes(1);

    final FixedSizeTimeWindowSpec spec1 =
        new FixedSizeTimeWindowSpec(timeWindowSize, timeWindowGrace, timeWindowAdvance);

    final Map<Long, TimeWindow> spec1Map = spec1.windowsFor(TIMESTAMP1.toEpochMilli());
    assertEquals(5, spec1Map.size());

    final int pastTimeWindowsLimit = 2;

    for (final TimeWindow timeWindow : spec1Map.values()) {
      final Instant limitStart1 =
          timeWindow
              .startTime()
              .minus(timeWindowSizeMinutes * pastTimeWindowsLimit, ChronoUnit.MINUTES);
      final Instant limitStart2 =
          timeWindow
              .startTime()
              .minus(timeWindowSize.toMillis() * pastTimeWindowsLimit, ChronoUnit.MILLIS);
      assertEquals(limitStart1, limitStart2);
      LOGGER.info(
          "{} - {} --- {}",
          timeWindow.startTime(),
          timeWindow.endTime(),
          timeWindow
              .startTime()
              .minus(timeWindowSizeMinutes * pastTimeWindowsLimit, ChronoUnit.MINUTES));
    }

    this.testPredictableNumberOfMatchingWindows(spec1, 5);

    /* --------- Spec 2 --------- */

    final FixedSizeTimeWindowSpec spec2 =
        new FixedSizeTimeWindowSpec(
            Duration.ofMinutes(timeWindowSizeMinutes), null, Duration.ofMinutes(4));

    final Map<Long, TimeWindow> spec2Map = spec2.windowsFor(TIMESTAMP1.toEpochMilli());
    assertEquals(1, spec2Map.size());

    this.testPredictableNumberOfMatchingWindows(spec2, 1);

    /* --------- Spec 3 --------- */

    final FixedSizeTimeWindowSpec spec3 =
        new FixedSizeTimeWindowSpec(
            Duration.ofMinutes(timeWindowSizeMinutes), null, Duration.ofMinutes(3));

    final Map<Long, TimeWindow> spec3Map = spec3.windowsFor(TIMESTAMP1.toEpochMilli());
    assertEquals(2, spec3Map.size());

    this.testPredictableNumberOfMatchingWindows(spec3, 2);
  }

  @Test
  void windowWithoutAdvanceBy_seconds() {
    final long timeWindowSizeSeconds = 12;
    final Duration timeWindowSize = Duration.ofSeconds(timeWindowSizeSeconds);
    final Duration timeWindowGrace = null;
    final Duration timeWindowAdvance = null;

    final FixedSizeTimeWindowSpec spec =
        new FixedSizeTimeWindowSpec(timeWindowSize, timeWindowGrace, timeWindowAdvance);

    final Map<Long, TimeWindow> timeWindowMap = spec.windowsFor(TIMESTAMP1.toEpochMilli());
    assertEquals(1, timeWindowMap.size());

    final TimeWindow timeWindow = timeWindowMap.values().stream().toList().get(0);
    assertEquals(timeWindow.startTime().toString(), "2023-01-01T10:10:24Z");
    assertEquals(timeWindow.endTime().toString(), "2023-01-01T10:10:36Z");

    final int pastTimeWindowsLimit = 2;

    final Instant limitStart1 =
        timeWindow
            .startTime()
            .minus(timeWindowSizeSeconds * pastTimeWindowsLimit, ChronoUnit.SECONDS);
    final Instant limitStart2 =
        timeWindow
            .startTime()
            .minus(timeWindowSize.toMillis() * pastTimeWindowsLimit, ChronoUnit.MILLIS);
    assertEquals(limitStart1, limitStart2);

    /*
    LOGGER.info(
        "{} - {} --- {}",
        timeWindow.startTime(),
        timeWindow.endTime(),
        timeWindow.startTime().minus(1, ChronoUnit.SECONDS));
    */

    this.testPredictableNumberOfMatchingWindows(spec, 1);
  }
}
