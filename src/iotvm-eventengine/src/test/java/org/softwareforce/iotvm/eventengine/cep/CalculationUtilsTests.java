package org.softwareforce.iotvm.eventengine.cep;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CalculationUtils}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Monday 18 March 2024
 */
public class CalculationUtilsTests {

  @Test
  public void testCalculateOfTimeWindowedDecayedTimeliness() {
    assertEquals(1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(0, 0, 0));
    assertEquals(1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(0, 0, 0.5));
    assertEquals(1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(0, 0, 1));

    assertEquals(1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(0, 1, 0));
    assertEquals(1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(0, 1, 0.5));
    assertEquals(1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(0, 1, 1));

    assertEquals(0, CalculationUtils.calculateTimeWindowedDecayedTimeliness(2, 1, 0));
    assertEquals(0, CalculationUtils.calculateTimeWindowedDecayedTimeliness(2, 1, 0.5));
    assertEquals(0, CalculationUtils.calculateTimeWindowedDecayedTimeliness(2, 1, 1));

    assertEquals(0, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 1, 0.0));
    assertEquals(0.1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 1, 0.1));
    assertEquals(0.5, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 1, 0.5));
    assertEquals(0.9, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 1, 0.9));

    assertEquals(0, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 1, 0.0));
    assertEquals(0.1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 1, 0.1));
    assertEquals(0.5, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 1, 0.5));
    assertEquals(1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 1, 1));

    assertEquals(0, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 2, 0.0));
    assertEquals(0.1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 2, 0.1));
    assertEquals(0.5, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 2, 0.5));
    assertEquals(1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(1, 2, 1));

    assertEquals(0, CalculationUtils.calculateTimeWindowedDecayedTimeliness(2, 2, 0.0));
    assertEquals(
        0.010000000000000002, CalculationUtils.calculateTimeWindowedDecayedTimeliness(2, 2, 0.1));
    assertEquals(0.25, CalculationUtils.calculateTimeWindowedDecayedTimeliness(2, 2, 0.5));
    assertEquals(1, CalculationUtils.calculateTimeWindowedDecayedTimeliness(2, 2, 1));
  }
}
