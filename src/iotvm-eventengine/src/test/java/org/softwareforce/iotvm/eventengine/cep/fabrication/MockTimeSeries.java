package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.Arrays;
import java.util.List;

/**
 * Mock time-series for testing.
 *
 * @author Dimitris Gkoulis
 * @createdAt Monday 18 March 2024
 */
public final class MockTimeSeries {

  private MockTimeSeries() {}

  protected static List<Double> getAirTemperatureValues() {
    // Array to hold 24 hours of temperature data in a greenhouse, taken every 5 minutes
    Double[] temperatureData = new Double[288];

    // Simulate temperature changes throughout the day
    for (int i = 0; i < temperatureData.length; i++) {
      // Convert the index to hours for the sake of the simulation.
      double hour = i / 12.0;

      // Basic simulation of temperature change, peaks at 14:00 (2 PM).
      double temperature = 20 + 10 * Math.sin(Math.PI * (hour - 8) / 16);

      // Round to 1 decimal place
      temperatureData[i] = Math.round(temperature * 10.0) / 10.0;
    }

    // Example:
    /*
    System.out.println(
        "Air temperature in the greenhouse at 08:00: " + temperatureData[96] + " °C");
    System.out.println(
        "Air temperature in the greenhouse at 14:00: " + temperatureData[168] + " °C");
    System.out.println(Arrays.toString(temperatureData));
    */

    return Arrays.asList(temperatureData);
  }
}
