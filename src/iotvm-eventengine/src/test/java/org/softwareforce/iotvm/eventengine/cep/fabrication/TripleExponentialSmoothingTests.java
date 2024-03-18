package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TripleExponentialSmoothingTests {

  private List<Double> getAirTemperatureValues() {
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

  @Test
  public void testForecastingAndOptimization() {
    final List<Double> airTemperatureValues = this.getAirTemperatureValues();
    final List<Double> data = new ArrayList<>(airTemperatureValues.subList(0, 100));

    // System.out.println(data);

    final double alpha = 0.3D;
    final double beta = 0.6D;
    final double gamma = 0.01D;
    final int seasonLength = 2;
    final int steps = 6;

    final TripleExponentialSmoothing tes =
        new TripleExponentialSmoothing(alpha, beta, gamma, seasonLength, steps);
    final List<Double> predictions = tes.forecast(data);
    System.out.println(predictions.subList(predictions.size() - steps, predictions.size()));

    // Define ranges for alpha, beta, gamma
    double[] alphaRange = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    double[] betaRange = {0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    double[] gammaRange = {0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    int[] seasonalRange = {2, 6, 10, 16, 20, 24, 30};

    final TripleExponentialSmoothingOptimization optimizer =
        new TripleExponentialSmoothingOptimization(alpha, beta, gamma, seasonLength, steps);
    double[] bestParams =
        optimizer.optimizeParameters(data, alphaRange, betaRange, gammaRange, seasonalRange);

    System.out.printf(
        "Optimal parameters: alpha = %.2f, beta = %.2f, gamma = %.2f, seasonal = %.0f with MSE ="
            + " %.4f%n",
        bestParams[0], bestParams[1], bestParams[2], bestParams[3], bestParams[4]);
  }
}
