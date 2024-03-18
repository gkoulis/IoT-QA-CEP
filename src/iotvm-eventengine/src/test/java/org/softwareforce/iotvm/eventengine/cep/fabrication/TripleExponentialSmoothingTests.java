package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TripleExponentialSmoothingTests {

  @Test
  public void testForecastingAndOptimization() {
    final List<Double> airTemperatureValues = MockTimeSeries.getAirTemperatureValues();
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
