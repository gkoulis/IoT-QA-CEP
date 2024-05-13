package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ExponentialSmoothingWithLinearTrendTests {

  @Test
  public void testForecastingAndOptimization() {
    final List<Double> airTemperatureValues = MockTimeSeries.getAirTemperatureValues();
    final List<Double> data = new ArrayList<>(airTemperatureValues.subList(0, 100));

    // System.out.println(data);

    final double alpha = 0.3D;
    final double beta = 0.6D;
    final int steps = 6;

    final ExponentialSmoothingWithLinearTrend forecaster =
        new ExponentialSmoothingWithLinearTrend(alpha, beta, steps);
    final List<Double> predictions = forecaster.forecast(data);
    System.out.println(predictions.subList(predictions.size() - steps, predictions.size()));

    // Define ranges for alpha, beta, gamma
    double[] alphaRange = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
    double[] betaRange = {0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};

    final ExponentialSmoothingWithLinearTrendOptimization optimizer =
        new ExponentialSmoothingWithLinearTrendOptimization(alpha, beta, steps);
    optimizer.optimizeParameters(data, alphaRange, betaRange);

    // expected duration: 10ms - 100ms

    System.out.printf(
        "Optimal parameters: alpha = %.2f, beta = %.2f, with MSE = %.4f%n, with MAE = %.4f%n",
        optimizer.getSelectedAlpha(),
        optimizer.getSelectedBeta(),
        optimizer.getMSE(),
        optimizer.getMAE());
  }
}
