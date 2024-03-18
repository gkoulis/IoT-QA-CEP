package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.List;
import org.softwareforce.iotvm.eventengine.cep.CalculationUtils;

/**
 * Brute-force grid optimization for {@link ExponentialSmoothingWithLinearTrend}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Monday 18 March 2024
 */
public class ExponentialSmoothingWithLinearTrendOptimization
    extends ExponentialSmoothingWithLinearTrend {

  public ExponentialSmoothingWithLinearTrendOptimization(double alpha, double beta, int horizon) {
    super(alpha, beta, horizon);
  }

  public double[] optimizeParameters(List<Double> data, double[] alphaRange, double[] betaRange) {
    double bestAlpha = 0;
    double bestBeta = 0;
    double bestMSE = Double.MAX_VALUE;

    for (double alpha : alphaRange) {
      for (double beta : betaRange) {
        this.setAlpha(alpha);
        this.setBeta(beta);

        final List<Double> forecast = this.forecast(data);
        if (data.size() != forecast.size() - this.getHorizon()) {
          throw new IllegalStateException();
        }

        final List<Double> yTrue = data.subList(0, data.size());
        // yPred (in sample predictions)
        final List<Double> yPred = forecast.subList(0, data.size());
        if (yTrue.size() != yPred.size()) {
          throw new IllegalStateException();
        }

        double mse = CalculationUtils.calculateMSE(yTrue, yPred);
        if (mse < bestMSE) {
          bestMSE = mse;
          bestAlpha = alpha;
          bestBeta = beta;
        }
      }
    }

    return new double[] {bestAlpha, bestBeta, bestMSE};
  }
}
