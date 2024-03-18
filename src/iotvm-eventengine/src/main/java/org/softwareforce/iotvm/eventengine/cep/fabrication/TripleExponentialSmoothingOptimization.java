package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.List;
import org.softwareforce.iotvm.eventengine.cep.CalculationUtils;

/**
 * Brute-force grid optimization for {@link TripleExponentialSmoothing}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Monday 18 March 2024
 */
public class TripleExponentialSmoothingOptimization extends TripleExponentialSmoothing {

  public TripleExponentialSmoothingOptimization(
      double alpha, double beta, double gamma, int seasonLength, int horizon) {
    super(alpha, beta, gamma, seasonLength, horizon);
  }

  public double[] optimizeParameters(
      List<Double> data,
      double[] alphaRange,
      double[] betaRange,
      double[] gammaRange,
      int[] seasonalRange) {
    double bestAlpha = 0;
    double bestBeta = 0;
    double bestGamma = 0;
    int bestSeasonal = 0;
    double bestMSE = Double.MAX_VALUE;

    for (double alpha : alphaRange) {
      for (double beta : betaRange) {
        for (double gamma : gammaRange) {
          for (int seasonal : seasonalRange) {
            this.setAlpha(alpha);
            this.setBeta(beta);
            this.setGamma(gamma);
            this.setSeasonLength(seasonal);

            final List<Double> forecast = this.forecast(data);
            if (data.size() != forecast.size() - this.getHorizon()) {
              throw new IllegalStateException();
            }

            final List<Double> yTrue = data.subList(this.getSeasonLength(), data.size());
            // yPred (in sample predictions)
            final List<Double> yPred = forecast.subList(this.getSeasonLength(), data.size());
            if (yTrue.size() != yPred.size()) {
              throw new IllegalStateException();
            }

            double mse = CalculationUtils.calculateMSE(yTrue, yPred);
            if (mse < bestMSE) {
              bestMSE = mse;
              bestAlpha = alpha;
              bestBeta = beta;
              bestGamma = gamma;
              bestSeasonal = seasonal;
            }
          }
        }
      }
    }

    return new double[] {bestAlpha, bestBeta, bestGamma, bestSeasonal, bestMSE};
  }
}
