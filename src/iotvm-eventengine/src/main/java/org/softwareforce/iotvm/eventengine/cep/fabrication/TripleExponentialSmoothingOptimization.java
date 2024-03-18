package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.List;

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

  private double calculateMSE(List<Double> actual, List<Double> forecast, int start) {
    double mse = 0.0;
    for (int i = start; i < actual.size(); i++) {
      mse += Math.pow(actual.get(i) - forecast.get(i), 2);
    }
    return mse / (actual.size() - start);
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
            List<Double> forecast = this.forecast(data);
            double mse = calculateMSE(data, forecast, this.getSeasonLength());
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
