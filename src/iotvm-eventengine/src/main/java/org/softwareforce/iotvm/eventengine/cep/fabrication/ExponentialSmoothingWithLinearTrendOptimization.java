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

  private double bestAlpha;
  private double bestBeta;
  private double bestMSE;
  private List<Double> bestForecasts;

  public ExponentialSmoothingWithLinearTrendOptimization(double alpha, double beta, int horizon) {
    super(alpha, beta, horizon);
    this.reset();
  }

  private void reset() {
    this.bestAlpha = 0;
    this.bestBeta = 0;
    this.bestMSE = Double.MAX_VALUE;
    this.bestForecasts = null;
  }

  public double getBestAlpha() {
    return this.bestAlpha;
  }

  public double getBestBeta() {
    return this.bestBeta;
  }

  public double getBestMSE() {
    return this.bestMSE;
  }

  public List<Double> getBestForecasts() {
    return this.bestForecasts;
  }

  public void optimizeParameters(List<Double> data, double[] alphaRange, double[] betaRange) {
    this.reset();

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
        if (mse < this.bestMSE) {
          this.bestMSE = mse;
          this.bestAlpha = alpha;
          this.bestBeta = beta;
          this.bestForecasts = forecast;
        }
      }
    }
  }
}
