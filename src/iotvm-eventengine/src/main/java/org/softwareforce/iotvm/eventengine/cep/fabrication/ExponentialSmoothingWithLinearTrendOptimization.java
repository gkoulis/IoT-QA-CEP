package org.softwareforce.iotvm.eventengine.cep.fabrication;

import com.google.common.base.Preconditions;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.CalculationUtils;

/**
 * Brute-force grid optimization for {@link ExponentialSmoothingWithLinearTrend}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Monday 18 March 2024
 */
public class ExponentialSmoothingWithLinearTrendOptimization
    extends ExponentialSmoothingWithLinearTrend {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExponentialSmoothingWithLinearTrendOptimization.class);

  private double selectedAlpha;
  private double selectedBeta;
  private double MSE;
  private double MAE;
  private List<Double> bestForecasts;
  private boolean debug;

  public ExponentialSmoothingWithLinearTrendOptimization(double alpha, double beta, int horizon) {
    super(alpha, beta, horizon);
    this.reset();
    this.debug = false;
  }

  private void reset() {
    this.selectedAlpha = 0;
    this.selectedBeta = 0;
    this.MSE = Double.MAX_VALUE;
    this.MAE = Double.MAX_VALUE;
    this.bestForecasts = null;
    // TODO Idea: Do not trust forecast if MAE is not in a specific range.
  }

  public double getSelectedAlpha() {
    return this.selectedAlpha;
  }

  public double getSelectedBeta() {
    return this.selectedBeta;
  }

  public double getMSE() {
    return this.MSE;
  }

  public double getMAE() {
    return this.MAE;
  }

  public List<Double> getBestForecasts() {
    return this.bestForecasts;
  }

  public boolean getDebug() {
    return this.debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public void optimizeParameters(List<Double> data, double[] alphaRange, double[] betaRange) {
    this.reset();

    final long startNs = System.nanoTime();

    int iterationCount = 0;
    for (final double alpha : alphaRange) {
      for (final double beta : betaRange) {
        this.setAlpha(alpha);
        this.setBeta(beta);

        final List<Double> forecast = this.forecast(data);
        Preconditions.checkState(data.size() == (forecast.size() - this.getHorizon()));

        final List<Double> yTrue = data.subList(0, data.size());
        // yPred (in sample predictions)
        final List<Double> yPred = forecast.subList(0, data.size());
        Preconditions.checkState(yTrue.size() == yPred.size());

        double MSE = CalculationUtils.calculateMSE(yTrue, yPred);
        double MAE = CalculationUtils.calculateMAE(yTrue, yPred);

        boolean isBest = false;
        if (MAE < this.MAE) {
          isBest = true;
        } else if (MAE == this.MAE) {
          if (MSE < this.MSE) {
            isBest = true;
          }
        }

        if (isBest) {
          this.selectedAlpha = alpha;
          this.selectedBeta = beta;
          this.MSE = MSE;
          this.MAE = MAE;
          this.bestForecasts = forecast;
        }

        iterationCount++;
      }
    }

    if (!this.debug) {
      return;
    }

    final long endNs = System.nanoTime();
    final long diffNs = endNs - startNs;
    final long diffMs = diffNs / 1000000;

    final List<Double> bestForecastsInHorizon =
        this.bestForecasts.subList(data.size(), this.bestForecasts.size());
    Preconditions.checkState(bestForecastsInHorizon.size() == this.getHorizon());
    LOGGER.info(
        "Completed parameter optimization ({} iterations, {} ms) with MSE {}, MAE {}, alpha {},"
            + " beta {}, last real {}, last forecast {}",
        iterationCount,
        diffMs,
        this.MSE,
        this.MAE,
        this.selectedAlpha,
        this.selectedBeta,
        data.getLast(),
        this.bestForecasts.getLast());
  }

  public static double[] generateRange(double start, double end, double step) {
    final int size = (int) ((end - start) / step) + 1;
    final double[] array = new double[size];
    for (int i = 0; i < size; i++) {
      array[i] = start + (i * step);
    }
    return array;
  }
}
