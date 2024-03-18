package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.ArrayList;
import java.util.List;

/**
 * Exponential Smoothing with linear trend for time-series forecasting.
 *
 * @author Dimitris Gkoulis
 * @createdAt Monday 18 March 2024
 */
public class ExponentialSmoothingWithLinearTrend {

  private double alpha;
  private double beta;
  private int horizon;

  public ExponentialSmoothingWithLinearTrend(double alpha, double beta, int horizon) {
    this.alpha = alpha;
    this.beta = beta;
    this.horizon = horizon;
  }

  public double getAlpha() {
    return this.alpha;
  }

  public void setAlpha(double alpha) {
    this.alpha = alpha;
  }

  public double getBeta() {
    return this.beta;
  }

  public void setBeta(double beta) {
    this.beta = beta;
  }

  public int getHorizon() {
    return this.horizon;
  }

  public void setHorizon(int horizon) {
    this.horizon = horizon;
  }

  public List<Double> forecast(List<Double> series) {
    int n = series.size();

    // Initializing the level and trend series.
    List<Double> level = new ArrayList<>(n);
    List<Double> trend = new ArrayList<>(n);
    List<Double> forecast = new ArrayList<>(n + horizon);

    // Initializing first values based on the series itself.
    level.add(series.get(0));
    trend.add(series.get(1) - series.get(0));

    // Start calculations from the second point.
    for (int i = 1; i < n; i++) {
      // Calculating level.
      double newLevel =
          this.alpha * series.get(i) + (1 - this.alpha) * (level.get(i - 1) + trend.get(i - 1));
      level.add(newLevel);

      // Calculating trend.
      double newTrend =
          this.beta * (level.get(i) - level.get(i - 1)) + (1 - this.beta) * trend.get(i - 1);
      trend.add(newTrend);
    }

    // Adding last known level and trend to the forecast.
    for (int i = 0; i < n; i++) {
      forecast.add(level.get(i) + trend.get(i));
    }

    // Forecasting future points.
    double lastLevel = level.get(n - 1);
    double lastTrend = trend.get(n - 1);
    for (int i = 1; i <= this.horizon; i++) {
      forecast.add(lastLevel + i * lastTrend);
    }

    return forecast;
  }
}
