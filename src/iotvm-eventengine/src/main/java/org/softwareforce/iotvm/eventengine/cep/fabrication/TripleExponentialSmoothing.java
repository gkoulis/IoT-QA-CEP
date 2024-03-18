package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.ArrayList;
import java.util.List;

/**
 * Triple Exponential Smoothing (Holt Winter’s Exponential Smoothing (HWES)) for time-series
 * forecasting.
 *
 * @author Dimitris Gkoulis
 * @createdAt Monday 18 March 2024
 */
public class TripleExponentialSmoothing {

  private double alpha;
  private double beta;
  private double gamma;
  private int seasonLength;
  private int horizon;

  public TripleExponentialSmoothing(
      double alpha, double beta, double gamma, int seasonLength, int horizon) {
    this.alpha = alpha;
    this.beta = beta;
    this.gamma = gamma;
    this.seasonLength = seasonLength;
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

  public double getGamma() {
    return this.gamma;
  }

  public void setGamma(double gamma) {
    this.gamma = gamma;
  }

  public int getSeasonLength() {
    return this.seasonLength;
  }

  public void setSeasonLength(int seasonLength) {
    this.seasonLength = seasonLength;
  }

  public int getHorizon() {
    return horizon;
  }

  public void setHorizon(int horizon) {
    this.horizon = horizon;
  }

  public List<Double> forecast(List<Double> data) {
    int n = data.size();
    double[] level = new double[n];
    double[] trend = new double[n];
    double[] season = new double[n + this.seasonLength];
    double[] forecast = new double[n + this.horizon];

    // Initialize components.
    level[0] = data.get(0);
    trend[0] = data.get(1) - data.get(0);
    for (int i = 0; i < this.seasonLength; i++) {
      season[i] = data.get(i) / level[0];
    }

    // Apply formula.
    for (int t = 1; t < n; t++) {
      int seasonIndex = (t + this.seasonLength) % this.seasonLength;
      level[t] =
          this.alpha * (data.get(t) / season[seasonIndex])
              + (1 - this.alpha) * (level[t - 1] + trend[t - 1]);
      trend[t] = this.beta * (level[t] - level[t - 1]) + (1 - this.beta) * trend[t - 1];
      season[seasonIndex + this.seasonLength] =
          this.gamma * (data.get(t) / level[t]) + (1 - this.gamma) * season[seasonIndex];
      forecast[t] = (level[t - 1] + trend[t - 1]) * season[seasonIndex];
    }

    // Forecast future values.
    for (int t = n; t < n + this.horizon; t++) {
      int seasonIndex = (t + this.seasonLength) % this.seasonLength;
      forecast[t] =
          (level[n - 1] + (t - n + 1) * trend[n - 1])
              * season[seasonIndex + this.seasonLength - this.seasonLength];
    }

    final List<Double> forecastList = new ArrayList<>();
    for (double f : forecast) {
      forecastList.add(f);
    }
    return forecastList;
  }
}
